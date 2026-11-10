//! Shard threads, the per-shard event loop and runtime start-up/shutdown.

use std::any::Any;
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::Waker;
use std::time::{Duration, Instant};

use io_uring::IoUring;
use slab::Slab;

use crate::error::{Error, Result};
use crate::buf::{FixedPool, Region};
use crate::ioqueue::{DeviceGroup, DeviceTable, IoProperties, IoScheduler};
use crate::reactor::{completions_pending, Reactor};
use crate::stall::{self, StallTimer};
use crate::stats::{self, Counters, StallReport};
use crate::sched::{GroupCache, GroupTable, RunQueues, DEFAULT_GROUP};
use crate::sync::Semaphore;
use crate::spsc::{self, Consumer, Producer};
use crate::task::Task;
use crate::timer::Timers;

/// Shard-local mutable state.
///
/// Access is scoped to a closure and reports re-entrant access as `None`
/// instead of panicking. The rule that makes `None` unreachable in practice:
/// a closure passed to `with` runs runtime code only. It never polls a
/// future, calls a waker, or clones or drops a value supplied by the user;
/// such values are moved out of the closure and handled afterwards.
pub(crate) struct Local<T>(RefCell<T>);

impl<T> Local<T> {
    pub(crate) fn new(value: T) -> Local<T> {
        Local(RefCell::new(value))
    }

    pub(crate) fn with<R>(&self, f: impl FnOnce(&mut T) -> R) -> Option<R> {
        match self.0.try_borrow_mut() {
            Ok(mut value) => Some(f(&mut value)),
            Err(_) => None,
        }
    }
}

/// Drops a user-supplied value, containing a panic from its `Drop`.
pub(crate) fn drop_quietly<T>(value: T) {
    if let Err(payload) = catch_unwind(AssertUnwindSafe(move || drop(value))) {
        discard_panic(payload);
    }
}

/// Frees what a caught panic carried. The payload is arbitrary user data
/// whose own `Drop` may panic; if it does, that second payload is leaked,
/// which is the one thing that cannot go wrong.
pub(crate) fn discard_panic(payload: Box<dyn Any + Send>) {
    if let Err(second) = catch_unwind(AssertUnwindSafe(move || drop(payload))) {
        std::mem::forget(second);
    }
}

/// Calls a waker that may not be ours, containing a panic from it.
pub(crate) fn wake_quietly(waker: Waker) {
    if let Err(payload) = catch_unwind(AssertUnwindSafe(move || waker.wake())) {
        discard_panic(payload);
    }
}

/// A message delivered to a shard from another thread.
pub(crate) enum Message {
    /// A task owned by the receiving shard was woken from elsewhere.
    Wake(Task),
    /// Run this closure on the receiving shard.
    Run(Box<dyn FnOnce() + Send>),
    /// Leave the event loop.
    Stop,
}

/// How many turns of the event loop pass before the application is asked
/// for memory again, after a request that left the heap over its limit.
const RECLAIM_BACKOFF: u64 = 64;
/// How many messages fit in the queue between two shards before the
/// sender has to hold further ones back.
const SMP_QUEUE_LENGTH: usize = 128;
/// How long a shard sleeps at most while it still holds messages that did
/// not fit into a full queue.
const SMP_RETRY: Duration = Duration::from_micros(50);

/// The part of a shard that other threads may touch.
pub(crate) struct ShardShared {
    pub(crate) id: usize,
    /// Messages from threads that are not shards of this runtime. Shards
    /// talk to each other over their own queues instead.
    alien: Sender<Message>,
    /// Set while the shard is blocked (or about to block) in the kernel.
    sleeping: AtomicBool,
    eventfd: OwnedFd,
}

impl ShardShared {
    /// A shard that exists only as a destination for wake-ups, with the
    /// receiving end of its inbox.
    #[cfg(test)]
    pub(crate) fn for_test() -> (Arc<ShardShared>, Receiver<Message>) {
        let (alien, receiver) = channel();
        let null = std::fs::File::open("/dev/null").map(OwnedFd::from);
        #[allow(clippy::expect_used)]
        let eventfd = null.expect("/dev/null is always there");
        let shared = Arc::new(ShardShared {
            id: 0,
            alien,
            sleeping: AtomicBool::new(false),
            eventfd,
        });
        (shared, receiver)
    }

    /// Delivers a message from a thread that is not a shard of this
    /// runtime. If the shard is gone the message is dropped, which resolves
    /// anything waiting on it with [`Error::Canceled`].
    pub(crate) fn send_alien(&self, msg: Message) {
        if self.alien.send(msg).is_ok() {
            self.notify();
        }
    }

    /// Makes sure the shard notices a message that was just queued for it.
    fn notify(&self) {
        // Only pay for the eventfd write when the shard is actually asleep.
        if self.sleeping.swap(false, Ordering::SeqCst) {
            let one: u64 = 1;
            // SAFETY: writes 8 bytes from a live u64 to an fd we own.
            unsafe {
                libc::write(self.eventfd.as_raw_fd(), (&one as *const u64).cast(), 8);
            }
        }
    }
}

/// One shard's sending end towards another shard.
struct Outgoing {
    queue: Producer<Message>,
    /// Messages that did not fit into the queue yet, oldest first. Its
    /// length is bounded: calls are limited by `Runtime::submit_limit`, and
    /// there is at most one wake-up in transit per task of the target.
    overflow: VecDeque<Message>,
    target: Arc<ShardShared>,
}

impl Outgoing {
    fn send(&mut self, msg: Message) {
        if !self.overflow.is_empty() {
            // Keep the order: the new message goes behind the held ones.
            self.overflow.push_back(msg);
            self.flush();
            return;
        }
        match self.queue.push(msg) {
            Ok(()) => self.target.notify(),
            Err(msg) => self.overflow.push_back(msg),
        }
    }

    /// Moves held messages into the queue. Returns whether some remain.
    fn flush(&mut self) -> bool {
        let mut moved = false;
        while let Some(msg) = self.overflow.pop_front() {
            match self.queue.push(msg) {
                Ok(()) => moved = true,
                Err(msg) => {
                    self.overflow.push_front(msg);
                    break;
                }
            }
        }
        if moved {
            self.target.notify();
        }
        !self.overflow.is_empty()
    }
}

/// Everything a shard thread is handed at start-up.
struct ShardParts {
    shared: Arc<ShardShared>,
    alien: Receiver<Message>,
    ring: IoUring,
    /// The ring was created disabled, to be enabled by the shard's thread.
    ring_disabled: bool,
    /// A second, tiny ring for the slice timer.
    preempt: IoUring,
    /// Memory of the shard's registered buffers, if configured.
    fixed: Option<Region>,
    /// Indexed by target shard; `None` at the shard's own index.
    outgoing: Vec<Option<Outgoing>>,
    /// One queue from every other shard.
    incoming: Vec<Consumer<Message>>,
}

pub(crate) struct RuntimeShared {
    pub(crate) shards: Vec<Arc<ShardShared>>,
    pub(crate) groups: Arc<GroupTable>,
    /// The first fatal error any shard hit.
    failure: Mutex<Option<io::Error>>,
}

impl RuntimeShared {
    /// Records a fatal shard error and stops every shard, so that
    /// [`Runtime::run`] returns the error.
    fn fail(&self, err: io::Error) {
        {
            let mut failure = self.failure.lock().unwrap_or_else(PoisonError::into_inner);
            if failure.is_none() {
                *failure = Some(err);
            }
        }
        for shard in &self.shards {
            shard.send_alien(Message::Stop);
        }
    }

    fn take_failure(&self) -> Option<io::Error> {
        self.failure.lock().unwrap_or_else(PoisonError::into_inner).take()
    }
}

/// All state owned by one shard thread.
pub(crate) struct Shard {
    pub(crate) shared: Arc<ShardShared>,
    pub(crate) runtime: Arc<RuntimeShared>,
    pub(crate) reactor: Local<Reactor>,
    pub(crate) run_queue: Local<RunQueues>,
    pub(crate) timers: Local<Timers>,
    pub(crate) services: Local<HashMap<u64, Rc<dyn Any>>>,
    pub(crate) fixed: Local<FixedPool>,
    /// The scheduling group of the task being polled; new tasks and disk
    /// requests inherit it.
    pub(crate) current_group: Cell<usize>,
    pub(crate) stats: Counters,
    /// Callbacks that free memory when the shard's heap is over its limit.
    pub(crate) reclaimers: Local<Vec<Rc<dyn Fn()>>>,
    /// Indexed by target shard: one permit for every call this shard may
    /// have in progress there (see `Runtime::submit_limit`).
    pub(crate) calls: Vec<Semaphore>,
    config: ShardConfig,
    tasks: Local<Slab<Task>>,
    outgoing: Local<Vec<Option<Outgoing>>>,
    incoming: Local<Vec<Consumer<Message>>>,
    alien: Receiver<Message>,
    /// Reused by `drain_inbox`.
    received: Cell<Vec<Message>>,
    /// The turn of the event loop before which the application is not
    /// asked for memory again, after a request that did not help.
    reclaim_after: Cell<u64>,
    stop: Cell<bool>,
}

thread_local! {
    static CURRENT: RefCell<Option<Rc<Shard>>> = const { RefCell::new(None) };
}

/// The shard running on this thread, if any.
pub(crate) fn try_current() -> Option<Rc<Shard>> {
    CURRENT
        .try_with(|current| current.try_borrow().ok().and_then(|shard| shard.clone()))
        .ok()
        .flatten()
}

pub(crate) fn current() -> Result<Rc<Shard>> {
    try_current().ok_or(Error::NoRuntime)
}

/// The index of the shard the caller is running on.
pub fn shard_id() -> Result<usize> {
    Ok(current()?.shared.id)
}

/// The number of shards in the running runtime.
pub fn shard_count() -> Result<usize> {
    Ok(current()?.runtime.shards.len())
}

impl Shard {
    fn new(
        parts: ShardParts,
        runtime: Arc<RuntimeShared>,
        config: ShardConfig,
        devices: Arc<DeviceTable>,
    ) -> Shard {
        let ShardParts {
            shared,
            alien,
            ring,
            ring_disabled,
            preempt,
            fixed,
            outgoing,
            incoming,
        } = parts;
        let eventfd = shared.eventfd.as_raw_fd();
        let disk = IoScheduler::new(devices, GroupCache::new(runtime.groups.clone()), config.io_depth);
        let reactor = Reactor::new(ring, ring_disabled, preempt, eventfd, disk, config.task_quota);
        let run_queue = RunQueues::new(runtime.groups.clone());
        let calls = runtime
            .shards
            .iter()
            .map(|_| Semaphore::new(config.submit_limit))
            .collect();
        Shard {
            shared,
            runtime,
            reactor: Local::new(reactor),
            run_queue: Local::new(run_queue),
            timers: Local::new(Timers::default()),
            services: Local::new(HashMap::new()),
            fixed: Local::new(FixedPool::new(fixed)),
            current_group: Cell::new(DEFAULT_GROUP),
            stats: Counters::default(),
            reclaimers: Local::new(Vec::new()),
            calls,
            config,
            tasks: Local::new(Slab::new()),
            outgoing: Local::new(outgoing),
            incoming: Local::new(incoming),
            alien,
            received: Cell::new(Vec::new()),
            reclaim_after: Cell::new(0),
            stop: Cell::new(false),
        }
    }

    /// Creates a task in `group` and makes it runnable. `detached` says
    /// that nobody will ask for the output. Returns `None`, dropping the
    /// future, if the task could not be registered.
    pub(crate) fn spawn_task<F: Future + 'static>(
        &self,
        group: usize,
        future: F,
        detached: bool,
    ) -> Option<Task> {
        let mut future = Some(future);
        let task = self
            .tasks
            .with(|tasks| {
                let entry = tasks.vacant_entry();
                let future = future.take()?;
                let task = Task::new(self.shared.clone(), entry.key(), group, future, detached);
                entry.insert(task.clone());
                Some(task)
            })
            .flatten();
        match &task {
            Some(task) => {
                self.run_queue.with(|queue| queue.push(task.clone()));
            }
            None => drop_quietly(future),
        }
        task
    }

    fn run_task(&self, task: Task) {
        // SAFETY: tasks only reach this shard's run queue if it spawned
        // them, and the loop that calls this is not inside any task.
        let finished = unsafe { task.run() };
        if finished {
            let key = task.key();
            // The check guards against a stale wake-up of a task whose
            // slot has since been given to another.
            let registered = self
                .tasks
                .with(|tasks| match tasks.get(key) {
                    Some(known) if known.same(&task) => tasks.try_remove(key),
                    _ => None,
                })
                .flatten();
            drop(registered);
        }
    }

    pub(crate) fn task_count(&self) -> usize {
        self.tasks.with(|tasks| tasks.len()).unwrap_or(0)
    }

    /// Sends a message to another shard of this runtime over the queue the
    /// two shards share.
    pub(crate) fn send_to(&self, target: usize, msg: Message) {
        stats::add(&self.stats.messages_sent, 1);
        let mut msg = Some(msg);
        self.outgoing.with(|outgoing| {
            if let Some(Some(outgoing)) = outgoing.get_mut(target) {
                if let Some(msg) = msg.take() {
                    outgoing.send(msg);
                }
            }
        });
        // No queue to that shard (or the queues are in use further up the
        // stack): fall back to the path foreign threads use.
        if let Some(msg) = msg {
            match self.runtime.shards.get(target) {
                Some(shard) => shard.send_alien(msg),
                None => drop_quietly(msg),
            }
        }
    }

    /// Retries messages that did not fit into a full queue. Returns whether
    /// some are still held back.
    fn flush_outgoing(&self) -> bool {
        self.outgoing
            .with(|outgoing| {
                let mut pending = false;
                for outgoing in outgoing.iter_mut().flatten() {
                    if !outgoing.overflow.is_empty() {
                        pending |= outgoing.flush();
                    }
                }
                pending
            })
            .unwrap_or(false)
    }

    /// Handles everything other threads have sent. Returns whether there was
    /// anything.
    fn drain_inbox(&self) -> bool {
        // Collect first, then act: handling a message runs runtime code
        // that may itself send messages.
        let mut received = self.received.take();
        self.incoming.with(|incoming| {
            for queue in incoming.iter_mut() {
                while let Some(msg) = queue.pop() {
                    received.push(msg);
                }
            }
        });
        while let Ok(msg) = self.alien.try_recv() {
            received.push(msg);
        }
        let any = !received.is_empty();
        stats::add(&self.stats.messages_received, received.len() as u64);
        for msg in received.drain(..) {
            match msg {
                Message::Wake(header) => {
                    self.run_queue.with(|queue| queue.push(header));
                }
                Message::Run(f) => f(),
                Message::Stop => self.stop.set(true),
            }
        }
        self.received.set(received);
        any
    }

    fn fire_timers(&self) {
        let expired = self.timers.with(|timers| timers.expired(Instant::now()));
        for waker in expired.unwrap_or_default() {
            wake_quietly(waker);
        }
    }

    /// The event loop. Returns when the shard is asked to stop, or with the
    /// error that made its io_uring unusable.
    fn run(&self) -> io::Result<()> {
        loop {
            stats::add(&self.stats.loops, 1);
            // Run one scheduling group: the tasks it has runnable now, until
            // the slice timer fires. Tasks woken meanwhile wait for a later
            // turn, so I/O, timers and the other groups are not starved.
            let picked = self.run_queue.with(|queue| queue.pick()).flatten();
            if let Some((group, batch)) = picked {
                if let Some(Err(err)) = self.reactor.with(|reactor| reactor.arm_preempt()) {
                    return Err(err);
                }
                let start = Instant::now();
                self.current_group.set(group);
                stall::slice_begin();
                let mut polled = 0u64;
                for _ in 0..batch {
                    match self.run_queue.with(|queue| queue.pop(group)).flatten() {
                        Some(header) => self.run_task(header),
                        None => break,
                    }
                    polled = polled.saturating_add(1);
                    if completions_pending() {
                        break;
                    }
                }
                stall::slice_end();
                self.current_group.set(DEFAULT_GROUP);
                let used = start.elapsed();
                let nanos = stats::nanos(used);
                self.run_queue.with(|queue| queue.charge(group, nanos, polled));
                stats::add(&self.stats.tasks_polled, polled);
                stats::add(&self.stats.busy_nanos, nanos);
                if used >= self.config.stall_threshold {
                    self.report_stall(group, used);
                }
            }
            self.reclaim_memory();

            self.drain_inbox();
            let held = self.flush_outgoing();
            crate::alloc::poll();
            self.fire_timers();
            if self.stop.get() {
                return Ok(());
            }

            let mut runnable = self.run_queue.with(|queue| !queue.is_empty()).unwrap_or(false);
            if !runnable && !self.config.spin.is_zero() {
                // Nothing to do. Before paying for a sleep and a wake-up
                // through the kernel, look around for a little while.
                runnable = self.spin()?;
                if self.stop.get() {
                    return Ok(());
                }
            }
            let completed = if runnable {
                self.reactor.with(|reactor| reactor.poll())
            } else {
                // Announce the intent to sleep, then look at the inbox once
                // more. A sender either sees the flag and writes the eventfd,
                // or its message is found by this check.
                self.shared.sleeping.store(true, Ordering::SeqCst);
                if self.drain_inbox() {
                    self.shared.sleeping.store(false, Ordering::SeqCst);
                    continue;
                }
                // Wake up for the next timer, or for disk requests that are
                // waiting on device capacity, whichever comes first.
                let timer = self.timers.with(|timers| timers.next_deadline()).flatten();
                let disk = self.reactor.with(|reactor| reactor.io_deadline()).flatten();
                let deadline = match (timer, disk) {
                    (Some(timer), Some(disk)) => Some(timer.min(disk)),
                    (timer, disk) => timer.or(disk),
                };
                let mut timeout =
                    deadline.map(|deadline| deadline.saturating_duration_since(Instant::now()));
                if held {
                    // A full queue drains without telling us; look again soon.
                    timeout = Some(timeout.map_or(SMP_RETRY, |timeout| timeout.min(SMP_RETRY)));
                }
                let asleep = Instant::now();
                let completed = self.reactor.with(|reactor| reactor.park(timeout));
                self.shared.sleeping.store(false, Ordering::SeqCst);
                stats::add(&self.stats.sleeps, 1);
                stats::add(&self.stats.idle_nanos, stats::nanos(asleep.elapsed()));
                completed
            };
            match completed {
                Some(Ok(completions)) => {
                    for waker in completions.wakers {
                        wake_quietly(waker);
                    }
                    for buffer in completions.garbage {
                        drop_quietly(buffer);
                    }
                }
                Some(Err(err)) => return Err(err),
                None => {}
            }
        }
    }

    /// Keeps looking for work, without sleeping, for at most the configured
    /// time. Returns whether there is now a task to run.
    ///
    /// While a shard is here, others that send it something do not have to
    /// wake it through the kernel, and it notices completions and messages
    /// within nanoseconds instead of microseconds.
    fn spin(&self) -> io::Result<bool> {
        let start = Instant::now();
        let found = loop {
            self.drain_inbox();
            self.flush_outgoing();
            match self.reactor.with(|reactor| reactor.poll()) {
                Some(Ok(completions)) => {
                    for waker in completions.wakers {
                        wake_quietly(waker);
                    }
                    for buffer in completions.garbage {
                        drop_quietly(buffer);
                    }
                }
                Some(Err(err)) => return Err(err),
                None => {}
            }
            self.fire_timers();
            let runnable = self.run_queue.with(|queue| !queue.is_empty()).unwrap_or(false);
            if runnable || self.stop.get() {
                break runnable;
            }
            if start.elapsed() >= self.config.spin {
                break false;
            }
            std::hint::spin_loop();
        };
        stats::add(&self.stats.spin_nanos, stats::nanos(start.elapsed()));
        Ok(found)
    }

    /// Records a slice that ran past the stall threshold and tells the
    /// application, if it asked to be told.
    fn report_stall(&self, group: usize, duration: Duration) {
        stats::add(&self.stats.stalls, 1);
        let nanos = stats::nanos(duration);
        if nanos > self.stats.longest_stall_nanos.get() {
            self.stats.longest_stall_nanos.set(nanos);
        }
        let backtrace = stall::take_backtrace();
        let Some(on_stall) = self.config.on_stall else { return };
        let report = StallReport {
            shard: self.shared.id,
            duration,
            group: self
                .run_queue
                .with(|queue| queue.group_name(group))
                .unwrap_or_default(),
            backtrace,
        };
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| on_stall(&report))) {
            discard_panic(payload);
        }
    }

    /// Asks the application to free memory while this shard's heap is over
    /// its limit.
    fn reclaim_memory(&self) {
        if !crate::alloc::over_limit() {
            return;
        }
        let turn = self.stats.loops.get();
        if turn < self.reclaim_after.get() {
            return;
        }
        let reclaimers = self.reclaimers.with(|reclaimers| reclaimers.clone());
        for reclaim in reclaimers.unwrap_or_default() {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| reclaim())) {
                discard_panic(payload);
            }
        }
        // Still over the limit: the application had nothing left to give.
        // It should not be asked again thousands of times a second.
        if crate::alloc::over_limit() {
            self.reclaim_after.set(turn.saturating_add(RECLAIM_BACKOFF));
        }
    }

    pub(crate) fn request_stop(&self) {
        self.stop.set(true);
    }

    fn shutdown(&self) {
        // Dropping a task or a service may spawn more of either, so repeat
        // until nothing is left. Each value is dropped outside any borrow
        // and with panics contained, so one bad `Drop` cannot stop the rest.
        loop {
            let tasks = self.tasks.with(std::mem::take).unwrap_or_default();
            let services = self.services.with(std::mem::take).unwrap_or_default();
            self.run_queue.with(|queue| queue.clear());
            if tasks.is_empty() && services.is_empty() {
                break;
            }
            for (_, task) in tasks {
                // SAFETY: this shard spawned every task in its table.
                unsafe { task.cancel() };
            }
            for (_, service) in services {
                drop_quietly(service);
            }
        }
        if let Some(timers) = self.timers.with(|timers| timers.take_all()) {
            drop_quietly(timers);
        }
        if let Some(reclaimers) = self.reclaimers.with(std::mem::take) {
            drop_quietly(reclaimers);
        }
        // Wait for the kernel to let go of every buffer before freeing them.
        if let Some(completions) = self.reactor.with(|reactor| reactor.drain()) {
            drop_quietly(completions.wakers);
            for buffer in completions.garbage {
                drop_quietly(buffer);
            }
        }
    }
}

/// Installs a shard as the current one and tears it down on drop.
struct Entered(Rc<Shard>, Option<StallTimer>);

impl Entered {
    fn new(shard: Rc<Shard>) -> Result<Entered> {
        let installed = CURRENT.try_with(|current| match current.try_borrow_mut() {
            Ok(mut current) if current.is_none() => {
                *current = Some(shard.clone());
                true
            }
            _ => false,
        });
        match installed {
            Ok(true) => {
                // The guard exists from here on, so that a failure below
                // still tears the shard down properly.
                let entered = Entered(shard, None);
                let shard = &entered.0;
                if let Some(Err(err)) = shard.reactor.with(|reactor| reactor.enable()) {
                    return Err(Error::Io(err));
                }
                shard.reactor.with(|reactor| reactor.publish());
                if let Some(limit) = shard.config.memory_limit {
                    crate::alloc::set_limit(limit);
                }
                // Best effort: without the timer, stalls are still counted,
                // only without a backtrace.
                let timer = if shard.config.stall_backtraces {
                    StallTimer::start(shard.config.stall_threshold).ok()
                } else {
                    None
                };
                let mut entered = entered;
                entered.1 = timer;
                Ok(entered)
            }
            Ok(false) => Err(Error::AlreadyRunning),
            Err(_) => Err(Error::NoRuntime),
        }
    }
}

impl Drop for Entered {
    fn drop(&mut self) {
        self.1 = None;
        self.0.shutdown();
        Reactor::unpublish();
        let shard = CURRENT
            .try_with(|current| current.try_borrow_mut().ok().and_then(|mut shard| shard.take()));
        drop_quietly(shard);
    }
}

/// Stops and joins the worker shards on drop.
struct Workers {
    runtime: Arc<RuntimeShared>,
    threads: Vec<std::thread::JoinHandle<()>>,
}

impl Drop for Workers {
    fn drop(&mut self) {
        for shard in self.runtime.shards.iter().skip(1) {
            shard.send_alien(Message::Stop);
        }
        for thread in self.threads.drain(..) {
            if let Err(payload) = thread.join() {
                discard_panic(payload);
            }
        }
    }
}

/// Stops the shard when the main task ends, whether it returned, panicked
/// or was dropped.
struct StopOnDrop;

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        if let Some(shard) = try_current() {
            shard.request_stop();
        }
    }
}

/// Upper bound on the shard count, so that a nonsensical request fails
/// cleanly instead of exhausting memory.
const MAX_SHARDS: usize = 1024;

#[derive(Clone, Copy, Debug)]
struct ShardConfig {
    task_quota: Duration,
    io_depth: usize,
    stall_threshold: Duration,
    stall_backtraces: bool,
    on_stall: Option<fn(&StallReport)>,
    memory_limit: Option<usize>,
    submit_limit: usize,
    spin: Duration,
}

/// Configures and runs a runtime.
#[derive(Debug)]
#[must_use = "a runtime does nothing until `run` is called"]
pub struct Runtime {
    shards: usize,
    pin: bool,
    ring_entries: u32,
    config: ShardConfig,
    io_properties: Vec<(PathBuf, IoProperties)>,
    io_latency_goal: Option<Duration>,
    fixed_buffers: Option<(u16, usize)>,
}

impl Default for Runtime {
    fn default() -> Self {
        Runtime::new()
    }
}

impl Runtime {
    /// A runtime with one shard per CPU the process is allowed to run on.
    pub fn new() -> Runtime {
        let shards = match affinity::allowed_cpus().len() {
            0 => std::thread::available_parallelism().map_or(1, |n| n.get()),
            n => n,
        };
        Runtime {
            shards: shards.clamp(1, MAX_SHARDS),
            pin: true,
            ring_entries: 1024,
            config: ShardConfig {
                task_quota: Duration::from_micros(500),
                io_depth: 64,
                stall_threshold: Duration::from_millis(25),
                stall_backtraces: false,
                on_stall: None,
                memory_limit: None,
                submit_limit: 1024,
                spin: Duration::ZERO,
            },
            io_properties: Vec::new(),
            io_latency_goal: None,
            fixed_buffers: None,
        }
    }

    /// Sets the number of shards, clamped to `1..=1024`.
    pub fn shards(mut self, shards: usize) -> Runtime {
        self.shards = shards.clamp(1, MAX_SHARDS);
        self
    }

    /// Whether to pin each shard to its own CPU. On by default.
    pub fn pin_threads(mut self, pin: bool) -> Runtime {
        self.pin = pin;
        self
    }

    /// Sets the submission queue size of each shard's io_uring.
    pub fn ring_entries(mut self, entries: u32) -> Runtime {
        self.ring_entries = entries;
        self
    }

    /// Sets the time slice: how long a shard runs tasks of one scheduling
    /// group before it checks I/O and timers and picks a group again. It
    /// bounds the latency a well-behaved task adds to the others. Default
    /// 500 microseconds.
    ///
    /// A task is never interrupted, so the bound only holds if long loops
    /// call [`maybe_yield`](crate::maybe_yield).
    pub fn task_quota(mut self, quota: Duration) -> Runtime {
        self.config.task_quota = quota;
        self
    }

    /// Sets how many requests per device each shard keeps in the kernel at
    /// once (at least one). Further requests wait in the shard's I/O
    /// scheduler, where they are ordered by scheduling-group I/O shares.
    /// Default 64.
    ///
    /// This is a per-shard ceiling. The load on a device as a whole is
    /// governed by [`io_properties`](Self::io_properties).
    pub fn io_depth(mut self, depth: usize) -> Runtime {
        self.config.io_depth = depth.max(1);
        self
    }

    /// Declares the capacity of the device that holds `path`, as measured
    /// by [`iotune::measure`](crate::iotune::measure).
    ///
    /// All shards together then send that device no more than it can
    /// sustain, with reads and writes charged at their own rates, and the
    /// excess waits in the shards' I/O schedulers where scheduling-group
    /// shares apply. Without this, a device is limited only by
    /// [`io_depth`](Self::io_depth).
    ///
    /// The device is identified by the filesystem `path` is on, so `path`
    /// must exist when the runtime starts, and files on other filesystems
    /// are not affected.
    pub fn io_properties(mut self, path: impl Into<PathBuf>, properties: IoProperties) -> Runtime {
        self.io_properties.push((path.into(), properties));
        self
    }

    /// Sets how much work may be queued inside a device with configured
    /// capacity, expressed as the time the device needs to finish it. A
    /// request that jumps the queue by priority still waits about this
    /// long. Defaults to one and a half times the
    /// [`task_quota`](Self::task_quota).
    pub fn io_latency_goal(mut self, goal: Duration) -> Runtime {
        self.io_latency_goal = Some(goal);
        self
    }

    /// Sets how long a time slice may run before it counts as a stall: a
    /// task failed to yield and held up everything else on its shard.
    /// Default 25 milliseconds. Stalls are counted in
    /// [`stats::shard`](crate::stats::shard).
    pub fn stall_threshold(mut self, threshold: Duration) -> Runtime {
        self.config.stall_threshold = threshold.max(Duration::from_micros(1));
        self
    }

    /// Calls `handler` on the shard's thread after every stall.
    pub fn on_stall(mut self, handler: fn(&StallReport)) -> Runtime {
        self.config.on_stall = Some(handler);
        self
    }

    /// Records the call stack of a stalled shard while it is stalled, for
    /// [`on_stall`](Self::on_stall). Off by default.
    ///
    /// This interrupts every shard thread once per stall threshold with a
    /// real-time signal (`SIGRTMIN + 5`), for which the runtime installs a
    /// process-wide handler. A stall is caught once it has lasted between
    /// one and two thresholds.
    pub fn stall_backtraces(mut self, enabled: bool) -> Runtime {
        self.config.stall_backtraces = enabled;
        self
    }

    /// Sets how much memory a shard's heap may hold before the runtime
    /// asks the application to free some, by calling the functions
    /// registered with [`alloc::on_pressure`](crate::alloc::on_pressure).
    /// It is a request, not a cap: allocations beyond it still succeed.
    /// Only meaningful with [`ShardAlloc`](crate::alloc::ShardAlloc)
    /// installed.
    pub fn memory_limit(mut self, bytes_per_shard: usize) -> Runtime {
        self.config.memory_limit = Some(bytes_per_shard);
        self
    }

    /// Sets how many [`submit_to`](crate::submit_to) calls one shard may
    /// have in progress on one other shard at a time (at least one).
    /// Default 1024.
    ///
    /// A call counts from when it is sent until the other shard has
    /// finished running it, even if the caller stopped waiting. Further
    /// calls wait their turn, in order. This is what keeps a shard that
    /// falls behind from being buried: its callers are slowed down instead
    /// of its queue and its task list growing without bound.
    ///
    /// The limit applies to each ordered pair of shards separately, and to
    /// everything built on `submit_to`: [`submit_to_all`](crate::submit_to_all)
    /// and the calls of a [`Sharded`](crate::Sharded) service.
    pub fn submit_limit(mut self, calls: usize) -> Runtime {
        self.config.submit_limit = calls.max(1);
        self
    }

    /// Sets how long a shard with nothing to do keeps looking for work
    /// before it goes to sleep. Default zero: it sleeps at once.
    ///
    /// A sleeping shard has to be woken through the kernel, which costs a
    /// couple of microseconds on each side, so a call to an idle shard
    /// takes several times as long as one to a busy shard. A shard that is
    /// still looking when the work arrives picks it up at once. A value
    /// around the gap between pieces of work, typically 50 to 200
    /// microseconds, removes most of that cost at light and moderate
    /// load.
    ///
    /// The price is CPU time: after every burst of work a shard spends
    /// this long at full power doing nothing. With a value as long as the
    /// gaps between work, shards never sleep and the process uses all its
    /// CPUs whatever the load. Time spent this way is reported as
    /// [`ShardStats::spinning`](crate::stats::ShardStats::spinning).
    pub fn spin_before_sleep(mut self, spin: Duration) -> Runtime {
        self.config.spin = spin;
        self
    }

    /// Gives every shard a pool of `count` buffers of `size` bytes
    /// (rounded up to whole pages) registered with its io_uring, to be
    /// used through [`FixedBuf`](crate::buf::FixedBuf).
    ///
    /// Registered memory is pinned, so the pools count against the
    /// process's locked-memory limit on kernels that enforce one.
    pub fn fixed_buffers(mut self, count: u16, size: usize) -> Runtime {
        self.fixed_buffers = (count > 0).then_some((count, size));
        self
    }

    /// The number of shards this runtime will start.
    pub fn shard_count(&self) -> usize {
        self.shards
    }

    /// Starts the shards, runs `main` on shard 0 and returns its output.
    ///
    /// The calling thread becomes shard 0. When `main` completes, every
    /// remaining task on every shard is dropped and the worker threads are
    /// joined.
    ///
    /// # Errors
    ///
    /// - [`Error::Io`] if a shard could not be set up, or if a shard's
    ///   io_uring failed while running.
    /// - [`Error::AlreadyRunning`] if called from inside a runtime.
    /// - [`Error::Panicked`] if `main` panicked.
    pub fn run<F, Fut>(self, main: F) -> Result<Fut::Output>
    where
        F: FnOnce() -> Fut,
        Fut: Future + 'static,
        Fut::Output: 'static,
    {
        if try_current().is_some() {
            return Err(Error::AlreadyRunning);
        }

        let cpus = if self.pin { affinity::allowed_cpus() } else { Vec::new() };
        let cpu_for = |shard: usize| {
            let index = shard.checked_rem(cpus.len())?;
            cpus.get(index).copied()
        };

        // Create every ring and eventfd up front so a failure is reported
        // here instead of on a worker thread.
        let mut shared = Vec::with_capacity(self.shards);
        let mut parts = Vec::with_capacity(self.shards);
        for id in 0..self.shards {
            let (alien, receiver) = channel();
            let shard = Arc::new(ShardShared {
                id,
                alien,
                sleeping: AtomicBool::new(false),
                eventfd: new_eventfd()?,
            });
            let (ring, ring_disabled) = Reactor::create_ring(self.ring_entries)?;
            let fixed = match self.fixed_buffers {
                Some((count, size)) => {
                    let region = Region::new(count, size)?;
                    // SAFETY: the region is kept by the shard for as long
                    // as its reactor, and so its ring, exists.
                    unsafe { Reactor::register_buffers(&ring, &region.iovecs())? };
                    Some(region)
                }
                None => None,
            };
            shared.push(shard.clone());
            parts.push(ShardParts {
                shared: shard,
                alien: receiver,
                ring,
                ring_disabled,
                preempt: IoUring::new(4)?,
                fixed,
                outgoing: Vec::with_capacity(self.shards),
                incoming: Vec::with_capacity(self.shards),
            });
        }
        // One queue for every ordered pair of shards.
        for from in 0..self.shards {
            for (to, target) in shared.iter().enumerate() {
                let outgoing = if from == to {
                    None
                } else {
                    let (producer, consumer) = spsc::channel(SMP_QUEUE_LENGTH);
                    if let Some(receiver) = parts.get_mut(to) {
                        receiver.incoming.push(consumer);
                    }
                    Some(Outgoing {
                        queue: producer,
                        overflow: VecDeque::new(),
                        target: target.clone(),
                    })
                };
                if let Some(sender) = parts.get_mut(from) {
                    sender.outgoing.push(outgoing);
                }
            }
        }
        let config = self.config;
        let latency_goal = self.io_latency_goal.unwrap_or_else(|| {
            let quota = config.task_quota;
            quota.saturating_add(quota.checked_div(2).unwrap_or_default())
        });
        let mut devices = HashMap::new();
        for (path, properties) in &self.io_properties {
            let device = std::fs::metadata(path)?.dev();
            devices.insert(device, Arc::new(DeviceGroup::new(*properties, latency_goal)));
        }
        let devices = Arc::new(DeviceTable::new(devices));
        let runtime = Arc::new(RuntimeShared {
            shards: shared,
            groups: Arc::new(GroupTable::new()),
            failure: Mutex::new(None),
        });

        let mut parts = parts.into_iter();
        let Some(parts0) = parts.next() else {
            return Err(Error::Io(io::Error::other("runtime has no shards")));
        };

        // Declared before the shard-0 guard so that it is dropped after it:
        // on every exit path the workers are stopped and joined.
        let mut workers = Workers {
            runtime: runtime.clone(),
            threads: Vec::new(),
        };
        for parts in parts {
            let runtime = runtime.clone();
            let devices = devices.clone();
            let cpu = cpu_for(parts.shared.id);
            let thread = std::thread::Builder::new()
                .name(format!("starfish-{}", parts.shared.id))
                .spawn(move || {
                    if let Some(cpu) = cpu {
                        affinity::pin_current_thread(cpu);
                    }
                    // Declared first so that it is released last: the shard
                    // is built from, and torn down into, its own heap.
                    let _heap = crate::alloc::enter();
                    let shard = Rc::new(Shard::new(parts, runtime.clone(), config, devices));
                    let result = match Entered::new(shard) {
                        Ok(entered) => entered.0.run(),
                        Err(err) => Err(err.into()),
                    };
                    if let Err(err) = result {
                        runtime.fail(err);
                    }
                })?;
            workers.threads.push(thread);
        }

        // The caller's thread is only borrowed, so give its affinity back.
        let _restore = affinity::Restore::capture();
        if let Some(cpu) = cpu_for(0) {
            affinity::pin_current_thread(cpu);
        }

        let _heap = crate::alloc::enter();
        let shard = Rc::new(Shard::new(parts0, runtime.clone(), config, devices));
        let entered = Entered::new(shard)?;

        let future = match catch_unwind(AssertUnwindSafe(main)) {
            Ok(future) => future,
            Err(payload) => {
                discard_panic(payload);
                return Err(Error::Panicked);
            }
        };
        let output = Rc::new(Cell::new(None));
        let slot = output.clone();
        let main = async move {
            let _stop = StopOnDrop;
            let value = future.await;
            slot.set(Some(value));
        };
        drop(entered.0.spawn_task(DEFAULT_GROUP, main, true));
        let ran = entered.0.run();

        drop(entered);
        drop(workers);
        ran?;
        if let Some(err) = runtime.take_failure() {
            return Err(Error::Io(err));
        }
        // The main task ended without a value only if it panicked.
        output.take().ok_or(Error::Panicked)
    }
}

fn new_eventfd() -> io::Result<OwnedFd> {
    // SAFETY: plain syscall; the returned fd is owned by nobody else.
    let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

mod affinity {
    use std::mem;

    const SET_SIZE: usize = libc::CPU_SETSIZE as usize;

    /// The CPUs this thread may currently run on.
    pub(super) fn allowed_cpus() -> Vec<usize> {
        match get() {
            Some(set) => (0..SET_SIZE)
                // SAFETY: the index is below CPU_SETSIZE.
                .filter(|&cpu| unsafe { libc::CPU_ISSET(cpu, &set) })
                .collect(),
            None => Vec::new(),
        }
    }

    /// Best effort: a failure leaves the thread unpinned.
    pub(super) fn pin_current_thread(cpu: usize) {
        if cpu >= SET_SIZE {
            return;
        }
        // SAFETY: the set is zero-initialised and the index is in range.
        unsafe {
            let mut set: libc::cpu_set_t = mem::zeroed();
            libc::CPU_SET(cpu, &mut set);
            libc::sched_setaffinity(0, mem::size_of::<libc::cpu_set_t>(), &set);
        }
    }

    fn get() -> Option<libc::cpu_set_t> {
        // SAFETY: the kernel fills a correctly sized set.
        unsafe {
            let mut set: libc::cpu_set_t = mem::zeroed();
            let rc = libc::sched_getaffinity(0, mem::size_of::<libc::cpu_set_t>(), &mut set);
            (rc == 0).then_some(set)
        }
    }

    /// Restores the thread's original affinity mask on drop.
    pub(super) struct Restore(Option<libc::cpu_set_t>);

    impl Restore {
        pub(super) fn capture() -> Restore {
            Restore(get())
        }
    }

    impl Drop for Restore {
        fn drop(&mut self) {
            if let Some(set) = &self.0 {
                // SAFETY: `set` came from sched_getaffinity.
                unsafe {
                    libc::sched_setaffinity(0, mem::size_of::<libc::cpu_set_t>(), set);
                }
            }
        }
    }
}
