//! The per-shard io_uring instance and the future type for one operation.

use std::any::Any;
use std::cell::Cell;
use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::os::fd::RawFd;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use io_uring::{opcode, squeue, types, IoUring};
use slab::Slab;

use crate::ioqueue::{Direction, IoScheduler, QueuedRequest};
use crate::runtime::try_current;

/// Completion of the eventfd read that wakes a sleeping shard.
const WAKE: u64 = u64::MAX;
/// Completion of a cancel request; nobody waits for it.
const CANCEL: u64 = u64::MAX - 1;
/// Completion of a close that nobody waits for (see `close_detached`).
const CLOSE: u64 = u64::MAX - 2;
/// How often `push` flushes a full submission queue before giving up.
const PUSH_ATTEMPTS: usize = 64;

enum Stage {
    /// Waiting in a disk queue; the kernel has not seen it.
    Queued,
    /// Submitted to the kernel.
    InKernel,
    /// Finished with this result; the future has not collected it yet.
    Completed(i32),
    /// The future was dropped while the kernel still used its buffers. They
    /// are parked here until the completion arrives.
    Orphaned { data: Box<dyn Any>, result_is_fd: bool },
    /// The future already has its result; the kernel still holds the
    /// buffer (see `OpState::held`).
    ResultTaken,
}

struct OpState {
    stage: Stage,
    waker: Option<Waker>,
    /// For an operation that went through the I/O scheduler: its device.
    disk: Option<u64>,
    /// The scheduling group it was queued under.
    class: usize,
    /// The result of an operation that reports in two steps (a zero-copy
    /// send: first the outcome, then that the buffer is no longer in use).
    partial: Option<i32>,
    /// A buffer the reactor owns on the operation's behalf, released when
    /// the kernel is done with it.
    held: Option<Box<dyn Any>>,
    /// Whether the future may have its result as soon as it is known,
    /// before the kernel lets go of `held`.
    early: bool,
}

/// What `Reactor::poll_op` found.
enum OpPoll {
    /// Finished. Carries the buffer the reactor held, if it is free now.
    Done(i32, Option<Box<dyn Any>>),
    /// Still running. Carries the waker this poll replaced, to be dropped
    /// once the reactor is no longer borrowed.
    Pending(Option<Waker>),
    /// The reactor has no record of the operation.
    Missing,
}

/// Errors from `io_uring_enter` that mean "try again", not "the ring is
/// broken".
fn transient(err: &io::Error) -> bool {
    matches!(
        err.raw_os_error(),
        Some(libc::ETIME | libc::EINTR | libc::EBUSY | libc::EAGAIN)
    )
}

thread_local! {
    /// The current shard's slice-timer ring, for `completions_pending`.
    static RING: Cell<*const IoUring> = const { Cell::new(std::ptr::null()) };
}

/// Whether the current time slice has run out. This is the preemption
/// signal. It costs two memory reads: the slice timer lives on a ring of
/// its own, the kernel posts a completion there when it fires, and the
/// shard only compares the two ends of that ring's completion queue.
///
/// Ordinary I/O completions deliberately do not count. They arrive on the
/// main ring, and a slice that ended at every completion would run one
/// task at a time under fast I/O.
pub(crate) fn completions_pending() -> bool {
    RING.try_with(|ring| {
        let ring = ring.get();
        if ring.is_null() {
            return false;
        }
        // SAFETY: the pointer is published by `Reactor::publish` for a ring
        // that outlives the publication, on this thread. Only this thread
        // reads the completion queue, and it is not doing so now: this
        // function is called from task code, never from inside the reactor.
        unsafe { !(*ring).completion_shared().is_empty() }
    })
    .unwrap_or(false)
}

/// What a turn of the reactor leaves for the shard to deal with once the
/// reactor is no longer borrowed.
#[derive(Default)]
pub(crate) struct Completions {
    /// Tasks to wake.
    pub(crate) wakers: Vec<Waker>,
    /// Buffers the kernel has let go of, to be dropped. Dropping one may
    /// run code that wakes tasks, hence not inside the reactor.
    pub(crate) garbage: Vec<Box<dyn Any>>,
}

pub(crate) struct Reactor {
    ring: IoUring,
    /// Carries nothing but the slice timer.
    preempt: IoUring,
    ops: Slab<OpState>,
    /// Wakers of operations completed since the last `poll`/`park`.
    ready: Vec<Waker>,
    /// Buffers released since the last `poll`/`park`.
    garbage: Vec<Box<dyn Any>>,
    /// Length of a time slice, as the kernel wants it.
    quota: Box<types::Timespec>,
    preempt_armed: bool,
    disk: IoScheduler,
    /// Reused by `dispatch`.
    batch: Vec<QueuedRequest>,
    eventfd: RawFd,
    wake_buf: Box<[u8; 8]>,
    wake_armed: bool,
    /// Closes sent to the kernel whose completions are still to come.
    closing: usize,
    /// `drain` has run: nothing will collect completions any more.
    drained: bool,
}

impl Reactor {
    pub(crate) fn new(
        ring: IoUring,
        preempt: IoUring,
        eventfd: RawFd,
        disk: IoScheduler,
        quota: Duration,
    ) -> Reactor {
        let quota = types::Timespec::new()
            .sec(quota.as_secs())
            .nsec(quota.subsec_nanos());
        Reactor {
            ring,
            preempt,
            ops: Slab::new(),
            ready: Vec::new(),
            garbage: Vec::new(),
            quota: Box::new(quota),
            preempt_armed: false,
            disk,
            batch: Vec::new(),
            eventfd,
            wake_buf: Box::new([0; 8]),
            wake_armed: false,
            closing: 0,
            drained: false,
        }
    }

    /// Makes this reactor's ring the one `completions_pending` looks at.
    /// The reactor must not move until `unpublish`.
    pub(crate) fn publish(&self) {
        let _ = RING.try_with(|ring| ring.set(&self.preempt));
    }

    pub(crate) fn unpublish() {
        let _ = RING.try_with(|ring| ring.set(std::ptr::null()));
    }

    /// Starts the timer that ends the current time slice, unless one is
    /// already running. When it fires the kernel posts a completion on the
    /// timer ring, which is what `completions_pending` sees.
    pub(crate) fn arm_preempt(&mut self) -> io::Result<()> {
        // A completion on the timer ring means the last timer has fired.
        let mut fired = false;
        for _ in self.preempt.completion() {
            fired = true;
        }
        if fired {
            self.preempt_armed = false;
        }
        if self.preempt_armed {
            return Ok(());
        }
        let sqe = opcode::Timeout::new(&*self.quota).build();
        // SAFETY: the kernel copies the timespec during this submit call,
        // and `quota` is alive for the call.
        if unsafe { self.preempt.submission().push(&sqe) }.is_ok() {
            match self.preempt.submit() {
                Ok(_) => self.preempt_armed = true,
                Err(err) if transient(&err) => {}
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }

    /// Queues an entry, flushing to the kernel when the queue is full.
    fn push(&mut self, sqe: &squeue::Entry) -> io::Result<()> {
        for _ in 0..PUSH_ATTEMPTS {
            // SAFETY: callers guarantee the entry's pointers stay valid until
            // its completion is reaped (see `Op::new` and `arm_wake`).
            if unsafe { self.ring.submission().push(sqe) }.is_ok() {
                return Ok(());
            }
            self.flush()?;
            // A full completion queue makes the kernel refuse submissions.
            self.reap();
        }
        Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "io_uring submission queue is full",
        ))
    }

    /// Hands queued entries to the kernel without waiting.
    fn flush(&mut self) -> io::Result<()> {
        if self.ring.submission().is_empty() {
            return Ok(());
        }
        match self.ring.submit() {
            Err(err) if !transient(&err) => Err(err),
            _ => Ok(()),
        }
    }

    /// Submits an operation straight to the kernel.
    fn submit_op(&mut self, sqe: squeue::Entry) -> io::Result<usize> {
        let key = self.ops.insert(OpState {
            stage: Stage::InKernel,
            waker: None,
            disk: None,
            class: 0,
            partial: None,
            held: None,
            early: false,
        });
        if let Err(err) = self.push(&sqe.user_data(key as u64)) {
            self.ops.try_remove(key);
            return Err(err);
        }
        Ok(key)
    }

    /// Submits an operation whose buffer the reactor keeps. The future gets
    /// the result as soon as the kernel reports it, and the buffer is
    /// released when the kernel reports that it no longer uses it. On
    /// failure the buffer is handed back.
    fn submit_held(
        &mut self,
        sqe: squeue::Entry,
        data: Box<dyn Any>,
    ) -> Result<usize, (io::Error, Box<dyn Any>)> {
        let key = self.ops.insert(OpState {
            stage: Stage::InKernel,
            waker: None,
            disk: None,
            class: 0,
            partial: None,
            held: None,
            early: true,
        });
        if let Err(err) = self.push(&sqe.user_data(key as u64)) {
            self.ops.try_remove(key);
            return Err((err, data));
        }
        if let Some(state) = self.ops.get_mut(key) {
            state.held = Some(data);
        }
        Ok(key)
    }

    /// Submits a disk operation through the I/O scheduler.
    fn submit_disk(
        &mut self,
        sqe: squeue::Entry,
        device: u64,
        class: usize,
        direction: Direction,
        bytes: u64,
    ) -> usize {
        let key = self.ops.insert(OpState {
            stage: Stage::Queued,
            waker: None,
            disk: Some(device),
            class,
            partial: None,
            held: None,
            early: false,
        });
        let sqe = sqe.user_data(key as u64);
        let class = self.disk.enqueue(key, device, class, direction, bytes, sqe);
        if let Some(state) = self.ops.get_mut(key) {
            state.class = class;
        }
        self.dispatch();
        key
    }

    /// Moves disk requests from the scheduler into the kernel, as far as
    /// queue depth and device capacity allow.
    fn dispatch(&mut self) {
        let mut batch = std::mem::take(&mut self.batch);
        self.disk.take_ready(&mut batch);
        for request in batch.drain(..) {
            let pushed = self.push(&request.sqe);
            let Some(state) = self.ops.get_mut(request.key) else {
                self.disk.completed(request.device);
                continue;
            };
            match pushed {
                Ok(()) => state.stage = Stage::InKernel,
                Err(err) => {
                    self.disk.completed(request.device);
                    let errno = err.raw_os_error().unwrap_or(libc::EIO);
                    state.stage = Stage::Completed(errno.saturating_neg());
                    if let Some(waker) = state.waker.take() {
                        self.ready.push(waker);
                    }
                }
            }
        }
        self.batch = batch;
    }

    /// Registers the shard's fixed buffers with the ring.
    ///
    /// # Safety
    ///
    /// The memory the entries describe must stay valid, and must not be
    /// freed, until the ring is closed.
    pub(crate) unsafe fn register_buffers(ring: &IoUring, buffers: &[libc::iovec]) -> io::Result<()> {
        ring.submitter().register_buffers(buffers)
    }

    pub(crate) fn device_stats(&self) -> Vec<crate::stats::DeviceStats> {
        self.disk.stats()
    }

    /// When the shard must wake up to dispatch disk requests that are
    /// waiting for device capacity.
    pub(crate) fn io_deadline(&self) -> Option<Instant> {
        self.disk.retry_at()
    }

    /// Best effort: an operation that cannot be cancelled still completes on
    /// its own, or is cancelled again by `drain`.
    fn cancel(&mut self, user_data: u64) {
        let sqe = opcode::AsyncCancel::new(user_data).build().user_data(CANCEL);
        let _ = self.push(&sqe);
    }

    /// Closes a file descriptor through the ring, with nobody waiting for
    /// the outcome, so that dropping a file does not block the shard.
    /// Returns whether the close was queued; if not, the caller still owns
    /// the descriptor.
    pub(crate) fn close_detached(&mut self, fd: RawFd) -> bool {
        if self.drained {
            return false;
        }
        let sqe = opcode::Close::new(types::Fd(fd)).build().user_data(CLOSE);
        if self.push(&sqe).is_err() {
            return false;
        }
        self.closing = self.closing.saturating_add(1);
        true
    }

    /// Keeps one read pending on the eventfd so that a write to it from
    /// another thread produces a completion and ends `park`.
    fn arm_wake(&mut self) -> io::Result<()> {
        if self.wake_armed {
            return Ok(());
        }
        let sqe = opcode::Read::new(types::Fd(self.eventfd), self.wake_buf.as_mut_ptr(), 8)
            .build()
            .user_data(WAKE);
        self.push(&sqe)?;
        self.wake_armed = true;
        Ok(())
    }

    fn reap(&mut self) {
        let mut completions = self.ring.completion();
        for cqe in &mut completions {
            match cqe.user_data() {
                WAKE => self.wake_armed = false,
                CANCEL => {}
                CLOSE => self.closing = self.closing.saturating_sub(1),
                user_data => {
                    let key = user_data as usize;
                    let Some(state) = self.ops.get_mut(key) else { continue };
                    if io_uring::cqueue::more(cqe.flags()) {
                        // More completions follow for this operation; the
                        // kernel still uses its buffer. Keep the outcome.
                        state.partial.get_or_insert(cqe.result());
                        if state.early {
                            // The future may have it already.
                            if let Some(waker) = state.waker.take() {
                                self.ready.push(waker);
                            }
                        }
                        continue;
                    }
                    if let Some(device) = state.disk {
                        self.disk.completed(device);
                    }
                    let result = state.partial.take().unwrap_or_else(|| cqe.result());
                    match std::mem::replace(&mut state.stage, Stage::Completed(result)) {
                        Stage::Orphaned { data, result_is_fd } => {
                            close_orphan(result_is_fd, result);
                            self.garbage.push(data);
                            if let Some(held) = self.ops.try_remove(key).and_then(|state| state.held) {
                                self.garbage.push(held);
                            }
                        }
                        Stage::ResultTaken => {
                            // Nobody is waiting; only the buffer was left.
                            if let Some(held) = self.ops.try_remove(key).and_then(|state| state.held) {
                                self.garbage.push(held);
                            }
                        }
                        _ => {
                            if let Some(waker) = state.waker.take() {
                                self.ready.push(waker);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Collects completions and refills the kernel from the disk queues.
    fn turn(&mut self) -> io::Result<Completions> {
        self.reap();
        // Completions and the passage of time free up capacity; send the
        // next requests now instead of on the next loop iteration.
        self.dispatch();
        self.flush()?;
        Ok(self.take_completions())
    }

    fn take_completions(&mut self) -> Completions {
        Completions {
            wakers: std::mem::take(&mut self.ready),
            garbage: std::mem::take(&mut self.garbage),
        }
    }

    /// Submits queued entries and collects completions without blocking.
    /// Returns the wakers to wake once the reactor is no longer borrowed.
    pub(crate) fn poll(&mut self) -> io::Result<Completions> {
        self.flush()?;
        self.turn()
    }

    /// Like `poll`, but blocks until a completion, a cross-thread wake-up or
    /// the timeout.
    pub(crate) fn park(&mut self, timeout: Option<Duration>) -> io::Result<Completions> {
        self.arm_wake()?;
        let result = match timeout {
            None => self.ring.submit_and_wait(1),
            Some(timeout) => {
                let ts = types::Timespec::new()
                    .sec(timeout.as_secs())
                    .nsec(timeout.subsec_nanos());
                let args = types::SubmitArgs::new().timespec(&ts);
                self.ring.submitter().submit_with_args(1, &args)
            }
        };
        if let Err(err) = result {
            if !transient(&err) {
                return Err(err);
            }
        }
        self.turn()
    }

    /// Looks at an operation on behalf of its future. Takes the waker out of
    /// `waker` only if the operation is still running.
    fn poll_op(&mut self, key: usize, waker: &mut Option<Waker>) -> OpPoll {
        let Some(state) = self.ops.get_mut(key) else {
            return OpPoll::Missing;
        };
        if let Stage::Completed(result) = state.stage {
            let held = self.ops.try_remove(key).and_then(|state| state.held);
            return OpPoll::Done(result, held);
        }
        if state.early {
            if let Some(result) = state.partial {
                // The outcome is known; the buffer stays until the kernel
                // releases it.
                state.stage = Stage::ResultTaken;
                return OpPoll::Done(result, None);
            }
        }
        match waker.take() {
            Some(waker) => OpPoll::Pending(state.waker.replace(waker)),
            None => OpPoll::Pending(None),
        }
    }

    /// Lets go of an operation whose future is being dropped. Returns the
    /// operation's waker for the caller to drop.
    ///
    /// If the kernel is still working on the operation, `data` is moved into
    /// the reactor until it finishes. Otherwise `data` is left with the
    /// caller, who may free it.
    fn abandon(
        &mut self,
        key: usize,
        data: &mut Option<Box<dyn Any>>,
        result_is_fd: bool,
    ) -> Option<Waker> {
        let state = self.ops.get_mut(key)?;
        match state.stage {
            Stage::Completed(result) => {
                close_orphan(result_is_fd, result);
                let state = self.ops.try_remove(key)?;
                if let Some(held) = state.held {
                    self.garbage.push(held);
                }
                state.waker
            }
            Stage::Queued => {
                // Never reached the kernel: forget the request entirely.
                if let Some(device) = state.disk {
                    self.disk.remove(device, state.class, key);
                }
                self.ops.try_remove(key).and_then(|state| state.waker)
            }
            Stage::InKernel => {
                let data = match state.held.take() {
                    Some(held) => held,
                    None => data.take()?,
                };
                state.stage = Stage::Orphaned { data, result_is_fd };
                let waker = state.waker.take();
                self.cancel(key as u64);
                waker
            }
            Stage::Orphaned { .. } | Stage::ResultTaken => None,
        }
    }

    fn in_flight(&self) -> bool {
        self.wake_armed
            || self.closing > 0
            || self.ops.iter().any(|(_, state)| {
                matches!(
                    state.stage,
                    Stage::InKernel | Stage::Orphaned { .. } | Stage::ResultTaken
                )
            })
    }

    /// Cancels everything still in the kernel and waits for it to finish, so
    /// that no buffer is freed while the kernel can still write to it.
    /// Returns leftover wakers and buffers for the caller to drop.
    pub(crate) fn drain(&mut self) -> Completions {
        // Requests still in a disk queue never reached the kernel.
        self.disk.clear();
        let mut in_kernel = Vec::new();
        let mut queued = Vec::new();
        for (key, state) in self.ops.iter() {
            match state.stage {
                Stage::Queued => queued.push(key),
                Stage::InKernel | Stage::Orphaned { .. } => in_kernel.push(key),
                Stage::Completed(_) | Stage::ResultTaken => {}
            }
        }
        let mut wakers = std::mem::take(&mut self.ready);
        for key in queued {
            if let Some(waker) = self.ops.try_remove(key).and_then(|state| state.waker) {
                wakers.push(waker);
            }
        }
        for key in in_kernel {
            self.cancel(key as u64);
        }
        if self.wake_armed {
            self.cancel(WAKE);
        }
        while self.in_flight() {
            match self.ring.submit_and_wait(1) {
                Ok(_) => {}
                Err(err) if transient(&err) => {}
                Err(_) => {
                    // Cannot learn when the kernel is done: leak the buffers
                    // instead of freeing memory it may still write to.
                    std::mem::forget(std::mem::take(&mut self.ops));
                    std::mem::forget(std::mem::replace(&mut self.wake_buf, Box::new([0; 8])));
                    break;
                }
            }
            self.reap();
        }
        wakers.append(&mut self.ready);
        // Files dropped from here on are closed directly.
        self.drained = true;
        Completions {
            wakers,
            garbage: std::mem::take(&mut self.garbage),
        }
    }
}

fn to_result(result: i32) -> io::Result<u32> {
    if result < 0 {
        Err(io::Error::from_raw_os_error(result.saturating_neg()))
    } else {
        Ok(result.unsigned_abs())
    }
}

fn close_orphan(result_is_fd: bool, result: i32) {
    if result_is_fd && result >= 0 {
        // SAFETY: the kernel just handed us this fd and nobody else has it.
        unsafe {
            libc::close(result);
        }
    }
}

/// One in-flight io_uring operation.
///
/// `data` owns every piece of memory the kernel was given a pointer to. It is
/// returned with the result, or, if the future is dropped early, kept alive
/// by the reactor until the kernel reports completion.
pub(crate) struct Op<D: Unpin + 'static> {
    /// `None` if the operation never reached the reactor.
    key: Option<usize>,
    /// Why it never reached the reactor.
    error: Option<io::Error>,
    data: Option<D>,
    result_is_fd: bool,
    /// The key is only meaningful on the shard that submitted the operation.
    _not_send: PhantomData<*const ()>,
}

impl<D: Unpin + 'static> Op<D> {
    /// Submits `sqe` on the current shard. If that is not possible the
    /// returned future resolves to the error, handing `data` back.
    ///
    /// # Safety
    ///
    /// Every pointer in `sqe` must point into heap memory owned by `data`
    /// (so that moving `data` does not move the memory) and stay valid for
    /// as long as `data` is alive, or point to memory that is never freed.
    pub(crate) unsafe fn new(sqe: squeue::Entry, data: D) -> Op<D> {
        let submitted = match try_current() {
            Some(shard) => match shard.reactor.with(|reactor| reactor.submit_op(sqe)) {
                Some(result) => result,
                None => Err(io::Error::other("reactor is busy")),
            },
            None => Err(io::Error::other(crate::Error::NoRuntime)),
        };
        Op::from_submission(submitted, data)
    }

    /// Like [`Op::new`], for a read or write of `bytes` bytes on a file that
    /// lives on `device`. It goes through the shard's I/O scheduler, charged
    /// to the scheduling group of the running task.
    ///
    /// # Safety
    ///
    /// Same contract as [`Op::new`].
    pub(crate) unsafe fn new_disk(
        sqe: squeue::Entry,
        data: D,
        device: u64,
        direction: Direction,
        bytes: u64,
    ) -> Op<D> {
        let submitted = match try_current() {
            Some(shard) => {
                let class = shard.current_group.get();
                shard
                    .reactor
                    .with(|reactor| reactor.submit_disk(sqe, device, class, direction, bytes))
                    .ok_or_else(|| io::Error::other("reactor is busy"))
            }
            None => Err(io::Error::other(crate::Error::NoRuntime)),
        };
        Op::from_submission(submitted, data)
    }

    fn from_submission(submitted: io::Result<usize>, data: D) -> Op<D> {
        let (key, error) = match submitted {
            Ok(key) => (Some(key), None),
            Err(err) => (None, Some(err)),
        };
        Op {
            key,
            error,
            data: Some(data),
            result_is_fd: false,
            _not_send: PhantomData,
        }
    }

    /// Marks the result as a new file descriptor, to be closed if nobody is
    /// left to receive it.
    pub(crate) fn result_is_fd(mut self) -> Op<D> {
        self.result_is_fd = true;
        self
    }
}

impl<D: Unpin + 'static> Future for Op<D> {
    type Output = (io::Result<u32>, D);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        let Some(key) = this.key else {
            // Never submitted, or already finished (then `data` is gone and
            // the future stays pending, as nothing more can be returned).
            return match this.data.take() {
                Some(data) => {
                    let err = this
                        .error
                        .take()
                        .unwrap_or_else(|| io::Error::other("operation was not submitted"));
                    Poll::Ready((Err(err), data))
                }
                None => Poll::Pending,
            };
        };
        let Some(shard) = try_current() else {
            return Poll::Pending;
        };
        let mut waker = Some(cx.waker().clone());
        let polled = shard.reactor.with(|reactor| reactor.poll_op(key, &mut waker));
        let result = match polled {
            Some(OpPoll::Done(result, _)) => to_result(result),
            Some(OpPoll::Missing) => Err(io::Error::other("operation state was lost")),
            Some(OpPoll::Pending(previous)) => {
                drop(previous);
                return Poll::Pending;
            }
            None => return Poll::Pending,
        };
        this.key = None;
        match this.data.take() {
            Some(data) => Poll::Ready((result, data)),
            None => Poll::Pending,
        }
    }
}

impl<D: Unpin + 'static> Drop for Op<D> {
    fn drop(&mut self) {
        let Some(data) = self.data.take() else { return };
        // Never submitted: the kernel has no pointer to the data.
        let Some(key) = self.key else { return };
        let Some(shard) = try_current() else {
            // No reactor to hand the buffers to; never free them under the
            // kernel.
            std::mem::forget(data);
            return;
        };
        let mut data: Option<Box<dyn Any>> = Some(Box::new(data));
        let result_is_fd = self.result_is_fd;
        match shard
            .reactor
            .with(|reactor| reactor.abandon(key, &mut data, result_is_fd))
        {
            // The waker is dropped here, after the reactor borrow has ended.
            // Any data the reactor did not take is freed normally.
            Some(waker) => drop(waker),
            // Could not reach the reactor, so the kernel may still be using
            // the buffers: leak them.
            None => std::mem::forget(data),
        }
    }
}

/// An operation whose buffer the reactor owns: the future resolves as soon
/// as the kernel reports the outcome, and the buffer is dropped (on this
/// shard) once the kernel reports that it has stopped using it.
///
/// This is how zero-copy sends are made. The network stack keeps reading
/// the buffer until the peer acknowledges the data, which can take far
/// longer than the send itself.
pub(crate) struct HeldOp {
    key: Option<usize>,
    error: Option<io::Error>,
    _not_send: PhantomData<*const ()>,
}

impl HeldOp {
    /// Submits `sqe` on the current shard, giving `data` to the reactor.
    ///
    /// # Safety
    ///
    /// Same contract as [`Op::new`].
    pub(crate) unsafe fn new<D: 'static>(sqe: squeue::Entry, data: D) -> HeldOp {
        let data: Box<dyn Any> = Box::new(data);
        let submitted = match try_current() {
            Some(shard) => {
                let mut data = Some(data);
                let submitted = shard.reactor.with(|reactor| {
                    data.take().map(|data| reactor.submit_held(sqe, data))
                });
                match submitted.flatten() {
                    Some(Ok(key)) => Ok(key),
                    // The buffer never reached the kernel; it is dropped
                    // here, outside the reactor.
                    Some(Err((err, data))) => {
                        drop(data);
                        Err(err)
                    }
                    None => Err(io::Error::other("reactor is busy")),
                }
            }
            None => Err(io::Error::other(crate::Error::NoRuntime)),
        };
        let (key, error) = match submitted {
            Ok(key) => (Some(key), None),
            Err(err) => (None, Some(err)),
        };
        HeldOp {
            key,
            error,
            _not_send: PhantomData,
        }
    }
}

impl Future for HeldOp {
    type Output = io::Result<u32>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(key) = self.key else {
            return match self.error.take() {
                Some(err) => Poll::Ready(Err(err)),
                None => Poll::Pending,
            };
        };
        let Some(shard) = try_current() else {
            return Poll::Pending;
        };
        let mut waker = Some(cx.waker().clone());
        let polled = shard.reactor.with(|reactor| reactor.poll_op(key, &mut waker));
        let result = match polled {
            Some(OpPoll::Done(result, released)) => {
                drop(released);
                to_result(result)
            }
            Some(OpPoll::Missing) => Err(io::Error::other("operation state was lost")),
            Some(OpPoll::Pending(previous)) => {
                drop(previous);
                return Poll::Pending;
            }
            None => return Poll::Pending,
        };
        self.key = None;
        Poll::Ready(result)
    }
}

impl Drop for HeldOp {
    fn drop(&mut self) {
        let (Some(key), Some(shard)) = (self.key, try_current()) else {
            return;
        };
        // The reactor already owns the buffer; it only needs to know that
        // nobody waits for the result any more.
        let waker = shard
            .reactor
            .with(|reactor| reactor.abandon(key, &mut None, false));
        drop(waker);
    }
}
