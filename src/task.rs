//! Tasks: one heap allocation holding the future, its output, and
//! everything needed to wake it from any thread.
//!
//! # Who may touch what
//!
//! A task is shared by reference count between its shard (which owns and
//! polls it), run queues, wakers (which may be on any thread) and its
//! [`JoinHandle`]. The allocation has two parts with different rules:
//!
//! - The [`Header`]'s reference count, queued flag, shard, key, group and
//!   vtable are atomic or never change. Any thread may read them.
//! - The state, the abort request, the join waker, the panic message, the
//!   future and the output belong to the shard that spawned the task. Only that thread
//!   reads or writes them.
//!
//! A future or output must never be dropped on a foreign thread, since
//! neither has to be `Send`. The shard keeps its own reference for as long
//! as the future exists and destroys the future itself (on completion,
//! panic or shutdown), and a `JoinHandle`, which cannot leave the shard,
//! destroys an output nobody took. So by the time the count can reach zero
//! on another thread, only the header is left.

use std::cell::{Cell, UnsafeCell};
use std::future::Future;
use std::marker::PhantomData;
use std::mem::{ManuallyDrop, MaybeUninit};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::atomic::{fence, AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use crate::error::{Error, Result};
use crate::runtime::{discard_panic, panic_message, try_current, wake_quietly, Message, ShardShared};

/// State of a task whose future has not finished: it may still be polled.
const RUNNING: u8 = 0;
/// State of a task whose future has returned, with its output stored in
/// the task, waiting to be collected by the handle or discarded.
const DONE: u8 = 1;
/// State of a task whose future has returned and whose output is gone:
/// collected by the handle, or discarded because nobody wanted it.
const TAKEN: u8 = 2;
/// State of a task whose future panicked. The future has been destroyed
/// and there is no output.
const PANICKED: u8 = 3;
/// State of a task whose future was destroyed before it finished, by an
/// abort or because the runtime shut down.
const CANCELED: u8 = 4;

/// The part of a task that does not depend on the type of its future.
///
/// It comes first in the allocation, so a pointer to it is a pointer to the
/// whole task, and code that does not know the future's type, such as wakers
/// and run queues, can work with any task through it.
///
/// The reference count, the queued flag, the shard, the key, the group and
/// the table of operations may be read from any thread. Everything else
/// belongs to the shard that spawned the task.
struct Header {
    refs: AtomicUsize,
    /// Set while the task sits in a run queue or a message queue, so that
    /// repeated wakes enqueue it once.
    queued: AtomicBool,
    state: Cell<u8>,
    /// Nobody will ask for the output.
    detached: Cell<bool>,
    /// The future is being polled right now, so it cannot be dropped.
    polling: Cell<bool>,
    /// The task was aborted while it was being polled: the future is to
    /// be dropped as soon as that poll returns.
    abort: Cell<bool>,
    /// The task awaiting this one's `JoinHandle`.
    join_waker: Cell<Option<Waker>>,
    /// What the future's panic said, until the handle collects it.
    panic: Cell<Option<String>>,
    shard: Arc<ShardShared>,
    /// The task's slot in its shard's task table.
    key: usize,
    /// The scheduling group whose run queue the task goes on.
    group: usize,
    vtable: &'static VTable,
}

/// The operations that depend on the future's type, as function pointers:
/// a hand-made virtual table. Each task's header points to the one for its
/// future type, which is how code holding only a header runs the right
/// `poll`, destructors and deallocation.
struct VTable {
    poll: unsafe fn(NonNull<Header>, &mut Context<'_>) -> Poll<()>,
    drop_future: unsafe fn(NonNull<Header>),
    drop_output: unsafe fn(NonNull<Header>),
    dealloc: unsafe fn(NonNull<Header>),
}

/// The allocation: the header, the future and room for the future's
/// output. `repr(C)` puts the header first, so a pointer to the header is a
/// pointer to the task.
///
/// One allocation holds all three, which is why spawning a task costs one
/// allocation and no more.
#[repr(C)]
struct RawTask<F: Future> {
    header: Header,
    /// Live while the state is `RUNNING`.
    future: UnsafeCell<ManuallyDrop<F>>,
    /// Initialised while the state is `DONE`.
    output: UnsafeCell<MaybeUninit<F::Output>>,
}

impl<F: Future> RawTask<F> {
    /// The table of operations for tasks whose future is `F`.
    const VTABLE: VTable = VTable {
        poll: Self::poll,
        drop_future: Self::drop_future,
        drop_output: Self::drop_output,
        dealloc: Self::dealloc,
    };

    /// SAFETY (all of these): `ptr` points to a live `RawTask<F>`. All but
    /// `dealloc` additionally require the owning thread and the state
    /// named in the field documentation.
    unsafe fn poll(ptr: NonNull<Header>, cx: &mut Context<'_>) -> Poll<()> {
        let raw = ptr.cast::<RawTask<F>>().as_ptr();
        // The future never moves: it lives in the heap allocation until it
        // is dropped in place.
        let future = Pin::new_unchecked(&mut **(*raw).future.get());
        match future.poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(output) => {
                (*raw).output.get().cast::<F::Output>().write(output);
                // Recorded before the future is destroyed: if its `Drop`
                // panics, the task is finished all the same.
                (*raw).header.state.set(DONE);
                Self::drop_future(ptr);
                Poll::Ready(())
            }
        }
    }

    /// Destroys the future in place, catching a panic from its destructor.
    ///
    /// Called exactly once per task, when the future completes, panics or is
    /// cancelled, always on the owning shard. After it, the task holds no value
    /// of a type that is not `Send`, apart from a possible output.
    unsafe fn drop_future(ptr: NonNull<Header>) {
        let raw = ptr.cast::<RawTask<F>>().as_ptr();
        let dropped = catch_unwind(AssertUnwindSafe(|| {
            ManuallyDrop::drop(&mut *(*raw).future.get());
        }));
        if let Err(payload) = dropped {
            discard_panic(payload);
        }
    }

    /// Destroys an output that nobody collected. The state must say the output
    /// is there.
    unsafe fn drop_output(ptr: NonNull<Header>) {
        let raw = ptr.cast::<RawTask<F>>().as_ptr();
        (*raw).output.get().cast::<F::Output>().drop_in_place();
    }

    /// Moves the output out of the task, for the join handle. The state must
    /// say the output is there, and the caller marks it as taken.
    unsafe fn take_output(ptr: NonNull<Header>) -> F::Output {
        let raw = ptr.cast::<RawTask<F>>().as_ptr();
        (*raw).output.get().cast::<F::Output>().read()
    }

    /// Frees the task's memory, once the last reference is gone. By then the
    /// future and the output have been destroyed, so only the header's own
    /// fields are left to drop.
    unsafe fn dealloc(ptr: NonNull<Header>) {
        // Frees the header's own fields; `future` and `output` are inert
        // wrappers whose contents were destroyed by the owning shard.
        drop(Box::from_raw(ptr.cast::<RawTask<F>>().as_ptr()));
    }
}

/// A counted reference to a task.
///
/// The shard's task table, its run queue, the join handle and every waker
/// each hold one. The task is freed when the last is dropped, which may
/// happen on any thread.
pub(crate) struct Task(NonNull<Header>);

// SAFETY: see the module documentation. Through a `Task`, foreign threads
// only reach the atomic and immutable header fields; everything else is
// behind methods that are `unsafe` or tied to the non-`Send` `JoinHandle`.
unsafe impl Send for Task {}
// SAFETY: as above.
unsafe impl Sync for Task {}

impl Clone for Task {
    /// Another reference to the same task.
    fn clone(&self) -> Task {
        acquire(self.header());
        Task(self.0)
    }
}

impl Drop for Task {
    /// Lets go of one reference, and frees the task if it was the last.
    fn drop(&mut self) {
        if self.header().refs.fetch_sub(1, Ordering::Release) == 1 {
            fence(Ordering::Acquire);
            let dealloc = self.header().vtable.dealloc;
            // SAFETY: this was the last reference.
            unsafe { dealloc(self.0) };
        }
    }
}

/// Adds one to a task's reference count.
///
/// A count large enough to overflow could only come from leaking references
/// in a loop, and overflowing it would free a task still in use, so the
/// process is aborted first, as `Arc` does.
fn acquire(header: &Header) {
    // A count this high can only come from leaking references in a loop.
    // Overflowing it would free a task that is still in use, so stop the
    // process instead, as `Arc` does.
    if header.refs.fetch_add(1, Ordering::Relaxed) > isize::MAX as usize {
        std::process::abort();
    }
}

impl Task {
    /// Allocates a task for `future`, in the state of having just been put on
    /// a run queue.
    ///
    /// `key` is its place in the shard's task table, `group` the scheduling group
    /// it runs in, and `detached` says that nobody will ask for its output, so
    /// the output is to be dropped as soon as it exists.
    pub(crate) fn new<F: Future + 'static>(
        shard: Arc<ShardShared>,
        key: usize,
        group: usize,
        future: F,
        detached: bool,
    ) -> Task {
        let raw = Box::new(RawTask {
            header: Header {
                refs: AtomicUsize::new(1),
                queued: AtomicBool::new(true),
                state: Cell::new(RUNNING),
                detached: Cell::new(detached),
                polling: Cell::new(false),
                abort: Cell::new(false),
                join_waker: Cell::new(None),
                panic: Cell::new(None),
                shard,
                key,
                group,
                vtable: &RawTask::<F>::VTABLE,
            },
            future: UnsafeCell::new(ManuallyDrop::new(future)),
            output: UnsafeCell::new(MaybeUninit::uninit()),
        });
        Task(Box::into_non_null(raw).cast())
    }

    /// The task's header. The reference this `Task` holds keeps it alive.
    fn header(&self) -> &Header {
        // SAFETY: the reference this `Task` holds keeps the allocation
        // alive.
        unsafe { self.0.as_ref() }
    }

    /// The task's place in its shard's task table.
    pub(crate) fn key(&self) -> usize {
        self.header().key
    }

    /// The scheduling group whose run queue the task goes on.
    pub(crate) fn group(&self) -> usize {
        self.header().group
    }

    /// Whether two references are to the same task. Used to tell a task from a
    /// later one that was given the same place in the table.
    pub(crate) fn same(&self, other: &Task) -> bool {
        self.0 == other.0
    }

    /// Polls the task once. Returns whether it is finished, in which case
    /// the shard should let go of it.
    ///
    /// # Safety
    ///
    /// Must be called on the thread of the shard that spawned the task,
    /// and not from inside the task itself.
    pub(crate) unsafe fn run(&self) -> bool {
        let header = self.header();
        header.queued.store(false, Ordering::Release);
        if header.state.get() != RUNNING {
            return true;
        }
        // A waker that borrows this reference: no count to adjust unless
        // the future clones it.
        let waker = ManuallyDrop::new(Waker::from_raw(RawWaker::new(
            self.0.as_ptr().cast(),
            &WAKER_VTABLE,
        )));
        let mut cx = Context::from_waker(&waker);
        // No shard state is borrowed here: the task is free to spawn,
        // submit I/O and arm timers. A panic stops at this boundary.
        header.polling.set(true);
        let polled = catch_unwind(AssertUnwindSafe(|| (header.vtable.poll)(self.0, &mut cx)));
        header.polling.set(false);
        match polled {
            Ok(Poll::Pending) => {
                if !header.abort.get() {
                    return false;
                }
                // Aborted from inside its own poll.
                self.cancel();
                return true;
            }
            Ok(Poll::Ready(())) => {
                if header.detached.get() {
                    self.discard_output();
                }
            }
            Err(payload) => {
                header.panic.set(panic_message(&*payload));
                discard_panic(payload);
                if header.state.get() == RUNNING {
                    header.state.set(PANICKED);
                    (header.vtable.drop_future)(self.0);
                }
            }
        }
        self.wake_joiner();
        true
    }

    /// Destroys the future if it has not finished, as happens to an aborted
    /// task and to every remaining task when the runtime shuts down.
    ///
    /// # Safety
    ///
    /// Must be called on the thread of the shard that spawned the task,
    /// and not while the task is being polled.
    pub(crate) unsafe fn cancel(&self) {
        let header = self.header();
        if header.state.get() == RUNNING {
            header.state.set(CANCELED);
            (header.vtable.drop_future)(self.0);
            self.wake_joiner();
        }
    }

    /// Drops the task's output if it is there, catching a panic from its
    /// destructor. Used for tasks whose handle was dropped before they
    /// finished.
    unsafe fn discard_output(&self) {
        let header = self.header();
        if header.state.get() == DONE {
            header.state.set(TAKEN);
            let dropped = catch_unwind(AssertUnwindSafe(|| (header.vtable.drop_output)(self.0)));
            if let Err(payload) = dropped {
                discard_panic(payload);
            }
        }
    }

    /// Wakes the task that awaits this one's handle, if there is one.
    fn wake_joiner(&self) {
        if let Some(waker) = self.header().join_waker.take() {
            wake_quietly(waker);
        }
    }

    /// Makes the task runnable: onto its shard's run queue if called there,
    /// otherwise sent to that shard as a message.
    ///
    /// A task that is already queued is left alone, so any number of wake-ups
    /// before it next runs cost one run.
    fn schedule(self) {
        if self.header().queued.swap(true, Ordering::AcqRel) {
            return;
        }
        let mut task = Some(self);
        let here = try_current();
        if let Some(shard) = &here {
            let at_home = task.as_ref().is_some_and(|task| Arc::ptr_eq(&shard.shared, &task.header().shard));
            if at_home {
                // Woken on its own shard, which is nearly always: straight
                // onto the run queue.
                shard.run_queue.with(|queue| {
                    if let Some(task) = task.take() {
                        queue.push(task);
                    }
                });
            }
        }
        let Some(task) = task else { return };
        // Only a wake from elsewhere needs its own reference to the shard.
        let home = task.header().shard.clone();
        match here {
            // From another shard of the same runtime: over the queue those
            // two shards share.
            Some(shard)
                if shard
                    .runtime
                    .shards
                    .get(home.id)
                    .is_some_and(|known| Arc::ptr_eq(known, &home)) =>
            {
                shard.send_to(home.id, Message::Wake(task));
            }
            // From a thread outside the runtime.
            _ => home.send_alien(Message::Wake(task)),
        }
    }
}

// A waker is a `Task` reference in disguise: its data pointer is the
// header pointer, and it owns one count.
/// The operations of a task's waker. A waker is a reference to the task in
/// another form: its data pointer is the header pointer, and it owns one
/// count of the reference count.
static WAKER_VTABLE: RawWakerVTable =
    RawWakerVTable::new(waker_clone, waker_wake, waker_wake_by_ref, waker_drop);

/// Clones a waker: one more reference to the task.
unsafe fn waker_clone(data: *const ()) -> RawWaker {
    acquire(&*data.cast::<Header>());
    RawWaker::new(data, &WAKER_VTABLE)
}

/// Wakes by value: makes the task runnable, using up the waker's
/// reference.
unsafe fn waker_wake(data: *const ()) {
    Task(NonNull::new_unchecked(data.cast_mut().cast())).schedule();
}

/// Wakes by reference: makes the task runnable and leaves the waker
/// usable.
unsafe fn waker_wake_by_ref(data: *const ()) {
    acquire(&*data.cast::<Header>());
    waker_wake(data);
}

/// Drops a waker: one reference fewer.
unsafe fn waker_drop(data: *const ()) {
    drop(Task(NonNull::new_unchecked(data.cast_mut().cast())));
}

/// What a [`JoinHandle`] refers to: a task, with the function that takes
/// its output out, or nothing, because the task was never started.
enum Joined<T> {
    Task {
        task: Task,
        take: unsafe fn(NonNull<Header>) -> T,
    },
    /// The task was never started.
    Failed(Option<Error>),
}

/// Awaits the outcome of a spawned task. Dropping it detaches the task: the
/// task keeps running and its output is discarded. To stop the task, call
/// [`abort`](Self::abort).
///
/// Resolves to [`Error::Panicked`] if the task panicked, [`Error::Canceled`]
/// if it was aborted or otherwise dropped before finishing, and
/// [`Error::NoRuntime`] if it was spawned outside a runtime.
pub struct JoinHandle<T> {
    inner: Joined<T>,
    /// The output is read on the shard that produced it.
    _not_send: PhantomData<*const T>,
}

impl<T> Unpin for JoinHandle<T> {}

impl<T> JoinHandle<T> {
    /// A handle for a task that could not be started; it resolves to `error`
    /// on its first poll.
    fn failed(error: Error) -> JoinHandle<T> {
        JoinHandle {
            inner: Joined::Failed(Some(error)),
            _not_send: PhantomData,
        }
    }

    /// Cancels the task. Its future is dropped before this call returns,
    /// which cancels any I/O it has in flight and releases whatever it
    /// holds: permits, gate guards, buffers, sockets. The handle then
    /// resolves to [`Error::Canceled`].
    ///
    /// The one exception is a task that aborts itself, from inside its own
    /// future. That future is dropped when it next yields, and never
    /// resumes after that.
    ///
    /// A task that has already finished is not affected and its output can
    /// still be collected.
    pub fn abort(&self) {
        let Joined::Task { task, .. } = &self.inner else { return };
        let header = task.header();
        if header.state.get() != RUNNING {
            return;
        }
        if header.polling.get() {
            header.abort.set(true);
            return;
        }
        // SAFETY: the handle cannot leave the shard that spawned the task,
        // and the task is not being polled.
        unsafe { task.cancel() };
        // Its shard still lists the task; a last turn makes it let go.
        task.clone().schedule();
    }

    /// Whether the task has ended: its future returned, panicked or was
    /// dropped. Awaiting the handle then completes without waiting.
    ///
    /// A snapshot. A task that has not finished may do so at any await of the
    /// caller's.
    pub fn is_finished(&self) -> bool {
        match &self.inner {
            Joined::Task { task, .. } => task.header().state.get() != RUNNING,
            Joined::Failed(_) => true,
        }
    }
}

impl<T> Future for JoinHandle<T> {
    /// The task's output, or why there is none.
    type Output = Result<T>;

    /// Completes with the task's outcome once it has ended; until then, leaves
    /// this task's waker to be called when it does.
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<T>> {
        let (task, take) = match &mut self.inner {
            Joined::Task { task, take } => (&*task, *take),
            Joined::Failed(error) => {
                return Poll::Ready(Err(error.take().unwrap_or(Error::Canceled)));
            }
        };
        let header = task.header();
        match header.state.get() {
            RUNNING => {
                let previous = header.join_waker.replace(Some(cx.waker().clone()));
                drop(previous);
                Poll::Pending
            }
            DONE => {
                header.state.set(TAKEN);
                // SAFETY: the handle is on the owning thread (it is not
                // `Send`), the state said the output is there, and `take`
                // was made for this task's future type.
                Poll::Ready(Ok(unsafe { take(task.0) }))
            }
            PANICKED => Poll::Ready(Err(Error::Panicked {
                message: header.panic.take(),
            })),
            _ => Poll::Ready(Err(Error::Canceled)),
        }
    }
}

impl<T> Drop for JoinHandle<T> {
    /// Detaches the task: it keeps running, and its output is dropped when it
    /// arrives, or now if it is already there.
    fn drop(&mut self) {
        let Joined::Task { task, .. } = &self.inner else { return };
        let header = task.header();
        header.detached.set(true);
        drop(header.join_waker.take());
        if header.state.get() == DONE {
            header.state.set(TAKEN);
            // SAFETY: owning thread, and the state said the output is
            // there. A panic from its `Drop` belongs to whoever dropped
            // the handle.
            unsafe { (header.vtable.drop_output)(task.0) };
        }
    }
}

/// Spawns a task on the current shard, in the caller's scheduling group.
/// The task never leaves this shard, so the future does not need to be
/// `Send`.
///
/// A panic inside the task is contained: the task ends and its handle
/// resolves to [`Error::Panicked`].
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + 'static,
    F::Output: 'static,
{
    spawn_in(None, future)
}

/// Spawns into `group`, or into the caller's group if `None`.
///
/// The common path of [`spawn`] and
/// [`SchedulingGroup::spawn`](crate::SchedulingGroup::spawn). Outside a
/// runtime, or for a group that does not exist, the future is dropped and
/// the handle resolves to the error.
pub(crate) fn spawn_in<F>(group: Option<usize>, future: F) -> JoinHandle<F::Output>
where
    F: Future + 'static,
    F::Output: 'static,
{
    let Some(shard) = try_current() else {
        return JoinHandle::failed(Error::NoRuntime);
    };
    let group = match group {
        None => shard.current_group.get(),
        Some(group)
            if shard
                .run_queue
                .with(|queue| queue.group_exists(group))
                .unwrap_or(false) =>
        {
            group
        }
        Some(_) => return JoinHandle::failed(Error::InvalidGroup),
    };
    match shard.spawn_task(group, future, false) {
        Some(task) => JoinHandle {
            inner: Joined::Task {
                task,
                take: RawTask::<F>::take_output,
            },
            _not_send: PhantomData,
        },
        None => JoinHandle::failed(Error::Canceled),
    }
}

/// Lets the other runnable tasks, I/O completions and timers of this shard
/// run before continuing.
///
/// The task goes to the back of its scheduling group's queue and is run
/// again when its turn comes. Long computations should give way like this
/// now and then, or every other task on the shard waits for them. In a loop,
/// prefer [`maybe_yield`](crate::maybe_yield), which yields only when the
/// time slice is used up and otherwise costs two memory reads.
///
/// ```no_run
/// # async fn example(items: Vec<u64>) -> u64 {
/// let mut total = 0;
/// for chunk in items.chunks(1024) {
///     total += chunk.iter().sum::<u64>();
///     starfish::yield_now().await;
/// }
/// total
/// # }
/// ```
pub fn yield_now() -> impl Future<Output = ()> {
    /// The future behind [`yield_now`]: pending the first time it is polled,
    /// ready the second. The flag records whether it has yielded yet.
    struct YieldNow(bool);

    impl Future for YieldNow {
        /// Nothing: the point is the pause.
        type Output = ();

        /// The first time, wakes its own task and reports pending, which sends the
        /// task round the run queue once; after that, completes.
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.0 {
                return Poll::Ready(());
            }
            self.0 = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }

    YieldNow(false)
}

debug_impl!(<T> JoinHandle<T>, "finished" => |handle| handle.is_finished());

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic, clippy::arithmetic_side_effects)]
mod tests {
    //! These drive tasks by hand, without a runtime, so that Miri can run
    //! them. Outside a shard every wake goes to the shard's inbox for
    //! foreign threads, which the tests read directly.

    use super::*;
    use std::future::poll_fn;
    use std::rc::Rc;
    use std::sync::mpsc::Receiver;

    /// Counts how many times it has been dropped.
    struct Tracked(Rc<Cell<u32>>);

    impl Drop for Tracked {
        fn drop(&mut self) {
            self.0.set(self.0.get() + 1);
        }
    }

    fn tracked() -> (Tracked, Rc<Cell<u32>>) {
        let drops = Rc::new(Cell::new(0));
        (Tracked(drops.clone()), drops)
    }

    /// Spawns as `spawn_in` does, minus the shard.
    fn start<F: Future + 'static>(
        shard: &Arc<ShardShared>,
        future: F,
    ) -> (Task, JoinHandle<F::Output>) {
        let task = Task::new(shard.clone(), 0, 0, future, false);
        let handle = JoinHandle {
            inner: Joined::Task {
                task: task.clone(),
                take: RawTask::<F>::take_output,
            },
            _not_send: PhantomData,
        };
        (task, handle)
    }

    /// Runs every task that has been woken; returns how many.
    fn run_woken(inbox: &Receiver<Message>) -> usize {
        let mut ran = 0;
        while let Ok(message) = inbox.try_recv() {
            if let Message::Wake(task) = message {
                unsafe { task.run() };
                ran += 1;
            }
        }
        ran
    }

    fn poll_once<T>(handle: &mut JoinHandle<T>) -> Poll<Result<T>> {
        let mut cx = Context::from_waker(Waker::noop());
        Pin::new(handle).poll(&mut cx)
    }

    /// Pending until `gate` is set; hands its waker to `slot`.
    fn gated(gate: Rc<Cell<bool>>, slot: Rc<Cell<Option<Waker>>>) -> impl Future<Output = ()> {
        poll_fn(move |cx| {
            if gate.get() {
                Poll::Ready(())
            } else {
                slot.set(Some(cx.waker().clone()));
                Poll::Pending
            }
        })
    }

    #[test]
    fn output_reaches_the_handle_and_is_dropped_once() {
        let (shard, inbox) = ShardShared::for_test();
        let (captured, captured_drops) = tracked();
        let (output, output_drops) = tracked();
        let (task, mut handle) = start(&shard, async move {
            let _captured = captured;
            output
        });
        assert!(poll_once(&mut handle).is_pending());
        assert!(unsafe { task.run() });
        // The future is gone as soon as it finishes; the output waits.
        assert_eq!((captured_drops.get(), output_drops.get()), (1, 0));
        let Poll::Ready(Ok(value)) = poll_once(&mut handle) else { panic!("not ready") };
        assert_eq!(output_drops.get(), 0);
        drop(value);
        assert_eq!(output_drops.get(), 1);
        drop(handle);
        drop(task);
        assert_eq!((captured_drops.get(), output_drops.get()), (1, 1));
        assert_eq!(run_woken(&inbox), 0);
    }

    #[test]
    fn an_unclaimed_output_is_dropped_with_the_handle_or_at_once_if_detached() {
        let (shard, _inbox) = ShardShared::for_test();

        let (output, drops) = tracked();
        let (task, handle) = start(&shard, async move { output });
        assert!(unsafe { task.run() });
        assert_eq!(drops.get(), 0);
        drop(handle);
        assert_eq!(drops.get(), 1);
        drop(task);
        assert_eq!(drops.get(), 1);

        let (output, drops) = tracked();
        let (task, handle) = start(&shard, async move { output });
        drop(handle);
        assert!(unsafe { task.run() });
        assert_eq!(drops.get(), 1);
        drop(task);
        assert_eq!(drops.get(), 1);
    }

    #[test]
    fn a_panic_is_contained_and_reported() {
        let (shard, _inbox) = ShardShared::for_test();
        let (captured, drops) = tracked();
        let (task, mut handle) = start(&shard, async move {
            let _captured = captured;
            if always() {
                panic!("boom in a unit test");
            }
            5
        });
        assert!(unsafe { task.run() });
        assert_eq!(drops.get(), 1);
        assert!(matches!(poll_once(&mut handle), Poll::Ready(Err(Error::Panicked { .. }))));
        // Running it again is harmless.
        assert!(unsafe { task.run() });
        assert_eq!(drops.get(), 1);
    }

    fn always() -> bool {
        true
    }

    #[test]
    fn cancelling_drops_the_future_and_wakes_the_joiner() {
        let (shard, inbox) = ShardShared::for_test();
        let (captured, drops) = tracked();
        let gate = Rc::new(Cell::new(false));
        let slot = Rc::new(Cell::new(None));
        let (task, handle) = start(&shard, {
            let wait = gated(gate.clone(), slot.clone());
            async move {
                let _captured = captured;
                wait.await;
            }
        });
        assert!(!unsafe { task.run() });
        assert_eq!(drops.get(), 0);

        // A second task awaits the first one's handle.
        let (joiner, mut joined) = start(&shard, handle);
        assert!(!unsafe { joiner.run() });
        unsafe { task.cancel() };
        assert_eq!(drops.get(), 1);
        drop(slot.take());
        assert_eq!(run_woken(&inbox), 1, "the joiner was woken");
        assert!(matches!(poll_once(&mut joined), Poll::Ready(Ok(Err(Error::Canceled)))));
        unsafe { task.cancel() };
        assert_eq!(drops.get(), 1);
    }

    #[test]
    fn aborting_drops_the_future_at_once() {
        let (shard, inbox) = ShardShared::for_test();
        let (captured, drops) = tracked();
        let gate = Rc::new(Cell::new(false));
        let slot = Rc::new(Cell::new(None));
        let (task, mut handle) = start(&shard, {
            let wait = gated(gate.clone(), slot.clone());
            async move {
                let _captured = captured;
                wait.await;
                7
            }
        });
        assert!(!unsafe { task.run() });
        assert!(!handle.is_finished());

        handle.abort();
        assert_eq!(drops.get(), 1);
        assert!(handle.is_finished());
        handle.abort();
        assert!(matches!(poll_once(&mut handle), Poll::Ready(Err(Error::Canceled))));
        // One more turn, for the shard to let go of the task.
        assert_eq!(run_woken(&inbox), 1);
        assert_eq!(drops.get(), 1);
        drop(slot.take());

        // A task that has finished keeps its output.
        let (task, mut handle) = start(&shard, async { 7 });
        assert!(unsafe { task.run() });
        handle.abort();
        assert_eq!(run_woken(&inbox), 0);
        assert!(matches!(poll_once(&mut handle), Poll::Ready(Ok(7))));
    }

    #[test]
    fn a_task_that_aborts_itself_is_dropped_when_it_yields() {
        let (shard, inbox) = ShardShared::for_test();
        let (captured, drops) = tracked();
        let own: Rc<Cell<Option<JoinHandle<()>>>> = Rc::new(Cell::new(None));
        let inner = own.clone();
        let inner_drops = drops.clone();
        let (task, handle) = start(&shard, async move {
            let _captured = captured;
            // Aborted from inside its own poll, then the handle is dropped.
            // The future cannot go while it runs; it goes when it yields.
            if let Some(handle) = inner.take() {
                handle.abort();
            }
            assert_eq!(inner_drops.get(), 0);
            std::future::pending::<()>().await;
        });
        own.set(Some(handle));
        assert!(unsafe { task.run() });
        assert_eq!(drops.get(), 1);
        assert_eq!(run_woken(&inbox), 0);
    }

    #[test]
    fn wakes_are_coalesced_and_survive_the_task() {
        let (shard, inbox) = ShardShared::for_test();
        let gate = Rc::new(Cell::new(false));
        let slot = Rc::new(Cell::new(None));
        let (task, mut handle) = start(&shard, gated(gate.clone(), slot.clone()));
        assert!(!unsafe { task.run() });
        let waker: Waker = slot.take().unwrap();

        // Many wakes, one run.
        waker.wake_by_ref();
        waker.wake_by_ref();
        let by_value = waker.clone();
        by_value.wake();
        assert_eq!(run_woken(&inbox), 1);
        drop(slot.take());

        gate.set(true);
        waker.wake_by_ref();
        assert_eq!(run_woken(&inbox), 1);
        assert!(matches!(poll_once(&mut handle), Poll::Ready(Ok(()))));

        // A waker that outlives the task is still safe to use and to drop
        // on another thread, where it may be the last reference.
        drop(handle);
        drop(task);
        waker.wake_by_ref();
        assert_eq!(run_woken(&inbox), 1);
        std::thread::spawn(move || {
            waker.wake_by_ref();
            drop(waker);
        })
        .join()
        .unwrap();
        assert_eq!(run_woken(&inbox), 1);
    }

    #[test]
    fn a_task_may_await_its_own_handle() {
        let (shard, inbox) = ShardShared::for_test();
        let own: Rc<Cell<Option<JoinHandle<()>>>> = Rc::new(Cell::new(None));
        let inner = own.clone();
        let (task, handle) = start(&shard, async move {
            if let Some(handle) = inner.take() {
                // Never resolves; must not misbehave either.
                let _ = handle.await;
            }
        });
        own.set(Some(handle));
        assert!(!unsafe { task.run() });
        unsafe { task.cancel() };
        assert_eq!(run_woken(&inbox), 0);
    }
}
