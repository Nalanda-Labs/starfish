//! A bounded queue from one shard to another.
//!
//! The values travel through the same lock-free ring that carries the
//! runtime's own messages between shards. What this adds is waiting: a
//! sender that finds the ring full, or a receiver that finds it empty,
//! leaves its waker for the other end and is woken when there is room or a
//! value.
//!
//! # Not missing a wake-up
//!
//! The end that is about to wait announces it, then looks at the ring once
//! more. The other end changes the ring, then looks for an announcement.
//! Each side writes one thing and then reads the other, with a full memory
//! barrier in between, so at least one of them sees what the other wrote:
//! either the waiter finds the change on its second look, or the other end
//! finds the announcement and wakes it.

use std::collections::VecDeque;
use std::future::poll_fn;
use std::sync::atomic::{fence, AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll, Waker};

use crate::runtime::wake_quietly;
use crate::spsc::{self, Consumer, Producer};
use crate::sync::{SendError, TrySendError};

/// One end's request to be woken by the other.
///
/// It is a flag saying "I am waiting" and a slot holding the waker to call.
/// The flag is what the other end checks after every change it makes, and it
/// is a single atomic read when nobody waits, so the common case, where
/// neither end ever waits, costs almost nothing.
#[derive(Default)]
struct Parked {
    waiting: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

impl Parked {
    /// Announces that this end is about to wait. The caller must look at
    /// the ring again afterwards, and wait only if nothing has changed.
    fn park(&self, waker: &Waker) {
        let waker = waker.clone();
        let replaced = self
            .waker
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .replace(waker);
        drop(replaced);
        self.waiting.store(true, Ordering::SeqCst);
        fence(Ordering::SeqCst);
    }

    /// Wakes the other end if it has announced that it waits. Called after
    /// every change to the ring that the other end could be waiting for.
    fn unpark(&self) {
        fence(Ordering::SeqCst);
        if self.waiting.swap(false, Ordering::SeqCst) {
            let waker = self.waker.lock().unwrap_or_else(PoisonError::into_inner).take();
            if let Some(waker) = waker {
                wake_quietly(waker);
            }
        }
    }
}

/// What the two ends of a [`shared_spsc`] have in common besides the
/// queue itself: each end's request to be woken, and whether each end still
/// exists.
#[derive(Default)]
struct Shared {
    receiver: Parked,
    sender: Parked,
    sender_gone: AtomicBool,
    receiver_gone: AtomicBool,
}

/// Creates a queue that carries values from one shard to another, holding
/// at most `capacity` of them (rounded up to a power of two, at least 2).
///
/// Create it anywhere and move each end to the shard that will use it, for
/// instance inside a [`submit_to`](crate::submit_to) closure. Each end is
/// used by one task at a time. When the queue is full,
/// [`send`](SharedSpscSender::send) waits, so a shard that produces faster
/// than another consumes is slowed down to its pace instead of filling
/// memory.
///
/// For a stream of values this is much cheaper than one
/// [`submit_to`](crate::submit_to) per value: a value costs no allocation
/// and no task, and the other shard is only woken when it is waiting.
///
/// ```no_run
/// use starfish::submit_to;
/// use starfish::sync::shared_spsc;
///
/// # async fn example() -> starfish::Result<()> {
/// let (mut sender, mut receiver) = shared_spsc(64);
/// let producer = starfish::spawn(submit_to(1, move || async move {
///     for n in 0..10_000u64 {
///         if sender.send(n).await.is_err() {
///             break; // the receiver is gone
///         }
///     }
/// }));
/// let mut sum = 0;
/// // Ends when the sender has been dropped.
/// while let Some(n) = receiver.recv().await {
///     sum += n;
/// }
/// producer.await??;
/// # Ok(())
/// # }
/// ```
pub fn shared_spsc<T: Send>(capacity: usize) -> (SharedSpscSender<T>, SharedSpscReceiver<T>) {
    let (producer, consumer) = spsc::channel(capacity);
    let shared = Arc::new(Shared::default());
    let sender = SharedSpscSender {
        queue: producer,
        shared: shared.clone(),
    };
    let receiver = SharedSpscReceiver {
        queue: consumer,
        shared,
    };
    (sender, receiver)
}

/// The sending end of a [`shared_spsc`].
///
/// It can be moved to another shard, and is used by one task at a time; the
/// methods that send take `&mut self`. Dropping it tells the receiver that no
/// more values will come, once it has taken those already queued.
pub struct SharedSpscSender<T> {
    queue: Producer<T>,
    shared: Arc<Shared>,
}

impl<T> SharedSpscSender<T> {
    /// Queues `value`, waiting for room if the queue is full.
    ///
    /// Dropping the future before it completes drops the value without
    /// sending it.
    ///
    /// # Errors
    ///
    /// Hands the value back if the receiver has been dropped, before or
    /// during the wait. A value that was queued just as the receiver went
    /// away is dropped unseen.
    pub async fn send(&mut self, value: T) -> std::result::Result<(), SendError<T>> {
        let mut value = Some(value);
        poll_fn(|cx| {
            // Nothing left to send: the future already completed.
            let Some(unsent) = value.take() else { return Poll::Pending };
            match self.poll_send(unsent, cx) {
                Ok(()) => Poll::Ready(Ok(())),
                Err(TrySendError::Closed(unsent)) => Poll::Ready(Err(SendError(unsent))),
                Err(TrySendError::Full(unsent)) => {
                    value = Some(unsent);
                    Poll::Pending
                }
            }
        })
        .await
    }

    /// Queues `value` if there is room right now.
    ///
    /// # Errors
    ///
    /// Hands the value back as [`TrySendError::Full`] if the queue is
    /// full, and as [`TrySendError::Closed`] if the receiver has been
    /// dropped.
    pub fn try_send(&mut self, value: T) -> std::result::Result<(), TrySendError<T>> {
        if self.shared.receiver_gone.load(Ordering::SeqCst) {
            return Err(TrySendError::Closed(value));
        }
        match self.queue.push(value) {
            Ok(()) => {
                self.shared.receiver.unpark();
                Ok(())
            }
            Err(value) => Err(TrySendError::Full(value)),
        }
    }

    /// Whether the receiver has been dropped, in which case nothing sent from
    /// now on can arrive. A producer doing expensive work can check this and
    /// stop.
    pub fn is_closed(&self) -> bool {
        self.shared.receiver_gone.load(Ordering::SeqCst)
    }

    /// One attempt to send, for [`send`](Self::send): tries, and if the queue
    /// is full, announces that it waits and tries once more.
    ///
    /// The second try is what makes the wait safe. Without it, the receiver could
    /// make room between the first try and the announcement, see nobody waiting,
    /// and never wake this end.
    fn poll_send(&mut self, value: T, cx: &mut Context<'_>) -> std::result::Result<(), TrySendError<T>> {
        match self.try_send(value) {
            Err(TrySendError::Full(value)) => {
                self.shared.sender.park(cx.waker());
                // The receiver may have made room, or left, in between.
                self.try_send(value)
            }
            sent_or_closed => sent_or_closed,
        }
    }
}

impl<T> Drop for SharedSpscSender<T> {
    /// Marks the sender gone and wakes the receiver, so that a receiver
    /// waiting on an empty queue learns that nothing more will come.
    fn drop(&mut self) {
        self.shared.sender_gone.store(true, Ordering::SeqCst);
        self.shared.receiver.unpark();
    }
}

/// The receiving end of a [`shared_spsc`].
///
/// It can be moved to another shard, and is used by one task at a time.
/// Dropping it makes every later send fail, and wakes a sender that is
/// waiting for room. Values still queued at that point are dropped with the
/// queue.
pub struct SharedSpscReceiver<T> {
    queue: Consumer<T>,
    shared: Arc<Shared>,
}

impl<T> SharedSpscReceiver<T> {
    /// Takes the next value, waiting for one if the queue is empty.
    /// `None` means no value will ever come: the queue is empty and the
    /// sender has been dropped.
    ///
    /// Dropping the future before it completes loses nothing.
    pub async fn recv(&mut self) -> Option<T> {
        poll_fn(|cx| self.poll_recv(cx)).await
    }

    /// Takes the next value if one is queued. `None` only says that none is
    /// queued right now, not that none will come; [`recv`](Self::recv) tells the
    /// two apart.
    pub fn try_recv(&mut self) -> Option<T> {
        let value = self.queue.pop()?;
        self.shared.sender.unpark();
        Some(value)
    }

    /// One attempt to receive, for [`recv`](Self::recv): looks, and if the
    /// queue is empty, announces that it waits and looks once more.
    ///
    /// Whether the sender is gone is read between the announcement and the second
    /// look, in that order, so that a value queued just before the sender left is
    /// still found rather than the stream being reported as ended.
    fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        if let Some(value) = self.try_recv() {
            return Poll::Ready(Some(value));
        }
        self.shared.receiver.park(cx.waker());
        // Read before the second look, so that what the sender queued just
        // before it left is still found.
        let sender_gone = self.shared.sender_gone.load(Ordering::SeqCst);
        match self.try_recv() {
            Some(value) => Poll::Ready(Some(value)),
            None if sender_gone => Poll::Ready(None),
            None => Poll::Pending,
        }
    }
}

impl<T> Drop for SharedSpscReceiver<T> {
    /// Marks the receiver gone and wakes the sender, so that a sender waiting
    /// for room learns that there will never be any.
    fn drop(&mut self) {
        self.shared.receiver_gone.store(true, Ordering::SeqCst);
        self.shared.sender.unpark();
    }
}

/// One sender's own queue towards the receiver, as the sender sees it: its
/// request to be woken when there is room, and whether it still exists.
#[derive(Default)]
struct LaneShared {
    /// The sender, waiting for room in its queue.
    sender: Parked,
    sender_gone: AtomicBool,
}

/// The receiver's end of one sender's queue: the queue to take from, and
/// the shared state through which that sender is woken.
struct Lane<T> {
    queue: Consumer<T>,
    shared: Arc<LaneShared>,
}

/// What all the senders and the receiver of a [`shared_mpsc`] have in
/// common: the receiver's request to be woken, whether it still exists, how
/// many senders exist, and the queues of senders made since the receiver
/// last looked.
struct Hub<T> {
    receiver: Parked,
    receiver_gone: AtomicBool,
    /// Senders alive.
    senders: std::sync::atomic::AtomicUsize,
    /// Queues of senders created since the receiver last looked. Locked
    /// only when a sender is cloned and when the receiver collects them.
    new_lanes: Mutex<Vec<Lane<T>>>,
    has_new_lanes: AtomicBool,
    capacity: usize,
}

impl<T> Hub<T> {
    /// Gives a new sender a queue of its own and leaves the receiving end of
    /// that queue where the receiver will find it.
    ///
    /// Making a sender is the only operation that takes the lock. Sending and
    /// receiving never do.
    fn add_sender(self: &Arc<Self>) -> SharedMpscSender<T> {
        let (producer, consumer) = spsc::channel(self.capacity);
        let shared = Arc::new(LaneShared::default());
        let lane = Lane {
            queue: consumer,
            shared: shared.clone(),
        };
        self.new_lanes.lock().unwrap_or_else(PoisonError::into_inner).push(lane);
        self.has_new_lanes.store(true, Ordering::SeqCst);
        SharedMpscSender {
            queue: producer,
            lane: shared,
            hub: self.clone(),
        }
    }
}

/// Creates a queue from several shards to one: any number of senders, each
/// on its own shard or task, and one receiver.
///
/// Every sender has a queue of its own to the receiver, holding at most
/// `capacity` values (rounded up to a power of two, at least 2), so
/// senders never contend with each other. A sender whose queue is full
/// waits, which slows that producer down without affecting the others.
/// [Cloning](SharedMpscSender::clone) a sender makes another one, with
/// another queue.
///
/// Values from one sender arrive in the order it sent them. Values from
/// different senders arrive in no particular order.
///
/// ```no_run
/// use starfish::sync::shared_mpsc;
/// use starfish::{shard_count, shard_id, spawn, submit_to};
///
/// # async fn example() -> starfish::Result<()> {
/// // Every shard reports to this one.
/// let (sender, mut receiver) = shared_mpsc(64);
/// for shard in 0..shard_count()? {
///     let mut sender = sender.clone();
///     spawn(submit_to(shard, move || async move {
///         for n in 0..100u32 {
///             if sender.send((shard_id().unwrap_or(0), n)).await.is_err() {
///                 break; // the receiver is gone
///             }
///         }
///     }));
/// }
/// // The receiver's loop ends when every sender is gone, this one too.
/// drop(sender);
/// while let Some((shard, n)) = receiver.recv().await {
///     println!("shard {shard} sent {n}");
/// }
/// # Ok(())
/// # }
/// ```
pub fn shared_mpsc<T: Send>(capacity: usize) -> (SharedMpscSender<T>, SharedMpscReceiver<T>) {
    let hub = Arc::new(Hub {
        receiver: Parked::default(),
        receiver_gone: AtomicBool::new(false),
        senders: std::sync::atomic::AtomicUsize::new(1),
        new_lanes: Mutex::new(Vec::new()),
        has_new_lanes: AtomicBool::new(false),
        capacity,
    });
    let sender = hub.add_sender();
    let receiver = SharedMpscReceiver {
        hub,
        lanes: Vec::new(),
        turn: 0,
    };
    (sender, receiver)
}

/// A sending end of a [`shared_mpsc`].
///
/// Each one has its own queue to the receiver and is used by one task at a
/// time; clone it for every further producer, on this shard or another.
/// Dropping the last one ends the receiver's stream.
pub struct SharedMpscSender<T> {
    queue: Producer<T>,
    lane: Arc<LaneShared>,
    hub: Arc<Hub<T>>,
}

impl<T> SharedMpscSender<T> {
    /// Queues `value`, waiting for room if this sender's queue is full.
    ///
    /// Dropping the future before it completes drops the value without
    /// sending it.
    ///
    /// # Errors
    ///
    /// Hands the value back if the receiver has been dropped, before or
    /// during the wait. A value that was queued just as the receiver went
    /// away is dropped unseen.
    pub async fn send(&mut self, value: T) -> std::result::Result<(), SendError<T>> {
        let mut value = Some(value);
        poll_fn(|cx| {
            // Nothing left to send: the future already completed.
            let Some(unsent) = value.take() else { return Poll::Pending };
            let attempt = match self.try_send(unsent) {
                Err(TrySendError::Full(unsent)) => {
                    self.lane.sender.park(cx.waker());
                    // The receiver may have made room, or left, in between.
                    self.try_send(unsent)
                }
                sent_or_closed => sent_or_closed,
            };
            match attempt {
                Ok(()) => Poll::Ready(Ok(())),
                Err(TrySendError::Closed(unsent)) => Poll::Ready(Err(SendError(unsent))),
                Err(TrySendError::Full(unsent)) => {
                    value = Some(unsent);
                    Poll::Pending
                }
            }
        })
        .await
    }

    /// Queues `value` if this sender's queue has room right now.
    ///
    /// # Errors
    ///
    /// Hands the value back as [`TrySendError::Full`] if the queue is
    /// full, and as [`TrySendError::Closed`] if the receiver has been
    /// dropped.
    pub fn try_send(&mut self, value: T) -> std::result::Result<(), TrySendError<T>> {
        if self.hub.receiver_gone.load(Ordering::SeqCst) {
            return Err(TrySendError::Closed(value));
        }
        match self.queue.push(value) {
            Ok(()) => {
                self.hub.receiver.unpark();
                Ok(())
            }
            Err(value) => Err(TrySendError::Full(value)),
        }
    }

    /// Whether the receiver has been dropped, in which case nothing sent from
    /// now on can arrive.
    pub fn is_closed(&self) -> bool {
        self.hub.receiver_gone.load(Ordering::SeqCst)
    }
}

impl<T> Clone for SharedMpscSender<T> {
    /// Makes another sender, with a queue of its own. It can be used on
    /// any shard, and by another task than the original at the same time.
    fn clone(&self) -> Self {
        // Counted before it can send, so the receiver never sees "no
        // senders" while one is being made.
        self.hub.senders.fetch_add(1, Ordering::SeqCst);
        self.hub.add_sender()
    }
}

impl<T> Drop for SharedMpscSender<T> {
    /// Marks this sender gone, takes it off the count of senders and wakes the
    /// receiver, which ends its stream when the count reaches zero and the queues
    /// are empty.
    fn drop(&mut self) {
        self.lane.sender_gone.store(true, Ordering::SeqCst);
        self.hub.senders.fetch_sub(1, Ordering::SeqCst);
        self.hub.receiver.unpark();
    }
}

/// The receiving end of a [`shared_mpsc`].
///
/// It collects from every sender's queue in turn. It can be moved to another
/// shard and is used by one task at a time. Dropping it makes every later
/// send fail and wakes every sender that is waiting for room.
pub struct SharedMpscReceiver<T> {
    hub: Arc<Hub<T>>,
    /// The queues of the senders known so far.
    lanes: Vec<Lane<T>>,
    /// The queue to look at first next time, so that all get their turn.
    turn: usize,
}

impl<T> SharedMpscReceiver<T> {
    /// Takes the next value, waiting for one if all queues are empty.
    /// `None` means no value will ever come: the queues are empty and
    /// every sender has been dropped.
    ///
    /// Dropping the future before it completes loses nothing.
    pub async fn recv(&mut self) -> Option<T> {
        poll_fn(|cx| self.poll_recv(cx)).await
    }

    /// Takes the next value if any sender has one queued. `None` only says
    /// that none is queued right now.
    ///
    /// The queues are looked at in turn, starting after the one that was served
    /// last, so that a busy sender cannot keep the others from being heard. A
    /// queue whose sender is gone and which has been emptied is forgotten.
    pub fn try_recv(&mut self) -> Option<T> {
        self.collect_new_lanes();
        let count = self.lanes.len();
        for step in 0..count {
            let index = self.turn.wrapping_add(step).checked_rem(count).unwrap_or(0);
            let Some(lane) = self.lanes.get_mut(index) else { continue };
            // Read before looking, so that what a sender queued just
            // before it left is still found.
            let sender_gone = lane.shared.sender_gone.load(Ordering::SeqCst);
            match lane.queue.pop() {
                Some(value) => {
                    lane.shared.sender.unpark();
                    // The next queue goes first next time.
                    self.turn = index.wrapping_add(1);
                    return Some(value);
                }
                None if sender_gone => {
                    // Empty for good. The indexes after it shift down;
                    // start over rather than skip one.
                    self.lanes.swap_remove(index);
                    return self.try_recv();
                }
                None => {}
            }
        }
        None
    }

    /// Takes over the queues of senders made since the last look. Costs one
    /// atomic read when there are none, which is nearly always.
    fn collect_new_lanes(&mut self) {
        if self.hub.has_new_lanes.swap(false, Ordering::SeqCst) {
            let mut new_lanes = self.hub.new_lanes.lock().unwrap_or_else(PoisonError::into_inner);
            self.lanes.append(&mut new_lanes);
        }
    }

    /// One attempt to receive, for [`recv`](Self::recv): looks at every queue,
    /// and if all are empty, announces that it waits and looks once more.
    ///
    /// The number of senders is read between the announcement and the second
    /// look. Once that number is zero no value can be queued and no sender can be
    /// made, so finding nothing after reading zero means the stream has ended.
    fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        if let Some(value) = self.try_recv() {
            return Poll::Ready(Some(value));
        }
        self.hub.receiver.park(cx.waker());
        // Read before the second look: once this is zero, nothing more can
        // be queued and no sender can be made.
        let senders = self.hub.senders.load(Ordering::SeqCst);
        match self.try_recv() {
            Some(value) => Poll::Ready(Some(value)),
            None if senders == 0 => Poll::Ready(None),
            None => Poll::Pending,
        }
    }
}

impl<T> Drop for SharedMpscReceiver<T> {
    /// Marks the receiver gone and wakes every sender, including those whose
    /// queues it had not yet taken over, so that none waits for room that will
    /// never come.
    fn drop(&mut self) {
        self.hub.receiver_gone.store(true, Ordering::SeqCst);
        // Senders waiting for room have to learn that none will come.
        self.collect_new_lanes();
        for lane in &self.lanes {
            lane.shared.sender.unpark();
        }
    }
}

/// What the ends of a [`shared_mpmc`] share: the queue and everything
/// that decides who waits, behind one lock.
struct MpmcState<T> {
    queue: VecDeque<T>,
    /// Sending ends alive.
    senders: usize,
    /// Receiving ends alive.
    receivers: usize,
    /// Tasks waiting for room, longest wait first, each under the number it
    /// was given when it began to wait.
    send_waiters: VecDeque<(u64, Waker)>,
    /// Tasks waiting for a value, longest wait first.
    recv_waiters: VecDeque<(u64, Waker)>,
    next_waiter: u64,
}

impl<T> MpmcState<T> {
    /// Puts a task in the queue of waiters, or, if it is there already,
    /// replaces its waker and leaves its place as it is. Returns the number
    /// it waits under.
    fn wait(waiters: &mut VecDeque<(u64, Waker)>, next: &mut u64, id: Option<u64>, waker: &Waker) -> u64 {
        if let Some(id) = id {
            if let Some(entry) = waiters.iter_mut().find(|entry| entry.0 == id) {
                // Replaced, not dropped under the lock: wakers are the
                // runtime's own and dropping one takes no lock.
                entry.1.clone_from(waker);
                return id;
            }
        }
        let id = *next;
        *next = next.wrapping_add(1);
        waiters.push_back((id, waker.clone()));
        id
    }

    /// Takes a task out of a queue of waiters. Returns whether it was
    /// there; if it was not, it had been woken.
    fn leave(waiters: &mut VecDeque<(u64, Waker)>, id: u64) -> bool {
        match waiters.iter().position(|entry| entry.0 == id) {
            Some(place) => {
                waiters.remove(place);
                true
            }
            None => false,
        }
    }
}

/// A [`shared_mpmc`] channel: its state behind the lock, and its capacity.
struct Mpmc<T> {
    state: Mutex<MpmcState<T>>,
    capacity: usize,
}

impl<T> Mpmc<T> {
    /// Locks the state, recovering it if a thread panicked while holding
    /// the lock. Nothing of the application's runs under the lock, so that
    /// cannot leave it inconsistent.
    fn lock(&self) -> std::sync::MutexGuard<'_, MpmcState<T>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Queues a value if there is room and a receiver. On success returns
    /// the receiver to wake, if one waits.
    fn push(&self, value: T) -> std::result::Result<Option<Waker>, TrySendError<T>> {
        let mut state = self.lock();
        if state.receivers == 0 {
            return Err(TrySendError::Closed(value));
        }
        if state.queue.len() >= self.capacity {
            return Err(TrySendError::Full(value));
        }
        state.queue.push_back(value);
        Ok(state.recv_waiters.pop_front().map(|entry| entry.1))
    }

    /// Takes the oldest value, with the sender to wake if one waits for
    /// room. `Err(true)` if there is none and never will be.
    fn pop(&self) -> std::result::Result<(T, Option<Waker>), bool> {
        let mut state = self.lock();
        match state.queue.pop_front() {
            Some(value) => Ok((value, state.send_waiters.pop_front().map(|entry| entry.1))),
            None => Err(state.senders == 0),
        }
    }
}

/// A task's place among those waiting on a [`shared_mpmc`], given up when
/// the wait ends, however it ends.
///
/// A wake-up is meant for one task. If that task's future is dropped after
/// it was woken but before it ran, the wake-up would be lost and a value,
/// or a free place, would sit unnoticed while others wait. So a wait that
/// ends without having taken what it was woken for passes the wake-up on.
struct MpmcWait<'a, T> {
    channel: &'a Mpmc<T>,
    /// The number this task waits under, once it has had to wait.
    id: Option<u64>,
    /// Waiting for a value, as opposed to for room.
    receiving: bool,
}

impl<T> MpmcWait<'_, T> {
    /// Ends the wait without passing anything on: the task got what it
    /// waited for, or learnt that it never will.
    fn finish(&mut self) {
        let Some(id) = self.id.take() else { return };
        let mut state = self.channel.lock();
        let waiters = if self.receiving {
            &mut state.recv_waiters
        } else {
            &mut state.send_waiters
        };
        MpmcState::<T>::leave(waiters, id);
    }
}

impl<T> Drop for MpmcWait<'_, T> {
    /// Leaves the queue of waiters. If this task had been woken and is going
    /// away without acting on it, wakes the next one in its place.
    fn drop(&mut self) {
        let Some(id) = self.id.take() else { return };
        let next = {
            let mut state = self.channel.lock();
            if self.receiving {
                let was_waiting = MpmcState::<T>::leave(&mut state.recv_waiters, id);
                if was_waiting || state.queue.is_empty() {
                    None
                } else {
                    state.recv_waiters.pop_front()
                }
            } else {
                let was_waiting = MpmcState::<T>::leave(&mut state.send_waiters, id);
                if was_waiting || state.queue.len() >= self.channel.capacity {
                    None
                } else {
                    state.send_waiters.pop_front()
                }
            }
        };
        if let Some((_, waker)) = next {
            wake_quietly(waker);
        }
    }
}

/// Creates a queue between any number of senders and any number of
/// receivers, on any shards: each value sent is received by exactly one
/// receiver, whichever asks first.
///
/// This is the channel for sharing work out: several shards feed it,
/// several take from it, and a value goes to a receiver that is free. It
/// holds at most `capacity` values (at least 1); a sender that finds it
/// full waits, and a receiver that finds it empty waits. Both ends can be
/// cloned and sent to other shards, and an end can also be shared by
/// several tasks of one shard, since sending and receiving take `&self`.
///
/// Values arrive in the order they were queued, though with several
/// receivers that says little about the order they are handled in. Waiting
/// tasks are served longest wait first.
///
/// Unlike [`shared_spsc`] and [`shared_mpsc`], which never lock, this
/// channel is one queue behind one lock, taken for the moment it takes to
/// add or remove a value. That is what lets any receiver take any value,
/// and it is cheap while the channel is lightly used, but shards that use
/// one channel heavily contend for the lock. Where each value has an
/// obvious destination, a queue per destination scales better.
///
/// ```no_run
/// use starfish::sync::shared_mpmc;
/// use starfish::{shard_count, spawn, submit_to};
///
/// # async fn example(jobs: Vec<String>) -> starfish::Result<()> {
/// // One worker on every shard; each job goes to whichever is free.
/// let (sender, receiver) = shared_mpmc::<String>(128);
/// let mut workers = Vec::new();
/// for shard in 0..shard_count()? {
///     let receiver = receiver.clone();
///     workers.push(spawn(submit_to(shard, move || async move {
///         let mut done = 0;
///         while let Some(job) = receiver.recv().await {
///             // ... do the job ...
///             done += job.len();
///         }
///         done
///     })));
/// }
/// drop(receiver);
/// for job in jobs {
///     if sender.send(job).await.is_err() {
///         break; // every worker is gone
///     }
/// }
/// // The workers' loops end when every sender is gone.
/// drop(sender);
/// for worker in workers {
///     worker.await??;
/// }
/// # Ok(())
/// # }
/// ```
pub fn shared_mpmc<T: Send>(capacity: usize) -> (SharedMpmcSender<T>, SharedMpmcReceiver<T>) {
    let channel = Arc::new(Mpmc {
        state: Mutex::new(MpmcState {
            queue: VecDeque::new(),
            senders: 1,
            receivers: 1,
            send_waiters: VecDeque::new(),
            recv_waiters: VecDeque::new(),
            next_waiter: 0,
        }),
        capacity: capacity.max(1),
    });
    let sender = SharedMpmcSender {
        channel: channel.clone(),
    };
    (sender, SharedMpmcReceiver { channel })
}

/// A sending end of a [`shared_mpmc`].
///
/// Clone it for every further producer; a clone can go to another shard.
/// Dropping the last one ends the receivers' streams, once they have taken
/// what is still queued.
pub struct SharedMpmcSender<T> {
    channel: Arc<Mpmc<T>>,
}

impl<T> SharedMpmcSender<T> {
    /// Queues `value`, waiting for room if the channel is full.
    ///
    /// Dropping the future before it completes drops the value without
    /// sending it.
    ///
    /// # Errors
    ///
    /// Hands the value back if every receiver has been dropped, before or
    /// during the wait.
    pub async fn send(&self, value: T) -> std::result::Result<(), SendError<T>> {
        let mut value = Some(value);
        let mut wait = MpmcWait {
            channel: &*self.channel,
            id: None,
            receiving: false,
        };
        poll_fn(|cx| {
            // Nothing left to send: the future already completed.
            let Some(unsent) = value.take() else { return Poll::Pending };
            match self.channel.push(unsent) {
                Ok(receiver) => {
                    wait.finish();
                    if let Some(receiver) = receiver {
                        wake_quietly(receiver);
                    }
                    Poll::Ready(Ok(()))
                }
                Err(TrySendError::Closed(unsent)) => {
                    wait.finish();
                    Poll::Ready(Err(SendError(unsent)))
                }
                Err(TrySendError::Full(unsent)) => {
                    // Checked again under the same lock that registers the
                    // wait, so room made in between is not missed.
                    let mut state = self.channel.lock();
                    if state.receivers == 0 {
                        drop(state);
                        wait.finish();
                        return Poll::Ready(Err(SendError(unsent)));
                    }
                    if state.queue.len() < self.channel.capacity {
                        state.queue.push_back(unsent);
                        let receiver = state.recv_waiters.pop_front();
                        drop(state);
                        wait.finish();
                        if let Some((_, receiver)) = receiver {
                            wake_quietly(receiver);
                        }
                        return Poll::Ready(Ok(()));
                    }
                    let state = &mut *state;
                    wait.id = Some(MpmcState::<T>::wait(
                        &mut state.send_waiters,
                        &mut state.next_waiter,
                        wait.id,
                        cx.waker(),
                    ));
                    value = Some(unsent);
                    Poll::Pending
                }
            }
        })
        .await
    }

    /// Queues `value` if the channel has room right now. It does not join
    /// the tasks waiting in [`send`](Self::send), so it can take a place
    /// ahead of them.
    ///
    /// # Errors
    ///
    /// Hands the value back as [`TrySendError::Full`] if the channel is
    /// full, and as [`TrySendError::Closed`] if every receiver has been
    /// dropped.
    pub fn try_send(&self, value: T) -> std::result::Result<(), TrySendError<T>> {
        let receiver = self.channel.push(value)?;
        if let Some(receiver) = receiver {
            wake_quietly(receiver);
        }
        Ok(())
    }

    /// Whether every receiver has been dropped, in which case nothing sent
    /// from now on can arrive.
    pub fn is_closed(&self) -> bool {
        self.channel.lock().receivers == 0
    }
}

impl<T> Clone for SharedMpmcSender<T> {
    /// Makes another sending end of the same channel.
    fn clone(&self) -> Self {
        let mut state = self.channel.lock();
        state.senders = state.senders.saturating_add(1);
        drop(state);
        SharedMpmcSender {
            channel: self.channel.clone(),
        }
    }
}

impl<T> Drop for SharedMpmcSender<T> {
    /// Counts the sender out. If it was the last, wakes every waiting
    /// receiver, so that each learns the stream has ended once the queue is
    /// empty.
    fn drop(&mut self) {
        let waiting = {
            let mut state = self.channel.lock();
            state.senders = state.senders.saturating_sub(1);
            if state.senders == 0 {
                std::mem::take(&mut state.recv_waiters)
            } else {
                VecDeque::new()
            }
        };
        for (_, waker) in waiting {
            wake_quietly(waker);
        }
    }
}

/// A receiving end of a [`shared_mpmc`].
///
/// Clone it for every further consumer; a clone can go to another shard.
/// Dropping the last one closes the channel: senders get their values
/// back, and values still queued are dropped.
pub struct SharedMpmcReceiver<T> {
    channel: Arc<Mpmc<T>>,
}

impl<T> SharedMpmcReceiver<T> {
    /// Takes the oldest value, waiting for one if the channel is empty.
    /// Returns `None` once the channel is empty and every sender has been
    /// dropped.
    ///
    /// Dropping the future loses nothing: a value is only taken at the
    /// moment the call returns it, and a wake-up this task does not use is
    /// passed to the next receiver.
    pub async fn recv(&self) -> Option<T> {
        let mut wait = MpmcWait {
            channel: &*self.channel,
            id: None,
            receiving: true,
        };
        poll_fn(|cx| {
            let ended = match self.channel.pop() {
                Ok((value, sender)) => {
                    wait.finish();
                    if let Some(sender) = sender {
                        wake_quietly(sender);
                    }
                    return Poll::Ready(Some(value));
                }
                Err(ended) => ended,
            };
            if ended {
                wait.finish();
                return Poll::Ready(None);
            }
            // Checked again under the same lock that registers the wait, so
            // a value queued in between is not missed.
            let mut state = self.channel.lock();
            if let Some(value) = state.queue.pop_front() {
                let sender = state.send_waiters.pop_front();
                drop(state);
                wait.finish();
                if let Some((_, sender)) = sender {
                    wake_quietly(sender);
                }
                return Poll::Ready(Some(value));
            }
            if state.senders == 0 {
                drop(state);
                wait.finish();
                return Poll::Ready(None);
            }
            let state = &mut *state;
            wait.id = Some(MpmcState::<T>::wait(
                &mut state.recv_waiters,
                &mut state.next_waiter,
                wait.id,
                cx.waker(),
            ));
            Poll::Pending
        })
        .await
    }

    /// Takes the oldest value if there is one right now. `None` means the
    /// channel is empty at the moment, not that it has ended.
    pub fn try_recv(&self) -> Option<T> {
        let (value, sender) = self.channel.pop().ok()?;
        if let Some(sender) = sender {
            wake_quietly(sender);
        }
        Some(value)
    }

    /// Number of values queued at this moment. With other shards sending
    /// and receiving, it may be out of date as soon as it is returned.
    pub fn len(&self) -> usize {
        self.channel.lock().queue.len()
    }

    /// Whether nothing is queued at this moment.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T> Clone for SharedMpmcReceiver<T> {
    /// Makes another receiving end of the same channel.
    fn clone(&self) -> Self {
        let mut state = self.channel.lock();
        state.receivers = state.receivers.saturating_add(1);
        drop(state);
        SharedMpmcReceiver {
            channel: self.channel.clone(),
        }
    }
}

impl<T> Drop for SharedMpmcReceiver<T> {
    /// Counts the receiver out. If it was the last, the channel is closed:
    /// every waiting sender is woken to take its value back, and the values
    /// still queued are dropped.
    fn drop(&mut self) {
        let (waiting, unreceived) = {
            let mut state = self.channel.lock();
            state.receivers = state.receivers.saturating_sub(1);
            if state.receivers == 0 {
                (
                    std::mem::take(&mut state.send_waiters),
                    std::mem::take(&mut state.queue),
                )
            } else {
                (VecDeque::new(), VecDeque::new())
            }
        };
        // Outside the lock: dropping a value may run code of the
        // application's.
        drop(unreceived);
        for (_, waker) in waiting {
            wake_quietly(waker);
        }
    }
}

debug_impl!(<T> SharedSpscSender<T>);
debug_impl!(<T> SharedSpscReceiver<T>);
debug_impl!(<T> SharedMpscSender<T>);
debug_impl!(<T> SharedMpscReceiver<T>, "senders" => |receiver| receiver.lanes.len());
debug_impl!(<T> SharedMpmcSender<T>);
debug_impl!(<T> SharedMpmcReceiver<T>);

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::panic,
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing
)]
mod tests {
    //! Driven by hand and by plain threads, without a runtime, so that
    //! Miri can run them.

    use super::*;
    use std::future::Future;
    use std::pin::{pin, Pin};
    use std::sync::atomic::AtomicUsize;
    use std::task::Wake;

    /// Counts how often it is woken.
    struct Wakes(AtomicUsize);

    impl Wake for Wakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn counting_waker() -> (Arc<Wakes>, Waker) {
        let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
        (wakes.clone(), Waker::from(wakes))
    }

    fn count(wakes: &Wakes) -> usize {
        wakes.0.load(Ordering::Relaxed)
    }

    fn poll<F: Future>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(waker))
    }

    /// Wakes a thread blocked in `block_on`.
    struct Unpark(std::thread::Thread);

    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
        let mut future = pin!(future);
        loop {
            match poll(future.as_mut(), &waker) {
                Poll::Ready(output) => return output,
                Poll::Pending => std::thread::park(),
            }
        }
    }

    #[test]
    fn a_full_queue_parks_the_sender_until_there_is_room() {
        let (mut sender, mut receiver) = shared_spsc(2);
        sender.try_send(1).unwrap();
        sender.try_send(2).unwrap();
        assert!(matches!(sender.try_send(3), Err(TrySendError::Full(3))));

        let (wakes, waker) = counting_waker();
        {
            let mut send = pin!(sender.send(3));
            assert!(poll(send.as_mut(), &waker).is_pending());
            assert_eq!(count(&wakes), 0);
            assert_eq!(receiver.try_recv(), Some(1));
            assert_eq!(count(&wakes), 1);
            assert!(matches!(poll(send.as_mut(), &waker), Poll::Ready(Ok(()))));
        }
        assert_eq!(receiver.try_recv(), Some(2));
        assert_eq!(receiver.try_recv(), Some(3));
        assert_eq!(receiver.try_recv(), None);
        // Nobody was waiting for those.
        assert_eq!(count(&wakes), 1);
    }

    #[test]
    fn an_empty_queue_parks_the_receiver_until_a_value_or_the_end() {
        let (mut sender, mut receiver) = shared_spsc::<u32>(2);
        let (wakes, waker) = counting_waker();
        {
            let mut receive = pin!(receiver.recv());
            assert!(poll(receive.as_mut(), &waker).is_pending());
            sender.try_send(7).unwrap();
            assert_eq!(count(&wakes), 1);
            assert!(matches!(poll(receive.as_mut(), &waker), Poll::Ready(Some(7))));
        }
        {
            let mut receive = pin!(receiver.recv());
            assert!(poll(receive.as_mut(), &waker).is_pending());
            // What was sent before the sender left still arrives.
            sender.try_send(8).unwrap();
            drop(sender);
            assert!(count(&wakes) >= 2);
            assert!(matches!(poll(receive.as_mut(), &waker), Poll::Ready(Some(8))));
        }
        let mut receive = pin!(receiver.recv());
        assert!(matches!(poll(receive.as_mut(), &waker), Poll::Ready(None)));
    }

    #[test]
    fn a_sender_learns_that_the_receiver_is_gone() {
        let (mut sender, receiver) = shared_spsc(2);
        sender.try_send(Arc::new(1)).unwrap();
        sender.try_send(Arc::new(2)).unwrap();
        let waiting_value = Arc::new(3);
        let (wakes, waker) = counting_waker();
        {
            let mut send = pin!(sender.send(waiting_value.clone()));
            assert!(poll(send.as_mut(), &waker).is_pending());
            drop(receiver);
            assert_eq!(count(&wakes), 1);
            let Poll::Ready(Err(SendError(returned))) = poll(send.as_mut(), &waker) else {
                panic!("not refused")
            };
            assert!(Arc::ptr_eq(&returned, &waiting_value));
        }
        assert!(sender.is_closed());
        assert!(matches!(sender.try_send(Arc::new(4)), Err(TrySendError::Closed(_))));
        // What was queued and never received goes with the last end.
        drop(sender);
        assert_eq!(Arc::strong_count(&waiting_value), 1);
    }

    #[test]
    fn two_threads_exchange_every_value_in_order() {
        let values: u64 = if cfg!(miri) { 300 } else { 200_000 };
        let (mut sender, mut receiver) = shared_spsc(4);
        let producer = std::thread::spawn(move || {
            block_on(async move {
                for value in 0..values {
                    sender.send(value).await.unwrap();
                }
            });
        });
        let received = block_on(async move {
            let mut expected = 0;
            while let Some(value) = receiver.recv().await {
                assert_eq!(value, expected);
                expected += 1;
            }
            expected
        });
        producer.join().unwrap();
        assert_eq!(received, values);
    }

    #[test]
    fn a_receiver_that_leaves_releases_a_sender_on_another_thread() {
        let (mut sender, mut receiver) = shared_spsc(2);
        let producer = std::thread::spawn(move || {
            block_on(async move {
                let mut sent = 0u64;
                while sender.send(sent).await.is_ok() {
                    sent += 1;
                }
                sent
            })
        });
        let taken = block_on(async move {
            for expected in 0..50u64 {
                assert_eq!(receiver.recv().await, Some(expected));
            }
            50u64
        });
        // The receiver is gone; the producer must notice and stop.
        let sent = producer.join().unwrap();
        assert!(sent >= taken);
    }

    #[test]
    fn several_senders_each_keep_their_order() {
        let (mut first, mut receiver) = shared_mpsc(2);
        let mut second = first.clone();
        first.try_send((1, 0)).unwrap();
        first.try_send((1, 1)).unwrap();
        // Each sender has a queue of its own: one being full does not
        // stop another.
        assert!(matches!(first.try_send((1, 2)), Err(TrySendError::Full(_))));
        second.try_send((2, 0)).unwrap();

        let (wakes, waker) = counting_waker();
        {
            let mut send = pin!(first.send((1, 2)));
            assert!(poll(send.as_mut(), &waker).is_pending());
            // Taking a value from the full queue wakes its sender.
            let mut seen = Vec::new();
            while count(&wakes) == 0 {
                seen.push(receiver.try_recv().unwrap());
            }
            assert!(seen.contains(&(1, 0)));
            assert!(matches!(poll(send.as_mut(), &waker), Poll::Ready(Ok(()))));
            let mut rest = seen;
            while let Some(value) = receiver.try_recv() {
                rest.push(value);
            }
            let from_first: Vec<_> = rest.iter().filter(|value| value.0 == 1).map(|value| value.1).collect();
            assert_eq!(from_first, [0, 1, 2]);
            assert_eq!(rest.len(), 4);
        }

        // The stream ends only when every sender is gone.
        let (end_wakes, end_waker) = counting_waker();
        {
            let mut receive = pin!(receiver.recv());
            assert!(poll(receive.as_mut(), &end_waker).is_pending());
            drop(first);
            assert!(poll(receive.as_mut(), &end_waker).is_pending());
            let mut third = second.clone();
            drop(second);
            assert!(poll(receive.as_mut(), &end_waker).is_pending());
            third.try_send((3, 0)).unwrap();
            drop(third);
            assert!(count(&end_wakes) >= 3);
            assert!(matches!(poll(receive.as_mut(), &end_waker), Poll::Ready(Some((3, 0)))));
        }
        let mut receive = pin!(receiver.recv());
        assert!(matches!(poll(receive.as_mut(), &end_waker), Poll::Ready(None)));
    }

    #[test]
    fn senders_learn_that_the_shared_receiver_is_gone() {
        let (mut sender, receiver) = shared_mpsc(2);
        let mut other = sender.clone();
        sender.try_send(1).unwrap();
        sender.try_send(2).unwrap();
        let (wakes, waker) = counting_waker();
        {
            let mut send = pin!(sender.send(3));
            assert!(poll(send.as_mut(), &waker).is_pending());
            drop(receiver);
            assert_eq!(count(&wakes), 1);
            assert!(matches!(poll(send.as_mut(), &waker), Poll::Ready(Err(SendError(3)))));
        }
        assert!(other.is_closed());
        assert!(matches!(other.try_send(4), Err(TrySendError::Closed(4))));
        // A sender made after the receiver left is closed from the start.
        let mut late = other.clone();
        assert!(matches!(late.try_send(5), Err(TrySendError::Closed(5))));
    }

    #[test]
    fn threads_feed_one_receiver_without_losing_or_reordering() {
        let values: u64 = if cfg!(miri) { 150 } else { 50_000 };
        let producers = 4u64;
        let (sender, mut receiver) = shared_mpsc(4);
        let threads: Vec<_> = (0..producers)
            .map(|producer| {
                let mut sender = sender.clone();
                std::thread::spawn(move || {
                    block_on(async move {
                        for value in 0..values {
                            sender.send((producer, value)).await.unwrap();
                        }
                    });
                })
            })
            .collect();
        drop(sender);
        let received = block_on(async move {
            let mut next = vec![0u64; producers as usize];
            let mut total = 0u64;
            while let Some((producer, value)) = receiver.recv().await {
                assert_eq!(value, next[producer as usize]);
                next[producer as usize] += 1;
                total += 1;
            }
            total
        });
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(received, producers * values);
    }

    #[test]
    fn several_receivers_share_one_queue_in_order() {
        let (sender, receiver) = shared_mpmc(2);
        let other = receiver.clone();
        sender.try_send(1).unwrap();
        sender.try_send(2).unwrap();
        assert!(matches!(sender.try_send(3), Err(TrySendError::Full(3))));
        assert_eq!(receiver.len(), 2);
        // Either end takes the oldest value.
        assert_eq!(other.try_recv(), Some(1));
        assert_eq!(receiver.try_recv(), Some(2));
        assert_eq!(other.try_recv(), None);
        assert!(receiver.is_empty());

        // A waiting sender is woken by a receive, on either end.
        sender.try_send(3).unwrap();
        sender.try_send(4).unwrap();
        let (wakes, waker) = counting_waker();
        {
            let mut send = pin!(sender.send(5));
            assert!(poll(send.as_mut(), &waker).is_pending());
            assert_eq!(other.try_recv(), Some(3));
            assert_eq!(count(&wakes), 1);
            assert!(matches!(poll(send.as_mut(), &waker), Poll::Ready(Ok(()))));
        }
        assert_eq!(receiver.try_recv(), Some(4));
        assert_eq!(receiver.try_recv(), Some(5));

        // The stream ends for every receiver when the last sender goes,
        // after what is queued.
        let second = sender.clone();
        drop(sender);
        second.try_send(6).unwrap();
        let (wakes, waker) = counting_waker();
        {
            let mut first = pin!(receiver.recv());
            assert!(matches!(poll(first.as_mut(), &waker), Poll::Ready(Some(6))));
            let mut waiting = pin!(other.recv());
            assert!(poll(waiting.as_mut(), &waker).is_pending());
            drop(second);
            assert_eq!(count(&wakes), 1);
            assert!(matches!(poll(waiting.as_mut(), &waker), Poll::Ready(None)));
        }
        assert!(block_on(receiver.recv()).is_none());
    }

    #[test]
    fn a_wake_up_nobody_uses_is_passed_on() {
        let (sender, receiver) = shared_mpmc::<u32>(1);
        let other = receiver.clone();
        let (first_wakes, first_waker) = counting_waker();
        let (second_wakes, second_waker) = counting_waker();
        let mut second = pin!(other.recv());
        {
            let mut first = pin!(receiver.recv());
            assert!(poll(first.as_mut(), &first_waker).is_pending());
            assert!(poll(second.as_mut(), &second_waker).is_pending());
            // The value wakes the one that has waited longest, and only it.
            sender.try_send(1).unwrap();
            assert_eq!((count(&first_wakes), count(&second_wakes)), (1, 0));
            // It goes away without taking the value: the next one is woken.
        }
        assert_eq!(count(&second_wakes), 1);
        assert!(matches!(poll(second.as_mut(), &second_waker), Poll::Ready(Some(1))));

        // The same for senders waiting for room.
        sender.try_send(2).unwrap();
        let again = sender.clone();
        let mut later = pin!(again.send(4));
        {
            let mut sooner = pin!(sender.send(3));
            assert!(poll(sooner.as_mut(), &first_waker).is_pending());
            assert!(poll(later.as_mut(), &second_waker).is_pending());
            assert_eq!(receiver.try_recv(), Some(2));
            assert_eq!((count(&first_wakes), count(&second_wakes)), (2, 1));
        }
        assert_eq!(count(&second_wakes), 2);
        assert!(matches!(poll(later.as_mut(), &second_waker), Poll::Ready(Ok(()))));
        assert_eq!(receiver.try_recv(), Some(4));

        // A wait that is given up before any wake-up passes nothing on.
        {
            let mut idle = pin!(receiver.recv());
            assert!(poll(idle.as_mut(), &first_waker).is_pending());
        }
        assert_eq!((count(&first_wakes), count(&second_wakes)), (2, 2));
    }

    #[test]
    fn the_last_receiver_closes_the_channel_and_drops_what_is_queued() {
        let dropped = Arc::new(AtomicUsize::new(0));
        struct Counted(Arc<AtomicUsize>);
        impl Drop for Counted {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        let (sender, receiver) = shared_mpmc(1);
        let other = receiver.clone();
        sender.try_send(Counted(dropped.clone())).ok().unwrap();
        let (wakes, waker) = counting_waker();
        {
            let mut send = pin!(sender.send(Counted(dropped.clone())));
            assert!(poll(send.as_mut(), &waker).is_pending());
            drop(receiver);
            // One receiver is left: still open.
            assert!(!sender.is_closed());
            assert_eq!(count(&wakes), 0);
            drop(other);
            assert_eq!(count(&wakes), 1);
            assert_eq!(dropped.load(Ordering::Relaxed), 1);
            // The waiting sender gets its value back.
            let Poll::Ready(Err(SendError(returned))) = poll(send.as_mut(), &waker) else {
                panic!("the send should have failed");
            };
            drop(returned);
        }
        assert_eq!(dropped.load(Ordering::Relaxed), 2);
        assert!(sender.is_closed());
        assert!(matches!(sender.try_send(Counted(dropped.clone())), Err(TrySendError::Closed(_))));
    }

    #[test]
    fn threads_share_work_without_losing_or_repeating_any() {
        let values: u64 = if cfg!(miri) { 100 } else { 20_000 };
        let (producers, consumers) = (4u64, 4);
        let (sender, receiver) = shared_mpmc(8);
        let sending: Vec<_> = (0..producers)
            .map(|producer| {
                let sender = sender.clone();
                std::thread::spawn(move || {
                    block_on(async move {
                        for value in 0..values {
                            sender.send(producer * values + value).await.unwrap();
                        }
                    });
                })
            })
            .collect();
        drop(sender);
        let receiving: Vec<_> = (0..consumers)
            .map(|_| {
                let receiver = receiver.clone();
                std::thread::spawn(move || {
                    block_on(async move {
                        let (mut sum, mut seen) = (0u64, 0u64);
                        while let Some(value) = receiver.recv().await {
                            sum += value;
                            seen += 1;
                        }
                        (sum, seen)
                    })
                })
            })
            .collect();
        drop(receiver);
        for thread in sending {
            thread.join().unwrap();
        }
        let (mut sum, mut seen) = (0, 0);
        for thread in receiving {
            let (part, count) = thread.join().unwrap();
            sum += part;
            seen += count;
        }
        let total = producers * values;
        assert_eq!(seen, total);
        assert_eq!(sum, total * (total - 1) / 2);
    }
}
