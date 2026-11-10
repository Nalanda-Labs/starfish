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

use std::future::poll_fn;
use std::sync::atomic::{fence, AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll, Waker};

use crate::runtime::wake_quietly;
use crate::spsc::{self, Consumer, Producer};
use crate::sync::{SendError, TrySendError};

/// One end's request to be woken by the other.
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
/// [`send`](SharedSender::send) waits, so a shard that produces faster
/// than another consumes is slowed down to its pace instead of filling
/// memory.
///
/// For a stream of values this is much cheaper than one
/// [`submit_to`](crate::submit_to) per value: a value costs no allocation
/// and no task, and the other shard is only woken when it is waiting.
///
/// ```no_run
/// use starfish::submit_to;
/// use starfish::sync::shared_channel;
///
/// # async fn example() -> starfish::Result<()> {
/// let (mut sender, mut receiver) = shared_channel(64);
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
pub fn shared_channel<T: Send>(capacity: usize) -> (SharedSender<T>, SharedReceiver<T>) {
    let (producer, consumer) = spsc::channel(capacity);
    let shared = Arc::new(Shared::default());
    let sender = SharedSender {
        queue: producer,
        shared: shared.clone(),
    };
    let receiver = SharedReceiver {
        queue: consumer,
        shared,
    };
    (sender, receiver)
}

/// The sending end of a [`shared_channel`].
pub struct SharedSender<T> {
    queue: Producer<T>,
    shared: Arc<Shared>,
}

impl<T> SharedSender<T> {
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

    /// Whether the receiver has been dropped.
    pub fn is_closed(&self) -> bool {
        self.shared.receiver_gone.load(Ordering::SeqCst)
    }

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

impl<T> Drop for SharedSender<T> {
    fn drop(&mut self) {
        self.shared.sender_gone.store(true, Ordering::SeqCst);
        self.shared.receiver.unpark();
    }
}

/// The receiving end of a [`shared_channel`]. Dropping it makes sends
/// fail.
pub struct SharedReceiver<T> {
    queue: Consumer<T>,
    shared: Arc<Shared>,
}

impl<T> SharedReceiver<T> {
    /// Takes the next value, waiting for one if the queue is empty.
    /// `None` means no value will ever come: the queue is empty and the
    /// sender has been dropped.
    ///
    /// Dropping the future before it completes loses nothing.
    pub async fn recv(&mut self) -> Option<T> {
        poll_fn(|cx| self.poll_recv(cx)).await
    }

    /// Takes the next value if one is queued. `None` only says that none
    /// is queued right now.
    pub fn try_recv(&mut self) -> Option<T> {
        let value = self.queue.pop()?;
        self.shared.sender.unpark();
        Some(value)
    }

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

impl<T> Drop for SharedReceiver<T> {
    fn drop(&mut self) {
        self.shared.receiver_gone.store(true, Ordering::SeqCst);
        self.shared.sender.unpark();
    }
}

/// One sender's own queue towards the receiver, as the sender sees it.
#[derive(Default)]
struct LaneShared {
    /// The sender, waiting for room in its queue.
    sender: Parked,
    sender_gone: AtomicBool,
}

/// The receiver's end of one sender's queue.
struct Lane<T> {
    queue: Consumer<T>,
    shared: Arc<LaneShared>,
}

/// What all the senders and the receiver of a [`shared_mpsc`] have in
/// common.
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
    /// Gives a new sender a queue of its own and tells the receiver.
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

/// A sending end of a [`shared_mpsc`]. Each one is used by one task at a
/// time; clone it for every further producer.
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

    /// Whether the receiver has been dropped.
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
    fn drop(&mut self) {
        self.lane.sender_gone.store(true, Ordering::SeqCst);
        self.hub.senders.fetch_sub(1, Ordering::SeqCst);
        self.hub.receiver.unpark();
    }
}

/// The receiving end of a [`shared_mpsc`]. Dropping it makes sends fail.
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

    /// Takes the next value if one is queued. `None` only says that none
    /// is queued right now.
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

    /// Takes over the queues of senders made since the last look.
    fn collect_new_lanes(&mut self) {
        if self.hub.has_new_lanes.swap(false, Ordering::SeqCst) {
            let mut new_lanes = self.hub.new_lanes.lock().unwrap_or_else(PoisonError::into_inner);
            self.lanes.append(&mut new_lanes);
        }
    }

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
    fn drop(&mut self) {
        self.hub.receiver_gone.store(true, Ordering::SeqCst);
        // Senders waiting for room have to learn that none will come.
        self.collect_new_lanes();
        for lane in &self.lanes {
            lane.shared.sender.unpark();
        }
    }
}

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
        let (mut sender, mut receiver) = shared_channel(2);
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
        let (mut sender, mut receiver) = shared_channel::<u32>(2);
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
        let (mut sender, receiver) = shared_channel(2);
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
        let (mut sender, mut receiver) = shared_channel(4);
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
        let (mut sender, mut receiver) = shared_channel(2);
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
}
