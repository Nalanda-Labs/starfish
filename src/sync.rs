//! Coordination between the tasks of one shard.
//!
//! A [`Semaphore`] bounds how much of something runs at once: connections,
//! requests in flight, bytes of memory in use. A [`Gate`] counts the work in
//! progress so that shutdown can refuse new work and wait for the rest to
//! finish. A [`channel`] passes values from some tasks to another and makes
//! the senders wait when the receiver falls behind.
//!
//! All of them belong to the shard they were created on. Their handles are
//! not `Send`: to coordinate across shards, keep one per shard in a
//! [`Sharded`](crate::Sharded) service. None needs a running runtime.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use crate::error::{Error, Result};
use crate::runtime::{wake_quietly, Local};

/// A task waiting for permits.
struct Waiter {
    need: usize,
    /// Taken when the task is woken, put back if it has to wait again.
    waker: Option<Waker>,
}

struct SemaphoreState {
    available: usize,
    closed: bool,
    /// Waiting tasks, oldest first.
    waiters: BTreeMap<u64, Waiter>,
    next_waiter: u64,
}

impl SemaphoreState {
    /// The waker of the task that has waited longest, if what is available
    /// now covers its request and it has not been woken already.
    fn next_ready(&mut self) -> Option<Waker> {
        let mut first = self.waiters.first_entry()?;
        let waiter = first.get_mut();
        if waiter.need <= self.available {
            waiter.waker.take()
        } else {
            None
        }
    }

    /// Takes a waiter out of the queue, returning its waker to be dropped.
    fn withdraw(&mut self, waiter: Option<u64>) -> Option<Waker> {
        self.waiters.remove(&waiter?)?.waker
    }
}

/// Returns permits and wakes the next task they are enough for.
fn release(state: &Local<SemaphoreState>, permits: usize) {
    let waker = state
        .with(|state| {
            state.available = state.available.saturating_add(permits);
            state.next_ready()
        })
        .flatten();
    if let Some(waker) = waker {
        wake_quietly(waker);
    }
}

/// A counter of permits that tasks take and give back, waiting when there
/// are not enough.
///
/// Permits are handed out in the order they were asked for. A request for
/// many permits is not overtaken by later, smaller ones, so it cannot be
/// starved; the price is that the smaller ones wait behind it.
///
/// ```no_run
/// use starfish::sync::Semaphore;
///
/// # async fn serve(listener: starfish::net::TcpListener) -> starfish::Result<()> {
/// // At most 1000 connections are served at a time on this shard.
/// let connections = Semaphore::new(1000);
/// loop {
///     let permit = connections.acquire(1).await?;
///     let stream = listener.accept().await?;
///     starfish::spawn(async move {
///         let _permit = permit;
///         // ... serve `stream` ...
///         # drop(stream);
///     });
/// }
/// # }
/// ```
#[derive(Clone)]
pub struct Semaphore {
    state: Rc<Local<SemaphoreState>>,
}

impl Semaphore {
    /// A semaphore holding `permits` permits.
    pub fn new(permits: usize) -> Semaphore {
        Semaphore {
            state: Rc::new(Local::new(SemaphoreState {
                available: permits,
                closed: false,
                waiters: BTreeMap::new(),
                next_waiter: 0,
            })),
        }
    }

    /// How many permits are free right now.
    pub fn available(&self) -> usize {
        self.state.with(|state| state.available).unwrap_or(0)
    }

    /// Whether [`close`](Self::close) has been called.
    pub fn is_closed(&self) -> bool {
        self.state.with(|state| state.closed).unwrap_or(true)
    }

    /// Takes `permits` permits if that is possible without waiting. `None`
    /// if there are too few, if other tasks are already waiting, or if the
    /// semaphore is closed.
    pub fn try_acquire(&self, permits: usize) -> Option<Permit> {
        let taken = self
            .state
            .with(|state| {
                if state.closed || !state.waiters.is_empty() || state.available < permits {
                    return false;
                }
                state.available = state.available.saturating_sub(permits);
                true
            })
            .unwrap_or(false);
        taken.then(|| Permit {
            state: self.state.clone(),
            permits,
        })
    }

    /// Takes `permits` permits, waiting until they are free and every
    /// earlier request has been served. The permits go back when the
    /// returned [`Permit`] is dropped.
    ///
    /// Dropping the future gives up the place in the queue; no permits are
    /// lost. A request for more permits than the semaphore will ever hold
    /// waits until it is closed.
    ///
    /// # Errors
    ///
    /// [`Error::Closed`] if the semaphore is closed, before or during the
    /// wait.
    pub fn acquire(&self, permits: usize) -> Acquire {
        Acquire {
            state: self.state.clone(),
            permits,
            waiter: None,
        }
    }

    /// Adds permits to the semaphore, beyond those it was created with.
    pub fn add_permits(&self, permits: usize) {
        release(&self.state, permits);
    }

    /// Closes the semaphore: every waiting and future
    /// [`acquire`](Self::acquire) fails with [`Error::Closed`]. Permits
    /// already handed out stay valid.
    pub fn close(&self) {
        let waiters = self.state.with(|state| {
            state.closed = true;
            std::mem::take(&mut state.waiters)
        });
        for (_, waiter) in waiters.unwrap_or_default() {
            if let Some(waker) = waiter.waker {
                wake_quietly(waker);
            }
        }
    }
}

/// Permits taken from a [`Semaphore`]. Dropping it gives them back.
pub struct Permit {
    state: Rc<Local<SemaphoreState>>,
    permits: usize,
}

impl Permit {
    /// How many permits this holds.
    pub fn count(&self) -> usize {
        self.permits
    }

    /// Keeps the permits for good: the semaphore permanently holds that
    /// many fewer.
    pub fn forget(mut self) {
        self.permits = 0;
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        if self.permits > 0 {
            release(&self.state, self.permits);
        }
    }
}

/// Future returned by [`Semaphore::acquire`].
#[must_use = "no permits are taken unless this is awaited"]
pub struct Acquire {
    state: Rc<Local<SemaphoreState>>,
    permits: usize,
    /// This task's place in the queue, once it had to wait.
    waiter: Option<u64>,
}

/// What one poll of [`Acquire`] found. The wakers are handed out of the
/// semaphore to be dropped or woken once it is no longer borrowed.
enum Acquired {
    Taken {
        replaced: Option<Waker>,
        next: Option<Waker>,
    },
    Closed {
        replaced: Option<Waker>,
    },
    Waiting {
        waiter: u64,
        replaced: Option<Waker>,
    },
}

impl Future for Acquire {
    type Output = Result<Permit>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut waker = Some(cx.waker().clone());
        let waiter = self.waiter;
        let permits = self.permits;
        let outcome = self.state.with(|state| {
            if state.closed {
                return Acquired::Closed {
                    replaced: state.withdraw(waiter),
                };
            }
            let first = match waiter {
                Some(waiter) => state.waiters.keys().next() == Some(&waiter),
                None => state.waiters.is_empty(),
            };
            if first && state.available >= permits {
                state.available = state.available.saturating_sub(permits);
                let replaced = state.withdraw(waiter);
                // What is left may be enough for the task behind this one.
                return Acquired::Taken {
                    replaced,
                    next: state.next_ready(),
                };
            }
            let waiter = waiter.unwrap_or_else(|| {
                let waiter = state.next_waiter;
                state.next_waiter = state.next_waiter.wrapping_add(1);
                waiter
            });
            let entry = Waiter {
                need: permits,
                waker: waker.take(),
            };
            let replaced = state.waiters.insert(waiter, entry).and_then(|old| old.waker);
            Acquired::Waiting { waiter, replaced }
        });
        match outcome {
            Some(Acquired::Taken { replaced, next }) => {
                drop(replaced);
                if let Some(next) = next {
                    wake_quietly(next);
                }
                self.waiter = None;
                Poll::Ready(Ok(Permit {
                    state: self.state.clone(),
                    permits,
                }))
            }
            Some(Acquired::Closed { replaced }) => {
                drop(replaced);
                self.waiter = None;
                Poll::Ready(Err(Error::Closed))
            }
            Some(Acquired::Waiting { waiter, replaced }) => {
                drop(replaced);
                self.waiter = Some(waiter);
                Poll::Pending
            }
            None => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

impl Drop for Acquire {
    fn drop(&mut self) {
        let Some(waiter) = self.waiter.take() else { return };
        // If this task was at the head of the queue, perhaps already woken,
        // its turn passes to the one behind it.
        let wakers = self
            .state
            .with(|state| (state.withdraw(Some(waiter)), state.next_ready()));
        if let Some((own, next)) = wakers {
            drop(own);
            if let Some(next) = next {
                wake_quietly(next);
            }
        }
    }
}

struct GateState {
    /// Guards alive.
    active: usize,
    closed: bool,
    /// Tasks waiting in [`Gate::close`].
    closers: BTreeMap<u64, Waker>,
    next_closer: u64,
}

/// Counts work in progress, so that shutdown can wait for it.
///
/// Work [`enter`](Self::enter)s the gate and holds the guard until it is
/// done. [`close`](Self::close) turns new work away and completes when the
/// last guard is gone. A closed gate stays closed.
///
/// ```no_run
/// use starfish::sync::Gate;
///
/// # async fn example() -> starfish::Result<()> {
/// let gate = Gate::new();
///
/// let guard = gate.enter()?;
/// starfish::spawn(async move {
///     let _guard = guard;
///     // ... handle one request ...
/// });
///
/// // On shutdown: no new requests, and wait for those in flight.
/// gate.close().await;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct Gate {
    state: Rc<Local<GateState>>,
}

impl Default for Gate {
    fn default() -> Self {
        Gate::new()
    }
}

impl Gate {
    /// An open gate with nothing inside.
    pub fn new() -> Gate {
        Gate {
            state: Rc::new(Local::new(GateState {
                active: 0,
                closed: false,
                closers: BTreeMap::new(),
                next_closer: 0,
            })),
        }
    }

    /// Registers a piece of work, which lasts until the guard is dropped.
    ///
    /// # Errors
    ///
    /// [`Error::Closed`] if the gate has been closed.
    pub fn enter(&self) -> Result<GateGuard> {
        let entered = self
            .state
            .with(|state| {
                if state.closed {
                    return false;
                }
                state.active = state.active.saturating_add(1);
                true
            })
            .unwrap_or(false);
        if entered {
            Ok(GateGuard {
                state: self.state.clone(),
            })
        } else {
            Err(Error::Closed)
        }
    }

    /// How many guards are alive.
    pub fn active(&self) -> usize {
        self.state.with(|state| state.active).unwrap_or(0)
    }

    /// Whether [`close`](Self::close) has been called.
    pub fn is_closed(&self) -> bool {
        self.state.with(|state| state.closed).unwrap_or(true)
    }

    /// Closes the gate and returns a future that completes when every
    /// guard has been dropped.
    ///
    /// The gate is closed by this call, not by awaiting the future:
    /// [`enter`](Self::enter) fails from here on. It may be called more
    /// than once, and every returned future completes.
    ///
    /// A task that awaits this while holding a guard of the same gate
    /// waits for itself, forever.
    pub fn close(&self) -> GateClose {
        self.state.with(|state| state.closed = true);
        GateClose {
            state: self.state.clone(),
            closer: None,
        }
    }
}

/// Keeps a [`Gate`] from finishing its [`close`](Gate::close).
pub struct GateGuard {
    state: Rc<Local<GateState>>,
}

impl Drop for GateGuard {
    fn drop(&mut self) {
        let closers = self.state.with(|state| {
            state.active = state.active.saturating_sub(1);
            if state.active == 0 && state.closed {
                std::mem::take(&mut state.closers)
            } else {
                BTreeMap::new()
            }
        });
        for (_, waker) in closers.unwrap_or_default() {
            wake_quietly(waker);
        }
    }
}

/// Future returned by [`Gate::close`].
#[must_use = "the gate is closed already, but only awaiting this waits for the work inside it"]
pub struct GateClose {
    state: Rc<Local<GateState>>,
    /// This task's entry among the closers, once it had to wait.
    closer: Option<u64>,
}

impl Future for GateClose {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut waker = Some(cx.waker().clone());
        let closer = self.closer;
        // `Ok` when the gate is empty, `Err` with this task's entry when it
        // is not; either way with the waker this poll replaced.
        let outcome = self.state.with(|state| {
            if state.active == 0 {
                return (Ok(()), closer.and_then(|closer| state.closers.remove(&closer)));
            }
            let closer = closer.unwrap_or_else(|| {
                let closer = state.next_closer;
                state.next_closer = state.next_closer.wrapping_add(1);
                closer
            });
            let replaced = waker.take().and_then(|waker| state.closers.insert(closer, waker));
            (Err(closer), replaced)
        });
        match outcome {
            Some((Ok(()), replaced)) => {
                drop(replaced);
                self.closer = None;
                Poll::Ready(())
            }
            Some((Err(closer), replaced)) => {
                drop(replaced);
                self.closer = Some(closer);
                Poll::Pending
            }
            None => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

impl Drop for GateClose {
    fn drop(&mut self) {
        let Some(closer) = self.closer.take() else { return };
        let waker = self.state.with(|state| state.closers.remove(&closer));
        drop(waker);
    }
}

struct ChannelState<T> {
    items: VecDeque<T>,
    /// The receiving task, if it is waiting for a value.
    receiver: Option<Waker>,
    /// Senders alive.
    senders: usize,
    /// The receiver has closed the channel or is gone.
    closed: bool,
}

struct Channel<T> {
    state: Local<ChannelState<T>>,
    /// One permit for every free place in the queue. Senders take one per
    /// value and the receiver puts it back, so senders wait in order.
    slots: Semaphore,
}

/// Creates a queue from any number of senders to one receiver, holding at
/// most `capacity` values (at least one).
///
/// When the queue is full, [`Sender::send`] waits, which slows producers
/// down to the pace of the consumer. Values arrive in the order they were
/// sent, and waiting senders are served in the order they started waiting.
///
/// ```no_run
/// # async fn example() {
/// let (sender, mut receiver) = starfish::sync::channel(16);
/// starfish::spawn(async move {
///     for request in 0..100 {
///         if sender.send(request).await.is_err() {
///             break; // the receiver is gone
///         }
///     }
/// });
/// // Ends when every sender has been dropped.
/// while let Some(request) = receiver.recv().await {
///     println!("{request}");
/// }
/// # }
/// ```
pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let channel = Rc::new(Channel {
        state: Local::new(ChannelState {
            items: VecDeque::new(),
            receiver: None,
            senders: 1,
            closed: false,
        }),
        slots: Semaphore::new(capacity.max(1)),
    });
    let sender = Sender {
        channel: channel.clone(),
    };
    (sender, Receiver { channel })
}

/// The sending side of a [`channel`]. Clone it to send from several tasks.
pub struct Sender<T> {
    channel: Rc<Channel<T>>,
}

impl<T> Sender<T> {
    /// Queues `value`, waiting for room if the queue is full.
    ///
    /// Dropping the future before it completes drops the value without
    /// sending it.
    ///
    /// # Errors
    ///
    /// Hands the value back if the receiver has closed the channel or been
    /// dropped, before or during the wait.
    pub async fn send(&self, value: T) -> std::result::Result<(), SendError<T>> {
        match self.channel.slots.acquire(1).await {
            Ok(slot) => self.push(value, slot).map_err(SendError),
            Err(_) => Err(SendError(value)),
        }
    }

    /// Queues `value` if there is room right now.
    ///
    /// # Errors
    ///
    /// Hands the value back as [`TrySendError::Full`] if the queue is full
    /// or other senders are already waiting for room, and as
    /// [`TrySendError::Closed`] if the receiver has closed the channel or
    /// been dropped.
    pub fn try_send(&self, value: T) -> std::result::Result<(), TrySendError<T>> {
        match self.channel.slots.try_acquire(1) {
            Some(slot) => self.push(value, slot).map_err(TrySendError::Closed),
            None if self.channel.slots.is_closed() => Err(TrySendError::Closed(value)),
            None => Err(TrySendError::Full(value)),
        }
    }

    /// Whether the receiver has closed the channel or been dropped.
    pub fn is_closed(&self) -> bool {
        self.channel.slots.is_closed()
    }

    /// Puts a value into the place `slot` stands for.
    fn push(&self, value: T, slot: Permit) -> std::result::Result<(), T> {
        let mut value = Some(value);
        let receiver = self.channel.state.with(|state| {
            if let Some(value) = value.take() {
                state.items.push_back(value);
            }
            state.receiver.take()
        });
        match value {
            // The queue could not be reached; the slot goes back.
            Some(value) => Err(value),
            None => {
                // The receiver returns the slot when it takes the value.
                slot.forget();
                if let Some(receiver) = receiver.flatten() {
                    wake_quietly(receiver);
                }
                Ok(())
            }
        }
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.channel
            .state
            .with(|state| state.senders = state.senders.saturating_add(1));
        Sender {
            channel: self.channel.clone(),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        // With the last sender gone, a waiting receiver has to be told that
        // nothing more will come.
        let receiver = self.channel.state.with(|state| {
            state.senders = state.senders.saturating_sub(1);
            if state.senders == 0 {
                state.receiver.take()
            } else {
                None
            }
        });
        if let Some(receiver) = receiver.flatten() {
            wake_quietly(receiver);
        }
    }
}

/// The receiving side of a [`channel`]. Dropping it closes the channel and
/// drops the values still queued.
pub struct Receiver<T> {
    channel: Rc<Channel<T>>,
}

impl<T> Receiver<T> {
    /// Takes the next value, waiting for one if the queue is empty.
    /// `None` means no value will ever come: the queue is empty and every
    /// sender has been dropped, or the channel was closed.
    ///
    /// Dropping the future before it completes loses nothing.
    pub async fn recv(&mut self) -> Option<T> {
        poll_fn(|cx| self.poll_recv(cx)).await
    }

    /// Takes the next value if one is queued. `None` only says that none
    /// is queued right now.
    pub fn try_recv(&mut self) -> Option<T> {
        let value = self.channel.state.with(|state| state.items.pop_front()).flatten();
        if value.is_some() {
            self.channel.slots.add_permits(1);
        }
        value
    }

    /// How many values are queued.
    pub fn len(&self) -> usize {
        self.channel.state.with(|state| state.items.len()).unwrap_or(0)
    }

    /// Whether the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Stops senders from queueing more: every waiting and future send
    /// fails. Values already queued can still be received.
    pub fn close(&mut self) {
        self.channel.state.with(|state| state.closed = true);
        self.channel.slots.close();
    }

    fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let mut waker = Some(cx.waker().clone());
        let outcome = self.channel.state.with(|state| {
            if let Some(value) = state.items.pop_front() {
                return (Poll::Ready(Some(value)), None);
            }
            if state.senders == 0 || state.closed {
                return (Poll::Ready(None), None);
            }
            (Poll::Pending, std::mem::replace(&mut state.receiver, waker.take()))
        });
        match outcome {
            Some((polled, replaced)) => {
                drop(replaced);
                if matches!(polled, Poll::Ready(Some(_))) {
                    // There is room for one more.
                    self.channel.slots.add_permits(1);
                }
                polled
            }
            None => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.close();
        // Nobody can receive these any more. They are dropped here, not
        // when the last sender goes.
        let leftover = self.channel.state.with(|state| {
            (std::mem::take(&mut state.items), state.receiver.take())
        });
        drop(leftover);
    }
}

/// Error from [`Sender::send`]: the channel is closed. Holds the value that
/// was not sent.
pub struct SendError<T>(pub T);

/// Error from [`Sender::try_send`]. Holds the value that was not sent.
pub enum TrySendError<T> {
    /// The queue is full, or other senders are waiting for room.
    Full(T),
    /// The receiver has closed the channel or been dropped.
    Closed(T),
}

impl<T> TrySendError<T> {
    /// The value that was not sent.
    pub fn into_inner(self) -> T {
        match self {
            TrySendError::Full(value) | TrySendError::Closed(value) => value,
        }
    }
}

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SendError(..)")
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("channel is closed")
    }
}

impl<T> std::error::Error for SendError<T> {}

impl<T> fmt::Debug for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            TrySendError::Full(_) => "Full(..)",
            TrySendError::Closed(_) => "Closed(..)",
        })
    }
}

impl<T> fmt::Display for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            TrySendError::Full(_) => "channel is full",
            TrySendError::Closed(_) => "channel is closed",
        })
    }
}

impl<T> std::error::Error for TrySendError<T> {}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic, clippy::arithmetic_side_effects)]
mod tests {
    //! Driven by hand, without a runtime, so that Miri can run them.

    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::task::Wake;

    /// Counts how often it is woken.
    struct Wakes(AtomicUsize);

    impl Wake for Wakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    impl Wakes {
        fn count(&self) -> usize {
            self.0.load(Ordering::Relaxed)
        }
    }

    fn counting_waker() -> (Arc<Wakes>, Waker) {
        let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
        (wakes.clone(), Waker::from(wakes))
    }

    fn poll<F: Future + Unpin>(future: &mut F, waker: &Waker) -> Poll<F::Output> {
        Pin::new(future).poll(&mut Context::from_waker(waker))
    }

    #[test]
    fn permits_go_out_in_the_order_they_were_asked_for() {
        let semaphore = Semaphore::new(2);
        let one = semaphore.try_acquire(1).unwrap();
        let two = semaphore.try_acquire(1).unwrap();
        assert_eq!(semaphore.available(), 0);
        assert!(semaphore.try_acquire(1).is_none());

        let (big_wakes, big_waker) = counting_waker();
        let (small_wakes, small_waker) = counting_waker();
        let mut big = semaphore.acquire(2);
        let mut small = semaphore.acquire(1);
        assert!(poll(&mut big, &big_waker).is_pending());
        assert!(poll(&mut small, &small_waker).is_pending());

        // One permit is not enough for the request at the head of the
        // queue, and nobody may overtake it.
        drop(one);
        assert_eq!(semaphore.available(), 1);
        assert_eq!((big_wakes.count(), small_wakes.count()), (0, 0));
        assert!(poll(&mut small, &small_waker).is_pending());
        assert!(semaphore.try_acquire(1).is_none());

        drop(two);
        assert_eq!((big_wakes.count(), small_wakes.count()), (1, 0));
        let Poll::Ready(Ok(permit)) = poll(&mut big, &big_waker) else { panic!("not ready") };
        assert_eq!(permit.count(), 2);
        assert_eq!(semaphore.available(), 0);

        drop(permit);
        assert_eq!(small_wakes.count(), 1);
        let Poll::Ready(Ok(permit)) = poll(&mut small, &small_waker) else { panic!("not ready") };
        assert_eq!(semaphore.available(), 1);
        drop(permit);
        assert_eq!(semaphore.available(), 2);
    }

    #[test]
    fn one_release_serves_every_request_it_covers() {
        let semaphore = Semaphore::new(0);
        let (first_wakes, first_waker) = counting_waker();
        let (second_wakes, second_waker) = counting_waker();
        let mut first = semaphore.acquire(1);
        let mut second = semaphore.acquire(1);
        assert!(poll(&mut first, &first_waker).is_pending());
        assert!(poll(&mut second, &second_waker).is_pending());

        semaphore.add_permits(2);
        // Only the head of the queue is woken; it passes the rest on.
        assert_eq!((first_wakes.count(), second_wakes.count()), (1, 0));
        let Poll::Ready(Ok(first)) = poll(&mut first, &first_waker) else { panic!("not ready") };
        assert_eq!(second_wakes.count(), 1);
        let Poll::Ready(Ok(second)) = poll(&mut second, &second_waker) else { panic!("not ready") };
        assert_eq!(semaphore.available(), 0);
        drop((first, second));
        assert_eq!(semaphore.available(), 2);
    }

    #[test]
    fn a_waiter_that_gives_up_passes_its_turn_on() {
        let semaphore = Semaphore::new(1);
        let held = semaphore.try_acquire(1).unwrap();
        let (first_wakes, first_waker) = counting_waker();
        let (second_wakes, second_waker) = counting_waker();
        let mut first = semaphore.acquire(1);
        let mut second = semaphore.acquire(1);
        assert!(poll(&mut first, &first_waker).is_pending());
        assert!(poll(&mut second, &second_waker).is_pending());

        drop(held);
        assert_eq!((first_wakes.count(), second_wakes.count()), (1, 0));
        // Woken, but dropped before it ran: the permit is not lost.
        drop(first);
        assert_eq!(second_wakes.count(), 1);
        let Poll::Ready(Ok(permit)) = poll(&mut second, &second_waker) else { panic!("not ready") };
        drop(permit);
        assert_eq!(semaphore.available(), 1);

        // Giving up further back in the queue disturbs nobody.
        let held = semaphore.try_acquire(1).unwrap();
        let mut first = semaphore.acquire(1);
        let mut second = semaphore.acquire(1);
        assert!(poll(&mut first, &first_waker).is_pending());
        assert!(poll(&mut second, &second_waker).is_pending());
        drop(second);
        assert_eq!(first_wakes.count(), 1);
        drop(held);
        assert_eq!(first_wakes.count(), 2);
        assert!(poll(&mut first, &first_waker).is_ready());
    }

    #[test]
    fn closing_fails_waiters_and_later_requests() {
        let semaphore = Semaphore::new(1);
        let held = semaphore.try_acquire(1).unwrap();
        let (wakes, waker) = counting_waker();
        let mut waiting = semaphore.acquire(1);
        assert!(poll(&mut waiting, &waker).is_pending());

        semaphore.close();
        assert!(semaphore.is_closed());
        assert_eq!(wakes.count(), 1);
        assert!(matches!(poll(&mut waiting, &waker), Poll::Ready(Err(Error::Closed))));
        assert!(matches!(poll(&mut semaphore.acquire(1), &waker), Poll::Ready(Err(Error::Closed))));

        // A permit from before still goes back, to no effect.
        drop(held);
        assert_eq!(semaphore.available(), 1);
        assert!(semaphore.try_acquire(1).is_none());
    }

    #[test]
    fn forgotten_permits_stay_gone() {
        let semaphore = Semaphore::new(3);
        semaphore.try_acquire(2).unwrap().forget();
        assert_eq!(semaphore.available(), 1);
        semaphore.add_permits(4);
        assert_eq!(semaphore.available(), 5);
        // A clone is the same semaphore.
        let _permit = semaphore.clone().try_acquire(5).unwrap();
        assert_eq!(semaphore.available(), 0);
    }

    #[test]
    fn closing_a_gate_waits_for_everything_inside() {
        let gate = Gate::new();
        let first = gate.enter().unwrap();
        let second = gate.clone().enter().unwrap();
        assert_eq!(gate.active(), 2);
        assert!(!gate.is_closed());

        let (wakes, waker) = counting_waker();
        let (other_wakes, other_waker) = counting_waker();
        let mut close = gate.close();
        // Closed by the call itself.
        assert!(gate.is_closed());
        assert!(matches!(gate.enter(), Err(Error::Closed)));
        assert!(poll(&mut close, &waker).is_pending());
        let mut other = gate.close();
        assert!(poll(&mut other, &other_waker).is_pending());

        drop(first);
        assert_eq!((wakes.count(), other_wakes.count()), (0, 0));
        assert!(poll(&mut close, &waker).is_pending());
        drop(second);
        assert_eq!((wakes.count(), other_wakes.count()), (1, 1));
        assert!(poll(&mut close, &waker).is_ready());
        assert!(poll(&mut other, &other_waker).is_ready());
        assert_eq!(gate.active(), 0);
    }

    #[test]
    fn an_empty_gate_closes_at_once_and_a_closer_may_give_up() {
        let gate = Gate::new();
        let (wakes, waker) = counting_waker();
        assert!(poll(&mut gate.close(), &waker).is_ready());
        assert!(matches!(gate.enter(), Err(Error::Closed)));

        let gate = Gate::new();
        let guard = gate.enter().unwrap();
        let mut close = gate.close();
        assert!(poll(&mut close, &waker).is_pending());
        drop(close);
        drop(guard);
        assert_eq!(wakes.count(), 0);
        assert!(poll(&mut gate.close(), &waker).is_ready());
    }

    fn poll_pinned<F: Future>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(waker))
    }

    #[test]
    fn a_full_channel_makes_senders_wait_their_turn() {
        let (sender, mut receiver) = channel(2);
        let other = sender.clone();
        sender.try_send(1).unwrap();
        sender.try_send(2).unwrap();
        assert!(matches!(sender.try_send(9), Err(TrySendError::Full(9))));
        assert_eq!(receiver.len(), 2);

        let (third_wakes, third_waker) = counting_waker();
        let (fourth_wakes, fourth_waker) = counting_waker();
        let mut third = std::pin::pin!(sender.send(3));
        let mut fourth = std::pin::pin!(other.send(4));
        assert!(poll_pinned(third.as_mut(), &third_waker).is_pending());
        assert!(poll_pinned(fourth.as_mut(), &fourth_waker).is_pending());

        // Room for one: it goes to the sender that has waited longest, and
        // nobody slips in ahead of it.
        assert_eq!(receiver.try_recv(), Some(1));
        assert_eq!((third_wakes.count(), fourth_wakes.count()), (1, 0));
        assert!(matches!(sender.try_send(9), Err(TrySendError::Full(9))));
        assert!(poll_pinned(fourth.as_mut(), &fourth_waker).is_pending());
        assert!(matches!(poll_pinned(third.as_mut(), &third_waker), Poll::Ready(Ok(()))));

        assert_eq!(receiver.try_recv(), Some(2));
        assert_eq!(fourth_wakes.count(), 1);
        assert!(matches!(poll_pinned(fourth.as_mut(), &fourth_waker), Poll::Ready(Ok(()))));
        assert_eq!(receiver.try_recv(), Some(3));
        assert_eq!(receiver.try_recv(), Some(4));
        assert_eq!(receiver.try_recv(), None);
        assert!(receiver.is_empty());
    }

    #[test]
    fn the_receiver_waits_for_values_and_learns_when_no_more_will_come() {
        let (sender, mut receiver) = channel::<u32>(0);
        let (wakes, waker) = counting_waker();
        {
            let mut receive = std::pin::pin!(receiver.recv());
            assert!(poll_pinned(receive.as_mut(), &waker).is_pending());
            // A capacity of zero still holds one value.
            sender.try_send(5).unwrap();
            assert_eq!(wakes.count(), 1);
            assert!(matches!(poll_pinned(receive.as_mut(), &waker), Poll::Ready(Some(5))));
        }
        {
            let mut receive = std::pin::pin!(receiver.recv());
            assert!(poll_pinned(receive.as_mut(), &waker).is_pending());
            let other = sender.clone();
            drop(sender);
            assert_eq!(wakes.count(), 1);
            other.try_send(6).unwrap();
            drop(other);
            // What was sent before the last sender left still arrives.
            assert!(matches!(poll_pinned(receive.as_mut(), &waker), Poll::Ready(Some(6))));
        }
        let mut receive = std::pin::pin!(receiver.recv());
        assert!(matches!(poll_pinned(receive.as_mut(), &waker), Poll::Ready(None)));
    }

    #[test]
    fn the_last_sender_leaving_wakes_the_receiver() {
        let (sender, mut receiver) = channel::<u32>(1);
        let (wakes, waker) = counting_waker();
        let mut receive = std::pin::pin!(receiver.recv());
        assert!(poll_pinned(receive.as_mut(), &waker).is_pending());
        drop(sender);
        assert_eq!(wakes.count(), 1);
        assert!(matches!(poll_pinned(receive.as_mut(), &waker), Poll::Ready(None)));
    }

    #[test]
    fn closing_hands_values_back_to_senders() {
        let (sender, mut receiver) = channel(1);
        sender.try_send(Rc::new(1)).unwrap();
        let (wakes, waker) = counting_waker();
        let waiting_value = Rc::new(2);
        let mut waiting = std::pin::pin!(sender.send(waiting_value.clone()));
        assert!(poll_pinned(waiting.as_mut(), &waker).is_pending());

        receiver.close();
        assert!(sender.is_closed());
        assert_eq!(wakes.count(), 1);
        let Poll::Ready(Err(SendError(returned))) = poll_pinned(waiting.as_mut(), &waker) else {
            panic!("not refused")
        };
        assert!(Rc::ptr_eq(&returned, &waiting_value));
        assert!(matches!(sender.try_send(Rc::new(3)), Err(TrySendError::Closed(_))));

        // What was queued before the close is still there.
        let Some(queued) = receiver.try_recv() else { panic!("lost") };
        assert_eq!(*queued, 1);
        let mut receive = std::pin::pin!(receiver.recv());
        assert!(matches!(poll_pinned(receive.as_mut(), &waker), Poll::Ready(None)));
    }

    #[test]
    fn dropping_the_receiver_drops_what_is_queued() {
        let (sender, receiver) = channel(4);
        let value = Rc::new(());
        sender.try_send(value.clone()).unwrap();
        sender.try_send(value.clone()).unwrap();
        assert_eq!(Rc::strong_count(&value), 3);
        drop(receiver);
        assert_eq!(Rc::strong_count(&value), 1);
        let (_, waker) = counting_waker();
        let mut send = std::pin::pin!(sender.send(value.clone()));
        assert!(matches!(poll_pinned(send.as_mut(), &waker), Poll::Ready(Err(_))));
        assert_eq!(TrySendError::Full(7).into_inner(), 7);
    }
}
