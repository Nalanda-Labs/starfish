//! Per-shard timers.

use std::collections::BTreeMap;
use std::fmt;
use std::future::{poll_fn, Future};
use std::marker::PhantomData;
use std::pin::{pin, Pin};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use crate::runtime::try_current;

/// The timers of one shard: every pending [`Sleep`] on it.
///
/// Timers are kept in a map ordered by deadline, so finding the next one to
/// fire, and all that are due, is cheap. The key is the deadline together with
/// a number unique to the timer, which lets any number of timers share a
/// deadline and lets one timer be found again to be re-armed or removed. The
/// value is the waker of the task waiting for it.
///
/// Each shard has one of these, behind its `Local` cell. It holds wakers,
/// which are values supplied from outside the runtime, so nothing in here
/// calls, clones or drops one: they are handed out to the caller to deal with
/// once the cell is no longer borrowed.
#[derive(Default)]
pub(crate) struct Timers {
    next_id: u64,
    entries: BTreeMap<(Instant, u64), Waker>,
}

impl Timers {
    /// The earliest deadline among the pending timers, or `None` if there are
    /// none. The event loop uses it to decide how long it may sleep.
    pub(crate) fn next_deadline(&self) -> Option<Instant> {
        self.entries.keys().next().map(|(deadline, _)| *deadline)
    }

    /// Removes every timer whose deadline is at or before `now` and returns
    /// their wakers, earliest first.
    ///
    /// The caller wakes them after it has let go of the timer set, since waking
    /// runs code that may arm new timers.
    pub(crate) fn expired(&mut self, now: Instant) -> Vec<Waker> {
        let mut wakers = Vec::new();
        while let Some(entry) = self.entries.first_entry() {
            if entry.key().0 > now {
                break;
            }
            wakers.push(entry.remove());
        }
        wakers
    }

    /// Arms a timer, or replaces the waker of one that is already armed.
    ///
    /// `id` is `None` for a timer being armed for the first time, in which case a
    /// fresh number is assigned, and `Some` for one that is polled again, which
    /// keeps its place. Returns the timer's number, to be remembered by the
    /// [`Sleep`], and the waker that was replaced, if any, for the caller to drop
    /// outside the borrow of the timer set.
    fn register(&mut self, deadline: Instant, id: Option<u64>, waker: Waker) -> (u64, Option<Waker>) {
        let id = id.unwrap_or_else(|| {
            let id = self.next_id;
            self.next_id = self.next_id.wrapping_add(1);
            id
        });
        let previous = self.entries.insert((deadline, id), waker);
        (id, previous)
    }

    /// Removes one timer, identified by its deadline and number, and returns
    /// its waker for the caller to drop. Returns `None` if the timer is not
    /// there, which is the case once it has fired.
    fn remove(&mut self, deadline: Instant, id: u64) -> Option<Waker> {
        self.entries.remove(&(deadline, id))
    }

    /// Empties the timer set and hands every waker to the caller to drop.
    /// Used when the shard shuts down: the tasks that were sleeping are being
    /// dropped, and their wakers must not outlive the shard's own state.
    pub(crate) fn take_all(&mut self) -> BTreeMap<(Instant, u64), Waker> {
        std::mem::take(&mut self.entries)
    }
}

/// Future returned by [`sleep`]: completes once its deadline has passed.
///
/// A `Sleep` does nothing until it is first polled. Then it registers its
/// deadline with the timer set of the shard it is polled on, and the shard's
/// event loop wakes the task when the deadline passes. It never completes
/// early, and completes late by however long the shard takes to come round
/// to its timers, which on a responsive shard is microseconds and on one
/// held up by a long-running task is as long as that task runs.
///
/// Dropping a `Sleep` disarms its timer, so wrapping one in
/// [`timeout`] or [`select`](crate::select) leaves nothing behind.
///
/// It belongs to the shard that first polled it and cannot be sent to
/// another thread.
#[must_use = "a sleep does nothing unless it is awaited"]
pub struct Sleep {
    /// `None` if the deadline is too far away to represent: sleeps forever.
    deadline: Option<Instant>,
    id: Option<u64>,
    /// The timer lives in the shard that first polled it.
    _not_send: PhantomData<*const ()>,
}

impl Sleep {
    /// Takes this sleep's timer out of the shard's timer set, if it is in
    /// there. Called when the sleep completes or is dropped, so that the set
    /// never holds a waker for a sleep that no longer exists.
    fn disarm(&mut self) {
        let (Some(deadline), Some(id)) = (self.deadline, self.id.take()) else {
            return;
        };
        if let Some(shard) = try_current() {
            let waker = shard.timers.with(|timers| timers.remove(deadline, id));
            drop(waker);
        }
    }
}

impl Future for Sleep {
    /// A sleep produces no value; it only takes time.
    type Output = ();

    /// Completes if the deadline has passed; otherwise arms (or re-arms) the
    /// timer with the current task's waker and stays pending.
    ///
    /// The clock is read on every poll, so a sleep that is polled for another
    /// reason after its deadline completes then, without waiting for the timer.
    /// Outside a runtime there is no timer set to arm; the sleep then asks to be
    /// polled again at once, which makes it end on time at the cost of keeping
    /// its thread busy.
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let Some(deadline) = self.deadline else {
            return Poll::Pending;
        };
        if Instant::now() >= deadline {
            self.disarm();
            return Poll::Ready(());
        }
        let waker = cx.waker().clone();
        let id = self.id;
        let registered = try_current()
            .and_then(|shard| shard.timers.with(|timers| timers.register(deadline, id, waker)));
        match registered {
            Some((id, previous)) => {
                self.id = Some(id);
                drop(previous);
            }
            // Outside a runtime there is no timer to arm. Ask to be polled
            // again, so the sleep still ends on time.
            None => cx.waker().wake_by_ref(),
        }
        Poll::Pending
    }
}

impl Drop for Sleep {
    /// Disarms the timer, so that the shard does not wake a task for a sleep
    /// that is gone.
    fn drop(&mut self) {
        self.disarm();
    }
}

/// Returns a future that completes after `duration`.
///
/// ```no_run
/// use std::time::Duration;
///
/// # async fn example() {
/// starfish::sleep(Duration::from_millis(10)).await;
/// # }
/// ```
///
/// The deadline is fixed when this function is called, not when the future is
/// first awaited, so time spent between the two counts towards the sleep. A
/// zero duration completes on the first poll. A duration too long to be added
/// to the current time never completes.
///
/// For something that repeats at a fixed rate, see
/// [`interval`](crate::interval), which does not drift.
pub fn sleep(duration: Duration) -> Sleep {
    Sleep {
        deadline: Instant::now().checked_add(duration),
        id: None,
        _not_send: PhantomData,
    }
}

/// Error returned by [`timeout`] when the time ran out before the future
/// finished. It carries no data: the future it was waiting for has been
/// dropped, and whatever that future had done so far is described under
/// [`timeout`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Elapsed;

impl fmt::Display for Elapsed {
    /// Writes "deadline elapsed".
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("deadline elapsed")
    }
}

impl std::error::Error for Elapsed {}

/// Runs `future` for at most `duration`.
///
/// Returns `Ok` with the future's output if it finishes in time, and
/// `Err(Elapsed)` otherwise.
///
/// ```no_run
/// use std::time::Duration;
/// use starfish::net::TcpStream;
/// use starfish::timeout;
///
/// # async fn example(addr: std::net::SocketAddr) -> std::io::Result<()> {
/// match timeout(Duration::from_secs(3), TcpStream::connect(addr)).await {
///     Ok(connected) => drop(connected?),
///     Err(_) => println!("no answer within three seconds"),
/// }
/// # Ok(())
/// # }
/// ```
///
/// On timeout the future is dropped, which is how it is stopped. For futures
/// that wait on the library's own things (a channel, a permit, a timer) that
/// is exact and loses nothing. For I/O it is a cancellation: the kernel is
/// asked to stop the operation, but it may already have happened in part or
/// in full. After a timed-out read or write on a socket, assume nothing about
/// the state of the stream and close it; the exception is reading through a
/// [`BufReader`](crate::io::BufReader), which keeps the read alive.
///
/// The future is polled before the timer, so one that completes at the very
/// moment the time runs out is reported as completed.
pub async fn timeout<F: Future>(
    duration: Duration,
    future: F,
) -> std::result::Result<F::Output, Elapsed> {
    let mut future = pin!(future);
    let mut sleep = pin!(sleep(duration));
    poll_fn(|cx| {
        if let Poll::Ready(value) = future.as_mut().poll(cx) {
            return Poll::Ready(Ok(value));
        }
        if sleep.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(Elapsed));
        }
        Poll::Pending
    })
    .await
}

debug_impl!(Sleep, "deadline" => |sleep| sleep.deadline);