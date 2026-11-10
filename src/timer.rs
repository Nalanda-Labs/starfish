//! Per-shard timers.

use std::collections::BTreeMap;
use std::fmt;
use std::future::{poll_fn, Future};
use std::marker::PhantomData;
use std::pin::{pin, Pin};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use crate::runtime::try_current;

#[derive(Default)]
pub(crate) struct Timers {
    next_id: u64,
    entries: BTreeMap<(Instant, u64), Waker>,
}

impl Timers {
    pub(crate) fn next_deadline(&self) -> Option<Instant> {
        self.entries.keys().next().map(|(deadline, _)| *deadline)
    }

    /// Removes and returns the wakers of every timer due at `now`.
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

    /// Arms a timer, or replaces the waker of an armed one. Returns the
    /// timer's id and the waker that was replaced, for the caller to drop.
    fn register(&mut self, deadline: Instant, id: Option<u64>, waker: Waker) -> (u64, Option<Waker>) {
        let id = id.unwrap_or_else(|| {
            let id = self.next_id;
            self.next_id = self.next_id.wrapping_add(1);
            id
        });
        let previous = self.entries.insert((deadline, id), waker);
        (id, previous)
    }

    fn remove(&mut self, deadline: Instant, id: u64) -> Option<Waker> {
        self.entries.remove(&(deadline, id))
    }

    /// Empties the timer set, handing the wakers to the caller to drop.
    pub(crate) fn take_all(&mut self) -> BTreeMap<(Instant, u64), Waker> {
        std::mem::take(&mut self.entries)
    }
}

/// Future returned by [`sleep`].
pub struct Sleep {
    /// `None` if the deadline is too far away to represent: sleeps forever.
    deadline: Option<Instant>,
    id: Option<u64>,
    /// The timer lives in the shard that first polled it.
    _not_send: PhantomData<*const ()>,
}

impl Sleep {
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
    type Output = ();

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
    fn drop(&mut self) {
        self.disarm();
    }
}

/// Completes after `duration`.
pub fn sleep(duration: Duration) -> Sleep {
    Sleep {
        deadline: Instant::now().checked_add(duration),
        id: None,
        _not_send: PhantomData,
    }
}

/// Error returned by [`timeout`] when the time ran out first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Elapsed;

impl fmt::Display for Elapsed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("deadline elapsed")
    }
}

impl std::error::Error for Elapsed {}

/// Runs `future` for at most `duration`. On timeout the future is dropped,
/// which cancels any I/O it had in flight.
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
