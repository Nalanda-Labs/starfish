//! Waiting for whichever of two things happens first.

use std::future::{poll_fn, Future};
use std::pin::pin;
use std::task::Poll;
use std::time::{Duration, Instant};

use crate::timer::sleep;

/// Which of the two futures given to [`select`] finished, with its output.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Either<A, B> {
    /// The first future finished.
    First(A),
    /// The second future finished.
    Second(B),
}

/// Waits for whichever of two futures finishes first and returns its
/// output.
///
/// ```no_run
/// use std::time::Duration;
/// use starfish::sync::channel;
/// use starfish::{select, sleep, Either};
///
/// # async fn example() {
/// let (_sender, mut inbox) = channel::<String>(16);
/// match select(inbox.recv(), sleep(Duration::from_secs(30))).await {
///     Either::First(Some(message)) => println!("got {message}"),
///     Either::First(None) => println!("no more messages"),
///     Either::Second(()) => println!("nothing for 30 seconds"),
/// }
/// # }
/// ```
///
/// # What happens to the other one
///
/// A future passed by value is dropped the moment the other wins, which
/// cancels what it was waiting for. To keep a future across several calls, pin it
/// and pass a reference; it then carries on where it was:
///
/// ```no_run
/// use std::pin::pin;
/// use std::time::Duration;
/// use starfish::{select, sleep, Either};
///
/// # async fn example(mut job: impl std::future::Future<Output = u32>) {
/// let mut job = pin!(job);
/// loop {
///     match select(job.as_mut(), sleep(Duration::from_secs(1))).await {
///         Either::First(result) => break println!("done: {result}"),
///         Either::Second(()) => println!("still working"),
///     }
/// }
/// # }
/// ```
///
/// Dropping is safe for waits on the library's own things (a channel, a
/// permit, a timer). For I/O it is cancellation, with what that implies: a
/// read or write that loses may still have happened.
///
/// # Order
///
/// The first future is looked at first. If both are ready at the same
/// moment, the first one wins and the second is not looked at; in a loop
/// where the first is always ready, the second never gets a turn. So put
/// the rarer event first: the stop signal before the timer, the timer
/// before the queue of work.
pub async fn select<A: Future, B: Future>(first: A, second: B) -> Either<A::Output, B::Output> {
    let mut first = pin!(first);
    let mut second = pin!(second);
    poll_fn(|cx| {
        if let Poll::Ready(output) = first.as_mut().poll(cx) {
            return Poll::Ready(Either::First(output));
        }
        if let Poll::Ready(output) = second.as_mut().poll(cx) {
            return Poll::Ready(Either::Second(output));
        }
        Poll::Pending
    })
    .await
}

/// Which of the three futures given to [`select3`] finished, with its
/// output.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Either3<A, B, C> {
    /// The first future finished.
    First(A),
    /// The second future finished.
    Second(B),
    /// The third future finished.
    Third(C),
}

/// Waits for whichever of three futures finishes first. Everything said
/// of [`select`] holds: the others are dropped unless passed by reference,
/// and they are looked at in order, so put the rarest event first.
pub async fn select3<A: Future, B: Future, C: Future>(
    first: A,
    second: B,
    third: C,
) -> Either3<A::Output, B::Output, C::Output> {
    let mut first = pin!(first);
    let mut second = pin!(second);
    let mut third = pin!(third);
    poll_fn(|cx| {
        if let Poll::Ready(output) = first.as_mut().poll(cx) {
            return Poll::Ready(Either3::First(output));
        }
        if let Poll::Ready(output) = second.as_mut().poll(cx) {
            return Poll::Ready(Either3::Second(output));
        }
        if let Poll::Ready(output) = third.as_mut().poll(cx) {
            return Poll::Ready(Either3::Third(output));
        }
        Poll::Pending
    })
    .await
}

/// Waits for whichever of a list of like futures finishes first, and
/// returns its place in the list with its output. The others are dropped.
///
/// The futures are looked at in a different order each time, starting one
/// further along, and each call starts where the last left off, so that
/// no position in the list is favoured. An empty list never finishes.
///
/// ```no_run
/// use starfish::net::TcpStream;
///
/// # async fn example(replicas: Vec<std::net::SocketAddr>) {
/// // Whichever replica answers first.
/// let attempts: Vec<_> = replicas.iter().map(|addr| TcpStream::connect(*addr)).collect();
/// let (winner, connection) = starfish::select_all(attempts).await;
/// # drop((winner, connection));
/// # }
/// ```
pub async fn select_all<F: Future>(futures: impl IntoIterator<Item = F>) -> (usize, F::Output) {
    thread_local! {
        /// Where the last call on this thread started looking.
        static TURN: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    }
    // Boxed so that each stays put while the list is walked.
    let mut futures: Vec<_> = futures.into_iter().map(Box::pin).collect();
    // A different starting point for every call as well as for every
    // look: a program that calls this in a loop, with a fresh list each
    // time, would otherwise see the same position win whenever several
    // are ready together.
    let mut start = TURN
        .try_with(|turn| {
            let start = turn.get();
            turn.set(start.wrapping_add(1));
            start
        })
        .unwrap_or(0);
    poll_fn(|cx| {
        let count = futures.len();
        for step in 0..count {
            let index = start.wrapping_add(step).checked_rem(count).unwrap_or(0);
            if let Some(future) = futures.get_mut(index) {
                if let Poll::Ready(output) = future.as_mut().poll(cx) {
                    return Poll::Ready((index, output));
                }
            }
        }
        start = start.wrapping_add(1);
        Poll::Pending
    })
    .await
}

/// Ticks at a fixed rate, without drifting. Made by [`interval`].
pub struct Interval {
    period: Duration,
    next: Instant,
}

impl Interval {
    /// Waits for the next tick.
    ///
    /// Ticks are `period` apart however long the work between them takes,
    /// as long as it takes less than a period. If a tick is missed by a
    /// whole period or more, because the task was busy or the shard was,
    /// the ticks that would have fallen in between are skipped and the
    /// schedule starts again from now: there is never a burst of ticks to
    /// catch up.
    pub async fn tick(&mut self) {
        let now = Instant::now();
        if let Some(wait) = self.next.checked_duration_since(now) {
            sleep(wait).await;
        }
        let due = self.next;
        let after = due.checked_add(self.period).unwrap_or(due);
        let now = Instant::now();
        // More than a period late already: start over from here.
        self.next = if after <= now { now.checked_add(self.period).unwrap_or(now) } else { after };
    }

    /// The time between ticks.
    pub fn period(&self) -> Duration {
        self.period
    }
}

/// Makes a timer that ticks every `period`, the first time one period
/// from now.
///
/// ```no_run
/// use std::time::Duration;
///
/// # async fn example() {
/// let mut every_second = starfish::interval(Duration::from_secs(1));
/// loop {
///     every_second.tick().await;
///     // ... once a second, without drifting ...
/// }
/// # }
/// ```
pub fn interval(period: Duration) -> Interval {
    let now = Instant::now();
    Interval {
        period,
        next: now.checked_add(period).unwrap_or(now),
    }
}

debug_impl!(Interval, "period" => |interval| interval.period);

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic, clippy::arithmetic_side_effects)]
mod tests {
    //! Driven by hand, without a runtime, so that Miri can run them.

    use super::*;
    use std::cell::Cell;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::task::{Context, Waker};

    /// Ready once it has been polled `after` times; counts its polls and
    /// says when it is dropped.
    struct Countdown {
        after: u32,
        polls: Rc<Cell<u32>>,
        dropped: Rc<Cell<bool>>,
    }

    impl Future for Countdown {
        type Output = u32;

        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<u32> {
            self.polls.set(self.polls.get() + 1);
            if self.polls.get() > self.after {
                Poll::Ready(self.polls.get())
            } else {
                Poll::Pending
            }
        }
    }

    impl Drop for Countdown {
        fn drop(&mut self) {
            self.dropped.set(true);
        }
    }

    fn countdown(after: u32) -> (Countdown, Rc<Cell<u32>>, Rc<Cell<bool>>) {
        let polls = Rc::new(Cell::new(0));
        let dropped = Rc::new(Cell::new(false));
        let future = Countdown {
            after,
            polls: polls.clone(),
            dropped: dropped.clone(),
        };
        (future, polls, dropped)
    }

    fn poll<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(Waker::noop()))
    }

    #[test]
    fn the_first_to_finish_wins_and_the_other_is_dropped() {
        let (slow, slow_polls, slow_dropped) = countdown(5);
        let (quick, _, _) = countdown(2);
        let mut race = pin!(select(slow, quick));
        assert!(poll(race.as_mut()).is_pending());
        assert!(poll(race.as_mut()).is_pending());
        assert!(!slow_dropped.get());
        assert_eq!(poll(race.as_mut()), Poll::Ready(Either::Second(3)));
        // Gone the moment the other one won, not when the race is dropped.
        assert!(slow_dropped.get());
        assert_eq!(slow_polls.get(), 3);
    }

    #[test]
    fn when_both_are_ready_the_first_wins_without_the_second_being_looked_at() {
        let (first, _, _) = countdown(0);
        let (second, second_polls, _) = countdown(0);
        let mut race = pin!(select(first, second));
        assert_eq!(poll(race.as_mut()), Poll::Ready(Either::First(1)));
        assert_eq!(second_polls.get(), 0);
    }

    #[test]
    fn a_future_passed_by_reference_carries_on_afterwards() {
        let (long, long_polls, long_dropped) = countdown(3);
        let mut long = pin!(long);
        for round in 1..=3 {
            let (short, _, _) = countdown(0);
            // The long one is looked at first each time, then loses.
            let mut race = pin!(select(long.as_mut(), short));
            assert_eq!(poll(race.as_mut()), Poll::Ready(Either::Second(1)));
            assert_eq!(long_polls.get(), round);
            assert!(!long_dropped.get());
        }
        let (never, _, _) = countdown(u32::MAX);
        let mut race = pin!(select(long.as_mut(), never));
        assert_eq!(poll(race.as_mut()), Poll::Ready(Either::First(4)));
    }
}
