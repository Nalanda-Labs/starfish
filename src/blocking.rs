//! Running a blocking call on a thread of its own.
//!
//! A few things the runtime needs have no asynchronous form in the kernel,
//! listing a directory among them. Doing them on a shard would stall every
//! task on it, so they run on a short-lived helper thread and the task
//! waits for the result.

use std::future::poll_fn;
use std::io;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Poll, Waker};

use crate::runtime::{discard_panic, wake_quietly};

struct Slot<T> {
    /// `Some(None)` if the work panicked.
    outcome: Option<Option<T>>,
    waker: Option<Waker>,
}

fn lock<T>(slot: &Mutex<Slot<T>>) -> MutexGuard<'_, Slot<T>> {
    // Nothing panics while holding this lock.
    slot.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Runs `work` on a new thread and returns what it returns.
///
/// Dropping the future does not stop the thread; its result is discarded.
/// Starting a thread costs tens of microseconds, so this is for rare
/// operations only.
pub(crate) async fn run<T, F>(work: F) -> io::Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let slot = Arc::new(Mutex::new(Slot {
        outcome: None,
        waker: None,
    }));
    let theirs = slot.clone();
    std::thread::Builder::new()
        .name(String::from("starfish-blocking"))
        .spawn(move || {
            let outcome = match catch_unwind(AssertUnwindSafe(work)) {
                Ok(value) => Some(value),
                Err(payload) => {
                    discard_panic(payload);
                    None
                }
            };
            let waker = {
                let mut slot = lock(&theirs);
                slot.outcome = Some(outcome);
                slot.waker.take()
            };
            // Waking from here sends the task back to its own shard.
            if let Some(waker) = waker {
                wake_quietly(waker);
            }
        })?;
    poll_fn(move |cx| {
        let mut waker = Some(cx.waker().clone());
        let (outcome, replaced) = {
            let mut slot = lock(&slot);
            match slot.outcome.take() {
                Some(outcome) => (Some(outcome), None),
                None => (None, std::mem::replace(&mut slot.waker, waker.take())),
            }
        };
        drop(replaced);
        match outcome {
            Some(Some(value)) => Poll::Ready(Ok(value)),
            Some(None) => Poll::Ready(Err(io::Error::other("blocking operation panicked"))),
            None => Poll::Pending,
        }
    })
    .await
}
