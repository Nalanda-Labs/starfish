//! Message passing between shards.

use std::future::{poll_fn, Future};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Poll, Waker};

use crate::error::{Error, Result};
use crate::runtime::{current, try_current, wake_quietly, Message};

struct Reply<T> {
    value: Option<Result<T>>,
    waker: Option<Waker>,
}

fn lock<T>(reply: &Mutex<Reply<T>>) -> MutexGuard<'_, Reply<T>> {
    // Nothing panics while holding this lock, and a poisoned lock would
    // still hold a consistent value.
    reply.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The sending half of a reply. If it is dropped without a value the caller
/// is told why: the remote closure panicked (dropped during unwinding), or
/// the target shard went away before running it.
struct ReplySender<T> {
    reply: Arc<Mutex<Reply<T>>>,
    done: bool,
}

impl<T> ReplySender<T> {
    fn finish(&mut self, value: Result<T>) {
        self.done = true;
        let waker = {
            let mut reply = lock(&self.reply);
            reply.value = Some(value);
            reply.waker.take()
        };
        // The waker belongs to a task on the calling shard; waking it from
        // here mails it back to that shard.
        if let Some(waker) = waker {
            wake_quietly(waker);
        }
    }
}

impl<T> Drop for ReplySender<T> {
    fn drop(&mut self) {
        if !self.done {
            let reason = if std::thread::panicking() {
                Error::Panicked
            } else {
                Error::Canceled
            };
            self.finish(Err(reason));
        }
    }
}

/// Runs `f` on shard `shard` and returns its result to the caller.
///
/// The closure and the result cross threads, so both must be `Send`. The
/// future the closure creates runs entirely on the target shard and may hold
/// non-`Send` state such as `Rc`s and that shard's sockets. It runs in the
/// caller's scheduling group.
///
/// # Errors
///
/// - [`Error::InvalidShard`] if `shard` is not a valid shard index.
/// - [`Error::Panicked`] if `f` or its future panicked on the target shard.
///   The target shard keeps running.
/// - [`Error::Canceled`] if the target shard shut down first.
/// - [`Error::NoRuntime`] if called outside a runtime.
pub async fn submit_to<F, Fut, T>(shard: usize, f: F) -> Result<T>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = T> + 'static,
    T: Send + 'static,
{
    let (here, group) = {
        let here = current()?;
        let group = here.current_group.get();
        if shard == here.shared.id {
            (None, group)
        } else {
            let count = here.runtime.shards.len();
            if shard >= count {
                return Err(Error::InvalidShard { shard, count });
            }
            (Some(here), group)
        }
    };
    let Some(here) = here else {
        // Already on the right shard: run in place.
        return Ok(f().await);
    };

    let reply = Arc::new(Mutex::new(Reply {
        value: None,
        waker: None,
    }));
    let mut sender = ReplySender {
        reply: reply.clone(),
        done: false,
    };
    here.send_to(shard, Message::Run(Box::new(move || {
        // If there is no shard to run on, dropping the closure's captures
        // reports `Canceled`.
        if let Some(shard) = try_current() {
            let call = async move {
                let value = f().await;
                sender.finish(Ok(value));
            };
            drop(shard.spawn_task(group, call, true));
        }
    })));
    drop(here);

    poll_fn(move |cx| {
        let mut waker = Some(cx.waker().clone());
        let (value, previous) = {
            let mut reply = lock(&reply);
            match reply.value.take() {
                Some(value) => (Some(value), None),
                None => (None, std::mem::replace(&mut reply.waker, waker.take())),
            }
        };
        drop(previous);
        match value {
            Some(value) => Poll::Ready(value),
            None => Poll::Pending,
        }
    })
    .await
}

/// Runs `f` on every shard concurrently and returns the results in shard
/// order. Fails with the first error if any shard's call fails; see
/// [`submit_to`] for the possible errors.
pub async fn submit_to_all<F, Fut, T>(f: F) -> Result<Vec<T>>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = T> + 'static,
    T: Send + 'static,
{
    let f = Arc::new(f);
    let count = current()?.runtime.shards.len();
    let handles: Vec<_> = (0..count)
        .map(|shard| {
            let f = f.clone();
            crate::spawn(submit_to(shard, move || f()))
        })
        .collect();
    let mut results = Vec::with_capacity(count);
    for handle in handles {
        results.push(handle.await??);
    }
    Ok(results)
}
