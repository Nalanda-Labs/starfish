//! Message passing between shards.

use std::future::{poll_fn, Future};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Poll, Waker};

use crate::error::{Error, Result};
use crate::runtime::{catch_panic, current, try_current, wake_quietly, Message};
use crate::sched::DEFAULT_GROUP;
use crate::sync::Permit;

struct Reply<T> {
    value: Option<Result<T>>,
    waker: Option<Waker>,
    /// The call is over: it returned, panicked or was never run.
    done: bool,
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
            reply.done = true;
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
                Error::Panicked { message: None }
            } else {
                Error::Canceled
            };
            self.finish(Err(reason));
        }
    }
}

/// The caller's side of a call in progress on another shard. It holds the
/// call's permit from the limit on calls between the two shards.
struct Call<T: Send + 'static> {
    reply: Arc<Mutex<Reply<T>>>,
    /// `None` once the call is over and the permit has been returned.
    permit: Option<Permit>,
}

impl<T: Send + 'static> Drop for Call<T> {
    fn drop(&mut self) {
        let Some(permit) = self.permit.take() else { return };
        if lock(&self.reply).done {
            return;
        }
        // The caller has stopped waiting, but the other shard is still
        // working on the call. The permit stays taken until it is done, or
        // giving up on calls would be a way around the limit.
        let reply = self.reply.clone();
        let Some(shard) = try_current() else { return };
        let hold = async move {
            poll_fn(|cx| {
                let mut waker = Some(cx.waker().clone());
                let (done, replaced) = {
                    let mut reply = lock(&reply);
                    match reply.done {
                        true => (true, None),
                        false => (false, std::mem::replace(&mut reply.waker, waker.take())),
                    }
                };
                drop(replaced);
                if done {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
            drop(permit);
        };
        drop(shard.spawn_task(DEFAULT_GROUP, hold, true));
    }
}

/// Runs `f` on shard `shard` and returns its result to the caller.
///
/// The closure and the result cross threads, so both must be `Send`. The
/// future the closure creates runs entirely on the target shard and may hold
/// non-`Send` state such as `Rc`s and that shard's sockets. It runs in the
/// caller's scheduling group.
///
/// Dropping the returned future stops the wait, not the call: the closure
/// still runs to completion on the target shard.
///
/// # Backpressure
///
/// A shard may have only so many calls in progress on one other shard
/// ([`Runtime::submit_limit`](crate::Runtime::submit_limit), 1024 by
/// default). Past that, this function waits until an earlier call has
/// finished before it sends, so a shard that is overloaded slows its
/// callers down.
///
/// Because of that, calls that depend on further calls in the same
/// direction can block each other: if every permit from shard A to shard
/// B is held by calls that are each waiting for a nested call from A to B,
/// none can proceed. Keep chains of calls from doubling back, or raise the
/// limit.
///
/// # Errors
///
/// - [`Error::InvalidShard`] if `shard` is not a valid shard index.
/// - [`Error::Panicked`] if `f` or its future panicked, with the panic's
///   message. The shard it ran on keeps running, and so does the caller,
///   whether that is another shard or the caller's own.
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
        // Already on the right shard: run in place. A panic is reported
        // the same way as one on another shard would be.
        return catch_panic(f).await.map_err(|message| Error::Panicked { message });
    };

    // Wait for a place among the calls in progress on that shard.
    let limit = here.calls.get(shard).cloned();
    drop(here);
    let permit = match limit {
        Some(limit) => Some(limit.acquire(1).await.map_err(|_| Error::Canceled)?),
        None => None,
    };
    let here = current()?;

    let reply = Arc::new(Mutex::new(Reply {
        value: None,
        waker: None,
        done: false,
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
                let outcome = catch_panic(f).await;
                sender.finish(outcome.map_err(|message| Error::Panicked { message }));
            };
            drop(shard.spawn_task(group, call, true));
        }
    })));
    drop(here);

    let mut call = Call { reply, permit };
    poll_fn(move |cx| {
        let mut waker = Some(cx.waker().clone());
        let (value, previous) = {
            let mut reply = lock(&call.reply);
            match reply.value.take() {
                Some(value) => (Some(value), None),
                None => (None, std::mem::replace(&mut reply.waker, waker.take())),
            }
        };
        drop(previous);
        match value {
            Some(value) => {
                // The call is over; the next one may go.
                drop(call.permit.take());
                Poll::Ready(value)
            }
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
