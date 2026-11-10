//! Message passing between shards.

use std::future::{poll_fn, Future};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Poll, Waker};

use crate::error::{Error, Result};
use crate::runtime::{catch_panic, current, try_current, wake_quietly, Message};
use crate::sched::DEFAULT_GROUP;
use crate::sync::Permit;

/// Where the result of a call to another shard is left for the caller.
///
/// It is shared between the two shards, behind a mutex: the shard that ran
/// the call puts the result in and wakes the caller; the caller takes it out.
/// The mutex is held only for those few instructions, never across an await
/// or a call into user code.
struct Reply<T> {
    value: Option<Result<T>>,
    waker: Option<Waker>,
    /// The call is over: it returned, panicked or was never run.
    done: bool,
}

/// Locks a reply slot, recovering it if a thread panicked while holding the
/// lock. Nothing panics while holding it, and the slot holds a consistent
/// value at every point, so a poisoned lock is safe to use.
fn lock<T>(reply: &Mutex<Reply<T>>) -> MutexGuard<'_, Reply<T>> {
    // Nothing panics while holding this lock, and a poisoned lock would
    // still hold a consistent value.
    reply.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The sending half of a reply: owned by the task that runs the call on the
/// target shard.
///
/// Its `Drop` is what guarantees that a caller is always answered. If it is
/// dropped without a result having been sent, the caller is told why: the
/// call panicked (the sender is being dropped by unwinding), or the target
/// shard went away before it could run the call.
struct ReplySender<T> {
    reply: Arc<Mutex<Reply<T>>>,
    done: bool,
}

impl<T> ReplySender<T> {
    /// Stores the call's outcome and wakes the caller, if it is already
    /// waiting. If it is not, it will find the outcome on its first look and no
    /// wake-up is needed.
    ///
    /// The waker belongs to a task on the calling shard. Calling it from here
    /// sends that task back to its own shard as a message.
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
    /// Answers the caller if nothing has been sent yet: with
    /// [`Error::Panicked`] if this drop is part of a panic's unwinding, otherwise
    /// with [`Error::Canceled`], which means the call was thrown away unrun.
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
/// call's permit from the limit on calls between the two shards
/// (`Runtime::submit_limit`).
///
/// It exists so that the permit is released at the right moment whatever
/// happens to the caller: when the reply is collected, or, if the caller
/// stops waiting first, only once the other shard has really finished.
struct Call<T: Send + 'static> {
    reply: Arc<Mutex<Reply<T>>>,
    /// `None` once the call is over and the permit has been returned.
    permit: Option<Permit>,
}

impl<T: Send + 'static> Drop for Call<T> {
    /// Called when the caller is done with the call, one way or the other.
    ///
    /// If the reply has been collected, or the call is over, the permit is simply
    /// returned. If the caller is giving up while the other shard is still at
    /// work, the permit is handed to a small task that waits for the call to end
    /// and returns it then. Returning it at once would let a caller that abandons
    /// its calls exceed the limit without bound.
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
/// order.
///
/// `f` is called once on each shard, the current one included, and the
/// futures it returns run at the same time, each on its own shard. Use it to
/// set something up everywhere, such as binding a listener per shard, or to
/// gather something from everywhere.
///
/// ```no_run
/// use starfish::{shard_id, submit_to_all};
///
/// # async fn example() -> starfish::Result<()> {
/// let ids = submit_to_all(|| async { shard_id() }).await?;
/// println!("{} shards answered", ids.len());
/// # Ok(())
/// # }
/// ```
///
/// `f` must be `Fn`, `Send` and `Sync`, since every shard calls it; its
/// result must be `Send`. The futures need not be.
///
/// # Errors
///
/// The first failure, in shard order: see [`submit_to`] for what can fail.
/// The other shards' results are discarded, though `f` has run there. To
/// learn what happened on each shard, use [`submit_to_each`].
pub async fn submit_to_all<F, Fut, T>(f: F) -> Result<Vec<T>>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = T> + 'static,
    T: Send + 'static,
{
    submit_to_each(f).await?.into_iter().collect()
}

/// Runs `f` on every shard concurrently and returns every shard's outcome,
/// in shard order: its result, or why there is none.
///
/// Where [`submit_to_all`] gives up at the first failure and discards the
/// rest, this says what happened on each shard. Use it when the calls
/// have effects and it matters which of them took place.
///
/// # Errors
///
/// The call as a whole fails only with [`Error::NoRuntime`], outside a
/// runtime. Every other failure is one shard's, and is in its place in
/// the list.
pub async fn submit_to_each<F, Fut, T>(f: F) -> Result<Vec<Result<T>>>
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
        // A failure of the local task that made the call counts as that
        // shard's failure.
        results.push(handle.await.and_then(|outcome| outcome));
    }
    Ok(results)
}
