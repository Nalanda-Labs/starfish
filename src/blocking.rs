//! Running blocking calls on threads of their own.
//!
//! Nothing on a shard may block, because every other task on it would
//! wait. Blocking work is handed to a pool of ordinary threads instead,
//! and the task waits for the result.
//!
//! The pool is shared by the whole process. It starts a thread when a
//! job arrives and none is free, up to a limit, and a thread that has had
//! nothing to do for a while exits.

use std::collections::VecDeque;
use std::future::poll_fn;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::task::{Poll, Waker};
use std::time::Duration;

use crate::error::{Error, Result};
use crate::runtime::{discard_panic, wake_quietly};

/// Most threads the pool runs at once; further jobs wait for one.
const MAX_THREADS: usize = 256;
/// How long a thread waits for another job before it exits.
const IDLE: Duration = Duration::from_secs(10);

type Job = Box<dyn FnOnce() + Send>;

struct Pool {
    jobs: VecDeque<Job>,
    /// Threads that exist.
    threads: usize,
    /// Threads waiting for a job.
    idle: usize,
}

static POOL: Mutex<Pool> = Mutex::new(Pool {
    jobs: VecDeque::new(),
    threads: 0,
    idle: 0,
});
static JOB_READY: Condvar = Condvar::new();

fn pool() -> MutexGuard<'static, Pool> {
    // Jobs run outside the lock, so nothing panics while holding it.
    POOL.lock().unwrap_or_else(PoisonError::into_inner)
}

fn worker() {
    let mut pool = pool();
    loop {
        if let Some(job) = pool.jobs.pop_front() {
            drop(pool);
            job();
            pool = self::pool();
            continue;
        }
        pool.idle = pool.idle.saturating_add(1);
        let (guard, waited) = JOB_READY
            .wait_timeout(pool, IDLE)
            .unwrap_or_else(PoisonError::into_inner);
        pool = guard;
        pool.idle = pool.idle.saturating_sub(1);
        if waited.timed_out() && pool.jobs.is_empty() {
            pool.threads = pool.threads.saturating_sub(1);
            return;
        }
    }
}

/// Queues a job, starting a thread for it if none is free.
fn submit(job: Job) -> Result<()> {
    let mut pool = pool();
    // Jobs already queued have a claim on the idle threads.
    let start = pool.idle <= pool.jobs.len() && pool.threads < MAX_THREADS;
    pool.jobs.push_back(job);
    if start {
        pool.threads = pool.threads.saturating_add(1);
        drop(pool);
        let spawned = std::thread::Builder::new()
            .name(String::from("starfish-blocking"))
            .spawn(worker);
        if let Err(err) = spawned {
            let mut pool = self::pool();
            pool.threads = pool.threads.saturating_sub(1);
            // With no thread at all, nobody would ever run the job.
            if pool.threads == 0 {
                drop(pool.jobs.pop_back());
                return Err(Error::Io(err));
            }
        }
    } else {
        drop(pool);
    }
    JOB_READY.notify_one();
    Ok(())
}

struct Slot<T> {
    /// `Some(None)` if the work panicked.
    outcome: Option<Option<T>>,
    waker: Option<Waker>,
}

fn lock<T>(slot: &Mutex<Slot<T>>) -> MutexGuard<'_, Slot<T>> {
    // Nothing panics while holding this lock.
    slot.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Runs `work` on a thread outside the runtime and returns what it
/// returns.
///
/// Use it for anything that blocks: a system call with no asynchronous
/// form, a library that does its own blocking I/O, a long computation that
/// cannot be cut into pieces. The shard carries on with its other tasks
/// meanwhile.
///
/// ```no_run
/// # async fn example() -> starfish::Result<()> {
/// let hostname = starfish::run_blocking(|| std::fs::read_to_string("/etc/hostname")).await??;
/// println!("running on {}", hostname.trim());
/// # Ok(())
/// # }
/// ```
///
/// The closure and its result cross threads, so both must be `Send`.
///
/// Dropping the returned future does not stop the work, which cannot be
/// interrupted; it runs to the end and its result is discarded.
///
/// The threads are shared by the whole process and limited in number
/// (256). When all are busy, further work waits, so work that blocks
/// indefinitely can starve the rest.
///
/// # Errors
///
/// [`Error::Panicked`] if `work` panicked, and [`Error::Io`] if no thread
/// could be started to run it.
pub async fn run_blocking<T, F>(work: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let slot = Arc::new(Mutex::new(Slot {
        outcome: None,
        waker: None,
    }));
    let theirs = slot.clone();
    submit(Box::new(move || {
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
    }))?;
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
            Some(None) => Poll::Ready(Err(Error::Panicked)),
            None => Poll::Pending,
        }
    })
    .await
}
