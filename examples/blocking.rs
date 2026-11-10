//! Calling blocking code without stalling a shard.
//!
//! A shard must never block: everything else on it would wait. Blocking
//! work goes to an ordinary thread, and the task waits for the result.
//! Wakers are safe to use from any thread; waking one from outside the
//! runtime sends the task back to its own shard.
//!
//! Run with `cargo run --example blocking`.

use std::future::poll_fn;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Poll, Waker};
use std::time::{Duration, Instant};

use starfish::{sleep, spawn, Runtime};

struct Slot<T> {
    value: Option<T>,
    waker: Option<Waker>,
}

/// Runs `work` on a new thread and waits for what it returns. (If `work`
/// panics, the wait never ends; a real version would report that.)
async fn run_blocking<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    let slot = Arc::new(Mutex::new(Slot {
        value: None,
        waker: None,
    }));
    let theirs = slot.clone();
    std::thread::spawn(move || {
        let value = work();
        let waker = {
            let mut slot = theirs.lock().unwrap_or_else(PoisonError::into_inner);
            slot.value = Some(value);
            slot.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    });
    poll_fn(move |cx| {
        let mut slot = slot.lock().unwrap_or_else(PoisonError::into_inner);
        match slot.value.take() {
            Some(value) => Poll::Ready(value),
            None => {
                slot.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    })
    .await
}

async fn app() -> starfish::Result<()> {
    // Shows that the shard keeps running while the blocking call is out.
    let ticker = spawn(async {
        let mut ticks = 0u32;
        loop {
            sleep(Duration::from_millis(10)).await;
            ticks += 1;
            if ticks.is_multiple_of(5) {
                println!("  the shard is still responsive ({ticks} ticks)");
            }
        }
    });

    let start = Instant::now();
    let answer = run_blocking(|| {
        std::thread::sleep(Duration::from_millis(200));
        6 * 7
    })
    .await;
    println!("the blocking call returned {answer} after {:?}", start.elapsed());
    ticker.abort();
    Ok(())
}

fn main() -> starfish::Result<()> {
    Runtime::new().shards(1).run(app)?
}
