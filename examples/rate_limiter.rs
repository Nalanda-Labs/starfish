//! A token-bucket rate limiter built from a semaphore and a timer.
//!
//! The semaphore holds the tokens. Each request takes one and keeps it; a
//! background task puts one back at the permitted rate, up to the size of
//! the bucket.
//!
//! Run with `cargo run --example rate_limiter`.

use std::time::{Duration, Instant};

use starfish::sync::Semaphore;
use starfish::{sleep, spawn, Runtime};

/// Requests per second in the long run.
const RATE: u32 = 100;
/// Requests that may go through at once after a quiet period.
const BURST: usize = 10;
const REQUESTS: u32 = 60;

async fn app() -> starfish::Result<()> {
    let bucket = Semaphore::new(BURST);
    let refill = spawn({
        let bucket = bucket.clone();
        async move {
            loop {
                sleep(Duration::from_secs(1) / RATE).await;
                if bucket.available() < BURST {
                    bucket.add_permits(1);
                }
            }
        }
    });

    let start = Instant::now();
    for request in 1..=REQUESTS {
        // `forget` keeps the token: only the refill task gives tokens back.
        bucket.acquire(1).await?.forget();
        if request.is_multiple_of(10) {
            println!("request {request:>2} admitted at {:>3} ms", start.elapsed().as_millis());
        }
    }
    let expected = Duration::from_secs(1) * (REQUESTS - BURST as u32) / RATE;
    println!("{REQUESTS} requests took {:?}; the limit predicts about {expected:?}", start.elapsed());

    refill.abort();
    Ok(())
}

fn main() -> starfish::Result<()> {
    Runtime::new().shards(1).run(app)?
}
