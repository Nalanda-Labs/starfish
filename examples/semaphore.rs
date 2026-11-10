//! Limiting how much runs at once.
//!
//! Run with `cargo run --example semaphore`.

use std::cell::Cell;
use std::rc::Rc;
use std::time::{Duration, Instant};

use starfish::sync::Semaphore;
use starfish::{sleep, spawn, timeout, Runtime};

async fn app() -> starfish::Result<()> {
    // Ten jobs, three at a time.
    let limit = Semaphore::new(3);
    let running = Rc::new(Cell::new(0u32));
    let peak = Rc::new(Cell::new(0u32));
    let start = Instant::now();
    let jobs: Vec<_> = (0..10)
        .map(|job| {
            let limit = limit.clone();
            let running = running.clone();
            let peak = peak.clone();
            spawn(async move {
                // Held until the end of the job, then given back.
                let _permit = limit.acquire(1).await?;
                running.set(running.get() + 1);
                peak.set(peak.get().max(running.get()));
                println!("job {job} starts at {:>3} ms", start.elapsed().as_millis());
                sleep(Duration::from_millis(50)).await;
                running.set(running.get() - 1);
                Ok::<(), starfish::Error>(())
            })
        })
        .collect();
    for job in jobs {
        job.await??;
    }
    println!("at most {} jobs ran at once", peak.get());

    // Permits can stand for anything countable, such as bytes of memory.
    let memory = Semaphore::new(1 << 20);
    let big = memory.acquire(900 << 10).await?;
    println!("holding {} KiB, {} KiB left", big.count() >> 10, memory.available() >> 10);
    println!("200 KiB right now? {}", memory.try_acquire(200 << 10).is_some());
    // Waiting can be given up without losing anything.
    let waited = timeout(Duration::from_millis(20), memory.acquire(200 << 10)).await;
    println!("200 KiB within 20 ms? {}", waited.is_ok());
    drop(big);
    println!("after the release: {}", memory.try_acquire(200 << 10).is_some());
    Ok(())
}

fn main() -> starfish::Result<()> {
    Runtime::new().shards(1).run(app)?
}
