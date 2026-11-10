//! Sleeping, timeouts and a periodic tick.
//!
//! Run with `cargo run --example timers`.

use std::time::{Duration, Instant};

use starfish::{sleep, spawn, timeout, Runtime};

async fn app() -> starfish::Result<()> {
    let start = Instant::now();

    // Timers fire in deadline order, whatever order they were started in.
    let sleepers: Vec<_> = [30u64, 10, 20]
        .into_iter()
        .map(|millis| {
            spawn(async move {
                sleep(Duration::from_millis(millis)).await;
                println!("{millis} ms timer fired at {:?}", start.elapsed());
            })
        })
        .collect();
    for sleeper in sleepers {
        sleeper.await?;
    }

    // `timeout` gives a future a limited time and drops it afterwards.
    let quick = timeout(Duration::from_millis(50), sleep(Duration::from_millis(5))).await;
    let slow = timeout(Duration::from_millis(50), sleep(Duration::from_secs(5))).await;
    println!("5 ms of work within 50 ms: {}", if quick.is_ok() { "done" } else { "timed out" });
    println!("5 s of work within 50 ms: {}", if slow.is_ok() { "done" } else { "timed out" });

    // A periodic tick that does not drift: sleep until the next multiple
    // of the period, not for a fixed time after the work.
    let period = Duration::from_millis(20);
    let start = Instant::now();
    for tick in 1..=5u32 {
        let due = start + period * tick;
        sleep(due.saturating_duration_since(Instant::now())).await;
        println!("tick {tick} at {:?}", start.elapsed());
    }
    Ok(())
}

fn main() -> starfish::Result<()> {
    Runtime::new().shards(1).run(app)?
}
