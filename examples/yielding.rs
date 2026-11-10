//! Why long computations must yield, and how cheaply they can.
//!
//! A heartbeat task wants to run every millisecond. Next to it, a
//! computation runs for 200 ms, first without yielding and then with
//! `maybe_yield` in its loop.
//!
//! Run with `cargo run --release --example yielding`.

use std::cell::Cell;
use std::hint::black_box;
use std::rc::Rc;
use std::time::{Duration, Instant};

use starfish::{maybe_yield, sleep, spawn, Runtime};

/// Records the longest it ever had to wait between two beats.
async fn heartbeat(worst: Rc<Cell<Duration>>) {
    let mut last = Instant::now();
    loop {
        sleep(Duration::from_millis(1)).await;
        let now = Instant::now();
        worst.set(worst.get().max(now - last));
        last = now;
    }
}

async fn compute(polite: bool) -> starfish::Result<(Duration, u64)> {
    let worst = Rc::new(Cell::new(Duration::ZERO));
    let beat = spawn(heartbeat(worst.clone()));
    sleep(Duration::from_millis(5)).await;

    let end = Instant::now() + Duration::from_millis(200);
    let mut value = 1u64;
    let mut rounds = 0u64;
    while Instant::now() < end {
        for _ in 0..1000 {
            value = black_box(value.wrapping_mul(31).wrapping_add(7));
        }
        rounds += 1;
        if polite {
            // Two memory reads; yields only when the time slice is over.
            maybe_yield().await;
        }
    }

    sleep(Duration::from_millis(5)).await;
    beat.abort();
    Ok((worst.get(), rounds))
}

async fn app() -> starfish::Result<()> {
    let (gap, rounds) = compute(false).await?;
    println!("without yielding: heartbeat waited up to {gap:?} ({rounds} rounds of work)");
    let (gap, rounds) = compute(true).await?;
    println!("with maybe_yield: heartbeat waited up to {gap:?} ({rounds} rounds of work)");
    Ok(())
}

fn main() -> starfish::Result<()> {
    Runtime::new().shards(1).run(app)?
}
