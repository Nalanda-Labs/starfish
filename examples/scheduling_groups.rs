//! Dividing a shard's CPU between classes of work.
//!
//! Two groups compete for one shard: "requests" with 800 shares and
//! "maintenance" with 200. Both want all the CPU they can get, and get it
//! four to one.
//!
//! Run with `cargo run --release --example scheduling_groups`.

use std::hint::black_box;
use std::time::{Duration, Instant};

use starfish::{maybe_yield, stats, Runtime, SchedulingGroup};

/// Computes until `end`, yielding whenever its time slice is over.
async fn work(end: Instant) -> u64 {
    let mut rounds = 0u64;
    let mut value = 1u64;
    while Instant::now() < end {
        for _ in 0..1000 {
            value = black_box(value.wrapping_mul(31).wrapping_add(7));
        }
        rounds += 1;
        maybe_yield().await;
    }
    rounds
}

async fn app() -> starfish::Result<()> {
    let requests = SchedulingGroup::create("requests", 800)?;
    let maintenance = SchedulingGroup::create("maintenance", 200)?;

    let end = Instant::now() + Duration::from_secs(1);
    // Everything a task spawns inherits its group.
    let foreground = requests.spawn(work(end));
    let background = maintenance.spawn(work(end));
    let (foreground, background) = (foreground.await?, background.await?);
    println!("rounds of work: requests {foreground}, maintenance {background}");

    for group in stats::groups()? {
        println!(
            "{:<12} {:>5} shares, {:>4} ms of CPU",
            group.name,
            group.cpu_shares,
            group.cpu_time.as_millis()
        );
    }

    // Shares only matter under contention: alone, a group gets everything.
    let end = Instant::now() + Duration::from_millis(200);
    let alone = maintenance.spawn(work(end)).await?;
    println!("maintenance alone for 200 ms: {alone} rounds");
    Ok(())
}

fn main() -> starfish::Result<()> {
    Runtime::new().shards(1).run(app)?
}
