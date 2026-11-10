//! Finding the task that did not yield.
//!
//! A task that computes for a long time without awaiting stalls its whole
//! shard. The runtime counts such stalls, and with backtraces switched on
//! it records where the shard was stuck.
//!
//! Run with `cargo run --example stall_detector`.

use std::hint::black_box;
use std::time::{Duration, Instant};

use starfish::stats::StallReport;
use starfish::{sleep, stats, Runtime};

/// The culprit: 100 ms of work with no await in it.
#[inline(never)]
fn forgot_to_yield() -> u64 {
    let end = Instant::now() + Duration::from_millis(100);
    let mut value = 1u64;
    while Instant::now() < end {
        value = black_box(value.wrapping_mul(31).wrapping_add(7));
    }
    value
}

/// Called on the shard's thread after each stall.
fn report(stall: &StallReport) {
    eprintln!(
        "shard {} stalled for {:?} in group {:?}",
        stall.shard, stall.duration, stall.group
    );
    for frame in stall.backtrace.iter().take(12) {
        eprintln!("    {frame}");
    }
}

async fn app() -> starfish::Result<()> {
    sleep(Duration::from_millis(10)).await;
    forgot_to_yield();
    sleep(Duration::from_millis(10)).await;

    let shard = stats::shard()?;
    println!("stalls: {}, longest {:?}", shard.stalls, shard.longest_stall);
    Ok(())
}

fn main() -> starfish::Result<()> {
    Runtime::new()
        .shards(1)
        .stall_threshold(Duration::from_millis(20))
        .stall_backtraces(true)
        .on_stall(report)
        .run(app)?
}
