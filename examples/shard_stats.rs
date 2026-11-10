//! Looking at what the shards have been doing.
//!
//! Run with `cargo run --example shard_stats`.

use std::time::Duration;

use starfish::{shard_count, sleep, stats, submit_to, submit_to_all, Runtime};

async fn app() -> starfish::Result<()> {
    // Some activity to look at: calls around the ring of shards.
    let shards = shard_count()?;
    for round in 0..200 {
        submit_to(round % shards, || async {}).await?;
    }
    sleep(Duration::from_millis(50)).await;

    // The counters are per shard; ask each one for its own.
    println!("shard  tasks  polled   loops  sleeps   sent  received      busy      idle");
    for shard in submit_to_all(|| async { stats::shard() }).await? {
        let shard = shard?;
        println!(
            "{:>5}  {:>5}  {:>6}  {:>6}  {:>6}  {:>5}  {:>8}  {:>8.1?}  {:>8.1?}",
            shard.shard,
            shard.tasks,
            shard.tasks_polled,
            shard.loops,
            shard.sleeps,
            shard.messages_sent,
            shard.messages_received,
            shard.busy,
            shard.idle
        );
    }
    Ok(())
}

fn main() -> starfish::Result<()> {
    Runtime::new().shards(4).run(app)?
}
