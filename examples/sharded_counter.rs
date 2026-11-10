//! A service with one instance per shard.
//!
//! Run with `cargo run --example sharded_counter`.

use std::cell::Cell;

use starfish::{shard_count, shard_id, Runtime, Sharded};

/// Each shard counts its own requests. Nothing is shared, so a plain
/// `Cell` does.
struct Counter {
    shard: usize,
    requests: Cell<u64>,
}

async fn app() -> starfish::Result<()> {
    // The factory runs once on every shard, with that shard's index.
    let counter = Sharded::start(|shard| Counter {
        shard,
        requests: Cell::new(0),
    })
    .await?;

    // The instance of the shard we are on, without any message passing.
    let local = counter.local()?;
    local.requests.set(local.requests.get() + 100);
    println!("shard {} counted 100 requests locally", shard_id()?);

    // Every instance at once.
    counter
        .invoke_on_all(|counter| async move {
            let work = counter.shard as u64 + 1;
            counter.requests.set(counter.requests.get() + work);
        })
        .await?;

    // One particular instance.
    let last = shard_count()? - 1;
    let there = counter
        .invoke_on(last, |counter| async move { counter.requests.get() })
        .await?;
    println!("shard {last} has counted {there}");

    // Gather from all shards and combine here.
    let total = counter
        .map_reduce(|counter| async move { counter.requests.get() }, 0, |sum, one| sum + one)
        .await?;
    println!("all shards together: {total}");

    counter.stop().await?;
    println!("after stop, local() fails: {}", counter.local().is_err());
    Ok(())
}

fn main() -> starfish::Result<()> {
    Runtime::new().run(app)?
}
