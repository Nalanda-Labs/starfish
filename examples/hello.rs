//! The smallest program: start a runtime, look around, return a value.
//!
//! Run with `cargo run --example hello`.

use starfish::{shard_count, shard_id, Runtime};

async fn app() -> starfish::Result<usize> {
    println!("hello from shard {} of {}", shard_id()?, shard_count()?);
    shard_count()
}

fn main() -> starfish::Result<()> {
    // `run` turns this thread into shard 0, starts the other shards, runs
    // `app` and hands back what it returned.
    let shards = Runtime::new().run(app)??;
    println!("the runtime had {shards} shards and has stopped");
    Ok(())
}
