//! A panic ends one task, not the program.
//!
//! The panic messages on stderr are from Rust's default panic hook.
//!
//! Run with `cargo run --example panics`.

use starfish::{shard_id, spawn, submit_to, yield_now, Error, Runtime};

async fn app() -> starfish::Result<()> {
    // In a task: whoever awaits it is told.
    let task = spawn(async {
        yield_now().await;
        if shard_id().is_ok() {
            panic!("this task has a bug");
        }
        1
    });
    match task.await {
        Err(Error::Panicked) => println!("the task panicked; this shard carries on"),
        other => println!("unexpected: {other:?}"),
    }

    // On another shard: the caller is told, and that shard carries on too.
    let remote = submit_to(1, || async {
        if shard_id().is_ok() {
            panic!("this request has a bug");
        }
    })
    .await;
    match remote {
        Err(Error::Panicked) => println!("the call to shard 1 panicked"),
        other => println!("unexpected: {other:?}"),
    }
    let alive = submit_to(1, || async { shard_id() }).await??;
    println!("shard {alive} still answers");
    Ok(())
}

fn main() -> starfish::Result<()> {
    Runtime::new().shards(2).run(app)?
}
