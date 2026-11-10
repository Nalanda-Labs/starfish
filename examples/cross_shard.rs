//! Running code on other shards.
//!
//! Run with `cargo run --example cross_shard`.

use std::cell::Cell;
use std::rc::Rc;

use starfish::{shard_count, shard_id, submit_to, submit_to_all, yield_now, Error, Runtime};

async fn app() -> starfish::Result<()> {
    let shards = shard_count()?;
    println!("main runs on shard {} of {shards}", shard_id()?);
    let target = shards - 1;

    // The closure travels to the other shard, so what it captures must be
    // `Send`, and so must the result that travels back.
    let words = [String::from("moved"), String::from("across")];
    let (ran_on, joined) = submit_to(target, move || async move {
        (shard_id().unwrap_or(usize::MAX), words.join(" "))
    })
    .await?;
    println!("shard {ran_on} joined the words: {joined:?}");

    // The future the closure builds never leaves the target shard, so it
    // may create and use state that is not `Send`.
    let total = submit_to(target, || async {
        let local = Rc::new(Cell::new(0));
        for i in 1..=10 {
            local.set(local.get() + i);
            yield_now().await;
        }
        local.get()
    })
    .await?;
    println!("summed on shard {target} with an Rc<Cell>: {total}");

    // The same closure on every shard, at once; results in shard order.
    let ids = submit_to_all(|| async { shard_id().unwrap_or(usize::MAX) }).await?;
    println!("every shard reported in: {ids:?}");

    match submit_to(shards, || async {}).await {
        Err(Error::InvalidShard { shard, count }) => {
            println!("shard {shard} does not exist: there are {count}")
        }
        other => println!("unexpected: {other:?}"),
    }
    Ok(())
}

fn main() -> starfish::Result<()> {
    Runtime::new().run(app)?
}
