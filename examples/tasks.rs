//! Spawning tasks and collecting their results.
//!
//! Run with `cargo run --example tasks`.

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use starfish::{spawn, yield_now, Runtime};

async fn app() -> starfish::Result<()> {
    // A task's output comes back through its handle.
    let handles: Vec<_> = (1..=5u64)
        .map(|n| {
            spawn(async move {
                yield_now().await;
                n * n
            })
        })
        .collect();
    let mut squares = Vec::new();
    for handle in handles {
        squares.push(handle.await?);
    }
    println!("squares: {squares:?}");

    // Tasks never leave their shard, so they can share state through `Rc`
    // and `RefCell`: no `Arc`, no locks.
    let log = Rc::new(RefCell::new(Vec::new()));
    let writers: Vec<_> = ["a", "b", "c"]
        .into_iter()
        .map(|name| {
            let log = log.clone();
            spawn(async move {
                for step in 0..2 {
                    log.borrow_mut().push(format!("{name}{step}"));
                    yield_now().await;
                }
            })
        })
        .collect();
    for writer in writers {
        writer.await?;
    }
    println!("three tasks took turns: {:?}", log.borrow());

    // Dropping the handle detaches the task: it still runs.
    let ran = Rc::new(Cell::new(false));
    drop(spawn({
        let ran = ran.clone();
        async move { ran.set(true) }
    }));
    yield_now().await;
    println!("the detached task ran: {}", ran.get());
    Ok(())
}

fn main() -> starfish::Result<()> {
    Runtime::new().shards(1).run(app)?
}
