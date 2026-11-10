//! A pipeline of stages connected by channels.
//!
//! numbers -> squares -> sum. Each stage is a task. The channels are
//! small, so a slow stage slows the ones before it, and every stage ends
//! by itself when the one before it has finished.
//!
//! Run with `cargo run --example pipeline`.

use starfish::sync::{channel, Receiver};
use starfish::{spawn, JoinHandle, Runtime};

fn numbers(count: u64) -> Receiver<u64> {
    let (sender, receiver) = channel(4);
    spawn(async move {
        for n in 1..=count {
            if sender.send(n).await.is_err() {
                return;
            }
        }
    });
    receiver
}

fn squares(mut input: Receiver<u64>) -> Receiver<u64> {
    let (sender, receiver) = channel(4);
    spawn(async move {
        while let Some(n) = input.recv().await {
            if sender.send(n * n).await.is_err() {
                return;
            }
        }
    });
    receiver
}

fn sum(mut input: Receiver<u64>) -> JoinHandle<u64> {
    spawn(async move {
        let mut total = 0;
        while let Some(n) = input.recv().await {
            total += n;
        }
        total
    })
}

async fn app() -> starfish::Result<()> {
    let total = sum(squares(numbers(100))).await?;
    println!("the squares of 1 to 100 add up to {total}");
    Ok(())
}

fn main() -> starfish::Result<()> {
    Runtime::new().shards(1).run(app)?
}
