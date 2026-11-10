//! Passing values between tasks, with backpressure.
//!
//! Run with `cargo run --example channel`.

use std::time::{Duration, Instant};

use starfish::sync::{channel, TrySendError};
use starfish::{sleep, spawn, Runtime};

async fn app() -> starfish::Result<()> {
    // A fast producer and a slow consumer. The queue holds four values, so
    // the producer is held to the consumer's pace instead of filling memory.
    let (sender, mut receiver) = channel(4);
    let producer = spawn(async move {
        let start = Instant::now();
        for item in 0..20u32 {
            if sender.send(item).await.is_err() {
                break;
            }
        }
        start.elapsed()
        // The sender is dropped here, which ends the consumer's loop.
    });
    let mut received = 0;
    while let Some(_item) = receiver.recv().await {
        received += 1;
        sleep(Duration::from_millis(5)).await;
    }
    println!("consumer took {received} items at 5 ms each");
    println!("producer needed {:?} to send them", producer.await?);

    // Several senders, one receiver.
    let (sender, mut receiver) = channel(8);
    for worker in 0..3 {
        let sender = sender.clone();
        spawn(async move {
            for step in 0..2 {
                let _ = sender.send(format!("worker {worker}, step {step}")).await;
            }
        });
    }
    drop(sender);
    while let Some(message) = receiver.recv().await {
        println!("{message}");
    }

    // Sending without waiting, and what happens when the receiver is gone.
    let (sender, receiver) = channel(1);
    println!("first try_send: {:?}", sender.try_send("a"));
    match sender.try_send("b") {
        Err(TrySendError::Full(value)) => println!("queue full, {value:?} handed back"),
        other => println!("unexpected: {other:?}"),
    }
    drop(receiver);
    match sender.send("c").await {
        Err(error) => println!("receiver gone, {:?} handed back", error.0),
        Ok(()) => println!("unexpected: sent"),
    }
    Ok(())
}

fn main() -> starfish::Result<()> {
    Runtime::new().shards(1).run(app)?
}
