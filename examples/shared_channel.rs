//! Streaming values from one shard to another, with backpressure.
//!
//! One shard produces numbers as fast as it can; another adds them up and
//! is slower. The queue between them holds 64 values, so the producer is
//! held to the consumer's pace, and compared with one `submit_to` per
//! value nothing is allocated per value.
//!
//! Run with `cargo run --release --example shared_channel`.

use std::time::{Duration, Instant};

use starfish::sync::shared_channel;
use starfish::{maybe_yield, sleep, spawn, submit_to, Runtime};

const VALUES: u64 = 2_000_000;

async fn app() -> starfish::Result<()> {
    // Made here, then each end is moved to the shard that uses it.
    let (mut sender, mut receiver) = shared_channel(64);
    let start = Instant::now();

    let producer = spawn(submit_to(1, move || async move {
        for value in 0..VALUES {
            if sender.send(value).await.is_err() {
                return; // the consumer is gone
            }
            maybe_yield().await;
        }
        // Dropping the sender ends the consumer's loop.
    }));
    let consumer = spawn(submit_to(2, move || async move {
        let mut sum = 0u64;
        while let Some(value) = receiver.recv().await {
            sum += value;
            maybe_yield().await;
        }
        sum
    }));

    producer.await??;
    let sum = consumer.await??;
    let elapsed = start.elapsed();
    println!("{VALUES} values crossed from shard 1 to shard 2 in {elapsed:?}");
    println!(
        "{:.0} ns per value; their sum is {sum}",
        elapsed.as_nanos() as f64 / VALUES as f64
    );

    // A slow consumer slows the producer down instead of letting the
    // queue grow: 100 values at 1 ms each take the producer 100 ms too.
    let (mut sender, mut receiver) = shared_channel(4);
    let producer = spawn(submit_to(1, move || async move {
        let start = Instant::now();
        for value in 0..100u32 {
            if sender.send(value).await.is_err() {
                break;
            }
        }
        start.elapsed()
    }));
    let consumer = spawn(submit_to(2, move || async move {
        while receiver.recv().await.is_some() {
            sleep(Duration::from_millis(1)).await;
        }
    }));
    let sending = producer.await??;
    consumer.await??;
    println!("against a consumer taking 1 ms per value, sending 100 values took {sending:?}");
    Ok(())
}

fn main() -> starfish::Result<()> {
    Runtime::new().shards(3).run(app)?
}
