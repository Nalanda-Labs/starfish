//! Sharing work out between shards: each job goes to whichever is free.
//!
//! One shard produces jobs of very different sizes. A worker on every
//! shard takes from the same queue, so a shard stuck on a long job does
//! not hold up the short ones behind it, as it would if jobs were dealt
//! out in turn.
//!
//! Run with `cargo run --example shared_mpmc`.

use std::time::{Duration, Instant};

use starfish::sync::shared_mpmc;
use starfish::{shard_count, shard_id, sleep, spawn, submit_to, Runtime};

/// A job: how long it takes.
struct Job(Duration);

async fn app() -> starfish::Result<()> {
    let shards = shard_count()?;
    // A small queue: the producer is slowed down when the workers are
    // all busy, instead of piling jobs up.
    let (jobs, queue) = shared_mpmc::<Job>(4);

    let mut workers = Vec::new();
    for shard in 0..shards {
        let queue = queue.clone();
        workers.push(spawn(submit_to(shard, move || async move {
            let (mut done, mut busy) = (0u32, Duration::ZERO);
            // Ends when the queue is empty and every sender is gone.
            while let Some(Job(length)) = queue.recv().await {
                sleep(length).await;
                done += 1;
                busy += length;
            }
            (shard_id().unwrap_or(0), done, busy)
        })));
    }
    // The workers hold the receiving ends now.
    drop(queue);

    let start = Instant::now();
    for number in 0..60u64 {
        // Every tenth job is a long one.
        let length = if number % 10 == 0 { 40 } else { 2 };
        if jobs.send(Job(Duration::from_millis(length))).await.is_err() {
            break; // every worker is gone
        }
    }
    drop(jobs);

    for worker in workers {
        let (shard, done, busy) = worker.await??;
        println!("shard {shard}: {done:2} jobs, busy for {busy:?}");
    }
    println!("60 jobs (348 ms of work) took {:?} on {shards} shards", start.elapsed());
    Ok(())
}

fn main() -> starfish::Result<()> {
    Runtime::new().shards(4).run(app)?
}
