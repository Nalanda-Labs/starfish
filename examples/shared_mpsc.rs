//! Many shards reporting to one.
//!
//! Every shard does some work and sends progress reports to shard 0, which
//! prints a summary. Each sender has a queue of its own to the receiver,
//! so the shards never contend with each other, and a shard whose queue is
//! full is slowed down by itself.
//!
//! Run with `cargo run --example shared_mpsc`.

use std::time::Duration;

use starfish::sync::shared_mpsc;
use starfish::{shard_count, shard_id, sleep, spawn, submit_to, Runtime};

struct Report {
    shard: usize,
    done: u32,
}

async fn app() -> starfish::Result<()> {
    let shards = shard_count()?;
    let (reports, mut inbox) = shared_mpsc(16);

    for shard in 0..shards {
        // A clone is a sender in its own right, with its own queue.
        let mut reports = reports.clone();
        spawn(submit_to(shard, move || async move {
            let shard = shard_id().unwrap_or(0);
            for done in 1..=20 {
                // Shards work at different speeds.
                sleep(Duration::from_micros(200 * (shard as u64 + 1))).await;
                if reports.send(Report { shard, done }).await.is_err() {
                    return; // nobody is listening any more
                }
            }
        }));
    }
    // The loop below ends when every sender is gone, so the original must
    // go too.
    drop(reports);

    let mut progress = vec![0u32; shards];
    let mut received = 0u32;
    while let Some(report) = inbox.recv().await {
        if let Some(done) = progress.get_mut(report.shard) {
            // Reports from one shard arrive in the order it sent them.
            assert_eq!(report.done, *done + 1);
            *done = report.done;
        }
        received += 1;
        if received.is_multiple_of(20) {
            println!("after {received:>3} reports: {progress:?}");
        }
    }
    println!("all {shards} shards finished; {received} reports in all");
    Ok(())
}

fn main() -> starfish::Result<()> {
    Runtime::new().shards(4).run(app)?
}
