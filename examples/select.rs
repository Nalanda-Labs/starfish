//! Waiting for whichever of several things happens first.
//!
//! A worker takes jobs from a channel, reports once in a while on a timer,
//! and stops when told to. It waits for all three at once with `select`,
//! nested for the third.
//!
//! Run with `cargo run --example select`.

use std::time::{Duration, Instant};

use starfish::sync::{channel, oneshot, OneshotReceiver, Receiver};
use starfish::{interval, select, sleep, spawn, Either, Runtime};

async fn worker(mut jobs: Receiver<u32>, stop: OneshotReceiver<()>) -> u32 {
    let start = Instant::now();
    let mut report = interval(Duration::from_millis(50));
    // Kept across the rounds: a reference is passed, so losing a round
    // does not drop it.
    let mut stop = std::pin::pin!(stop);
    let mut done = 0;
    loop {
        // The first future is looked at first, and when it is ready the
        // second is not looked at. So the rare events come first: with the
        // jobs in front, which are always waiting, the report would never
        // get a turn.
        match select(stop.as_mut(), select(report.tick(), jobs.recv())).await {
            Either::First(_) => {
                println!("{:>3} ms: told to stop", start.elapsed().as_millis());
                return done;
            }
            Either::Second(Either::First(())) => {
                println!("{:>3} ms: {done} jobs done so far", start.elapsed().as_millis());
            }
            Either::Second(Either::Second(Some(job))) => {
                sleep(Duration::from_millis(u64::from(job))).await;
                done += 1;
            }
            Either::Second(Either::Second(None)) => {
                println!("{:>3} ms: no more jobs", start.elapsed().as_millis());
                return done;
            }
        }
    }
}

async fn app() -> starfish::Result<()> {
    let (jobs, inbox) = channel(4);
    let (stop, stopped) = oneshot();
    let worker = spawn(worker(inbox, stopped));

    // Jobs of 7 ms each, as fast as the worker takes them.
    let producer = spawn(async move {
        loop {
            if jobs.send(7).await.is_err() {
                return;
            }
        }
    });

    sleep(Duration::from_millis(180)).await;
    let _ = stop.send(());
    println!("the worker finished {} jobs", worker.await?);
    producer.abort();
    Ok(())
}

fn main() -> starfish::Result<()> {
    Runtime::new().shards(1).run(app)?
}
