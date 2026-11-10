//! Cancelling a task.
//!
//! Run with `cargo run --example abort`.

use std::time::Duration;

use starfish::{sleep, spawn, Error, Runtime};

/// Announces when it is dropped, to show when a task's state goes away.
struct Cleanup(&'static str);

impl Drop for Cleanup {
    fn drop(&mut self) {
        println!("  {}: cleaned up", self.0);
    }
}

async fn app() -> starfish::Result<()> {
    let ticker = spawn(async {
        let _cleanup = Cleanup("ticker");
        let mut tick = 0u64;
        loop {
            tick += 1;
            println!("  tick {tick}");
            sleep(Duration::from_millis(100)).await;
        }
    });
    sleep(Duration::from_millis(350)).await;

    // The task's future, and everything it owns, is dropped inside this
    // call. Its timer is disarmed; I/O would be cancelled the same way.
    println!("aborting");
    ticker.abort();
    println!("abort returned; finished: {}", ticker.is_finished());
    match ticker.await {
        Err(Error::Canceled) => println!("the handle reports: canceled"),
        other => println!("unexpected: {other:?}"),
    }

    // A task that has already finished keeps its result.
    let done = spawn(async { 42 });
    sleep(Duration::from_millis(1)).await;
    done.abort();
    println!("aborting a finished task changes nothing: {}", done.await?);
    Ok(())
}

fn main() -> starfish::Result<()> {
    Runtime::new().shards(1).run(app)?
}
