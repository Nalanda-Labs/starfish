//! Waiting for work in progress before shutting down.
//!
//! Run with `cargo run --example gate`.

use std::time::{Duration, Instant};

use starfish::sync::Gate;
use starfish::{sleep, spawn, timeout, Runtime};

async fn app() -> starfish::Result<()> {
    let gate = Gate::new();
    let start = Instant::now();

    // Every request enters the gate and holds the guard until it is done.
    for request in 1..=4u64 {
        let guard = gate.enter()?;
        spawn(async move {
            let _guard = guard;
            sleep(Duration::from_millis(30 * request)).await;
            println!("request {request} finished at {:>3} ms", start.elapsed().as_millis());
        });
    }
    println!("{} requests in flight", gate.active());

    // Closing refuses new work at once and completes when the last guard
    // is gone.
    let closing = gate.close();
    println!("a new request now: {}", if gate.enter().is_ok() { "admitted" } else { "refused" });
    closing.await;
    println!("gate closed at {:>3} ms, {} in flight", start.elapsed().as_millis(), gate.active());

    // A request that never finishes should not hold up shutdown forever.
    let gate = Gate::new();
    let guard = gate.enter()?;
    spawn(async move {
        let _guard = guard;
        std::future::pending::<()>().await;
    });
    if timeout(Duration::from_millis(50), gate.close()).await.is_err() {
        println!("gave up after 50 ms with {} request still running", gate.active());
    }
    Ok(())
}

fn main() -> starfish::Result<()> {
    Runtime::new().shards(1).run(app)?
}
