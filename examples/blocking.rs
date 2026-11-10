//! Calling blocking code without stalling a shard.
//!
//! A shard must never block: everything else on it would wait. Blocking
//! work goes to `run_blocking`, which runs it on a thread outside the
//! runtime while the task waits for the result.
//!
//! Run with `cargo run --example blocking`.

use std::time::{Duration, Instant};

use starfish::{run_blocking, sleep, spawn, Runtime};

async fn app() -> starfish::Result<()> {
    // Shows that the shard keeps running while the blocking call is out.
    let ticker = spawn(async {
        let mut ticks = 0u32;
        loop {
            sleep(Duration::from_millis(10)).await;
            ticks += 1;
            if ticks.is_multiple_of(5) {
                println!("  the shard is still responsive ({ticks} ticks)");
            }
        }
    });

    let start = Instant::now();
    let answer = run_blocking(|| {
        std::thread::sleep(Duration::from_millis(200));
        6 * 7
    })
    .await?;
    println!("the blocking call returned {answer} after {:?}", start.elapsed());
    ticker.abort();

    // The usual candidates: system calls and libraries with no
    // asynchronous form. The closure's own result comes back inside the
    // runtime's, hence the two question marks.
    let addresses = run_blocking(|| {
        use std::net::ToSocketAddrs;
        "localhost:80".to_socket_addrs().map(|found| found.collect::<Vec<_>>())
    })
    .await??;
    println!("the system resolver says localhost is {addresses:?}");

    // Several at once each get a thread; together they take as long as one.
    let start = Instant::now();
    let jobs: Vec<_> = (0..8)
        .map(|_| spawn(run_blocking(|| std::thread::sleep(Duration::from_millis(100)))))
        .collect();
    for job in jobs {
        job.await??;
    }
    println!("8 blocking calls of 100 ms took {:?} together", start.elapsed());
    Ok(())
}

fn main() -> starfish::Result<()> {
    Runtime::new().shards(1).run(app)?
}
