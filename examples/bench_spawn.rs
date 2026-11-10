//! Cost of spawning a task and awaiting its result, starfish against Tokio.
//!
//! `cargo run --release --example bench_spawn -- <starfish|tokio> [tasks]`

use std::time::Instant;

#[global_allocator]
static GLOBAL: starfish::alloc::ShardAlloc = starfish::alloc::ShardAlloc;

const BATCH: usize = 1000;

fn run_starfish(tasks: usize) -> (u64, f64) {
    starfish::Runtime::new()
        .shards(1)
        .run(move || async move {
            let start = Instant::now();
            let mut sum = 0u64;
            let mut handles = Vec::with_capacity(BATCH);
            for batch in 0..tasks / BATCH {
                for i in 0..BATCH {
                    let value = (batch * BATCH + i) as u64;
                    handles.push(starfish::spawn(async move { value }));
                }
                for handle in handles.drain(..) {
                    sum += handle.await.unwrap();
                }
            }
            (sum, start.elapsed().as_nanos() as f64 / tasks as f64)
        })
        .unwrap()
}

fn run_tokio(tasks: usize) -> (u64, f64) {
    let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async move {
        let start = Instant::now();
        let mut sum = 0u64;
        let mut handles = Vec::with_capacity(BATCH);
        for batch in 0..tasks / BATCH {
            for i in 0..BATCH {
                let value = (batch * BATCH + i) as u64;
                handles.push(tokio::task::spawn_local(async move { value }));
            }
            for handle in handles.drain(..) {
                sum += handle.await.unwrap();
            }
        }
        (sum, start.elapsed().as_nanos() as f64 / tasks as f64)
    })
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let which = args.get(1).map(String::as_str).unwrap_or("starfish");
    let tasks: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(5_000_000);
    let (sum, nanos) = match which {
        "tokio" => run_tokio(tasks),
        _ => run_starfish(tasks),
    };
    println!("{which:6} {tasks} tasks: {nanos:6.1} ns per spawn and join (checksum {sum})");
}
