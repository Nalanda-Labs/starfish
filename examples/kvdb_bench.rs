//! A load generator for the `kvdb` example.
//!
//! Start the server, then run
//! `cargo run --release --example kvdb_bench -- <set|get|mixed> [connections] [seconds] [keys]`.
//!
//! Every connection sends one request, waits for the reply, and sends the
//! next. `set` writes, `get` reads, and `mixed` reads nine times for every
//! write. It reports throughput and how long requests took.
//!
//! For meaningful numbers keep the two programs off each other's CPUs, for
//! example `taskset -c 0-7` for the server and `taskset -c 8-15` here.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use starfish::io::BufReader;
use starfish::net::TcpStream;
use starfish::{shard_count, shard_id, spawn, submit_to_all, Runtime};

const PORT: u16 = 7006;
const VALUE: usize = 100;

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Set,
    Get,
    Mixed,
}

/// One connection's work. Returns how long each request took, in
/// microseconds.
async fn client(mode: Mode, keys: u64, seed: u64, deadline: Instant) -> std::io::Result<Vec<u32>> {
    let stream = TcpStream::connect(SocketAddr::from(([127, 0, 0, 1], PORT))).await?;
    stream.set_nodelay(true)?;
    let mut replies = BufReader::new(&stream);
    let value = "v".repeat(VALUE);
    let mut state = seed | 1;
    let mut took = Vec::new();
    while Instant::now() < deadline {
        // xorshift: cheap, and random enough to spread over the keys.
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let key = state % keys;
        let write = match mode {
            Mode::Set => true,
            Mode::Get => false,
            Mode::Mixed => (state >> 32).is_multiple_of(10),
        };
        let request = if write {
            format!("SET key{key} {value}\n")
        } else {
            format!("GET key{key}\n")
        };
        let start = Instant::now();
        let (result, _) = stream.write_all(request).await;
        result?;
        if replies.next_line().await?.is_none() {
            break; // the server went away
        }
        took.push(start.elapsed().as_micros().min(u128::from(u32::MAX)) as u32);
    }
    Ok(took)
}

async fn app(mode: Mode, connections: usize, seconds: u64, keys: u64) -> starfish::Result<()> {
    let shards = shard_count()?;
    let deadline = Instant::now() + Duration::from_secs(seconds);
    let start = Instant::now();
    let per_shard = submit_to_all(move || async move {
        let shard = shard_id().unwrap_or(0);
        let mine = (0..connections).filter(|connection| connection % shards == shard).count();
        let clients: Vec<_> = (0..mine)
            .map(|n| {
                let seed = 0x9E37_79B9_7F4A_7C15u64.wrapping_mul((shard * 1000 + n + 1) as u64);
                spawn(client(mode, keys, seed, deadline))
            })
            .collect();
        let mut took = Vec::new();
        let mut failed = 0;
        for client in clients {
            match client.await {
                Ok(Ok(mut times)) => took.append(&mut times),
                _ => failed += 1,
            }
        }
        (took, failed)
    })
    .await?;
    let elapsed = start.elapsed();

    let mut took = Vec::new();
    let mut failed = 0;
    for (mut times, errors) in per_shard {
        took.append(&mut times);
        failed += errors;
    }
    if took.is_empty() {
        println!("no request completed; is the kvdb example running?");
        return Ok(());
    }
    took.sort_unstable();
    let at = |fraction: f64| took[((took.len() - 1) as f64 * fraction) as usize];
    println!(
        "{} requests in {:.1?} over {connections} connections: {:.0} per second",
        took.len(),
        elapsed,
        took.len() as f64 / elapsed.as_secs_f64()
    );
    println!(
        "time per request: median {} us, 99% within {} us, 99.9% within {} us, slowest {} us",
        at(0.5),
        at(0.99),
        at(0.999),
        at(1.0)
    );
    if failed > 0 {
        println!("{failed} connections failed");
    }
    Ok(())
}

fn main() -> starfish::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let mode = match args.get(1).map(String::as_str) {
        Some("get") => Mode::Get,
        Some("mixed") => Mode::Mixed,
        _ => Mode::Set,
    };
    let connections = args.get(2).and_then(|arg| arg.parse().ok()).unwrap_or(256);
    let seconds = args.get(3).and_then(|arg| arg.parse().ok()).unwrap_or(5);
    let keys = args.get(4).and_then(|arg| arg.parse().ok()).unwrap_or(100_000);
    Runtime::new().run(move || app(mode, connections, seconds, keys))?
}
