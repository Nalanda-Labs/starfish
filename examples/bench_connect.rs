//! How fast connections can be made and dropped: starfish against Tokio
//! and Monoio, on one thread.
//!
//! `cargo run --release --example bench_connect -- <starfish|tokio|monoio> [clients] [seconds]`
//!
//! A server accepts connections on a Unix-domain socket and closes each
//! one at once; clients connect, wait for the close, and connect again.
//! Unix sockets are used because TCP on loopback runs out of ports at
//! these rates. Everything runs on a single thread, so this measures the
//! cost of the accept and connect paths and nothing else.

use std::path::PathBuf;
use std::time::{Duration, Instant};

#[global_allocator]
static GLOBAL: starfish::alloc::ShardAlloc = starfish::alloc::ShardAlloc;

fn socket_path() -> PathBuf {
    let path = std::env::temp_dir().join(format!("starfish-bench-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&path);
    path
}

fn run_starfish(clients: usize, seconds: u64) -> u64 {
    use starfish::net::{UnixListener, UnixStream};
    use starfish::spawn;

    let path = socket_path();
    let total = starfish::Runtime::new()
        .shards(1)
        .run({
            let path = path.clone();
            move || async move {
                let listener = UnixListener::bind(&path).unwrap();
                spawn(async move {
                    // Accepted and dropped: the client sees the close.
                    while listener.accept().await.is_ok() {}
                });
                let deadline = Instant::now() + Duration::from_secs(seconds);
                let tasks: Vec<_> = (0..clients)
                    .map(|_| {
                        let path = path.clone();
                        spawn(async move {
                            let mut made = 0u64;
                            while Instant::now() < deadline {
                                let stream = UnixStream::connect(&path).await.unwrap();
                                let (result, _) = stream.read(Vec::with_capacity(8)).await;
                                result.unwrap();
                                made += 1;
                            }
                            made
                        })
                    })
                    .collect();
                let mut total = 0;
                for task in tasks {
                    total += task.await.unwrap();
                }
                total
            }
        })
        .unwrap();
    let _ = std::fs::remove_file(&path);
    total
}

fn run_monoio(clients: usize, seconds: u64) -> u64 {
    use monoio::io::AsyncReadRent;
    use monoio::net::{UnixListener, UnixStream};

    let path = socket_path();
    let mut runtime = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
        .enable_timer()
        .build()
        .unwrap();
    let total = runtime.block_on({
        let path = path.clone();
        async move {
            // Its default asks for SO_REUSEPORT, which Unix sockets refuse.
            let options = monoio::net::ListenerOpts::new().reuse_port(false);
            let listener = UnixListener::bind_with_config(&path, &options).unwrap();
            monoio::spawn(async move { while listener.accept().await.is_ok() {} });
            let deadline = Instant::now() + Duration::from_secs(seconds);
            let tasks: Vec<_> = (0..clients)
                .map(|_| {
                    let path = path.clone();
                    monoio::spawn(async move {
                        let mut made = 0u64;
                        while Instant::now() < deadline {
                            let mut stream = UnixStream::connect(&path).await.unwrap();
                            let (result, _) = stream.read(Vec::with_capacity(8)).await;
                            result.unwrap();
                            made += 1;
                        }
                        made
                    })
                })
                .collect();
            let mut total = 0;
            for task in tasks {
                total += task.await;
            }
            total
        }
    });
    let _ = std::fs::remove_file(&path);
    total
}

fn run_tokio(clients: usize, seconds: u64) -> u64 {
    use tokio::io::AsyncReadExt;
    use tokio::net::{UnixListener, UnixStream};

    let path = socket_path();
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let total = runtime.block_on({
        let path = path.clone();
        async move {
            let listener = UnixListener::bind(&path).unwrap();
            tokio::spawn(async move { while listener.accept().await.is_ok() {} });
            let deadline = Instant::now() + Duration::from_secs(seconds);
            let tasks: Vec<_> = (0..clients)
                .map(|_| {
                    let path = path.clone();
                    tokio::spawn(async move {
                        let mut made = 0u64;
                        let mut buf = [0u8; 8];
                        while Instant::now() < deadline {
                            let mut stream = UnixStream::connect(&path).await.unwrap();
                            let closed = stream.read(&mut buf).await.unwrap();
                            assert_eq!(closed, 0);
                            made += 1;
                        }
                        made
                    })
                })
                .collect();
            let mut total = 0;
            for task in tasks {
                total += task.await.unwrap();
            }
            total
        }
    });
    let _ = std::fs::remove_file(&path);
    total
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let which = args.get(1).map(String::as_str).unwrap_or("starfish");
    let clients = args.get(2).and_then(|arg| arg.parse().ok()).unwrap_or(64);
    let seconds = args.get(3).and_then(|arg| arg.parse().ok()).unwrap_or(3);
    let start = Instant::now();
    let made = match which {
        "tokio" => run_tokio(clients, seconds),
        "monoio" => run_monoio(clients, seconds),
        _ => run_starfish(clients, seconds),
    };
    let rate = made as f64 / start.elapsed().as_secs_f64();
    println!("{which:8} clients={clients}: {rate:>9.0} connections/s");
}
