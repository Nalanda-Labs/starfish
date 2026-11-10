//! Request-response throughput over loopback TCP: starfish against Tokio
//! and Monoio.
//!
//! `cargo run --release --example bench_echo -- <starfish|tokio|monoio> [threads] [connections] [seconds]`
//!
//! Server and clients share the same threads in both cases: each
//! connection sends a 64-byte message and waits for it to come back.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[global_allocator]
static GLOBAL: starfish::alloc::ShardAlloc = starfish::alloc::ShardAlloc;

const MESSAGE: usize = 64;

fn run_starfish(threads: usize, connections: usize, seconds: u64) -> u64 {
    use starfish::net::{TcpListener, TcpStream};
    use starfish::{shard_id, sleep, spawn, submit_to_all, Runtime};

    Runtime::new()
        .shards(threads)
        .run(move || async move {
            let probe = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
            let addr: SocketAddr = probe.local_addr().unwrap();
            drop(probe);
            submit_to_all(move || async move {
                let listener = TcpListener::bind(addr).unwrap();
                spawn(async move {
                    while let Ok(stream) = listener.accept().await {
                        spawn(async move {
                            stream.set_nodelay(true).ok();
                            let mut buf = Vec::with_capacity(4096);
                            loop {
                                let (result, returned) = stream.read(buf).await;
                                if !matches!(result, Ok(n) if n > 0) {
                                    return;
                                }
                                let (result, returned) = stream.write_all(returned).await;
                                if result.is_err() {
                                    return;
                                }
                                buf = returned;
                            }
                        });
                    }
                });
            })
            .await
            .unwrap();

            let stop = Arc::new(AtomicBool::new(false));
            let flag = stop.clone();
            let counters = spawn(submit_to_all(move || {
                let stop = flag.clone();
                async move {
                    let shard = shard_id().unwrap();
                    let mine = (0..connections).filter(|c| c % threads == shard).count();
                    let clients: Vec<_> = (0..mine)
                        .map(|_| {
                            let stop = stop.clone();
                            spawn(async move {
                                let stream = TcpStream::connect(addr).await.unwrap();
                                stream.set_nodelay(true).ok();
                                let mut out = vec![7u8; MESSAGE];
                                let mut buf = Vec::with_capacity(4096);
                                let mut done = 0u64;
                                while !stop.load(Ordering::Relaxed) {
                                    let (result, returned) = stream.write_all(out).await;
                                    result.unwrap();
                                    out = returned;
                                    let mut got = 0;
                                    while got < MESSAGE {
                                        let (result, returned) = stream.read(buf).await;
                                        got += result.unwrap();
                                        buf = returned;
                                    }
                                    done += 1;
                                }
                                done
                            })
                        })
                        .collect();
                    let mut total = 0;
                    for client in clients {
                        total += client.await.unwrap();
                    }
                    total
                }
            }));
            sleep(Duration::from_secs(seconds)).await;
            stop.store(true, Ordering::Relaxed);
            counters.await.unwrap().unwrap().into_iter().sum()
        })
        .unwrap()
}

fn run_tokio(threads: usize, connections: usize, seconds: u64) -> u64 {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async move {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    stream.set_nodelay(true).ok();
                    let mut buf = vec![0u8; 4096];
                    loop {
                        match stream.read(&mut buf).await {
                            Ok(n) if n > 0 => {
                                if stream.write_all(&buf[..n]).await.is_err() {
                                    return;
                                }
                            }
                            _ => return,
                        }
                    }
                });
            }
        });
        let stop = Arc::new(AtomicBool::new(false));
        let total = Arc::new(AtomicU64::new(0));
        let mut clients = Vec::new();
        for _ in 0..connections {
            let (stop, total) = (stop.clone(), total.clone());
            clients.push(tokio::spawn(async move {
                let mut stream = TcpStream::connect(addr).await.unwrap();
                stream.set_nodelay(true).ok();
                let out = [7u8; MESSAGE];
                let mut buf = [0u8; MESSAGE];
                let mut done = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    stream.write_all(&out).await.unwrap();
                    stream.read_exact(&mut buf).await.unwrap();
                    done += 1;
                }
                total.fetch_add(done, Ordering::Relaxed);
            }));
        }
        tokio::time::sleep(Duration::from_secs(seconds)).await;
        stop.store(true, Ordering::Relaxed);
        for client in clients {
            client.await.unwrap();
        }
        total.load(Ordering::Relaxed)
    })
}

/// Monoio is thread-per-core like starfish: every thread runs its own
/// runtime with a listener on the shared port and its share of the
/// clients, and is pinned to a CPU.
fn run_monoio(threads: usize, connections: usize, seconds: u64) -> u64 {
    use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
    use monoio::net::{ListenerOpts, TcpListener, TcpStream};
    use std::sync::atomic::AtomicUsize;

    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr: SocketAddr = probe.local_addr().unwrap();
    drop(probe);
    let stop = Arc::new(AtomicBool::new(false));
    let finished = Arc::new(AtomicUsize::new(0));
    let bound = Arc::new(std::sync::Barrier::new(threads));

    let workers: Vec<_> = (0..threads)
        .map(|thread| {
            let (stop, finished, bound) = (stop.clone(), finished.clone(), bound.clone());
            std::thread::spawn(move || {
                pin_to_cpu(thread);
                let mut runtime = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
                    .enable_timer()
                    .build()
                    .unwrap();
                runtime.block_on(async move {
                    let options = ListenerOpts::new().reuse_port(true);
                    let listener = TcpListener::bind_with_config(addr, &options).unwrap();
                    // Nobody connects before every thread listens.
                    bound.wait();
                    monoio::spawn(async move {
                        while let Ok((mut stream, _)) = listener.accept().await {
                            monoio::spawn(async move {
                                stream.set_nodelay(true).ok();
                                let mut buf = Vec::with_capacity(4096);
                                loop {
                                    let (result, returned) = stream.read(buf).await;
                                    if !matches!(result, Ok(n) if n > 0) {
                                        return;
                                    }
                                    let (result, returned) = stream.write_all(returned).await;
                                    if result.is_err() {
                                        return;
                                    }
                                    buf = returned;
                                }
                            });
                        }
                    });

                    let mine = (0..connections).filter(|c| c % threads == thread).count();
                    let clients: Vec<_> = (0..mine)
                        .map(|_| {
                            let stop = stop.clone();
                            monoio::spawn(async move {
                                let mut stream = TcpStream::connect_addr(addr).await.unwrap();
                                stream.set_nodelay(true).ok();
                                let mut out = vec![7u8; MESSAGE];
                                let mut buf = Vec::with_capacity(4096);
                                let mut done = 0u64;
                                while !stop.load(Ordering::Relaxed) {
                                    let (result, returned) = stream.write_all(out).await;
                                    result.unwrap();
                                    out = returned;
                                    let mut got = 0;
                                    while got < MESSAGE {
                                        let (result, returned) = stream.read(buf).await;
                                        buf = returned;
                                        match result.unwrap() {
                                            0 => return done,
                                            received => got += received,
                                        }
                                    }
                                    done += 1;
                                }
                                done
                            })
                        })
                        .collect();
                    let mut total = 0;
                    for client in clients {
                        total += client.await;
                    }
                    // Keep serving until the other threads' clients are
                    // done too.
                    finished.fetch_add(1, Ordering::SeqCst);
                    while finished.load(Ordering::SeqCst) < threads {
                        monoio::time::sleep(Duration::from_millis(1)).await;
                    }
                    total
                })
            })
        })
        .collect();

    std::thread::sleep(Duration::from_secs(seconds));
    stop.store(true, Ordering::Relaxed);
    workers.into_iter().map(|worker| worker.join().unwrap()).sum()
}

/// Pins the calling thread to the `index`-th CPU the process may use.
fn pin_to_cpu(index: usize) {
    // SAFETY: plain system calls on a zero-initialised CPU set.
    unsafe {
        let mut allowed: libc::cpu_set_t = std::mem::zeroed();
        if libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut allowed) != 0 {
            return;
        }
        let cpus: Vec<usize> = (0..libc::CPU_SETSIZE as usize)
            .filter(|cpu| libc::CPU_ISSET(*cpu, &allowed))
            .collect();
        if cpus.is_empty() {
            return;
        }
        let mut one: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(cpus[index % cpus.len()], &mut one);
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &one);
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let which = args.get(1).map(String::as_str).unwrap_or("starfish");
    let threads = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(4);
    let connections = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(64);
    let seconds = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(3);
    let start = Instant::now();
    let requests = match which {
        "tokio" => run_tokio(threads, connections, seconds),
        "monoio" => run_monoio(threads, connections, seconds),
        _ => run_starfish(threads, connections, seconds),
    };
    let rate = requests as f64 / start.elapsed().as_secs_f64();
    println!("{which:6} threads={threads} connections={connections}: {rate:>10.0} requests/s");
}
