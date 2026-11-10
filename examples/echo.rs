//! A TCP echo server with one listener per shard, a limit on connections
//! and an orderly shutdown.
//!
//! Run with `cargo run --release --example echo`, then `nc 127.0.0.1 7000`.
//! Ctrl-C stops it: the shards stop accepting, connections get a few
//! seconds to finish, and the process exits.

use std::cell::Cell;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

use starfish::net::{TcpListener, TcpStream};
use starfish::sync::{Gate, Semaphore};
use starfish::{shard_id, spawn, timeout, JoinHandle, Runtime, Sharded};

/// Connections each shard serves at a time; further ones wait in the
/// kernel's accept queue.
const MAX_CONNECTIONS: usize = 10_000;
/// How long connections get to finish once the server is asked to stop.
const GRACE: Duration = Duration::from_secs(5);

/// One shard's part of the server.
struct Server {
    /// One permit per connection being served.
    connections: Semaphore,
    /// Entered by every connection, so that shutdown can wait for them.
    serving: Gate,
    acceptor: Cell<Option<JoinHandle<()>>>,
}

async fn echo(stream: TcpStream) -> std::io::Result<()> {
    let mut buf = Vec::with_capacity(16 * 1024);
    loop {
        let (result, returned) = stream.read(buf).await;
        if result? == 0 {
            return Ok(());
        }
        let (result, returned) = stream.write_all(returned).await;
        result?;
        buf = returned;
    }
}

async fn accept(listener: TcpListener, server: Rc<Server>) {
    let shard = shard_id().unwrap_or(0);
    loop {
        // Both fail only once the server is shutting down.
        let Ok(permit) = server.connections.acquire(1).await else { return };
        match listener.accept().await {
            Ok(stream) => {
                let Ok(guard) = server.serving.enter() else { return };
                spawn(async move {
                    let _held = (permit, guard);
                    if let Err(err) = echo(stream).await {
                        eprintln!("shard {shard}: connection error: {err}");
                    }
                });
            }
            Err(err) => eprintln!("shard {shard}: accept failed: {err}"),
        }
    }
}

fn main() -> starfish::Result<()> {
    let addr: SocketAddr = match "127.0.0.1:7000".parse() {
        Ok(addr) => addr,
        Err(err) => return Err(std::io::Error::other(err).into()),
    };
    Runtime::new().run(move || async move {
        let server = Sharded::start(|_shard| Server {
            connections: Semaphore::new(MAX_CONNECTIONS),
            serving: Gate::new(),
            acceptor: Cell::new(None),
        })
        .await?;

        // Every shard binds the same port; the kernel balances connections.
        let bound = server
            .invoke_on_all(move |server| async move {
                let listener = TcpListener::bind(addr)?;
                let acceptor = spawn(accept(listener, server.clone()));
                server.acceptor.set(Some(acceptor));
                Ok::<(), std::io::Error>(())
            })
            .await?;
        for result in bound {
            result?;
        }
        println!("echo server on {addr}, {} shards", starfish::shard_count()?);

        let signal = starfish::signal::termination().await?;
        println!("signal {signal}: shutting down");

        // On every shard: stop accepting, which also closes the listener,
        // then give the connections some time.
        let unfinished = server
            .map_reduce(
                |server| async move {
                    if let Some(acceptor) = server.acceptor.take() {
                        acceptor.abort();
                    }
                    let _ = timeout(GRACE, server.serving.close()).await;
                    server.serving.active()
                },
                0,
                |total, shard| total + shard,
            )
            .await?;
        if unfinished > 0 {
            println!("dropping {unfinished} connections that did not finish");
        }
        Ok(())
    })?
}
