//! Putting a time limit on I/O.
//!
//! Dropping an I/O future cancels the operation in the kernel, so
//! `timeout` works on any of them.
//!
//! Run with `cargo run --example io_timeout`.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use starfish::net::{TcpListener, TcpStream};
use starfish::{spawn, timeout, Runtime};

async fn app() -> starfish::Result<()> {
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))?;
    let addr = listener.local_addr()?;

    // Nobody connects.
    let start = Instant::now();
    let accepted = timeout(Duration::from_millis(50), listener.accept()).await;
    println!("accept: {} after {:?}", outcome(accepted.is_ok()), start.elapsed());

    // The listener is unharmed and takes the next connection.
    let connecting = spawn(async move { TcpStream::connect(addr).await });
    let server = timeout(Duration::from_secs(1), listener.accept()).await;
    let Ok(server) = server else {
        println!("unexpected: no connection");
        return Ok(());
    };
    let server = server?;
    let client = connecting.await??;
    println!("accept: connection from {}", server.peer_addr()?);

    // The peer is connected but silent.
    let start = Instant::now();
    let read = timeout(Duration::from_millis(50), server.read(Vec::with_capacity(64))).await;
    println!("read: {} after {:?}", outcome(read.is_ok()), start.elapsed());

    // The connection is unharmed too.
    let (result, _) = client.write_all("late, but here").await;
    result?;
    let read = timeout(Duration::from_secs(1), server.read(Vec::with_capacity(64))).await;
    if let Ok((result, buf)) = read {
        result?;
        println!("read: {:?}", String::from_utf8_lossy(&buf));
    }

    // A connect to a port nobody listens on fails by itself, quickly.
    drop(listener);
    match timeout(Duration::from_secs(1), TcpStream::connect(addr)).await {
        Ok(Err(err)) => println!("connect: {err}"),
        Ok(Ok(_)) => println!("connect: unexpectedly connected"),
        Err(_) => println!("connect: timed out"),
    }
    Ok(())
}

fn outcome(completed: bool) -> &'static str {
    if completed {
        "completed"
    } else {
        "timed out"
    }
}

fn main() -> starfish::Result<()> {
    Runtime::new().shards(1).run(app)?
}
