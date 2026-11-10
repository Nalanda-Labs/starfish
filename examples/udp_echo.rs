//! A UDP echo server with one socket per shard.
//!
//! Every shard binds the same port; the kernel spreads the datagrams over
//! the sockets by sender address.
//!
//! Run with `cargo run --release --example udp_echo`, then
//! `nc -u 127.0.0.1 7002`. Ctrl-C stops it.

use std::net::SocketAddr;

use starfish::net::UdpSocket;
use starfish::{shard_count, spawn, submit_to_all, Runtime};

async fn echo(socket: UdpSocket) {
    let mut buf = Vec::with_capacity(64 * 1024);
    loop {
        let (result, filled) = socket.recv_from(buf).await;
        buf = match result {
            Ok((_, from)) => socket.send_to(filled, from).await.1,
            Err(err) => {
                eprintln!("receive failed: {err}");
                filled
            }
        };
    }
}

async fn app() -> starfish::Result<()> {
    let addr = SocketAddr::from(([127, 0, 0, 1], 7002));
    let bound = submit_to_all(move || async move {
        let socket = UdpSocket::bind(addr)?;
        spawn(echo(socket));
        Ok::<(), std::io::Error>(())
    })
    .await?;
    for result in bound {
        result?;
    }
    println!("UDP echo on {addr}, {} shards", shard_count()?);

    // Returning from here drops the echo tasks and closes the sockets.
    let signal = starfish::signal::termination().await?;
    println!("signal {signal}: stopping");
    Ok(())
}

fn main() -> starfish::Result<()> {
    Runtime::new().run(app)?
}
