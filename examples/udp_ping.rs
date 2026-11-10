//! Datagrams back and forth between two UDP sockets.
//!
//! Run with `cargo run --example udp_ping`.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use starfish::net::UdpSocket;
use starfish::{spawn, timeout, Runtime};

async fn app() -> starfish::Result<()> {
    let any_port = SocketAddr::from(([127, 0, 0, 1], 0));
    let server = UdpSocket::bind(any_port)?;
    let server_addr = server.local_addr()?;

    // Answers every datagram with the same bytes, to whoever sent it.
    let responder = spawn(async move {
        let mut buf = Vec::with_capacity(1500);
        loop {
            let (result, filled) = server.recv_from(buf).await;
            let Ok((_, from)) = result else { return };
            let (_, sent) = server.send_to(filled, from).await;
            buf = sent;
        }
    });

    let client = UdpSocket::bind(any_port)?;
    for sequence in 1..=5u32 {
        let start = Instant::now();
        let (result, _) = client.send_to(format!("ping {sequence}"), server_addr).await;
        result?;
        let (result, reply) = client.recv_from(Vec::with_capacity(1500)).await;
        let (bytes, from) = result?;
        println!(
            "{bytes} bytes from {from}: {:?} in {:?}",
            String::from_utf8_lossy(&reply),
            start.elapsed()
        );
    }

    // UDP gives no sign that nobody is listening: guard receives with a
    // timeout.
    responder.abort();
    let (result, _) = client.send_to("anyone there?", server_addr).await;
    result?;
    let waited = timeout(Duration::from_millis(100), client.recv_from(Vec::with_capacity(1500))).await;
    println!("after the server stopped: {}", if waited.is_err() { "no answer" } else { "an answer" });
    Ok(())
}

fn main() -> starfish::Result<()> {
    Runtime::new().shards(1).run(app)?
}
