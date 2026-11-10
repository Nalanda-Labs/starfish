//! Accepting on one shard and serving on others.
//!
//! The usual design is one listener per shard (see the echo example). But
//! a socket is not tied to the shard that accepted it: it can be moved
//! into a closure for another shard and used there. Here shard 0 accepts
//! and deals the connections out in turn.
//!
//! Run with `cargo run --example dispatcher`.

use std::net::SocketAddr;

use starfish::net::{TcpListener, TcpStream};
use starfish::{shard_count, shard_id, spawn, submit_to, Runtime};

async fn app() -> starfish::Result<()> {
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))?;
    let addr = listener.local_addr()?;
    let shards = shard_count()?;
    let connections = shards * 2;

    // The clients: each connects and prints whatever it is told.
    let clients = spawn(async move {
        let mut replies = Vec::new();
        for _ in 0..connections {
            let stream = TcpStream::connect(addr).await?;
            let (result, reply) = stream.read(Vec::with_capacity(64)).await;
            result?;
            replies.push(String::from_utf8_lossy(&reply).into_owned());
        }
        Ok::<_, std::io::Error>(replies)
    });

    for n in 0..connections {
        let stream = listener.accept().await?;
        let target = n % shards;
        // Spawned, so that accepting goes on while the connection is served.
        spawn(submit_to(target, move || async move {
            let greeting = format!("served by shard {}", shard_id().unwrap_or(usize::MAX));
            let _ = stream.write_all(greeting).await;
        }));
    }

    for (n, reply) in clients.await??.iter().enumerate() {
        println!("connection {n}: {reply}");
    }
    Ok(())
}

fn main() -> starfish::Result<()> {
    Runtime::new().shards(4).run(app)?
}
