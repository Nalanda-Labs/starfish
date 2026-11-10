//! A TCP echo server with one listener per shard.
//!
//! Run with `cargo run --release --example echo`, then `nc 127.0.0.1 7000`.

use std::net::SocketAddr;

use brine::net::{TcpListener, TcpStream};
use brine::{shard_id, sleep, spawn, submit_to_all, Runtime};

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

fn main() -> brine::Result<()> {
    let addr: SocketAddr = match "127.0.0.1:7000".parse() {
        Ok(addr) => addr,
        Err(err) => return Err(std::io::Error::other(err).into()),
    };
    Runtime::new().run(move || async move {
        // Every shard binds the same port; the kernel balances connections.
        let bound = submit_to_all(move || async move {
            let listener = TcpListener::bind(addr)?;
            let shard = shard_id()?;
            spawn(async move {
                loop {
                    match listener.accept().await {
                        Ok(stream) => {
                            spawn(async move {
                                if let Err(err) = echo(stream).await {
                                    eprintln!("shard {shard}: connection error: {err}");
                                }
                            });
                        }
                        Err(err) => eprintln!("shard {shard}: accept failed: {err}"),
                    }
                }
            });
            Ok::<(), brine::Error>(())
        })
        .await?;
        for result in bound {
            result?;
        }
        println!("echo server on {addr}, {} shards", brine::shard_count()?);
        loop {
            sleep(std::time::Duration::from_secs(3600)).await;
        }
    })?
}
