//! A TCP proxy: accepts connections and relays them to another server.
//!
//! Each connection is two copies running at once, one per direction, on
//! the same pair of sockets. Socket calls take `&self`, so the two tasks
//! share the sockets through an `Rc`.
//!
//! Start the echo example first (`cargo run --release --example echo`),
//! then `cargo run --release --example proxy` and `nc 127.0.0.1 7001`.
//! Ctrl-C stops it.

use std::net::{Shutdown, SocketAddr};
use std::rc::Rc;

use starfish::net::{TcpListener, TcpStream};
use starfish::{shard_count, spawn, submit_to_all, Runtime};

const LISTEN: ([u8; 4], u16) = ([127, 0, 0, 1], 7001);
const UPSTREAM: ([u8; 4], u16) = ([127, 0, 0, 1], 7000);

/// Copies until `from` has nothing more to send, then tells `to` so.
async fn copy(from: Rc<TcpStream>, to: Rc<TcpStream>) -> std::io::Result<u64> {
    let mut total = 0;
    let mut buf = Vec::with_capacity(64 * 1024);
    loop {
        let (result, filled) = from.read(buf).await;
        let received = result?;
        if received == 0 {
            // Pass the end of the stream on, but keep the other direction.
            to.shutdown(Shutdown::Write)?;
            return Ok(total);
        }
        total += received as u64;
        let (result, sent) = to.write_all(filled).await;
        result?;
        buf = sent;
    }
}

async fn relay(client: TcpStream) -> std::io::Result<(u64, u64)> {
    let upstream = Rc::new(TcpStream::connect(SocketAddr::from(UPSTREAM)).await?);
    let client = Rc::new(client);
    let outbound = spawn(copy(client.clone(), upstream.clone()));
    let inbound = copy(upstream, client).await;
    // If the way back failed, the way out has no purpose any more.
    if inbound.is_err() {
        outbound.abort();
    }
    let sent = outbound.await.map_err(std::io::Error::from)??;
    Ok((sent, inbound?))
}

async fn app() -> starfish::Result<()> {
    let addr = SocketAddr::from(LISTEN);
    let bound = submit_to_all(move || async move {
        let listener = TcpListener::bind(addr)?;
        spawn(async move {
            loop {
                match listener.accept().await {
                    Ok(client) => {
                        spawn(async move {
                            match relay(client).await {
                                Ok((sent, received)) => {
                                    println!("connection done: {sent} bytes out, {received} back")
                                }
                                Err(err) => eprintln!("connection failed: {err}"),
                            }
                        });
                    }
                    Err(err) => eprintln!("accept failed: {err}"),
                }
            }
        });
        Ok::<(), std::io::Error>(())
    })
    .await?;
    for result in bound {
        result?;
    }
    println!(
        "proxy on {addr} to {}, {} shards",
        SocketAddr::from(UPSTREAM),
        shard_count()?
    );

    let signal = starfish::signal::termination().await?;
    println!("signal {signal}: stopping");
    Ok(())
}

fn main() -> starfish::Result<()> {
    Runtime::new().run(app)?
}
