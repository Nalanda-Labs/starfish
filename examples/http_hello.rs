//! A minimal HTTP/1.1 server with one listener per shard.
//!
//! It answers every request with a short text and keeps connections open
//! for further requests. It does not parse requests beyond finding where
//! each one ends, so it is a demonstration, not a web server.
//!
//! Run with `cargo run --release --example http_hello`, then
//! `curl http://127.0.0.1:7005/`. Ctrl-C stops it.

use std::net::SocketAddr;

use starfish::net::{TcpListener, TcpStream};
use starfish::{shard_count, shard_id, spawn, submit_to_all, Runtime};

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|window| window == needle)
}

async fn serve(stream: TcpStream, shard: usize) -> std::io::Result<()> {
    stream.set_nodelay(true)?;
    // Bytes received that do not make a whole request yet.
    let mut pending = Vec::new();
    let mut buf = Vec::with_capacity(8192);
    loop {
        let (result, filled) = stream.read(buf).await;
        if result? == 0 {
            return Ok(());
        }
        pending.extend_from_slice(&filled);
        buf = filled;
        // A request without a body ends at the first empty line. Several
        // may have arrived at once.
        while let Some(end) = find(&pending, b"\r\n\r\n") {
            pending.drain(..end + 4);
            let body = format!("hello from shard {shard}\n");
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: {}\r\n\r\n{body}",
                body.len()
            );
            let (result, _) = stream.write_all(response).await;
            result?;
        }
    }
}

async fn app() -> starfish::Result<()> {
    let addr = SocketAddr::from(([127, 0, 0, 1], 7005));
    let bound = submit_to_all(move || async move {
        let listener = TcpListener::bind(addr)?;
        let shard = shard_id()?;
        spawn(async move {
            loop {
                match listener.accept().await {
                    Ok(stream) => {
                        spawn(async move {
                            // A client that hangs up is not worth a message.
                            let _ = serve(stream, shard).await;
                        });
                    }
                    Err(err) => eprintln!("shard {shard}: accept failed: {err}"),
                }
            }
        });
        Ok::<(), starfish::Error>(())
    })
    .await?;
    for result in bound {
        result?;
    }
    println!("http://{addr}/ on {} shards", shard_count()?);

    let signal = starfish::signal::termination().await?;
    println!("signal {signal}: stopping");
    Ok(())
}

fn main() -> starfish::Result<()> {
    Runtime::new().run(app)?
}
