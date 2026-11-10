//! A TCP server and a client in one program.
//!
//! Run with `cargo run --example tcp_roundtrip`.

use std::net::SocketAddr;

use starfish::net::{TcpListener, TcpStream};
use starfish::{spawn, Runtime};

/// Sends back everything it receives, in upper case, until the client
/// stops sending.
async fn shout(stream: TcpStream) -> std::io::Result<usize> {
    let mut total = 0;
    let mut buf = Vec::with_capacity(4096);
    loop {
        // The buffer goes in by value and comes back with the result: the
        // kernel writes into it while the call is in flight.
        let (result, mut filled) = stream.read(buf).await;
        let received = result?;
        if received == 0 {
            return Ok(total);
        }
        total += received;
        filled.make_ascii_uppercase();
        let (result, sent) = stream.write_all(filled).await;
        result?;
        buf = sent;
    }
}

async fn app() -> starfish::Result<()> {
    // Port 0 lets the kernel pick a free port.
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))?;
    let addr = listener.local_addr()?;
    println!("listening on {addr}");
    let server = spawn(async move {
        let stream = listener.accept().await?;
        println!("server: connection from {}", stream.peer_addr()?);
        shout(stream).await
    });

    let client = TcpStream::connect(addr).await?;
    client.set_nodelay(true)?;
    for line in ["hello", "thread per core", "goodbye"] {
        let (result, _) = client.write_all(line).await;
        result?;
        let (result, reply) = client.read(Vec::with_capacity(4096)).await;
        result?;
        println!("client: sent {line:?}, got {:?}", String::from_utf8_lossy(&reply));
    }
    // Closing our sending side is what ends the server's loop.
    client.shutdown(std::net::Shutdown::Write)?;

    println!("server: echoed {} bytes", server.await??);
    Ok(())
}

fn main() -> starfish::Result<()> {
    Runtime::new().shards(1).run(app)?
}
