//! Receiving with one standing request and buffers the kernel picks.
//!
//! An ordinary `read` is a request each time: here is a buffer, fill it.
//! A `RecvStream` asks once. From then on the kernel takes a buffer from
//! the shard's pool whenever data arrives and hands it over, and a buffer
//! received can be sent on as it is, without copying.
//!
//! The program runs a line-counting server on a receive stream, sends it
//! a megabyte in pieces, and reports what came back and how the data was
//! cut up on the way.
//!
//! Run with `cargo run --release --example recv_stream`.

use std::net::{Shutdown, SocketAddr};

use starfish::net::{TcpListener, TcpStream};
use starfish::{spawn, Runtime};

/// Buffers in each shard's pool, and the size of each.
const BUFFERS: u16 = 16;
const SIZE: usize = 4096;

/// Counts newlines in everything received on one connection, then sends
/// the count back. Returns how many buffers the data arrived in.
async fn serve(stream: TcpStream) -> std::io::Result<usize> {
    let mut incoming = stream.recv_stream()?;
    let (mut lines, mut pieces) = (0usize, 0usize);
    // `None` is the peer closing its side.
    while let Some(data) = incoming.next().await? {
        lines += data.iter().filter(|&&byte| byte == b'\n').count();
        pieces += 1;
        // `data` goes back to the pool here, at the end of the turn. The
        // pool has 16 buffers; holding on to them would stop the stream.
    }
    let (result, _) = stream.write_all(format!("{lines}\n")).await;
    result?;
    Ok(pieces)
}

async fn app() -> starfish::Result<()> {
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))?;
    let addr = listener.local_addr()?;
    let server = spawn(async move { serve(listener.accept().await?).await });

    let stream = TcpStream::connect(addr).await?;
    let line = b"all work and no play makes a dull benchmark\n";
    let block: Vec<u8> = line.iter().copied().cycle().take(line.len() * 1024).collect();
    let mut sent = 0;
    let mut block = block;
    while sent < 1 << 20 {
        let (result, returned) = stream.write_all(block).await;
        result?;
        block = returned;
        sent += block.len();
    }
    stream.shutdown(Shutdown::Write)?;

    let (result, reply) = stream.read(Vec::with_capacity(32)).await;
    result?;
    let pieces = server.await??;
    println!("sent {sent} bytes, {} lines", sent / line.len());
    println!("the server counted {} lines", String::from_utf8_lossy(&reply).trim());
    println!("received in {pieces} buffers of at most {SIZE} bytes, from a pool of {BUFFERS}");
    Ok(())
}

fn main() -> starfish::Result<()> {
    Runtime::new().shards(1).recv_buffers(BUFFERS, SIZE).run(app)?
}
