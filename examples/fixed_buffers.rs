//! Registered buffers and zero-copy sends.
//!
//! Each shard gets a pool of buffers that the kernel has mapped and pinned
//! in advance. Reads into them skip the per-request mapping, and
//! `send_zc` transmits from them without copying the data.
//!
//! Run with `cargo run --release --example fixed_buffers`.

use std::net::{Shutdown, SocketAddr};

use starfish::buf::FixedBuf;
use starfish::net::{TcpListener, TcpStream};
use starfish::{spawn, Runtime};

const BUFFERS: u16 = 8;
const SIZE: usize = 64 * 1024;
const ROUNDS: usize = 64;

async fn app() -> starfish::Result<()> {
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))?;
    let addr = listener.local_addr()?;

    let receiver = spawn(async move {
        let stream = listener.accept().await?;
        let mut total = 0;
        let mut buf = FixedBuf::acquire().await?;
        loop {
            let (result, filled) = stream.read(buf).await;
            buf = filled;
            match result? {
                0 => return Ok::<usize, starfish::Error>(total),
                received => total += received,
            }
        }
    });

    let stream = TcpStream::connect(addr).await?;
    let mut sent = 0;
    for round in 0..ROUNDS {
        // Waits here when all buffers are still with the kernel, which
        // holds each one until the receiver has acknowledged its data.
        let mut buf = FixedBuf::acquire().await?;
        buf.set_len(SIZE);
        buf.fill(round as u8);
        // The buffer is given away for good; it returns to the pool by
        // itself. The call reports how much of it was accepted.
        sent += stream.send_zc(buf).await?;
    }
    stream.shutdown(Shutdown::Write)?;

    let received = receiver.await??;
    println!("sent {sent} bytes without copying, received {received}");
    println!(
        "with a pool of {BUFFERS} buffers of {} KiB for {ROUNDS} sends",
        SIZE >> 10
    );
    Ok(())
}

fn main() -> starfish::Result<()> {
    Runtime::new().shards(1).fixed_buffers(BUFFERS, SIZE).run(app)?
}
