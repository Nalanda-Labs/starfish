//! A chat room: every line a client sends goes to all the others.
//!
//! The room is a map from client to the sending side of that client's
//! queue. It lives on one shard, so it is an `Rc<RefCell<..>>`, and the
//! runtime is started with a single shard.
//!
//! Run with `cargo run --example chat`, then connect a few times with
//! `nc 127.0.0.1 7003`. Ctrl-C stops it.

use std::cell::RefCell;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::rc::Rc;

use starfish::net::{TcpListener, TcpStream};
use starfish::sync::{channel, Sender};
use starfish::{spawn, Runtime};

type Room = Rc<RefCell<HashMap<u64, Sender<String>>>>;

/// Splits what a socket delivers into lines.
struct Lines<'a> {
    stream: &'a TcpStream,
    pending: Vec<u8>,
    buf: Vec<u8>,
}

impl<'a> Lines<'a> {
    fn new(stream: &'a TcpStream) -> Lines<'a> {
        Lines {
            stream,
            pending: Vec::new(),
            buf: Vec::with_capacity(4096),
        }
    }

    /// The next line, without its line ending. `None` when the peer has
    /// stopped sending.
    async fn next(&mut self) -> std::io::Result<Option<String>> {
        loop {
            if let Some(end) = self.pending.iter().position(|byte| *byte == b'\n') {
                let line: Vec<u8> = self.pending.drain(..=end).collect();
                return Ok(Some(String::from_utf8_lossy(&line).trim_end().to_owned()));
            }
            let (result, filled) = self.stream.read(std::mem::take(&mut self.buf)).await;
            self.buf = filled;
            if result? == 0 {
                return Ok(None);
            }
            self.pending.extend_from_slice(&self.buf);
        }
    }
}

fn broadcast(room: &Room, from: u64, text: &str) {
    for (client, queue) in room.borrow().iter() {
        if *client != from {
            // `try_send`, not `send`: a client that does not read misses
            // messages once its queue is full, and nobody else waits.
            let _ = queue.try_send(format!("[{from}] {text}\n"));
        }
    }
}

async fn client(id: u64, stream: TcpStream, room: Room) {
    let stream = Rc::new(stream);
    let (queue, mut outgoing) = channel(64);
    room.borrow_mut().insert(id, queue);
    broadcast(&room, id, "joined");

    // Writes what others said; reading happens below, at the same time.
    let writer = spawn({
        let stream = stream.clone();
        async move {
            while let Some(message) = outgoing.recv().await {
                if stream.write_all(message).await.0.is_err() {
                    break;
                }
            }
        }
    });

    let mut lines = Lines::new(&stream);
    while let Ok(Some(line)) = lines.next().await {
        broadcast(&room, id, &line);
    }

    room.borrow_mut().remove(&id);
    writer.abort();
    broadcast(&room, id, "left");
}

async fn app() -> starfish::Result<()> {
    let addr = SocketAddr::from(([127, 0, 0, 1], 7003));
    let listener = TcpListener::bind(addr)?;
    let room: Room = Rc::default();
    spawn(async move {
        let mut next_id = 1;
        loop {
            match listener.accept().await {
                Ok(stream) => {
                    spawn(client(next_id, stream, room.clone()));
                    next_id += 1;
                }
                Err(err) => eprintln!("accept failed: {err}"),
            }
        }
    });
    println!("chat room on {addr}");

    let signal = starfish::signal::termination().await?;
    println!("signal {signal}: stopping");
    Ok(())
}

fn main() -> starfish::Result<()> {
    Runtime::new().shards(1).run(app)?
}
