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

use starfish::net::{BufReader, TcpListener, TcpStream};
use starfish::sync::{channel, Sender};
use starfish::{spawn, Runtime};

type Room = Rc<RefCell<HashMap<u64, Sender<String>>>>;

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

    // A line may be at most 1 KiB; a longer one ends the connection.
    let mut lines = BufReader::new(stream.clone()).limit(1024);
    while let Ok(Some(line)) = lines.next_line().await {
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
