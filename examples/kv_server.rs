//! A key-value server: a text protocol over TCP, data split by key.
//!
//! Every shard accepts connections, and every key belongs to one shard,
//! chosen by hashing it. A connection handled on one shard reaches a key
//! owned by another by sending the owner a closure. No map is ever touched
//! by two threads, so there are no locks anywhere.
//!
//! Run with `cargo run --release --example kv_server`, then
//! `nc 127.0.0.1 7004` and type:
//!
//! ```text
//! SET colour blue
//! GET colour
//! DEL colour
//! COUNT
//! ```
//!
//! Ctrl-C stops it.

use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;

use starfish::net::{TcpListener, TcpStream};
use starfish::{shard_count, spawn, submit_to_all, Runtime, Sharded};

/// The keys one shard owns.
#[derive(Default)]
struct Partition {
    map: RefCell<HashMap<String, String>>,
}

#[derive(Clone, Copy)]
struct Store {
    partitions: Sharded<Partition>,
    shards: usize,
}

impl Store {
    fn owner(&self, key: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() % self.shards as u64) as usize
    }

    async fn get(&self, key: String) -> starfish::Result<Option<String>> {
        self.partitions
            .invoke_on(self.owner(&key), move |partition| async move {
                partition.map.borrow().get(&key).cloned()
            })
            .await
    }

    async fn set(&self, key: String, value: String) -> starfish::Result<()> {
        self.partitions
            .invoke_on(self.owner(&key), move |partition| async move {
                partition.map.borrow_mut().insert(key, value);
            })
            .await
    }

    async fn delete(&self, key: String) -> starfish::Result<bool> {
        self.partitions
            .invoke_on(self.owner(&key), move |partition| async move {
                partition.map.borrow_mut().remove(&key).is_some()
            })
            .await
    }

    async fn count(&self) -> starfish::Result<usize> {
        self.partitions
            .map_reduce(|partition| async move { partition.map.borrow().len() }, 0, |sum, one| sum + one)
            .await
    }

    /// Carries out one line of the protocol and returns the reply.
    async fn execute(&self, line: &str) -> starfish::Result<String> {
        let mut words = line.splitn(3, ' ');
        Ok(match (words.next(), words.next(), words.next()) {
            (Some("GET"), Some(key), None) => match self.get(key.to_owned()).await? {
                Some(value) => format!("{value}\n"),
                None => String::from("(none)\n"),
            },
            (Some("SET"), Some(key), Some(value)) => {
                self.set(key.to_owned(), value.to_owned()).await?;
                String::from("OK\n")
            }
            (Some("DEL"), Some(key), None) => {
                let existed = self.delete(key.to_owned()).await?;
                format!("{}\n", u8::from(existed))
            }
            (Some("COUNT"), None, None) => format!("{}\n", self.count().await?),
            _ => String::from("ERROR: use GET key, SET key value, DEL key or COUNT\n"),
        })
    }
}

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

async fn serve(stream: TcpStream, store: Store) -> starfish::Result<()> {
    stream.set_nodelay(true)?;
    let mut lines = Lines::new(&stream);
    while let Some(line) = lines.next().await? {
        let reply = store.execute(&line).await?;
        let (result, _) = stream.write_all(reply).await;
        result?;
    }
    Ok(())
}

async fn app() -> starfish::Result<()> {
    let addr = SocketAddr::from(([127, 0, 0, 1], 7004));
    let store = Store {
        partitions: Sharded::start(|_shard| Partition::default()).await?,
        shards: shard_count()?,
    };

    let bound = submit_to_all(move || async move {
        let listener = TcpListener::bind(addr)?;
        spawn(async move {
            loop {
                match listener.accept().await {
                    Ok(stream) => {
                        spawn(async move {
                            let _ = serve(stream, store).await;
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
    println!("key-value server on {addr}, {} shards", store.shards);

    let signal = starfish::signal::termination().await?;
    println!("signal {signal}: stopping with {} keys", store.count().await?);
    Ok(())
}

fn main() -> starfish::Result<()> {
    Runtime::new().run(app)?
}
