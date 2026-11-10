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

use starfish::net::{BufReader, BufWriter, TcpListener, TcpStream};
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

async fn serve(stream: TcpStream, store: Store) -> starfish::Result<()> {
    stream.set_nodelay(true)?;
    // A request may be at most 64 KiB; a longer one ends the connection.
    let mut requests = BufReader::new(&stream).limit(64 * 1024);
    let mut replies = BufWriter::new(&stream);
    while let Some(line) = requests.next_line().await? {
        let reply = store.execute(&line).await?;
        replies.write_all(reply.as_bytes()).await?;
        // Answer at once unless the next request has already arrived:
        // a client that sends many requests together gets the replies
        // together.
        if requests.buffer().is_empty() {
            replies.flush().await?;
        }
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
