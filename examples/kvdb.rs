//! A durable key-value server: data split across shards by key, a
//! write-ahead log per shard on direct I/O, group commit, recovery after a
//! crash, log compaction and an orderly shutdown.
//!
//! This is the library's pieces working together, not a product. It keeps
//! all data in memory and logs every change.
//!
//! Run with `cargo run --release --example kvdb -- [directory]` (default
//! `kvdb-data`, on a filesystem with `O_DIRECT`), then `nc 127.0.0.1 7006`:
//!
//! ```text
//! SET colour blue      -> OK        (once the change is on the device)
//! GET colour           -> blue
//! DEL colour           -> 1
//! COUNT                -> number of keys
//! STATS                -> commits and batches per shard
//! COMPACT              -> rewrite the logs to hold only live data
//! ```
//!
//! Ctrl-C stops it cleanly. Killing it loses nothing that was answered
//! with `OK`: the next start replays the logs.
//!
//! # How it works
//!
//! - Every key belongs to one shard, by a hash of the key. A request that
//!   arrives on another shard is sent to the owner.
//! - Each shard has one task, the committer, that owns the shard's log.
//!   Requests hand it their change and wait. It writes everything that has
//!   queued up, syncs once, applies the changes to the shard's map and
//!   only then answers: many clients share one sync (group commit).
//! - A log record carries its length and a checksum. After a crash the
//!   log ends in padding or a torn record; replay stops at the first
//!   record that does not check out.
//! - When a log has grown large, the committer writes the live data to a
//!   new file and renames it over the old one.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Duration;

use starfish::fs::{create_dir_all, rename, DmaFile, DmaStreamReader, DmaStreamWriter, File};
use starfish::io::{BufReader, BufWriter};
use starfish::net::{TcpListener, TcpStream};
use starfish::sync::{channel, Gate, Receiver, Semaphore, Sender};
use starfish::{shard_count, shard_id, signal, spawn, timeout, JoinHandle, Runtime, Sharded};

const PORT: u16 = 7006;
/// Connections each shard serves at a time.
const MAX_CONNECTIONS: usize = 10_000;
/// How long connections get to finish once the server is asked to stop.
const GRACE: Duration = Duration::from_secs(5);
const LOG_BUFFER: usize = 256 * 1024;
/// Log space reserved ahead of the data, so appends do not wait for the
/// filesystem to find room.
const PREALLOCATE: u64 = 32 << 20;
/// A log this long is rewritten to hold only live data.
const COMPACT_AFTER: u64 = 24 << 20;
/// Changes written and synced together at most.
const MAX_BATCH: usize = 4096;
const MAX_RECORD: usize = 1 << 20;

#[derive(Clone)]
enum Change {
    Set { key: String, value: String },
    Delete { key: String },
}

/// FNV-1a. A fixed function: it decides which shard owns a key, and that
/// must not change between runs.
fn hash(data: &[u8]) -> u64 {
    data.iter()
        .fold(0xcbf2_9ce4_8422_2325, |hash, byte| (hash ^ u64::from(*byte)).wrapping_mul(0x0100_0000_01b3))
}

/// Appends one log record: length, checksum, then tag, key length, key
/// and value.
fn encode(change: &Change, out: &mut Vec<u8>) {
    let (tag, key, value) = match change {
        Change::Set { key, value } => (1u8, key, value.as_str()),
        Change::Delete { key } => (2u8, key, ""),
    };
    let mut payload = Vec::with_capacity(5 + key.len() + value.len());
    payload.push(tag);
    payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
    payload.extend_from_slice(key.as_bytes());
    payload.extend_from_slice(value.as_bytes());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&(hash(&payload) as u32).to_le_bytes());
    out.extend_from_slice(&payload);
}

fn decode(payload: &[u8]) -> Option<Change> {
    let (tag, rest) = payload.split_first()?;
    let (length, rest) = rest.split_first_chunk::<4>()?;
    let (key, value) = rest.split_at_checked(u32::from_le_bytes(*length) as usize)?;
    let key = String::from_utf8(key.to_vec()).ok()?;
    match tag {
        1 => Some(Change::Set {
            key,
            value: String::from_utf8(value.to_vec()).ok()?,
        }),
        2 => Some(Change::Delete { key }),
        _ => None,
    }
}

fn apply(map: &mut HashMap<String, String>, change: Change) -> bool {
    match change {
        Change::Set { key, value } => map.insert(key, value).is_none(),
        Change::Delete { key } => map.remove(&key).is_some(),
    }
}

/// What a request asks of its shard's committer.
enum LogRequest {
    /// Make this change durable, apply it, and say whether that worked
    /// and whether the key was there before (for `DEL`).
    Commit {
        change: Change,
        done: Sender<Option<bool>>,
    },
    Compact {
        done: Sender<bool>,
    },
}

/// One shard's part of the database.
struct Partition {
    shard: usize,
    map: RefCell<HashMap<String, String>>,
    /// The way to the committer, once the log is open.
    log: RefCell<Option<Sender<LogRequest>>>,
    committer: Cell<Option<JoinHandle<()>>>,
    commits: Cell<u64>,
    batches: Cell<u64>,
    compactions: Cell<u64>,
    // The network side of the shard.
    connections: Semaphore,
    serving: Gate,
    acceptor: Cell<Option<JoinHandle<()>>>,
}

fn log_path(directory: &Path, shard: usize) -> PathBuf {
    directory.join(format!("shard-{shard}.log"))
}

/// Reads a log into a map. Returns the map and where the valid data ends.
async fn replay(file: &DmaFile) -> std::io::Result<(HashMap<String, String>, u64)> {
    let mut log = DmaStreamReader::new(file, LOG_BUFFER)?.read_ahead(2)?;
    let mut map = HashMap::new();
    let mut valid = 0;
    loop {
        let mut header = [0u8; 8];
        if log.read_exact(&mut header).await.is_err() {
            break; // the end of the file
        }
        let length = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
        let checksum = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
        // Padding reads as a record of length zero, which is never written.
        if length == 0 || length > MAX_RECORD {
            break;
        }
        let mut payload = vec![0u8; length];
        if log.read_exact(&mut payload).await.is_err() || hash(&payload) as u32 != checksum {
            break; // torn by a crash
        }
        let Some(change) = decode(&payload) else { break };
        apply(&mut map, change);
        valid = log.position();
    }
    Ok((map, valid))
}

/// Opens a log for appending at `end`, with space reserved beyond it.
async fn append_to(file: DmaFile, end: u64) -> std::io::Result<DmaStreamWriter<DmaFile>> {
    // Cut off padding and anything torn, so the writer starts at the data's
    // true end; then reserve space again.
    file.set_len(end).await?;
    let writer = DmaStreamWriter::append(file, LOG_BUFFER).await?;
    writer.get_ref().allocate(0, end + PREALLOCATE).await?;
    Ok(writer)
}

/// Makes a rename in `directory` survive a crash.
async fn sync_directory(directory: &Path) -> std::io::Result<()> {
    let handle = File::open_with(directory, libc::O_RDONLY | libc::O_DIRECTORY, 0).await?;
    handle.sync_all().await?;
    handle.close().await
}

impl Partition {
    fn new(shard: usize) -> Partition {
        Partition {
            shard,
            map: RefCell::default(),
            log: RefCell::new(None),
            committer: Cell::new(None),
            commits: Cell::new(0),
            batches: Cell::new(0),
            compactions: Cell::new(0),
            connections: Semaphore::new(MAX_CONNECTIONS),
            serving: Gate::new(),
            acceptor: Cell::new(None),
        }
    }

    /// Recovers the shard's data from its log and starts its committer.
    /// Returns how many keys were recovered.
    async fn open(self: Rc<Self>, directory: PathBuf) -> std::io::Result<usize> {
        let path = log_path(&directory, self.shard);
        let file = DmaFile::open_with(&path, libc::O_RDWR | libc::O_CREAT, 0o644).await?;
        let (map, valid) = replay(&file).await?;
        let keys = map.len();
        *self.map.borrow_mut() = map;
        let writer = append_to(file, valid).await?;

        let (requests, inbox) = channel(MAX_BATCH);
        *self.log.borrow_mut() = Some(requests);
        self.committer.set(Some(spawn(commit_loop(self.clone(), directory, writer, inbox))));
        Ok(keys)
    }

    /// Hands a change to the committer and waits until it is durable.
    /// `None` if it could not be stored.
    async fn commit(&self, change: Change) -> Option<bool> {
        // A clone, so that no borrow is held while waiting.
        let log = self.log.borrow().clone()?;
        let (done, mut answer) = channel(1);
        log.send(LogRequest::Commit { change, done }).await.ok()?;
        answer.recv().await.flatten()
    }

    async fn compact(&self) -> bool {
        let Some(log) = self.log.borrow().clone() else { return false };
        let (done, mut answer) = channel(1);
        if log.send(LogRequest::Compact { done }).await.is_err() {
            return false;
        }
        answer.recv().await.unwrap_or(false)
    }

    /// Stops the committer once nobody can send to it any more, which
    /// leaves the log with its true length.
    async fn close(&self) {
        drop(self.log.borrow_mut().take());
        if let Some(committer) = self.committer.take() {
            let _ = committer.await;
        }
    }
}

/// Writes the shard's live data to a new log and puts it in place of the
/// old one. Nothing else touches the map meanwhile: this runs in the
/// committer, between batches.
async fn compact_log(partition: &Partition, directory: &Path) -> std::io::Result<DmaStreamWriter<DmaFile>> {
    let path = log_path(directory, partition.shard);
    let temporary = path.with_extension("log.new");
    let file = DmaFile::create(&temporary).await?;
    let mut writer = DmaStreamWriter::new(file, LOG_BUFFER)?;

    // A copy, so that no borrow of the map is held across the writes.
    let live: Vec<(String, String)> = partition
        .map
        .borrow()
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    let mut record = Vec::new();
    for (key, value) in live {
        record.clear();
        encode(&Change::Set { key, value }, &mut record);
        writer.write_all(&record).await?;
    }
    let end = writer.position();
    // Durable and of its true length before it takes the old log's name.
    let file = writer.finish().await?;
    rename(&temporary, &path).await?;
    sync_directory(directory).await?;
    partition.compactions.set(partition.compactions.get() + 1);
    append_to(file, end).await
}

/// The committer: the one task that writes a shard's log.
async fn commit_loop(
    partition: Rc<Partition>,
    directory: PathBuf,
    mut writer: DmaStreamWriter<DmaFile>,
    mut inbox: Receiver<LogRequest>,
) {
    let mut record = Vec::new();
    while let Some(first) = inbox.recv().await {
        // Everything that queued up while the last batch was being synced
        // goes into this one.
        let mut batch = vec![first];
        while batch.len() < MAX_BATCH {
            match inbox.try_recv() {
                Some(request) => batch.push(request),
                None => break,
            }
        }

        let mut stored = true;
        let mut changes = 0;
        for request in &batch {
            if let LogRequest::Commit { change, .. } = request {
                record.clear();
                encode(change, &mut record);
                stored &= writer.write_all(&record).await.is_ok();
                changes += 1;
            }
        }
        if changes > 0 {
            stored &= writer.sync().await.is_ok();
            partition.commits.set(partition.commits.get() + changes);
            partition.batches.set(partition.batches.get() + 1);
        }

        // Durable: now the changes may be seen, and answered.
        let mut compactions = Vec::new();
        for request in batch {
            match request {
                LogRequest::Commit { change, done } => {
                    let outcome = stored.then(|| apply(&mut partition.map.borrow_mut(), change));
                    let _ = done.try_send(outcome);
                }
                LogRequest::Compact { done } => compactions.push(done),
            }
        }

        if !compactions.is_empty() || writer.position() > COMPACT_AFTER {
            let compacted = match compact_log(&partition, &directory).await {
                Ok(fresh) => {
                    writer = fresh;
                    true
                }
                Err(err) => {
                    eprintln!("shard {}: compaction failed: {err}", partition.shard);
                    false
                }
            };
            for done in compactions {
                let _ = done.try_send(compacted);
            }
        }
    }
    // Every sender is gone: a clean shutdown. Give the log its true length.
    if let Err(err) = writer.finish().await {
        eprintln!("shard {}: closing the log failed: {err}", partition.shard);
    }
}

#[derive(Clone, Copy)]
struct Database {
    partitions: Sharded<Partition>,
    shards: usize,
}

impl Database {
    fn owner(&self, key: &str) -> usize {
        (hash(key.as_bytes()) % self.shards as u64) as usize
    }

    /// Carries out one line of the protocol and returns the reply.
    async fn execute(&self, line: &str) -> starfish::Result<String> {
        let mut words = line.splitn(3, ' ');
        Ok(match (words.next(), words.next(), words.next()) {
            (Some("GET"), Some(key), None) => {
                let key = key.to_owned();
                let found = self
                    .partitions
                    .invoke_on(self.owner(&key), move |partition| async move {
                        partition.map.borrow().get(&key).cloned()
                    })
                    .await?;
                found.map_or_else(|| String::from("(none)\n"), |value| format!("{value}\n"))
            }
            (Some("SET"), Some(key), Some(value)) => {
                let change = Change::Set {
                    key: key.to_owned(),
                    value: value.to_owned(),
                };
                match self.commit(key, change).await? {
                    Some(_) => String::from("OK\n"),
                    None => String::from("ERROR: could not be stored\n"),
                }
            }
            (Some("DEL"), Some(key), None) => {
                let change = Change::Delete { key: key.to_owned() };
                match self.commit(key, change).await? {
                    Some(existed) => format!("{}\n", u8::from(existed)),
                    None => String::from("ERROR: could not be stored\n"),
                }
            }
            (Some("COUNT"), None, None) => {
                let keys = self
                    .partitions
                    .map_reduce(|partition| async move { partition.map.borrow().len() }, 0, |sum, one| sum + one)
                    .await?;
                format!("{keys}\n")
            }
            (Some("STATS"), None, None) => {
                let shards = self
                    .partitions
                    .invoke_on_all(|partition| async move {
                        format!(
                            "shard {}: {} keys, {} commits in {} batches, {} compactions\n",
                            partition.shard,
                            partition.map.borrow().len(),
                            partition.commits.get(),
                            partition.batches.get(),
                            partition.compactions.get()
                        )
                    })
                    .await?;
                shards.concat() + "END\n"
            }
            (Some("COMPACT"), None, None) => {
                let done = self
                    .partitions
                    .invoke_on_all(|partition| async move { partition.compact().await })
                    .await?;
                if done.iter().all(|done| *done) {
                    String::from("OK\n")
                } else {
                    String::from("ERROR: compaction failed\n")
                }
            }
            _ => String::from("ERROR: use GET key, SET key value, DEL key, COUNT, STATS or COMPACT\n"),
        })
    }

    async fn commit(&self, key: &str, change: Change) -> starfish::Result<Option<bool>> {
        self.partitions
            .invoke_on(self.owner(key), move |partition| async move { partition.commit(change).await })
            .await
    }
}

async fn serve(stream: TcpStream, database: Database) -> starfish::Result<()> {
    stream.set_nodelay(true)?;
    let mut requests = BufReader::new(&stream).limit(MAX_RECORD / 2);
    let mut replies = BufWriter::new(&stream);
    while let Some(line) = requests.next_line().await? {
        let reply = database.execute(&line).await?;
        replies.write_all(reply.as_bytes()).await?;
        // Answer at once unless the next request has already arrived.
        if requests.buffer().is_empty() {
            replies.flush().await?;
        }
    }
    Ok(())
}

async fn accept(listener: TcpListener, partition: Rc<Partition>, database: Database) {
    loop {
        // Both fail only once the server is shutting down.
        let Ok(permit) = partition.connections.acquire(1).await else { return };
        match listener.accept().await {
            Ok(stream) => {
                let Ok(guard) = partition.serving.enter() else { return };
                spawn(async move {
                    let _held = (permit, guard);
                    // A client that hangs up is not worth a message.
                    let _ = serve(stream, database).await;
                });
            }
            Err(err) => eprintln!("shard {}: accept failed: {err}", partition.shard),
        }
    }
}

/// Makes sure the data was written with this number of shards: the owner
/// of a key depends on it.
async fn check_layout(directory: &Path, shards: usize) -> starfish::Result<()> {
    let path = directory.join("SHARDS");
    match File::open(&path).await {
        Ok(file) => {
            let (result, contents) = file.read_at(Vec::with_capacity(32), 0).await;
            result?;
            let written: usize = String::from_utf8_lossy(&contents).trim().parse().unwrap_or(0);
            if written != shards {
                let message = format!(
                    "{} holds data for {written} shards, but this run has {shards}",
                    directory.display()
                );
                return Err(std::io::Error::other(message).into());
            }
        }
        Err(_) => {
            let file = File::create(&path).await?;
            let (result, _) = file.write_all_at(format!("{shards}\n"), 0).await;
            result?;
            file.sync_all().await?;
            file.close().await?;
            sync_directory(directory).await?;
        }
    }
    Ok(())
}

async fn app(directory: PathBuf) -> starfish::Result<()> {
    create_dir_all(&directory).await?;
    let shards = shard_count()?;
    check_layout(&directory, shards).await?;

    let database = Database {
        partitions: Sharded::start(Partition::new).await?,
        shards,
    };

    // Recover every shard's data before anyone can connect.
    let recovered = {
        let directory = directory.clone();
        database
            .partitions
            .invoke_on_all(move |partition| partition.open(directory.clone()))
            .await?
    };
    let mut keys = 0;
    for shard in recovered {
        keys += shard?;
    }

    let addr = SocketAddr::from(([127, 0, 0, 1], PORT));
    let bound = database
        .partitions
        .invoke_on_all(move |partition| async move {
            let listener = TcpListener::bind(addr)?;
            let acceptor = spawn(accept(listener, partition.clone(), database));
            partition.acceptor.set(Some(acceptor));
            Ok::<(), std::io::Error>(())
        })
        .await?;
    for result in bound {
        result?;
    }
    println!(
        "kvdb on {addr}: {shards} shards, {keys} keys recovered from {}",
        directory.display()
    );

    let signal = signal::termination().await?;
    println!("signal {signal}: shutting down");

    // First stop serving, on every shard: a connection on one shard may be
    // writing to another shard's log.
    let unfinished = database
        .partitions
        .map_reduce(
            |partition| async move {
                if let Some(acceptor) = partition.acceptor.take() {
                    acceptor.abort();
                }
                let _ = timeout(GRACE, partition.serving.close()).await;
                partition.serving.active()
            },
            0,
            |total, shard| total + shard,
        )
        .await?;
    if unfinished > 0 {
        println!("dropping {unfinished} connections that did not finish");
    }
    // Then close the logs.
    database
        .partitions
        .invoke_on_all(|partition| async move {
            partition.close().await;
            shard_id().unwrap_or(partition.shard)
        })
        .await?;
    println!("logs closed");
    Ok(())
}

fn main() -> starfish::Result<()> {
    let directory = std::env::args_os().nth(1).unwrap_or_else(|| "kvdb-data".into());
    Runtime::new().run(move || app(directory.into()))?
}
