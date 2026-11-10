A tour of the library, with examples.

This guide goes through everything starfish offers, in the order a server
usually needs it. The API reference has the details of each item; the
examples here are compiled with the documentation, so they match the code.

- [The model](#the-model)
- [Starting a runtime](#starting-a-runtime)
- [Tasks](#tasks)
- [Working across shards](#working-across-shards)
- [Coordinating tasks on a shard](#coordinating-tasks-on-a-shard)
- [Buffers](#buffers)
- [Network](#network)
- [Files](#files)
- [Registered buffers and zero-copy sends](#registered-buffers-and-zero-copy-sends)
- [Scheduling groups](#scheduling-groups)
- [Device capacity](#device-capacity)
- [Memory](#memory)
- [Shutting down](#shutting-down)
- [Diagnostics](#diagnostics)
- [Errors, panics and cancellation](#errors-panics-and-cancellation)
- [What is not there](#what-is-not-there)

# The model

A runtime starts one *shard* per CPU. A shard is a thread pinned to its CPU
with everything it needs to itself: a queue of tasks, an io_uring instance,
timers and, optionally, a heap.

A task runs on the shard that spawned it, from start to finish. Two things
follow:

- Futures do not have to be `Send`. State that only one shard touches can
  live in `Rc`, `Cell` and `RefCell`, with no locks and no atomics.
- Nothing is shared by default. To use data that lives on another shard,
  send that shard a closure to run ([`submit_to`](crate::submit_to)).

The usual design is therefore to split the data by shard, for example by
hashing a key, and to give every shard a full copy of the service that
works on its part ([`Sharded`](crate::Sharded)).

Tasks are never interrupted. A task runs until it awaits something that is
not ready, so one that computes for a long time holds up everything else on
its shard. See [`maybe_yield`](crate::maybe_yield) below.

# Starting a runtime

[`Runtime::run`](crate::Runtime::run) turns the calling thread into shard
0, starts the others, runs one future on shard 0 and returns its output.
When that future completes, every task still running on any shard is
dropped and the threads are joined.

```rust,no_run
use starfish::Runtime;

fn main() -> starfish::Result<()> {
    let answer = Runtime::new().run(|| async { 6 * 7 })?;
    assert_eq!(answer, 42);
    Ok(())
}
```

`Runtime` is a builder. Its settings:

| Setting | Default | Meaning |
|---|---|---|
| `shards` | one per allowed CPU | Number of shards, 1 to 1024 |
| `pin_threads` | on | Pin each shard to its own CPU |
| `ring_entries` | 1024 | Submission queue size of each shard's io_uring |
| `task_quota` | 500 µs | Length of a time slice |
| `io_depth` | 64 | Requests per device a shard keeps in the kernel |
| `io_properties` | none | Measured capacity of a storage device |
| `io_latency_goal` | 1.5 × `task_quota` | Work queued inside such a device |
| `submit_limit` | 1024 | Calls one shard may have in progress on another |
| `fixed_buffers` | none | Per-shard pool of registered buffers |
| `memory_limit` | none | Heap size at which a shard asks for memory back |
| `stall_threshold` | 25 ms | Slice length that counts as a stall |
| `stall_backtraces` | off | Record the stack of a stalled shard |
| `on_stall` | none | Function called after every stall |

Linux 6.0 or later on x86-64 is required.

# Tasks

[`spawn`](crate::spawn) starts a task on the current shard and returns a
[`JoinHandle`](crate::JoinHandle). Awaiting the handle gives the task's
output. Dropping the handle lets the task run on by itself.
[`abort`](crate::JoinHandle::abort) stops it: its future is dropped before
the call returns, which cancels its I/O and releases what it holds.

```rust,no_run
use std::time::Duration;
use starfish::{sleep, spawn, timeout, Error};

async fn tasks() -> starfish::Result<()> {
    let sum = spawn(async { 1 + 1 });
    assert_eq!(sum.await?, 2);

    let sleeper = spawn(sleep(Duration::from_secs(60)));
    sleeper.abort();
    assert!(matches!(sleeper.await, Err(Error::Canceled)));

    // `timeout` drops the future it was given when the time runs out.
    let late = timeout(Duration::from_millis(10), sleep(Duration::from_secs(1))).await;
    assert!(late.is_err());
    Ok(())
}
```

A loop that may run for long should give way to other tasks.
[`maybe_yield`](crate::maybe_yield) does so only when the time slice is
used up, and checking costs two memory reads, so it can go inside the loop.
[`yield_now`](crate::yield_now) yields unconditionally.

```rust,no_run
async fn checksum(data: &[u8]) -> u64 {
    let mut sum = 0u64;
    for block in data.chunks(4096) {
        sum = block.iter().fold(sum, |sum, byte| sum.wrapping_add(u64::from(*byte)));
        starfish::maybe_yield().await;
    }
    sum
}
```

# Working across shards

[`submit_to`](crate::submit_to) runs a closure on another shard and returns
its result. The closure and the result cross threads and must be `Send`.
The future the closure creates stays on the target shard and may use that
shard's `Rc`s and sockets. [`submit_to_all`](crate::submit_to_all) does the
same on every shard at once.

[`Sharded`](crate::Sharded) keeps one instance of a service on every shard.
The handle is a small `Copy` value that can be sent anywhere; the instances
never leave their shards.

```rust,no_run
use std::cell::Cell;
use starfish::{shard_count, shard_id, submit_to, Sharded};

async fn shards() -> starfish::Result<()> {
    let last = shard_count()? - 1;
    let ran_on = submit_to(last, || async { shard_id() }).await??;
    assert_eq!(ran_on, last);

    // One counter per shard. `Cell` is enough: nothing is shared.
    let hits = Sharded::start(|_shard| Cell::new(0u64)).await?;
    hits.local()?.set(5);
    hits.invoke_on_all(|hits| async move { hits.set(hits.get() + 1) }).await?;
    let total = hits
        .map_reduce(|hits| async move { hits.get() }, 0, |sum, one| sum + one)
        .await?;
    assert_eq!(total, 5 + shard_count()? as u64);
    hits.stop().await
}
```

Each pair of shards has its own lock-free queue. A call to another shard
still costs far more than a local function call, and a wake-up through the
kernel when that shard is asleep, so batch small pieces of work.

## When a shard falls behind

A shard may have only so many calls in progress on one other shard:
[`submit_limit`](crate::Runtime::submit_limit), 1024 by default. Past
that, `submit_to` waits for an earlier call to finish before it sends. An
overloaded shard therefore slows its callers down, instead of collecting
an ever longer list of calls to run. A call keeps its place until the
other shard has finished it, even if the caller gave up waiting.

One consequence: calls that wait for further calls in the same direction
can block each other when the limit is reached. Keep chains of calls from
doubling back to where they came from.

## A stream of values

For many values going the same way, one `submit_to` each is wasteful.
[`shared_channel`](crate::sync::shared_channel) is a bounded queue with
one end on each shard. A value costs no allocation and no task, the other
shard is woken only when it is waiting, and a full queue makes the sender
wait.

```rust,no_run
use starfish::submit_to;
use starfish::sync::shared_channel;

async fn stream() -> starfish::Result<u64> {
    // Made here; each end is then moved to the shard that uses it.
    let (mut sender, mut receiver) = shared_channel(64);
    let producer = starfish::spawn(submit_to(1, move || async move {
        for n in 0..10_000u64 {
            if sender.send(n).await.is_err() {
                break; // the receiver is gone
            }
        }
    }));
    let mut sum = 0;
    // Ends when the sender has been dropped.
    while let Some(n) = receiver.recv().await {
        sum += n;
    }
    producer.await??;
    Ok(sum)
}
```

# Coordinating tasks on a shard

The [`sync`](crate::sync) module has three tools. All are local to a shard
and none takes a lock.

- [`Semaphore`](crate::sync::Semaphore) limits how much runs at once.
  Permits are handed out strictly in the order they were asked for, so a
  large request is never starved by small ones.
- [`channel`](crate::sync::channel) is a bounded queue from many senders to
  one receiver. A full queue makes senders wait, which slows producers to
  the pace of the consumer.
- [`Gate`](crate::sync::Gate) counts work in progress. Closing it turns new
  work away and waits for the rest.

```rust,no_run
use starfish::spawn;
use starfish::sync::{channel, Gate, Semaphore};

async fn coordination() -> starfish::Result<()> {
    // At most 100 jobs at a time.
    let limit = Semaphore::new(100);
    let permit = limit.acquire(1).await?;
    spawn(async move {
        let _permit = permit;
        // ... the job ...
    });

    let (sender, mut receiver) = channel(16);
    spawn(async move {
        for number in 0..100 {
            if sender.send(number).await.is_err() {
                break; // the receiver is gone
            }
        }
    });
    // Ends when every sender has been dropped.
    while let Some(number) = receiver.recv().await {
        println!("{number}");
    }

    let gate = Gate::new();
    let guard = gate.enter()?;
    spawn(async move {
        let _guard = guard;
        // ... one request ...
    });
    // No new requests from here on; wait for those in flight.
    gate.close().await;
    Ok(())
}
```

Waiting in any of them can be given up, by dropping the future, by
[`timeout`](crate::timeout) or by aborting the task, without losing a
permit, a value or a place for others.

To coordinate across shards, keep one of these per shard in a
[`Sharded`](crate::Sharded) service, or use a
[`shared_channel`](crate::sync::shared_channel).

# Buffers

I/O calls take their buffer by value and give it back with the result:

```text
let (result, buf) = stream.read(buf).await;
```

The reason is that the kernel works on the buffer while the operation is in
flight, and keeps doing so if the future is dropped. The runtime therefore
has to own the buffer until the kernel is done with it.

Anything that implements [`IoBuf`](crate::buf::IoBuf) can be written:
`Vec<u8>`, `Box<[u8]>`, `String`, `&'static [u8]`, `&'static str`,
[`AlignedBuf`](crate::fs::AlignedBuf) and [`FixedBuf`](crate::buf::FixedBuf).
Reads need an [`IoBufMut`](crate::buf::IoBufMut): `Vec<u8>`, `AlignedBuf` or
`FixedBuf`. A read fills the buffer from its start, up to its *capacity*,
and sets its length to what was read.

# Network

[`TcpListener::bind`](crate::net::TcpListener::bind) sets `SO_REUSEPORT`,
so every shard can bind its own listener to the same address and the kernel
spreads the connections over them. That is the normal way to write a
server: no shard accepts on behalf of another.

```rust,no_run
use std::net::SocketAddr;
use starfish::net::TcpListener;

async fn echo_server() -> std::io::Result<()> {
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 7000)))?;
    loop {
        let stream = listener.accept().await?;
        starfish::spawn(async move {
            let mut buf = Vec::with_capacity(16 * 1024);
            loop {
                let (result, filled) = stream.read(buf).await;
                match result {
                    Ok(received) if received > 0 => {}
                    _ => return, // closed, or failed
                }
                let (result, sent) = stream.write_all(filled).await;
                if result.is_err() {
                    return;
                }
                buf = sent;
            }
        });
    }
}
```

[`TcpStream`](crate::net::TcpStream) also has `connect`, `write` (which may
send only part of the buffer), `shutdown`, `set_nodelay` and the two
address getters. [`UdpSocket`](crate::net::UdpSocket) has `send_to` and
`recv_from`, and `send` and `recv` once it is connected to a peer.

A socket is not tied to the shard that created it. Each call goes to the
io_uring of the shard it is made on, so a connection can be moved to
another shard inside a [`submit_to`](crate::submit_to) closure.

# Files

Files have no cursor: every read and write names its offset.

- [`File`](crate::fs::File) goes through the kernel's page cache.
- [`DmaFile`](crate::fs::DmaFile) opens with `O_DIRECT` and bypasses it,
  for programs that keep their own cache. Buffers, offsets and lengths
  must be aligned as the device demands; the file reports the
  requirements and [`alloc`](crate::fs::DmaFile::alloc) returns a buffer
  that meets them.

```rust,no_run
use starfish::fs::{DmaFile, File};

async fn files() -> std::io::Result<()> {
    let file = File::create("/tmp/greeting").await?;
    let (result, _) = file.write_all_at("hello", 0).await;
    result?;
    file.sync_all().await?;
    let (result, buf) = file.read_at(Vec::with_capacity(64), 0).await;
    assert_eq!(result?, 5);
    assert_eq!(buf, b"hello");

    let file = DmaFile::create("/var/data/segment").await?;
    let mut block = file.alloc(4096)?;
    block.fill_padded(b"record"); // zero-padded to whole blocks
    let (result, block) = file.write_at(block, 0).await;
    result?;
    let (result, block) = file.read_at(block, 0).await;
    result?;
    assert!(block.starts_with(b"record"));
    Ok(())
}
```

`File::open_with` and `DmaFile::open_with` take raw `open(2)` flags.
[`remove_file`](crate::fs::remove_file) deletes a file.

Reads, writes and syncs on both types go through the shard's I/O
scheduler, described under [Scheduling groups](#scheduling-groups) and
[Device capacity](#device-capacity).

# Registered buffers and zero-copy sends

With [`Runtime::fixed_buffers`](crate::Runtime::fixed_buffers), every shard
gets a pool of buffers that are registered with its io_uring. The kernel
maps and pins them once instead of on every request. A
[`FixedBuf`](crate::buf::FixedBuf) from the pool works with every read and
write call.

[`TcpStream::send_zc`](crate::net::TcpStream::send_zc) transmits from such
a buffer without copying it. The call returns when the data is queued, but
the buffer goes back to the pool only when the peer has acknowledged the
data. A sender that outruns its peer runs out of buffers and waits in
[`FixedBuf::acquire`](crate::buf::FixedBuf::acquire), which is the
backpressure. Zero-copy pays off from roughly 10 KiB per send; below that,
`write` is faster.

```rust,no_run
use starfish::buf::FixedBuf;
use starfish::net::TcpStream;

// In a runtime started with `Runtime::new().fixed_buffers(256, 64 * 1024)`.
async fn send_block(stream: &TcpStream, block: &[u8]) -> starfish::Result<usize> {
    let mut buf = FixedBuf::acquire().await?;
    buf.fill_from(block);
    Ok(stream.send_zc(buf).await?)
}
```

A `FixedBuf` cannot leave its shard. Registered memory is pinned and counts
against the process's locked-memory limit where the kernel enforces one.

# Scheduling groups

A [`SchedulingGroup`](crate::SchedulingGroup) is a class of work with two
weights. When several groups have tasks ready on a shard, each gets CPU
time in proportion to its *CPU shares*. When several have file requests
waiting for a device, each gets device time in proportion to its *I/O
shares*. A group with nothing to do costs nothing and saves up nothing.

Everything starts in the default group, with 1000 shares of each kind.
Spawned tasks and calls to other shards inherit the group of the caller.

```rust,no_run
use starfish::SchedulingGroup;

async fn compact() -> starfish::Result<()> {
    // A fifth of the weight of request handling, for CPU and for disk.
    let compaction = SchedulingGroup::create("compaction", 200)?;
    let job = compaction.spawn(async {
        // ... everything spawned and every file request made in here
        // belongs to the group ...
    });
    job.await?;

    // Weights can be changed while running, on all shards at once.
    compaction.set_io_shares(50)?;
    Ok(())
}
```

Shares only matter under contention: a group alone on a shard gets all of
it. And CPU shares only work if tasks yield, since a task that never awaits
keeps the CPU whatever its group.

# Device capacity

By default a shard limits each device only by
[`io_depth`](crate::Runtime::io_depth), the number of its requests in the
kernel. If more is sent than the device can do, the surplus queues up
inside the kernel and the device, where group shares have no say.

To avoid that, measure the device once with
[`iotune::measure`](crate::iotune::measure) and pass the result to
[`Runtime::io_properties`](crate::Runtime::io_properties). All shards
together then send the device no more than it can sustain, and the surplus
waits in the shards' own queues, ordered by I/O shares.

```rust,no_run
use starfish::iotune::{measure, Options};
use starfish::Runtime;

fn main() -> starfish::Result<()> {
    // Writes a 1 GiB scratch file and takes about 20 seconds. Do it once,
    // on an idle machine, and store the figures.
    let measured = Runtime::new().run(|| measure("/var/data", Options::default()))??;

    Runtime::new()
        .io_properties("/var/data", measured)
        .run(|| async {
            // ... the server ...
        })?;
    Ok(())
}
```

`examples/iotune.rs` is the first half as a program. A device is identified
by the filesystem the path is on.

# Memory

[`ShardAlloc`](crate::alloc::ShardAlloc) is an optional global allocator
that gives every shard its own heap, with no locks on the fast path. Memory
freed on a different shard than it was allocated on finds its way back to
its owner. Threads that are not shards use the system allocator.

```rust,no_run
#[global_allocator]
static GLOBAL: starfish::alloc::ShardAlloc = starfish::alloc::ShardAlloc;
```

With it installed, [`Runtime::memory_limit`](crate::Runtime::memory_limit)
sets a size per shard above which the runtime asks the application to give
memory back, by calling the functions registered with
[`alloc::on_pressure`](crate::alloc::on_pressure) on that shard. The limit
is a request, not a cap: allocations beyond it still succeed.

```rust,no_run
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

fn evict_under_pressure(cache: Rc<RefCell<HashMap<u64, Vec<u8>>>>) -> starfish::Result<()> {
    starfish::alloc::on_pressure(move || cache.borrow_mut().clear())
}
```

# Shutting down

When the future given to `run` completes, all remaining tasks are dropped
where they stand. For an orderly stop, three pieces work together:

1. [`signal::termination`](crate::signal::termination) tells the main task
   that the process was asked to stop.
2. [`JoinHandle::abort`](crate::JoinHandle::abort) stops the accept loops.
3. A [`Gate`](crate::sync::Gate) that every request entered waits for the
   requests in flight, usually under a [`timeout`](crate::timeout).

```rust,no_run
use std::time::Duration;
use starfish::sync::Gate;
use starfish::{signal, timeout, JoinHandle};

async fn serve_until_stopped(acceptor: JoinHandle<()>, serving: Gate) -> starfish::Result<()> {
    let signal = signal::termination().await?;
    println!("signal {signal}: shutting down");

    acceptor.abort();
    if timeout(Duration::from_secs(5), serving.close()).await.is_err() {
        println!("{} requests did not finish", serving.active());
    }
    Ok(())
}
```

`examples/echo.rs` does this on every shard through a `Sharded` service.

Listening for a signal replaces what the process would do on receiving it,
for good: after the first `termination` call, Ctrl-C no longer ends the
program by itself. [`Signals`](crate::signal::Signals) listens for any set
of signals and keeps reporting them, for instance to give up on a slow
shutdown at the second Ctrl-C.

# Diagnostics

[`stats`](crate::stats) describes the current shard: its event loop, its
scheduling groups and its device queues. [`alloc::stats`](crate::alloc::stats)
describes its heap. To see all shards, collect them with
[`submit_to_all`](crate::submit_to_all).

```rust,no_run
fn report() -> starfish::Result<()> {
    let shard = starfish::stats::shard()?;
    println!(
        "shard {}: {} tasks, busy {:?}, idle {:?}, {} stalls",
        shard.shard, shard.tasks, shard.busy, shard.idle, shard.stalls
    );
    for group in starfish::stats::groups()? {
        println!("  group {}: {:?} of CPU", group.name, group.cpu_time);
    }
    for device in starfish::stats::devices()? {
        println!("  device {}: {} queued", device.device, device.queued);
    }
    if let Some(heap) = starfish::alloc::stats() {
        println!("  heap: {} bytes in use", heap.allocated_bytes);
    }
    Ok(())
}
```

A *stall* is a time slice that ran past
[`stall_threshold`](crate::Runtime::stall_threshold): some task did not
yield and held up its shard. Stalls are always counted. To find the
culprit, turn on [`stall_backtraces`](crate::Runtime::stall_backtraces) and
print the report:

```rust,no_run
use starfish::Runtime;

fn main() -> starfish::Result<()> {
    Runtime::new()
        .stall_backtraces(true)
        .on_stall(|stall| {
            eprintln!("shard {} stalled for {:?}", stall.shard, stall.duration);
            for frame in &stall.backtrace {
                eprintln!("  {frame}");
            }
        })
        .run(|| async {
            // ... the server ...
        })
}
```

# Errors, panics and cancellation

Runtime calls return [`Error`](crate::Error); I/O calls return
`std::io::Error`. Each converts into the other, so `?` works both ways.

The library itself does not panic. A panic in a task is caught: the task
ends, whoever awaits its handle gets [`Error::Panicked`](crate::Error::Panicked),
and the shard carries on. The same holds for a closure sent to another
shard. This relies on unwinding and does not apply under `panic = "abort"`.

Dropping a future cancels what it was waiting for. For I/O, the runtime
asks the kernel to cancel the operation and keeps the buffer until the
kernel has let go of it. Cancellation is not instant in the kernel: a write
that was dropped may still have happened, in part or in full.

# What is not there

- Networking uses the kernel's stack. There is no userspace TCP.
- No TLS, no HTTP, no DNS resolution: addresses are `SocketAddr`s.
- No Unix-domain sockets.
- File support covers data: open, read, write, sync, remove. There are no
  calls for directories, renaming, truncating, preallocating or file
  metadata.
- A [`shared_channel`](crate::sync::shared_channel) has one sender and one
  receiver. There is no cross-shard channel with several senders.
- Only Linux on x86-64.
