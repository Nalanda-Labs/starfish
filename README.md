# starfish

A shared-nothing, thread-per-core async runtime for Rust on io_uring,
modelled on [Seastar](https://seastar.io).

The runtime starts one *shard* per core. A shard is a pinned thread with its
own task queue, io_uring instance, timers and heap. Tasks never move between
shards, so they need not be `Send` and shard-local state needs no locks.
Shards talk only by message passing.

**Status: experimental.** The test suite passes and the unsafe code has been
run under Miri and the thread sanitizer where those tools apply, but nothing
here has seen production load. See [Checks](#checks) for exactly what has
and has not been verified.

## Requirements

- Linux 6.0 or later, x86-64. (`send_zc` needs 6.0; direct-I/O alignment is
  read from the kernel from 6.1 and assumed to be 4096 before that.)
- A filesystem with `O_DIRECT` support for `DmaFile`.

## Example

```rust
use starfish::{Runtime, Sharded};
use std::cell::Cell;

fn main() -> starfish::Result<()> {
    let total = Runtime::new().run(|| async {
        // One counter per shard; `Cell` is fine, nothing is shared.
        let counter = Sharded::start(|_shard| Cell::new(0u64)).await?;
        counter.invoke_on_all(|c| async move { c.set(c.get() + 1) }).await?;
        counter.map_reduce(|c| async move { c.get() }, 0, |a, b| a + b).await
    })??;
    println!("{total} shards counted");
    Ok(())
}
```

[docs/guide.md](docs/guide.md) covers every feature: what it is for, how
to use it, and its caveats (limits, surprises, and what is easy to get
wrong). `cargo doc --open` shows it together with the API reference.

## Examples

There are 50 programs in `examples/`. Run one with
`cargo run --release --example <name>`. Those marked *server* run until
Ctrl-C; the others do their work and exit.

| Area | Example | What it shows |
|---|---|---|
| Basics | `hello` | Starting a runtime and returning a value |
| | `tasks` | Spawning tasks, collecting results, sharing state with `Rc` |
| | `timers` | `sleep`, `timeout` and a periodic tick that does not drift |
| | `abort` | Cancelling a task and what happens to its state |
| | `select` | Waiting for a job, a timer or a stop signal, whichever is first |
| | `yielding` | The cost of not yielding, and `maybe_yield` |
| | `panics` | A panic ends one task, not the shard |
| | `blocking` | Running blocking code off the shard with `run_blocking` |
| Shards | `cross_shard` | `submit_to` and `submit_to_all` |
| | `sharded_counter` | A `Sharded` service with one instance per shard |
| | `kv_store` | Data split across shards by key hash |
| | `parallel_primes` | Dividing a computation over all shards |
| | `shared_channel` | Streaming values between shards with backpressure |
| | `shared_mpsc` | Many shards reporting to one |
| Coordination | `semaphore` | Limiting concurrency and budgeting memory |
| | `rate_limiter` | A token bucket from a semaphore and a timer |
| | `channel` | Backpressure between a producer and a consumer |
| | `pipeline` | Stages connected by channels |
| | `gate` | Waiting for work in progress |
| | `signals` | Reacting to `SIGINT`, `SIGTERM` and `SIGUSR1` |
| Network | `tcp_roundtrip` | A TCP server and client in one program |
| | `io_timeout` | Time limits on accept, read and connect |
| | `dispatcher` | Accepting on one shard, serving on others |
| | `udp_ping` | Datagrams between two UDP sockets |
| | `unix_socket` | A Unix-domain server shared by all shards, with caller identity |
| | `fixed_buffers` | Registered buffers and zero-copy sends |
| | `echo` | *Server.* Echo with a connection limit and orderly shutdown |
| | `http_hello` | *Server.* A minimal HTTP/1.1 responder on every shard |
| | `chat` | *Server.* A chat room with a queue per client |
| | `proxy` | *Server.* A TCP relay, two tasks per connection |
| | `kv_server` | *Server.* A text-protocol key-value store split by key |
| | `udp_echo` | *Server.* UDP echo with a socket per shard |
| Files | `file_copy` | Reading and writing at offsets through the page cache |
| | `line_count` | Reading a file line by line through a cursor |
| | `direct_io` | Aligned block I/O with `DmaFile` |
| | `wal` | A write-ahead log with checksummed records on direct I/O |
| | `read_ahead` | What reading ahead does for a scan with direct I/O |
| | `log_writer` | Many tasks appending to one file through a channel |
| | `atomic_update` | Replacing a file safely: write, sync, rename |
| | `iotune` | Measuring a device for `Runtime::io_properties` |
| Scheduling | `scheduling_groups` | Dividing a shard's CPU four to one |
| Diagnostics | `shard_stats` | Event-loop counters of every shard |
| | `stall_detector` | Finding the task that did not yield |
| | `memory_pressure` | A cache that shrinks at the shard's memory limit |
| Complete | `kvdb` | *Server.* A durable key-value store: log per shard, group commit, recovery, compaction |
| | `kvdb_bench` | A load generator for `kvdb` |
| Benchmarks | `bench_echo`, `bench_spawn`, `bench_connect`, `bench_disk` | See [Benchmarks](#benchmarks) |

The servers listen on 127.0.0.1, ports 7000 to 7006.

## What is in it

- **Tasks:** `spawn`, `JoinHandle` (with `abort`), `sleep`, `timeout`,
  `interval`, `select`, `yield_now`. A task is one allocation.
- **Blocking work:** `run_blocking` runs a closure on a thread outside the
  runtime, for system calls and libraries with no asynchronous form.
- **Coordination:** `sync::Semaphore` to bound what runs at once on a shard,
  `sync::channel` to pass values between its tasks with backpressure,
  `sync::oneshot` for one task's single answer to another, and
  `sync::Gate` to refuse new work and wait for work in progress when
  shutting down.
- **Signals:** `signal::termination` and `signal::Signals`, to shut down in
  an orderly way on Ctrl-C or `SIGTERM`.
- **Shards:** `submit_to` runs a closure on another shard over a lock-free
  queue dedicated to that pair of shards; `Sharded<T>` keeps one instance of
  a service per shard; `sync::shared_channel` streams values from one shard
  to another, and `sync::shared_mpsc` from several shards to one. A shard
  that falls behind slows its senders down: calls in progress between two
  shards are limited, and the channels are bounded. An idle shard can be
  told to look for work for a while before it sleeps
  (`Runtime::spin_before_sleep`), which makes calls to it faster at the
  price of CPU time.
- **Scheduling groups:** classes of work with separate CPU and I/O shares.
  Preemption is cooperative (`maybe_yield`), and the check is two memory
  reads.
- **Disk:** `fs::File` (page cache) and `fs::DmaFile` (direct I/O), with
  sizes, truncation, preallocation and syncing, plus directories, renaming,
  listing and metadata. `DmaStreamWriter` and `DmaStreamReader` write and
  read a direct-I/O file in pieces of any size, the reader with optional
  read-ahead. Dropping a file closes it without blocking the shard. Every
  device has an I/O queue per shard that orders waiting requests by group
  shares. Declare a device's capacity (`Runtime::io_properties`, measured
  with `iotune::measure`) and all shards together stay within it.
- **Network:** `net::TcpListener`, `TcpStream`, `UdpSocket`, and
  `UnixListener`/`UnixStream` for local connections. Networking
  uses the kernel stack; there is no userspace TCP stack.
- **Buffered I/O:** `io::BufReader` and `io::BufWriter` for lines and
  length-framed data, over TCP streams and, through `fs::Cursor`, files.
- **Buffers:** I/O takes owned buffers (`Vec<u8>`, `AlignedBuf`, and others
  through `buf::IoBuf`). `buf::FixedBuf` comes from a per-shard pool
  registered with io_uring; `TcpStream::send_zc` sends from one without
  copying.
- **Memory:** `alloc::ShardAlloc`, an optional global allocator with one heap
  per shard, and `alloc::on_pressure` to shed memory when a shard is over
  its limit.
- **Diagnostics:** `stats::{shard, groups, devices}`, a stall counter, and
  optional backtraces of stalled shards.

To use the per-shard allocator:

```rust
#[global_allocator]
static GLOBAL: starfish::alloc::ShardAlloc = starfish::alloc::ShardAlloc;
```

## Errors and panics

The library does not panic. Every failure is a `starfish::Error`, and the
clippy lints at the top of `src/lib.rs` keep panicking constructs out; run
`cargo clippy --all-targets` after any change.

A panic in *your* code is caught at the task boundary: the task ends, whoever
awaits it gets `Error::Panicked`, and the shard carries on. This relies on
unwinding, so it does not hold under `panic = "abort"`.

## Checks

```sh
cargo clippy --all-targets          # includes the no-panic lints
RUSTDOCFLAGS='-D warnings' cargo doc --no-deps
cargo test                          # unit, integration and doc tests
MIRIFLAGS=-Zmiri-disable-isolation cargo +nightly miri test --lib
RUSTFLAGS=-Zsanitizer=thread cargo +nightly test -Zbuild-std \
    --target x86_64-unknown-linux-gnu --lib --test basic --test alloc
```

- **Miri** runs the unit tests: the task type, the semaphore, channels and gate, the
  shard-to-shard queue, the I/O scheduler's arithmetic and the allocator's
  size tables. It cannot run
  io_uring or the allocator's memory mapping.
- **The thread sanitizer** runs the integration tests.
- **Fuzzing:** `cargo +nightly fuzz run alloc_ops` (needs `cargo-fuzz`).
- **Soak test:** `python3 scripts/kvdb_soak.py [rounds]` runs the `kvdb`
  example under load, kills it at random moments, restarts it and checks
  that every write it acknowledged is still there. It exercises most of
  the library together: sockets, cross-shard calls, direct I/O, logs,
  compaction and recovery. One hundred rounds (80 kills, 20 orderly
  stops) passed on the one machine it was run on.

Some integration tests assert on timing (CPU and disk shares, device
capacity) and can fail on a heavily loaded machine.

## Benchmarks

`examples/bench_echo.rs`, `bench_spawn.rs`, `bench_connect.rs` and
`bench_disk.rs`. The first three compare with Tokio and with Monoio,
another thread-per-core runtime on io_uring. On the one machine they have been run on (24 cores, loopback,
load generator sharing the cores), as the range of medians from two sets of
runs:

| | starfish | Monoio | Tokio |
|---|---|---|---|
| Echo, 1 thread, 64 connections (requests/s) | 347–353k | 347–354k | 372–379k |
| Echo, 4 threads, 64 connections (requests/s) | 1.09–1.12M | 1.08M | 0.59–0.62M |
| Spawn a task and await it (ns) | 52 | 38–39 | 107–109 |
| Connect and close, 1 thread, 64 clients (connections/s) | 300–306k | 279–285k | 244–252k |

Read as: level with Monoio on the echo test, slower than Monoio at spawning
tasks, behind Tokio on one thread and well ahead of it on four, and ahead
of both at making and dropping connections. (That last row is the range of
five runs of three seconds on one pinned core, over a Unix-domain socket.)

### Where the time goes

A profile of the one-thread echo test (`perf`, same machine) puts the
figures above in proportion:

| Share of CPU time | starfish | Tokio |
|---|---|---|
| Kernel network stack | 69% | 69% |
| The machine's firewall (nftables and connection tracking, on loopback) | 17% | 19% |
| The runtime and the benchmark's own code | 14% | 11% |

- The runtime is about a seventh of the cost of a request. No change to
  it can make this test much faster.
- The firewall on this machine costs more than the runtime does, and
  holds down every network figure here.
- Per request, both io_uring runtimes execute about 2,000 more
  instructions in the kernel than Tokio's epoll path (30.8k against
  28.7k), and starfish about 1,300 more of its own (4,467 against 3,150;
  Monoio 4,097). That is the whole of Tokio's lead on one thread.

Throughput varies by several per cent from run to run on this machine, as
the processor's clock speed does. To compare two builds, count cycles and
instructions per request with `perf stat`, pinned to one core, alternating
the builds.

### A complete server

`examples/kvdb.rs` is a durable key-value server built from the library's
pieces, and `kvdb_bench` loads it. With the server on 8 cores and the load
generator on 8 others of the same machine, over loopback, 100-byte values
and one request at a time per connection:

| | Requests/s | Median | 99% within |
|---|---|---|---|
| Reads, 64 connections | 1.07M | 55 us | 127 us |
| Reads, 512 connections | 1.10M | 465 us | 819 us |
| Durable writes, 8 connections | 6.0k | 1.2 ms | 3.2 ms |
| Durable writes, 512 connections | 118k | 3.9 ms | 8.9 ms |
| Durable writes, 2048 connections | 413–446k | 4.2–4.6 ms | 9.4–10.1 ms |
| 9 reads to 1 write, 512 connections | 919k | 115 us | 6.1 ms |

A write is answered only after it has been synced to the device (a consumer
NVMe drive here), which takes about a millisecond; throughput grows with
the number of connections because writes that arrive together share one
sync. The slowest 0.1% of writes take about 32 ms, because now and then
the drive takes 10 to 30 ms over a sync. One run of five seconds each,
four for the last write row.

These are first indications, not a study. Each median is of five runs of
four seconds (three runs for spawning), on one machine, with the load
generator on the same threads as the server. There is no comparison with
Seastar, and none with Glommio, whose current release does not build with a
recent C compiler.

## License

GNU General Public License, version 3 or later. See [LICENSE](LICENSE).
