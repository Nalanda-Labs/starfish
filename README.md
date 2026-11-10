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

[docs/guide.md](docs/guide.md) is a tour of the whole library with
examples; `cargo doc --open` shows it together with the API reference.

## Examples

There are 41 programs in `examples/`. Run one with
`cargo run --release --example <name>`. Those marked *server* run until
Ctrl-C; the others do their work and exit.

| Area | Example | What it shows |
|---|---|---|
| Basics | `hello` | Starting a runtime and returning a value |
| | `tasks` | Spawning tasks, collecting results, sharing state with `Rc` |
| | `timers` | `sleep`, `timeout` and a periodic tick that does not drift |
| | `abort` | Cancelling a task and what happens to its state |
| | `yielding` | The cost of not yielding, and `maybe_yield` |
| | `panics` | A panic ends one task, not the shard |
| | `blocking` | Running blocking code on a thread and awaiting it |
| Shards | `cross_shard` | `submit_to` and `submit_to_all` |
| | `sharded_counter` | A `Sharded` service with one instance per shard |
| | `kv_store` | Data split across shards by key hash |
| | `parallel_primes` | Dividing a computation over all shards |
| | `shared_channel` | Streaming values between shards with backpressure |
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
| | `fixed_buffers` | Registered buffers and zero-copy sends |
| | `echo` | *Server.* Echo with a connection limit and orderly shutdown |
| | `http_hello` | *Server.* A minimal HTTP/1.1 responder on every shard |
| | `chat` | *Server.* A chat room with a queue per client |
| | `proxy` | *Server.* A TCP relay, two tasks per connection |
| | `kv_server` | *Server.* A text-protocol key-value store split by key |
| | `udp_echo` | *Server.* UDP echo with a socket per shard |
| Files | `file_copy` | Reading and writing at offsets through the page cache |
| | `line_count` | Reading a file from start to end |
| | `direct_io` | Aligned block I/O with `DmaFile` |
| | `log_writer` | Many tasks appending to one file through a channel |
| | `iotune` | Measuring a device for `Runtime::io_properties` |
| Scheduling | `scheduling_groups` | Dividing a shard's CPU four to one |
| Diagnostics | `shard_stats` | Event-loop counters of every shard |
| | `stall_detector` | Finding the task that did not yield |
| | `memory_pressure` | A cache that shrinks at the shard's memory limit |
| Benchmarks | `bench_echo`, `bench_spawn`, `bench_disk` | See [Benchmarks](#benchmarks) |

The servers listen on 127.0.0.1, ports 7000 to 7005.

## What is in it

- **Tasks:** `spawn`, `JoinHandle` (with `abort`), `sleep`, `timeout`,
  `yield_now`. A task is one allocation.
- **Coordination:** `sync::Semaphore` to bound what runs at once on a shard,
  `sync::channel` to pass values between its tasks with backpressure, and
  `sync::Gate` to refuse new work and wait for work in progress when
  shutting down.
- **Signals:** `signal::termination` and `signal::Signals`, to shut down in
  an orderly way on Ctrl-C or `SIGTERM`.
- **Shards:** `submit_to` runs a closure on another shard over a lock-free
  queue dedicated to that pair of shards; `Sharded<T>` keeps one instance of
  a service per shard; `sync::shared_channel` streams values from one shard
  to another. A shard that falls behind slows its senders down: calls in
  progress between two shards are limited, and the channel is bounded.
- **Scheduling groups:** classes of work with separate CPU and I/O shares.
  Preemption is cooperative (`maybe_yield`), and the check is two memory
  reads.
- **Disk:** `fs::File` (page cache) and `fs::DmaFile` (direct I/O). Every
  device has an I/O queue per shard that orders waiting requests by group
  shares. Declare a device's capacity (`Runtime::io_properties`, measured
  with `iotune::measure`) and all shards together stay within it.
- **Network:** `net::TcpListener`, `TcpStream`, `UdpSocket`. Networking uses
  the kernel stack; there is no userspace TCP stack.
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

Some integration tests assert on timing (CPU and disk shares, device
capacity) and can fail on a heavily loaded machine.

## Benchmarks

`examples/bench_echo.rs`, `bench_spawn.rs` and `bench_disk.rs`. On the one
machine they have been run on (24 cores, loopback, load generator sharing
the cores):

| | starfish | Tokio |
|---|---|---|
| Echo, 1 thread, 64 connections | about 330k requests/s | about 250–320k |
| Echo, 4 threads, 64 connections | about 1.0M requests/s | about 450–620k |
| Spawn a task and await it | about 57 ns | about 110–125 ns |

These are first indications, not a study: single runs of a few seconds, one
machine, no comparison with Seastar, Monoio or Glommio.

## License

GNU General Public License, version 3 or later. See [LICENSE](LICENSE).
