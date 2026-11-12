A tour of the library: every feature, how to use it, and its caveats.

This guide goes through everything starfish offers, in the order a server
usually needs it. Each section says what the feature is for, shows it in
use, and ends with its caveats: the limits, the surprises and the things
that are easy to get wrong. The API reference has the details of each
item; the examples here are compiled with the documentation, so they match
the code.

- [The model](#the-model)
- [Starting a runtime](#starting-a-runtime)
- [Tasks](#tasks)
- [Time](#time)
- [Yielding](#yielding)
- [Waiting for one of several things](#waiting-for-one-of-several-things)
- [Blocking work](#blocking-work)
- [Working across shards](#working-across-shards)
- [Services with an instance per shard](#services-with-an-instance-per-shard)
- [Backpressure between shards](#backpressure-between-shards)
- [Streaming between shards](#streaming-between-shards)
- [Semaphores](#semaphores)
- [Channels on a shard](#channels-on-a-shard)
- [Gates](#gates)
- [Buffers](#buffers)
- [TCP](#tcp)
- [Buffered reading and writing](#buffered-reading-and-writing)
- [UDP](#udp)
- [Unix-domain sockets](#unix-domain-sockets)
- [Files](#files)
- [Direct I/O](#direct-io)
- [Streams on direct I/O](#streams-on-direct-io)
- [Directories and file management](#directories-and-file-management)
- [Registered buffers and zero-copy sends](#registered-buffers-and-zero-copy-sends)
- [Receiving with buffers the kernel picks](#receiving-with-buffers-the-kernel-picks)
- [Scheduling groups](#scheduling-groups)
- [The disk I/O scheduler and device capacity](#the-disk-io-scheduler-and-device-capacity)
- [Memory](#memory)
- [Signals](#signals)
- [Shutting down](#shutting-down)
- [Statistics](#statistics)
- [Stall detection](#stall-detection)
- [Errors and panics](#errors-and-panics)
- [Cancellation](#cancellation)
- [Testing code that uses the runtime](#testing-code-that-uses-the-runtime)
- [A complete example](#a-complete-example)
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

**Caveats**

- Tasks are never interrupted. One that computes for long without awaiting
  holds up every other task, every timer and all I/O on its shard. See
  [Yielding](#yielding).
- Nothing on a shard may block: no `std::thread::sleep`, no blocking file
  or socket calls, no waiting on a `std::sync::Mutex` that another thread
  holds for long. Blocking work belongs on a thread of its own; see
  [Blocking work](#blocking-work).
- Work does not move between shards. If one shard has more to do than the
  others, they do not help; spreading the load evenly is the
  application's job.
- Holding a `RefCell` borrow across an `.await` lets another task on the
  shard find it borrowed and panic. Borrow, use, release, then await.

# Starting a runtime

[`Runtime::run`](crate::Runtime::run) turns the calling thread into shard
0, starts the others, runs one future on shard 0 and returns its output.

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
| `submit_limit` | 1024 | Calls one shard may have in progress on another |
| `spin_before_sleep` | zero | How long an idle shard looks for work before sleeping |
| `io_depth` | 64 | Requests per device a shard keeps in the kernel |
| `io_properties` | none | Measured capacity of a storage device |
| `io_latency_goal` | 1.5 × `task_quota` | Work queued inside such a device |
| `fixed_buffers` | none | Per-shard pool of registered buffers |
| `memory_limit` | none | Heap size at which a shard asks for memory back |
| `stall_threshold` | 25 ms | Slice length that counts as a stall |
| `stall_backtraces` | off | Record the stack of a stalled shard |
| `on_stall` | none | Function called after every stall |

**Caveats**

- Linux 6.0 or later on x86-64 only.
- io_uring must be available to the process. Some container runtimes block
  it with their default seccomp profile, and it can be switched off
  system-wide (`kernel.io_uring_disabled`). `run` then fails with
  [`Error::Io`](crate::Error::Io) before anything starts.
- When the future given to `run` completes, every task still running on
  any shard is dropped where it stands, without a chance to finish. An
  orderly stop has to be arranged; see [Shutting down](#shutting-down).
- A `Runtime` is only a description until `run` is called. Building one
  and not running it does nothing, and the compiler warns about it.
- `run` cannot be called from inside a runtime
  ([`Error::AlreadyRunning`](crate::Error::AlreadyRunning)). Several
  runtimes can run one after another, or on different threads.
- The calling thread is pinned for the duration of `run` and gets its
  original CPU affinity back afterwards.
- Shards are pinned to the CPUs the process is allowed to use, in order.
  Asking for more shards than there are CPUs puts several shards on one
  CPU, which defeats the design.
- If a shard's io_uring fails while running, the whole runtime stops and
  `run` returns the error.
- Outside a runtime, functions that need one return
  [`Error::NoRuntime`](crate::Error::NoRuntime), and I/O futures resolve
  to an error instead of doing anything.
- More operations in flight on a shard than `ring_entries` is normal; the
  queue is flushed to the kernel as it fills. Only if the kernel refuses
  for long does an operation fail, with `WouldBlock`.

# Tasks

[`spawn`](crate::spawn) starts a task on the current shard and returns a
[`JoinHandle`](crate::JoinHandle). Awaiting the handle gives the task's
output. Dropping the handle lets the task run on by itself.
[`abort`](crate::JoinHandle::abort) stops it: its future is dropped before
the call returns, which cancels its I/O and releases what it holds.

```rust,no_run
use std::time::Duration;
use starfish::{sleep, spawn, Error};

async fn tasks() -> starfish::Result<()> {
    let sum = spawn(async { 1 + 1 });
    assert_eq!(sum.await?, 2);

    let sleeper = spawn(sleep(Duration::from_secs(60)));
    sleeper.abort();
    assert!(sleeper.is_finished());
    assert!(matches!(sleeper.await, Err(Error::Canceled)));
    Ok(())
}
```

**Caveats**

- A `JoinHandle` cannot leave its shard, and neither can a task. To get a
  result to another shard, return it from a
  [`submit_to`](crate::submit_to) closure.
- `abort` runs the destructors of everything the task owned inside the
  `abort` call, on the caller's time. Aborting a task that owns a great
  deal is itself a long computation.
- A task that aborts itself, through its own handle, keeps running until
  it next yields, and is dropped then.
- A task that has finished is not affected by `abort`; its output can
  still be collected.
- A task that awaits its own handle waits forever.
- Detached tasks (handle dropped) that are still running when the runtime
  stops are dropped without notice. Nobody is told whether they finished.
- Outside a runtime, `spawn` drops the future and returns a handle that
  resolves to [`Error::NoRuntime`](crate::Error::NoRuntime).
- A task costs one allocation, which holds the future and later its
  output; a large future makes a large task.

# Time

[`sleep`](crate::sleep) completes after a duration.
[`timeout`](crate::timeout) gives a future a limited time and drops it
when the time runs out.

```rust,no_run
use std::time::{Duration, Instant};
use starfish::{sleep, timeout};

async fn time() {
    sleep(Duration::from_millis(10)).await;

    let late = timeout(Duration::from_millis(10), sleep(Duration::from_secs(1))).await;
    assert!(late.is_err());

    // A periodic tick that does not drift: sleep until the next multiple
    // of the period, not for a fixed time after the work.
    let period = Duration::from_millis(100);
    let start = Instant::now();
    for tick in 1..=5u32 {
        sleep((start + period * tick).saturating_duration_since(Instant::now())).await;
        // ... periodic work ...
    }
}
```

**Caveats**

- A timer fires when its shard next goes round its event loop after the
  deadline, never before. While a task hogs the shard, timers are late by
  as long as it does.
- For a periodic tick there is [`interval`](crate::interval), which does
  what the loop above does. If a tick is missed by a whole period or
  more, the ticks in between are skipped: there is never a burst to catch
  up, so a count of ticks is not a measure of time.
- `timeout` stops the wait by dropping the future. What that means for
  the operation inside is described under [Cancellation](#cancellation):
  in short, a timed-out read or write may still have happened.
- A [`Sleep`](crate::Sleep) belongs to the shard that first polled it.
- A duration too long to add to the current time sleeps forever.
- Outside a runtime, `sleep` still ends on time, but by asking to be
  polled again and again, which keeps a CPU busy.

# Yielding

A loop that may run for long should give way to other tasks.
[`maybe_yield`](crate::maybe_yield) does so only when the time slice
([`task_quota`](crate::Runtime::task_quota)) is used up, and checking costs
two memory reads, so it can go inside the loop.
[`yield_now`](crate::yield_now) yields unconditionally, and
[`should_yield`](crate::should_yield) is the bare check.

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

In the `yielding` example, a task that wants to run every millisecond
waits 200 ms next to a computation that never yields, and 2 ms next to one
that calls `maybe_yield`.

**Caveats**

- The time slice is a target, not a guarantee. The runtime cannot take
  the CPU away; it can only tell a task that asks.
- `yield_now` in a tight loop wastes most of the time on going round the
  event loop. Prefer `maybe_yield`.
- A task that yields goes to the back of its scheduling group's queue.
- Code that cannot await (a long sort, a compression call, a library that
  takes its time) cannot yield either. Cut the work into pieces or move
  it to a thread.

# Waiting for one of several things

[`select`](crate::select()) waits for whichever of two futures finishes
first: a message or a timer, a connection or the order to stop.
[`select3`](crate::select3) takes three, and
[`select_all`](crate::select_all) any number of the same kind, such as
connection attempts to several servers, returning which one finished.

```rust,no_run
use std::time::Duration;
use starfish::sync::{channel, oneshot};
use starfish::{interval, select, Either};

async fn worker() {
    let (_jobs, mut inbox) = channel::<u32>(16);
    let (_stop, stopped) = oneshot::<()>();
    // Pinned and passed by reference, so that it survives losing a round.
    let mut stopped = std::pin::pin!(stopped);
    let mut report = interval(Duration::from_secs(10));
    loop {
        // Three things at once, by nesting. Rarest first.
        match select(stopped.as_mut(), select(report.tick(), inbox.recv())).await {
            Either::First(_) => return,
            Either::Second(Either::First(())) => println!("still here"),
            Either::Second(Either::Second(Some(job))) => println!("job {job}"),
            Either::Second(Either::Second(None)) => return,
        }
    }
}
```

**Caveats**

- The first future is looked at first, and if it is ready the second is
  not looked at at all. In a loop where the first is always ready, the
  second starves. Put the rarer event first: the stop signal before the
  timer, the timer before the queue of work. The `select` example was
  first written the other way round and its timer never fired.
- A future passed by value is dropped the moment the other one wins.
  That is cancellation, with all it implies (see
  [Cancellation](#cancellation)): harmless for a channel, a permit or a
  timer, not for a read or a write.
- To keep a future across rounds of a loop, pin it and pass
  `future.as_mut()`. Recreating it each round is fine for things that
  lose nothing when dropped, such as `recv` or `tick`, and wrong for
  anything that holds progress, such as a half-finished request.
- For three futures of different kinds there is
  [`select3`](crate::select3), and for any number of the same kind
  [`select_all`](crate::select_all), which says which one finished and
  takes them in turn so that none is favoured. Past three kinds, a task
  per source feeding one channel is clearer.
- `select_all` boxes each future, one allocation apiece, and drops all
  but the winner. It is for a handful of alternatives, not for waiting on
  thousands of things.
- It is a function, not a macro: both futures are created before it is
  called, even the one that will lose.

# Blocking work

Nothing on a shard may block. [`run_blocking`](crate::run_blocking) runs a
closure on a thread outside the runtime and gives back its result, while
the shard carries on with its other tasks. It is for system calls and
libraries that have no asynchronous form, and for long computations that
cannot be cut into pieces.

```rust,no_run
use std::net::{SocketAddr, ToSocketAddrs};

async fn resolve(name: String) -> starfish::Result<Vec<SocketAddr>> {
    // The system's name resolver blocks.
    let found = starfish::run_blocking(move || {
        (name.as_str(), 0).to_socket_addrs().map(|found| found.collect())
    })
    .await??;
    Ok(found)
}
```

**Caveats**

- Each call costs a hand-over to another thread and back: microseconds,
  where a call on the shard costs nanoseconds. Not for anything on a hot
  path.
- The work cannot be interrupted. Dropping the future, by `timeout` or
  `abort`, stops the wait; the work runs to the end and its result is
  discarded.
- The threads are shared by the whole process and there are at most 256.
  When all are busy, further work waits. Work that blocks indefinitely,
  such as waiting on a lock or a socket that may never answer, can use
  them all up and starve everything else that needs one, `read_dir`
  included.
- The closure and its result cross threads and must be `Send`. The work
  cannot use anything that belongs to a shard: no `Rc`, and none of the
  runtime's own calls, which fail there with
  [`Error::NoRuntime`](crate::Error::NoRuntime).
- These threads use the system allocator, not the per-shard heaps, and
  are not pinned. Heavy computation on them competes with the shards for
  CPU.
- A panic in the work is caught and returned as
  [`Error::Panicked`](crate::Error::Panicked).
- A thread that has been idle for ten seconds exits, so a burst of
  blocking work does not leave threads behind for good.

# Working across shards

[`submit_to`](crate::submit_to) runs a closure on another shard and returns
its result. The closure and the result cross threads and must be `Send`.
The future the closure creates stays on the target shard and may use that
shard's `Rc`s and sockets. [`submit_to_all`](crate::submit_to_all) does the
same on every shard at once and returns the results in shard order, or the
first failure. [`submit_to_each`](crate::submit_to_each) returns every
shard's outcome, for when it matters which calls took place.

```rust,no_run
use starfish::{shard_count, shard_id, submit_to, submit_to_all};

async fn shards() -> starfish::Result<()> {
    let last = shard_count()? - 1;
    let ran_on = submit_to(last, || async { shard_id() }).await??;
    assert_eq!(ran_on, last);

    let ids = submit_to_all(|| async { shard_id() }).await?;
    assert_eq!(ids.len(), shard_count()?);
    Ok(())
}
```

**Caveats**

- A call costs far more than a local function call: an allocation for the
  closure, a task on the other shard, and a wake-up through the kernel
  when that shard is asleep (see
  [Idle shards](#idle-shards-and-spin_before_sleep) below). Batch small pieces of work, or use a
  [`shared_spsc`](crate::sync::shared_spsc) for a stream.
- Dropping the returned future stops the wait, not the call. The closure
  still runs to the end on the other shard; only its result is discarded.
- A call to the shard it is made on runs in place, without a message or
  a task. A panic in it is still caught and returned as
  [`Error::Panicked`](crate::Error::Panicked), as for any other shard.
- Calls run as separate tasks on the other shard and may finish in any
  order.
- [`Error::Canceled`](crate::Error::Canceled) means the other shard shut
  down before running or finishing the call.
- `submit_to_all` returns the first error and discards the other results,
  though the closure has run on the shards that succeeded.
  [`submit_to_each`](crate::submit_to_each) returns every shard's
  outcome instead.
- The closure runs in the caller's scheduling group.
- The number of calls in progress is limited; see
  [Backpressure between shards](#backpressure-between-shards).

## Idle shards and `spin_before_sleep`

A shard with nothing to do sleeps in the kernel, and whoever next has
something for it has to wake it there. That costs microseconds, where
handing work to a busy shard costs a fraction of one. It is why a call to
an idle shard takes several times as long as a call to a busy one.

[`Runtime::spin_before_sleep`](crate::Runtime::spin_before_sleep) makes an
idle shard keep looking for work for a while before it sleeps. Measured on
one machine:

| | No spin | 100 µs spin |
|---|---|---|
| Calls to another shard, one after another | 2.3 µs each | 0.9 µs |
| `kvdb` reads, 1 connection | 39k/s, median 23 µs | 80k/s, median 10 µs |
| `kvdb` reads, 8 connections | 362k/s, 99% within 79 µs | 502k/s, 99% within 28 µs |
| `kvdb` reads, 64 or more connections | 1.25M/s | the same |

**Caveats**

- It trades CPU for latency. After every burst of work a shard runs at
  full power for the spin time doing nothing. In the `kvdb` runs above the
  server used 60% more CPU time with the spin than without.
- It helps at light and moderate load only. A shard that always has work
  never sleeps anyway, which is why the busiest rows do not change.
- A spin longer than the gaps between pieces of work means the shards
  never sleep: every CPU the process has shows 100% whatever the load.
  That confuses monitoring and autoscaling that go by CPU use, and wastes
  power. [`ShardStats::spinning`](crate::stats::ShardStats::spinning) says
  how much time went that way.
- On a machine shared with other programs, a spinning shard takes CPU
  from them and gains nothing while descheduled. This is for dedicated
  cores.
- A spin shorter than the gap does little: the shard is asleep again by
  the time the next work arrives. Tens to a few hundred microseconds is
  the useful range.

# Services with an instance per shard

[`Sharded`](crate::Sharded) keeps one instance of a service on every shard.
The handle is a small `Copy` value that can be sent anywhere; the instances
never leave their shards.

[`start`](crate::Sharded::start) builds each instance with a plain
function. When building has to wait, to open a file or recover data, use
[`start_with`](crate::Sharded::start_with), whose factory is a future, or
[`try_start_with`](crate::Sharded::try_start_with) if it can fail.
[`stop_with`](crate::Sharded::stop_with) runs a future against every
instance before removing it, for flushing and closing.

```rust,no_run
use std::cell::Cell;
use starfish::{shard_count, Sharded};

async fn counters() -> starfish::Result<()> {
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

**Caveats**

- `start` takes an ordinary function. For an instance that has to wait
  for something while it is built, such as opening its files, use
  [`start_with`](crate::Sharded::start_with) or, if building can fail,
  [`try_start_with`](crate::Sharded::try_start_with), which removes the
  service from every shard if any one fails.
- The instance type needs no `Send`, but the factory and the closures
  given to `invoke_on` do, since they travel to the other shards.
- `invoke_on_all` returns the first failure and drops the other shards'
  results. [`invoke_on_each`](crate::Sharded::invoke_on_each) returns
  every shard's outcome, for when it matters which calls took place.
- `stop` removes the instances; it does not call anything on them. For
  an instance that must finish work first,
  [`stop_with`](crate::Sharded::stop_with) runs a future against each
  before removing them.
- An instance is dropped when the last `Rc` to it goes. A task that keeps
  the `Rc` from [`local`](crate::Sharded::local) past `stop` keeps its
  instance alive.
- After `stop`, the handle's methods return
  [`Error::ServiceStopped`](crate::Error::ServiceStopped).
- If the factory panics on one shard, the service is removed from the
  others and `start` returns [`Error::Panicked`](crate::Error::Panicked).
- Services not stopped by the time the runtime ends are dropped with it.

# Backpressure between shards

A shard may have only so many calls in progress on one other shard:
[`submit_limit`](crate::Runtime::submit_limit), 1024 by default. Past
that, `submit_to` waits for an earlier call to finish before it sends. An
overloaded shard therefore slows its callers down, instead of collecting
an ever longer list of calls to run.

**Caveats**

- A call keeps its place until the other shard has finished it, even if
  the caller gave up waiting. Abandoning slow calls does not make room.
- Calls that wait for further calls in the same direction can block each
  other. If every place from shard A to shard B is taken by calls that
  each wait for a nested call from A to B, none can proceed, and nothing
  reports it. Keep chains of calls from doubling back, or raise the limit.
- The limit is per ordered pair of shards. With many shards, one shard
  can still receive that many calls from each of the others at once.
- It covers everything built on `submit_to`: `submit_to_all` and the
  calls of a `Sharded` service.
- The limit counts calls, whatever they cost. A few very expensive calls
  can overload a shard well below it.

# Streaming between shards

For many values going the same way, one `submit_to` each is wasteful.
[`shared_spsc`](crate::sync::shared_spsc) is a bounded queue with
one end on each shard. A value costs no allocation and no task, the other
shard is woken only when it is waiting, and a full queue makes the sender
wait.

```rust,no_run
use starfish::submit_to;
use starfish::sync::shared_spsc;

async fn stream() -> starfish::Result<u64> {
    // Made here; each end is then moved to the shard that uses it.
    let (mut sender, mut receiver) = shared_spsc(64);
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

**Caveats**

- One sender and one receiver, and neither end can be cloned. For several
  producers use `shared_mpsc`, below.
- The capacity is rounded up to a power of two, and is at least 2.
- A value queued just as the receiver is dropped is dropped unseen, with
  no error to the sender. A successful `send` means "queued", not
  "received".
- Values still queued when both ends are gone are dropped on whichever
  thread lets go last, which is why they must be `Send`.
- There is no way to close a channel except dropping an end.

## From several shards to one

[`shared_mpsc`](crate::sync::shared_mpsc) is the same thing with any
number of senders: for many shards reporting to one, such as a logger or a
collector of statistics. Cloning a sender makes another sender.

```rust,no_run
use starfish::sync::shared_mpsc;
use starfish::{shard_count, spawn, submit_to};

async fn collect() -> starfish::Result<u64> {
    let (sender, mut receiver) = shared_mpsc(64);
    for shard in 0..shard_count()? {
        let mut sender = sender.clone();
        spawn(submit_to(shard, move || async move {
            for n in 0..1000u64 {
                if sender.send(n).await.is_err() {
                    break; // the receiver is gone
                }
            }
        }));
    }
    // The loop ends when every sender is gone, the original included.
    drop(sender);
    let mut sum = 0;
    while let Some(n) = receiver.recv().await {
        sum += n;
    }
    Ok(sum)
}
```

**Caveats**

- Every sender has a queue of its own, so the capacity is per sender. A
  hundred senders with a capacity of 64 can hold 6,400 values between
  them, and each clone allocates its queue.
- Values from one sender arrive in the order it sent them. Values from
  different senders arrive in no particular order, and not in the order
  they were sent.
- A sender is used by one task at a time. Two tasks that both send need
  a clone each, even on the same shard.
- The receiver's loop ends only when *every* sender is gone, including
  the original that the clones were made from.
- The receiver takes from the senders in turn. A sender that produces
  faster than the others gets no more than its turn, and waits.
- One receiver. For several shards taking from one queue, see the next
  section.
- As with `shared_spsc`, a value queued just as the receiver is
  dropped is lost without an error, and the capacity is rounded up to a
  power of two, at least 2.

## To whichever of several shards is free

[`shared_mpmc`](crate::sync::shared_mpmc) has any number of senders and
any number of receivers, and each value goes to exactly one receiver:
whichever asks first. It is the channel for sharing work out between
shards, where it does not matter which of them does a job as long as one
that is free does.

```rust,no_run
use starfish::sync::shared_mpmc;
use starfish::{shard_count, spawn, submit_to};

async fn process(jobs: Vec<Vec<u8>>) -> starfish::Result<usize> {
    let (sender, receiver) = shared_mpmc::<Vec<u8>>(64);
    let mut workers = Vec::new();
    for shard in 0..shard_count()? {
        let receiver = receiver.clone();
        workers.push(spawn(submit_to(shard, move || async move {
            let mut bytes = 0;
            while let Some(job) = receiver.recv().await {
                bytes += job.len(); // stands for the work
            }
            bytes
        })));
    }
    drop(receiver);
    for job in jobs {
        if sender.send(job).await.is_err() {
            break; // every worker is gone
        }
    }
    // The workers' loops end when every sender is gone.
    drop(sender);
    let mut total = 0;
    for worker in workers {
        total += worker.await??;
    }
    Ok(total)
}
```

Both ends take `&self` and can be cloned, so an end can be shared by
several tasks of a shard as well as sent to other shards. Tasks that wait,
for a value or for room, are served longest wait first, and `recv` can be
dropped at any moment without losing a value.

**Caveats**

- It is one queue behind one lock, where the other two cross-shard
  channels never lock. The lock is held for the moment it takes to add or
  remove a value, which is cheap while the channel is lightly used; shards
  that all use one channel heavily contend for it, and a shard that has to
  wait for the lock does nothing else meanwhile. Use it to hand out jobs,
  not for every packet. Where each value has an obvious destination, a
  [`shared_spsc`](crate::sync::shared_spsc) per destination scales
  better.
- The order values are queued in is the order they are taken in, but with
  several receivers that says nothing about the order they are finished
  in.
- The receivers' loops end only when every sender is gone, and senders
  fail only when every receiver is gone: remember to drop the originals
  that the clones were made from.
- When the last receiver is dropped, values still queued are dropped with
  it, without an error to whoever sent them.
- This is not work stealing: a job is taken by the shard that asks, and
  stays there. Nothing moves a task that is already running.

# Semaphores

A [`Semaphore`](crate::sync::Semaphore) limits how much runs at once on a
shard. Permits can stand for anything countable: connections, requests,
bytes of memory.

```rust,no_run
use starfish::spawn;
use starfish::sync::Semaphore;

async fn limited() -> starfish::Result<()> {
    // At most 100 jobs at a time.
    let limit = Semaphore::new(100);
    loop {
        let permit = limit.acquire(1).await?;
        spawn(async move {
            let _permit = permit; // given back when the job ends
            // ... the job ...
        });
    }
}
```

**Caveats**

- Permits are handed out strictly in the order they were asked for. A
  large request at the head of the queue holds up smaller ones behind it
  even when there are permits enough for them. That is the price of large
  requests never being starved. If it hurts, use separate semaphores for
  the two kinds of work.
- [`try_acquire`](crate::sync::Semaphore::try_acquire) fails whenever
  anyone is waiting, even if permits are free, for the same reason.
- A request for more permits than the semaphore will ever hold waits
  until the semaphore is closed. Nothing detects it.
- A semaphore belongs to its shard and is not `Send`. It limits that
  shard only: a limit of 100 on each of 8 shards allows 800 in all.
- [`forget`](crate::sync::Permit::forget) removes permits for good;
  [`add_permits`](crate::sync::Semaphore::add_permits) can add more than
  the semaphore started with. The count is whatever these make it.
- After [`close`](crate::sync::Semaphore::close), permits already handed
  out stay valid and are returned harmlessly.
- A permit is given back the moment it is dropped. `let _ =
  limit.acquire(1).await?;` drops it at once and limits nothing; bind it
  to a name, such as `_permit`, that lives as long as the work. The
  compiler warns when a permit is not bound at all, but not about
  `let _ =`.

# Channels on a shard

[`channel`](crate::sync::channel) is a bounded queue from many senders to
one receiver, all on one shard. A full queue makes senders wait, which
slows producers to the pace of the consumer.

```rust,no_run
use starfish::spawn;
use starfish::sync::channel;

async fn pipeline() {
    let (sender, mut receiver) = channel(16);
    for worker in 0..4 {
        let sender = sender.clone();
        spawn(async move {
            for step in 0..100 {
                if sender.send((worker, step)).await.is_err() {
                    break; // the receiver is gone
                }
            }
        });
    }
    // The loop below ends when every sender has been dropped, so the
    // original must go too.
    drop(sender);
    while let Some((worker, step)) = receiver.recv().await {
        println!("worker {worker} reached step {step}");
    }
}
```

**Caveats**

- The receiver's loop only ends when *every* sender is gone, including
  the original one that the clones were made from. Forgetting to drop it
  is the usual reason a consumer never finishes.
- One receiver. For several consumers, hand work out to one channel each.
- There is no unbounded channel, on purpose: a queue without a bound hides
  overload until memory runs out. A capacity of 0 is treated as 1.
- [`try_send`](crate::sync::Sender::try_send) reports the queue as full
  while other senders are waiting, even at a moment when it has room, so
  that it cannot jump the queue.
- Dropping the receiver drops the values still queued, and every waiting
  and later send fails, handing its value back.
- Both ends belong to their shard. Between shards, use
  [`shared_spsc`](crate::sync::shared_spsc).

## A single answer

[`oneshot`](crate::sync::oneshot) is a channel for exactly one value: the
way for a task to wait for something another task on the shard will
finish. The waiting task awaits the receiver; whoever produces the result
gets the sender.

```rust,no_run
use starfish::sync::{channel, oneshot, OneshotSender};

/// A request to a task that does the work, with the way to answer it.
struct Request {
    number: u64,
    answer: OneshotSender<u64>,
}

async fn ask() -> Option<u64> {
    let (requests, mut inbox) = channel::<Request>(16);
    starfish::spawn(async move {
        while let Some(request) = inbox.recv().await {
            let _ = request.answer.send(request.number * 2);
        }
    });

    let (answer, reply) = oneshot();
    requests.send(Request { number: 21, answer }).await.ok()?;
    reply.await // `None` if the worker dropped the request unanswered
}
```

**Caveats**

- The receiver resolves to `None` when the sender is dropped without
  sending. That is how a waiting task learns that its request was
  abandoned, so do not treat `None` as a value.
- `send` never waits, and fails, handing the value back, if the receiver
  is gone. A task doing expensive work for an answer can check
  [`is_closed`](crate::sync::OneshotSender::is_closed) and stop early.
- Both ends belong to their shard. Across shards,
  [`submit_to`](crate::submit_to) already returns its closure's result.

# Gates

A [`Gate`](crate::sync::Gate) counts work in progress. Closing it turns new
work away and waits for the rest. It is the tool for stopping cleanly.

```rust,no_run
use starfish::spawn;
use starfish::sync::Gate;

async fn serve(gate: Gate) -> starfish::Result<()> {
    let guard = gate.enter()?; // fails once the gate is closed
    spawn(async move {
        let _guard = guard;
        // ... one request ...
    });

    // On shutdown: no new requests, and wait for those in flight.
    gate.close().await;
    Ok(())
}
```

**Caveats**

- The gate is closed by the call to `close`, not by awaiting what it
  returns. `gate.close();` alone closes the gate and waits for nothing;
  the compiler warns about the unused future.
- A task that awaits `close` while holding a guard of the same gate waits
  for itself, forever. Nothing detects it.
- `close` waits as long as the work takes. Put a
  [`timeout`](crate::timeout) around it if a stuck request must not hold
  up shutdown.
- A closed gate stays closed. For a restart, make a new one.
- Entering is the caller's job. Work that never entered is not waited for.
- A guard counts only while it is alive. As with a permit, `let _ =
  gate.enter()?;` drops it at once; move the guard into the task that
  does the work.
- A gate belongs to its shard; a server needs one per shard.

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
`FixedBuf`.

**Caveats**

- A read fills the buffer from its start, up to its *capacity*, and sets
  its length to what was read. Whatever the buffer held is replaced; a
  read never appends.
- A read needs a buffer with capacity. `Vec::new()` and a `Vec` left
  behind by `std::mem::take` have none, and a read into one fails with
  `InvalidInput`. Use `Vec::with_capacity(n)`.
- A `Vec` may have more capacity than was asked for, and a read uses all
  of it.
- If the future of an I/O call is dropped, the buffer does not come back.
  The runtime keeps it until the kernel is finished and then frees it.
- One call transfers at most 4 GiB.
- Writes take the buffer by value too, even though they only read it. To
  send the same bytes many times, use a `&'static` slice or clone.
- Implementing `IoBuf` for a type of your own is `unsafe`: the memory
  must not move when the value is moved.

# TCP

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

[`TcpStream`](crate::net::TcpStream) has `connect`, `read`, `write`,
`write_all`, `shutdown`, `set_nodelay` and the two address getters.
[`lookup_host`](crate::net::lookup_host) turns a host name into addresses
to connect to. [`TcpListener::bind_with`](crate::net::TcpListener::bind_with)
takes [`BindOptions`](crate::net::BindOptions): whether the port may be
shared, and how many connections may wait to be accepted. A
socket is not tied to the shard that created it: each call goes to the
io_uring of the shard it is made on, so a connection can be moved to
another shard inside a [`submit_to`](crate::submit_to) closure.

**Caveats**

- `bind` sets `SO_REUSEPORT`, since that is what gives every shard its
  own listener. With it, any process of the same user can bind the same
  port and receive a share of the connections, and a second copy of a
  server starts without complaint. To rule that out, bind with
  [`BindOptions::new().reuse_port(false)`](crate::net::BindOptions): a
  second bind then fails, and so only one shard can listen, which then
  hands connections to the others as the `dispatcher` example does.
- The kernel assigns each connection to a listener by hashing its
  addresses, once. A few busy, long-lived connections can load one shard
  far more than the others, and nothing moves them.
- With one listener per shard, a shard that stops accepting (a stall, a
  full connection limit) leaves its share of new connections waiting in
  its own queue; the other shards do not take them.
- The accept queue is 1024 connections long unless
  [`BindOptions::backlog`](crate::net::BindOptions::backlog) says
  otherwise, and never longer than the kernel's own limit
  (`net.core.somaxconn`).
- `write` may send only part of the buffer. `write_all` sends everything,
  in as many steps as it takes, so two tasks calling `write_all` on one
  stream at the same time can interleave their data. Give each stream one
  writer.
- `read` returning `Ok(0)` means the peer closed its sending side.
- There is no built-in time limit on anything. A peer that goes silent
  holds its task and its connection forever unless the read is wrapped in
  [`timeout`](crate::timeout), and after a timeout the connection should
  be closed (see [Cancellation](#cancellation)).
- Dropping `accept` just as a connection arrives closes that connection.
- Addresses are `SocketAddr`s. [`lookup_host`](crate::net::lookup_host)
  turns a name into addresses with the system's resolver, on a thread
  outside the runtime, which is slow: look a name up once and keep the
  result. Nothing here caches names or notices when they change.
- Writing to a connection the peer has closed returns an error
  (`EPIPE`); the process does not get `SIGPIPE`.
- `shutdown` and dropping a socket are immediate system calls, not
  awaited operations. Neither blocks. (Closing sockets through io_uring
  was tried and made connections about 4% slower to open and close, for
  no gain.)
- Networking uses the kernel's stack. There is no TLS.

# Buffered reading and writing

For protocols made of lines or of pieces of known length,
[`BufReader`](crate::net::BufReader) and [`BufWriter`](crate::net::BufWriter)
put a buffer in front of a stream. They borrow, own or share the stream
(`&TcpStream`, `TcpStream`, `Rc<TcpStream>`), so one task can read while
another writes.

```rust,no_run
use starfish::net::{BufReader, BufWriter, TcpStream};

async fn shout_lines(stream: TcpStream) -> std::io::Result<()> {
    // Refuse lines over 64 KiB instead of buffering them without end.
    let mut reader = BufReader::new(&stream).limit(64 * 1024);
    let mut writer = BufWriter::new(&stream);
    while let Some(line) = reader.next_line().await? {
        writer.write_all(line.to_uppercase().as_bytes()).await?;
        writer.write_all(b"\n").await?;
        // Nothing is sent until the buffer is full or flushed.
        writer.flush().await?;
    }
    Ok(())
}
```

The reader has `next_line`, `read_line`, `read_until`, `read_exact`,
`read`, and `fill_buf` with `consume` underneath.

The same two types work on files. A file has no position of its own, so a
[`Cursor`](crate::fs::Cursor) supplies one:

```rust,no_run
use starfish::fs::{Cursor, File};
use starfish::io::{BufReader, BufWriter};

async fn copy_lines(from: &File, to: &File) -> std::io::Result<u64> {
    let mut lines = BufReader::new(Cursor::new(from));
    // To append instead, start the cursor at the file's size.
    let mut out = BufWriter::new(Cursor::new(to));
    let mut count = 0;
    while let Some(line) = lines.next_line().await? {
        out.write_all(line.as_bytes()).await?;
        out.write_all(b"\n").await?;
        count += 1;
    }
    out.flush().await?;
    to.sync_data().await?; // flushed is not yet on the device
    Ok(count)
}
```

Both types live in [`io`](crate::io) and are re-exported from `net`.

**Caveats**

- Without [`limit`](crate::net::BufReader::limit), a peer that never
  sends a line ending makes the reader collect data until memory runs
  out. Set a limit on anything that faces a network.
- Dropping a `BufWriter` does **not** flush it. Whatever is still in the
  buffer is lost, silently. Flush before the writer goes, and before
  waiting for a reply.
- A read that is given up, by `timeout`, `select` or an aborted task,
  loses nothing with `next_line`, `read_line`, `read` and `fill_buf`:
  the reader keeps the read it had started and the part of a line it had
  collected, and the next call carries on. So a line can be waited for
  with a time limit, again and again.
- `read_until` appends to a buffer of yours, so it carries on only if
  called again with that same buffer, and `read_exact` cannot be resumed
  at all: after a timeout on it, the position in the stream is unknown.
- Each read from a socket through the reader allocates a small box for
  the read in progress, which is what lets it outlive a caller that has
  stopped waiting. That is noise next to the system call, but it is
  there.
- If a write fails or is dropped part-way, some of the buffer may have
  been sent and the rest is lost. The connection is of no further use.
- `into_inner` discards what the reader has received but not handed out.
  Read on through the same reader instead of switching back to the
  stream.
- A line that is not valid UTF-8 is an error from `read_line` and
  `next_line`, and is consumed. Use `read_until` for bytes.
- On a file, `flush` only hands the data to the kernel. It is on the
  device after the file's own `sync_data`.
- A cursor's position runs ahead of a reader by what is still in the
  reader's buffer, and lags behind a writer by what is not yet flushed.
  Do not mix reads through a `BufReader` with reads through its cursor.
- Two writers on one file, each with its own cursor, overwrite each
  other. Appending from several tasks needs one writer that the others
  send to; the `log_writer` example does this.
- A `Cursor` works on [`File`](crate::fs::File) only. For direct I/O,
  see [Streams on direct I/O](#streams-on-direct-io).

# UDP

[`UdpSocket`](crate::net::UdpSocket) sends and receives datagrams:
`send_to` and `recv_from` with addresses, or `send` and `recv` after
`connect` has fixed the peer. Like the listener, it binds with
`SO_REUSEPORT`, so every shard can have a socket on the same port.

```rust,no_run
use std::net::SocketAddr;
use starfish::net::UdpSocket;

async fn udp_echo() -> std::io::Result<()> {
    let socket = UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 7002)))?;
    let mut buf = Vec::with_capacity(64 * 1024);
    loop {
        let (result, filled) = socket.recv_from(buf).await;
        let (_, from) = result?;
        let (result, sent) = socket.send_to(filled, from).await;
        result?;
        buf = sent;
    }
}
```

**Caveats**

- A datagram longer than the buffer's capacity cannot be received
  whole: the receive fails with `InvalidData`, the buffer comes back
  holding the part that fitted, and the rest is gone. Size the buffer for
  the largest datagram the protocol allows.
- Nothing tells a sender that nobody is listening. Guard every receive
  that expects an answer with a [`timeout`](crate::timeout).
- With a socket per shard, the kernel picks the socket by the sender's
  address, so one peer's datagrams keep arriving on the same shard, and a
  peer that sends a great deal loads one shard.
- `connect` on a UDP socket only sets the peer; nothing is exchanged, and
  datagrams from anyone else are discarded by the kernel from then on.
- As with TCP, the port is shared unless the socket is bound with
  [`BindOptions::new().reuse_port(false)`](crate::net::BindOptions).

# Unix-domain sockets

[`UnixListener`](crate::net::UnixListener) and
[`UnixStream`](crate::net::UnixStream) connect processes on the same
machine through a path in the filesystem. A `UnixStream` reads and writes
like a `TcpStream`, works with the buffered reader and writer, and can
tell who is on the other end.

```rust,no_run
use starfish::net::UnixListener;
use starfish::{shard_count, spawn, submit_to};

async fn serve(path: &str) -> starfish::Result<()> {
    // A file left by an earlier run would make `bind` fail.
    let _ = starfish::fs::remove_file(path).await;
    let listener = UnixListener::bind(path)?;
    // One socket, a handle for every shard.
    for shard in 0..shard_count()? {
        let listener = listener.try_clone()?;
        spawn(submit_to(shard, move || async move {
            while let Ok(stream) = listener.accept().await {
                spawn(async move {
                    if let Ok(caller) = stream.peer_credentials() {
                        println!("connection from user {}", caller.user);
                    }
                    // ... serve `stream` ...
                });
            }
        }));
    }
    Ok(())
}
```

**Caveats**

- A path can have only one listening socket. There is no listener per
  shard as with TCP: every shard accepts on a handle to the same socket
  ([`try_clone`](crate::net::UnixListener::try_clone)), or one shard
  accepts and hands connections out.
- The kernel gives each connection to one of the shards waiting in
  `accept`, with no regard for how busy they are. A shard that is not
  waiting gets nothing.
- `bind` fails with `AddrInUse` if anything is at the path, including the
  socket file of an earlier run. The file is not removed when the
  listener is dropped, even on a clean exit. Remove it before binding and
  after shutting down.
- Who may connect is decided by the file's permissions and those of the
  directories above it. A socket in a world-writable directory with
  default permissions is open to every user on the machine;
  [`peer_credentials`](crate::net::UnixStream::peer_credentials) says who
  did connect.
- The credentials are those of the process that called `connect`, at
  that moment. A process that received the socket from another, or
  changed user afterwards, still shows the original ones.
- Paths are limited to about 100 bytes by the kernel. Longer ones are
  refused.
- Only stream sockets and filesystem paths: no datagram sockets, no
  abstract (path-less) addresses, and no passing of file descriptors.
- There are no zero-copy sends on Unix sockets.

# Files

Files have no cursor: every read and write names its offset, so
concurrent operations on one file do not disturb each other.
[`File`](crate::fs::File) goes through the kernel's page cache.

```rust,no_run
use starfish::fs::File;

async fn files() -> std::io::Result<()> {
    let file = File::create("/tmp/greeting").await?;
    let (result, _) = file.write_all_at("hello", 0).await;
    result?;
    file.sync_data().await?;
    assert_eq!(file.size().await?, 5);

    let (result, buf) = file.read_at(Vec::with_capacity(64), 0).await;
    assert_eq!(result?, 5);
    assert_eq!(buf, b"hello");

    file.set_len(2).await?; // now "he"
    file.allocate(0, 1 << 20).await?; // reserve 1 MiB; the file grows
    file.close().await
}
```

| Call | Does |
|---|---|
| `open`, `create`, `OpenOptions`, `open_raw` | Open for reading; create or truncate; any combination in words; raw `open(2)` flags |
| `read_at`, `write_at`, `write_all_at` | Transfer data at an offset |
| `sync_all`, `sync_data` | Flush to the device, with or without all metadata |
| `size`, `set_len`, `allocate` | Ask, set and reserve the length |
| `close` | Close and report the outcome |

[`OpenOptions`](crate::fs::OpenOptions) covers every way of opening:

```rust,no_run
use starfish::fs::OpenOptions;

async fn open() -> std::io::Result<()> {
    // Read and write an existing file, without emptying it.
    let table = OpenOptions::new().read(true).write(true).open("/var/data/table").await?;
    // Create a file that must not exist yet.
    let lock = OpenOptions::new().write(true).create_new(true).open("/var/data/LOCK").await?;
    // Add to the end of a log, creating it if need be.
    let log = OpenOptions::new().append(true).create(true).open("/var/log/events").await?;
    // The same options open a file for direct I/O.
    let segment = OpenOptions::new().read(true).write(true).open_direct("/var/data/segment").await?;
    drop((table, lock, log, segment));
    Ok(())
}
```

**Caveats**

- `File::open` opens for reading only, and `File::create` truncates an
  existing file. For anything else (append, read-write without
  truncating, exclusive creation) use
  [`OpenOptions`](crate::fs::OpenOptions).
- A file opened for appending puts every write at its end, whatever
  offset the write names. That is the kernel's rule, and it is what makes
  appending safe from several writers; it also means `write_at` cannot
  be used to change such a file in the middle.
- Data is not safe until a sync returns. A successful write has only
  reached the page cache.
- `read_at` returning fewer bytes than the buffer holds means the end of
  the file was reached; `Ok(0)` means the offset is at or past the end.
- `write_at` may write only part of the buffer; `write_all_at` writes all
  of it. Writes past the end leave a gap that reads back as zeros.
- Reads of data that is not in the page cache, and writes that have to
  wait for the cache to flush, cannot be completed by the kernel without
  blocking, and it hands them to worker threads of its own. Page-cache
  I/O is therefore asynchronous for the shard but not especially cheap.
  For predictable storage performance use [direct I/O](#direct-io).
- `size` asks the kernel every time. It is not a field.
- `set_len` uses an io_uring operation that kernels before 6.9 lack. On
  those it makes the ordinary system call on a thread outside the
  runtime, which costs more but does not hold the shard up.
- Dropping a file closes it through the shard's io_uring without waiting
  and without reporting failure. Filesystems that only find out at close
  that data could not be stored (network filesystems above all) can only
  tell you through [`close`](crate::fs::File::close).
- `allocate` fails on filesystems that cannot reserve space.

# Direct I/O

[`DmaFile`](crate::fs::DmaFile) opens with `O_DIRECT` and bypasses the page
cache, for programs that keep their own cache. Reads and writes go
straight to the device and never wait behind the kernel's caching.
Buffers, offsets and lengths must be aligned as the device demands; the
file reports the requirements and [`alloc_buf`](crate::fs::DmaFile::alloc_buf)
returns a buffer that meets them.

```rust,no_run
use starfish::fs::DmaFile;

async fn direct() -> std::io::Result<()> {
    let file = DmaFile::create("/var/data/segment").await?;
    // Space reserved in advance keeps writes from waiting on the
    // filesystem.
    file.allocate(0, 64 << 20).await?;

    let mut block = file.alloc_buf(4096)?;
    block.fill_padded(b"record"); // zero-padded to whole blocks
    let (result, block) = file.write_at(block, 0).await;
    result?;
    file.sync_data().await?;

    let (result, block) = file.read_at(block, 0).await;
    result?;
    assert!(block.starts_with(b"record"));
    Ok(())
}
```

`DmaFile` has the same management calls as `File`: `sync_all`,
`sync_data`, `size`, `set_len`, `allocate` and `close`.

**Caveats**

- The filesystem must support `O_DIRECT`. Where it does not, opening
  fails with `EINVAL`. `tmpfs` gained support only in recent kernels, so
  `/tmp` is often the wrong place to try this.
- Every write is a whole number of blocks, so a file written this way has
  a length that is a multiple of the block size. To give it its true
  length, write the last block padded and then call
  [`set_len`](crate::fs::DmaFile::set_len).
- A buffer whose memory, offset or length breaks the alignment rules is
  refused with `InvalidInput` before the kernel sees it. An ordinary
  `Vec<u8>` is almost never aligned; use `alloc_buf`, an `AlignedBuf` or a
  `FixedBuf`.
- A read takes as many whole blocks as fit in the buffer's capacity; a
  capacity below one block is refused with `InvalidInput`.
- Alignment is asked of the kernel when the file is opened, which needs
  Linux 6.1. Before that, 4096 bytes is assumed for both memory and
  offsets, which is right for most devices but not all.
- Direct I/O skips the page cache, not the device's own write cache.
  Data is safe only after `sync_data` or `sync_all`.
- Writing to space that has not been allocated makes the filesystem
  allocate on the spot, which can block inside the kernel. Preallocate
  with `allocate`.
- Nothing is cached for you. Reading the same block twice goes to the
  device twice.
- Mixing direct and page-cache access to the same file works but is slow
  and easy to get wrong. Pick one per file.

# Streams on direct I/O

Direct I/O transfers whole aligned blocks, but data rarely comes in
blocks. [`DmaStreamWriter`](crate::fs::DmaStreamWriter) takes pieces of
any size and writes a file from front to back;
[`DmaStreamReader`](crate::fs::DmaStreamReader) reads one the same way.
Both keep one aligned buffer and do the blocking-up themselves. This is
what a write-ahead log or a file written once and read many times uses.

```rust,no_run
use starfish::fs::{DmaFile, DmaStreamReader, DmaStreamWriter};

async fn log() -> std::io::Result<()> {
    let file = DmaFile::create("/var/data/log").await?;
    file.allocate(0, 64 << 20).await?;

    let mut out = DmaStreamWriter::new(&file, 128 * 1024)?;
    for n in 0..1000u32 {
        out.write_all(&n.to_le_bytes()).await?;
    }
    out.sync().await?; // durable; the writer goes on
    out.write_all(b"more").await?;
    out.finish().await?; // durable, and the file has its true length

    // Two further buffers are fetched while the current one is used.
    let mut input = DmaStreamReader::new(&file, 128 * 1024)?.read_ahead(2)?;
    let mut number = [0u8; 4];
    input.read_exact(&mut number).await?;
    assert_eq!(u32::from_le_bytes(number), 0);
    Ok(())
}
```

The reader implements [`io::Read`](crate::io::Read), so a
[`BufReader`](crate::io::BufReader) on top of it reads lines from a file
opened for direct I/O. [`DmaStreamWriter::append`](crate::fs::DmaStreamWriter::append)
continues a finished file. The `wal` example builds a log with checksummed
records on these.

**Caveats**

- Only [`finish`](crate::fs::DmaStreamWriter::finish) gives the file its
  true length. Until then the file is as long as its last whole block, or
  as whatever was preallocated, with zeros after the data. After a crash,
  a reader cannot tell from the file where the data ends: put a length
  and a checksum in every record, as the `wal` example does.
- `sync` writes the last, partial block padded with zeros and writes it
  again in full later. Syncing after every small record therefore
  rewrites the same block many times; group records into batches.
- Dropping the writer writes nothing. What is in its buffer is lost, and
  the file keeps a block-sized length.
- If a write or read is dropped part-way (a `timeout`, an aborted task),
  the stream's buffer is gone with it and every later call fails. Start
  again with a new reader or writer.
- One writer per file, from front to back. There is no seeking, and two
  writers on one file overwrite each other.
- `append` expects a file whose length is its true length, as `finish`
  leaves it. On a file that still has padding or preallocated space, it
  appends after that, not after the data.
- By default the reader fetches a buffer only when the previous one is
  used up, so a scan waits for the device once per buffer.
  [`read_ahead`](crate::fs::DmaStreamReader::read_ahead) keeps further
  buffers on their way meanwhile. In the `read_ahead` example a scan of
  512 MiB went from about 2,400 to about 4,300 MiB/s, and one buffer
  ahead gave most of that.
- Reading ahead holds one more buffer per read on its way, and reads
  data that a reader which stops early never uses. The reads go through
  the I/O scheduler under the reader's scheduling group, like any others,
  so a scan in a low-priority group stays low-priority.
- Data passes through the stream's buffer, which is one copy more than
  reading or writing blocks directly.

# Directories and file management

Free functions in [`fs`](crate::fs) cover the rest:
[`create_dir`](crate::fs::create_dir),
[`create_dir_all`](crate::fs::create_dir_all),
[`remove_dir`](crate::fs::remove_dir),
[`remove_dir_all`](crate::fs::remove_dir_all),
[`remove_file`](crate::fs::remove_file), [`rename`](crate::fs::rename),
[`read_dir`](crate::fs::read_dir), [`metadata`](crate::fs::metadata) and
[`sync_dir`](crate::fs::sync_dir), which makes a change to a directory
durable.

```rust,no_run
use starfish::fs::{create_dir_all, read_dir, rename, sync_dir, File};

/// Replaces a file's contents so that a crash leaves either the old
/// version or the new one, never a mixture.
async fn replace(dir: &str, name: &str, contents: Vec<u8>) -> std::io::Result<()> {
    create_dir_all(dir).await?;
    let temporary = format!("{dir}/{name}.tmp");
    let file = File::create(&temporary).await?;
    let (result, _) = file.write_all_at(contents, 0).await;
    result?;
    file.sync_data().await?;
    file.close().await?;
    rename(&temporary, format!("{dir}/{name}")).await?;
    // The rename itself is durable once the directory is synced.
    sync_dir(dir).await?;

    for entry in read_dir(dir).await? {
        println!("{}", entry.path().display());
    }
    Ok(())
}
```

**Caveats**

- `read_dir` is the one call that is not done through io_uring, because
  the kernel offers no way. It runs as [blocking work](#blocking-work) on
  a thread outside the runtime and returns the whole listing at once.
  That is fine for finding one's files at start-up, and wrong for a hot
  path.
- For a directory too large to hold as one list, or of which only the
  beginning is wanted, [`ReadDir`](crate::fs::ReadDir) reads a batch of
  entries at a time (256 unless told otherwise) and hands them out one by
  one. A `ReadDir::next` that is dropped while it is reading a batch
  takes the listing's position with it, and later calls fail: do not put
  it under `timeout` or in `select`.
- The order of a listing is whatever the filesystem returns. Sort it if
  the order matters.
- `rename` replaces the target if it exists, and both names must be on
  the same filesystem. It is atomic for readers, but it only survives a
  crash once the directory itself has been synced with
  [`sync_dir`](crate::fs::sync_dir). The same goes for creating and
  removing files. The `atomic_update` example shows the whole sequence.
- `create_dir_all` and `read_dir` are sequences of steps, not atomic:
  another process changing the same directories at the same time can
  make them fail or return a listing that was never true at one instant.
- `remove_dir` removes only an empty directory.
  [`remove_dir_all`](crate::fs::remove_dir_all) removes a whole tree, on
  a thread outside the runtime and not as one step: stopped part-way, it
  leaves part of the tree.
- New directories get mode `0777` less the process's umask.
- Relative paths are relative to the process's current directory, which
  all shards share.
- [`metadata`](crate::fs::metadata) gives a path's length, kind,
  permission bits and modification time, following symbolic links. There
  are no calls for creating links or changing permissions.

# Registered buffers and zero-copy sends

With [`Runtime::fixed_buffers`](crate::Runtime::fixed_buffers), every shard
gets a pool of buffers that are registered with its io_uring. The kernel
maps and pins them once instead of on every request. A
[`FixedBuf`](crate::buf::FixedBuf) from the pool works with every read and
write call, on sockets and on files, including direct I/O.

[`TcpStream::write_zc`](crate::net::TcpStream::write_zc) transmits from such
a buffer without copying it, as much of it as the kernel takes at once;
[`write_all_zc`](crate::net::TcpStream::write_all_zc) transmits all of it.

```rust,no_run
use starfish::buf::FixedBuf;
use starfish::net::TcpStream;

// In a runtime started with `Runtime::new().fixed_buffers(256, 64 * 1024)`.
async fn send_block(stream: &TcpStream, block: &[u8]) -> starfish::Result<usize> {
    let mut buf = FixedBuf::acquire().await?;
    let length = buf.fill_from(block);
    stream.write_all_zc(buf).await?;
    Ok(length)
}
```

**Caveats**

- `write_zc` returns when the data is queued, but the buffer goes back to
  the pool only when the peer has acknowledged the data. A peer that
  reads slowly, or has silently gone away, holds buffers for as long as
  TCP takes to notice. With a small pool, one bad connection can leave
  every sender on the shard waiting in `acquire`. Size the pool for the
  number of connections, and put a `timeout` on `acquire`.
- `write_zc` may accept only part of the buffer. It returns how much, and
  the buffer is gone either way. To send all of a buffer use
  [`write_all_zc`](crate::net::TcpStream::write_all_zc), which keeps going
  until everything is queued.
- Zero-copy pays off from roughly 10 KiB per send. For small messages the
  bookkeeping costs more than the copy it saves; use `write`.
- Registered memory is pinned and counts against the process's
  locked-memory limit (`ulimit -l`) on kernels that enforce one. Too
  large a pool makes `run` fail at start.
- The pool is fixed when the runtime starts: at most 65,535 buffers per
  shard, all of one size, rounded up to whole pages.
- A `FixedBuf` cannot leave its shard.
- A buffer taken from the pool holds whatever its last user left in it.
  Set the length and fill it before sending; do not send bytes you did
  not write.
- [`acquire`](crate::buf::FixedBuf::acquire) fails with
  [`Error::NoFixedBuffers`](crate::Error::NoFixedBuffers) if the runtime
  was started without a pool.

# Receiving with buffers the kernel picks

A `read` is a request to the kernel each time: here is a buffer, fill it.
A [`RecvStream`](crate::net::RecvStream) asks once. With
[`Runtime::recv_buffers`](crate::Runtime::recv_buffers), every shard offers
the kernel a pool of buffers; a receive stream leaves one standing request
on a connection, and from then on the kernel takes a buffer from the pool
whenever data arrives, fills it, and reports it.
[`next`](crate::net::RecvStream::next) hands the buffers over one at a time
as [`RecvBuf`](crate::buf::RecvBuf)s.

```rust,no_run
use starfish::net::TcpStream;

// In a runtime started with `Runtime::new().recv_buffers(1024, 4096)`.
async fn echo(stream: TcpStream) -> std::io::Result<()> {
    let mut incoming = stream.recv_stream()?;
    while let Some(data) = incoming.next().await? {
        // The buffer received is sent back as it is; nothing is copied.
        let (result, _) = stream.write_all(data).await;
        result?;
    }
    Ok(())
}
```

Two things are gained. A request per receive is saved: on the echo
benchmark that is about a tenth of the whole cost of a request (see the
README). And memory follows the data: ten thousand idle connections each
waiting in `read` hold ten thousand buffers, while ten thousand idle
receive streams hold none.

A `RecvBuf` reads as a byte slice, can be changed in place, and can be
passed to any write call. Dropping it gives the buffer back to the pool.
`next` is safe to drop: data that arrives meanwhile waits in the stream, so
it can be used in `select` and under `timeout`. Unix-domain streams have
the same call.

**Caveats**

- The pool is shared by every receive stream of the shard, and data
  arrives whether or not `next` is being called. When every buffer is
  held, by tasks keeping `RecvBuf`s or by streams nobody is reading from,
  all the shard's receive streams wait. Drop each `RecvBuf` as soon as its
  bytes have been used or copied, and do not leave a receive stream open
  on a connection that is no longer being read.
- One receive fills at most one buffer, so data arrives in pieces no
  larger than the pool's buffer size, cut without regard to the protocol.
  There is no buffered reader over a receive stream; a protocol that
  needs lines or frames has to assemble them itself.
- Dropping a receive stream discards data it had received and not handed
  over. Those bytes are gone from the connection. Drop it when done with
  the connection, not to go back to `read` in the middle of one.
- Use one way of receiving per connection at a time. A `read` or a second
  receive stream alongside the first would each get some of the data.
- A receive stream belongs to the shard it was made on and borrows the
  socket, so the connection cannot be moved to another shard while one
  exists.
- The pool is fixed when the runtime starts: at most 32,768 buffers per
  shard, all of one size. Without `recv_buffers`, `recv_stream` fails
  with `Unsupported`.

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

In the `scheduling_groups` example, groups with 800 and 200 shares that
both want the whole CPU get 799 ms and 200 ms of a second.

**Caveats**

- A group stays until [`remove`](crate::SchedulingGroup::remove) is
  called on it, and every shard keeps a little state for each group that
  exists. A group per tenant or per job is fine if it is removed when the
  tenant or job goes; one per request or per connection is not what
  groups are for, since creating and removing take a lock shared by all
  shards.
- Removing a group does not stop its tasks. They carry on in the default
  group, with its shares, and so does whatever they spawn. Handles to a
  removed group fail with `InvalidGroup`, including the one a task still
  running gets from `SchedulingGroup::current`.
- Shares only matter under contention. A group alone on a shard gets all
  of it, whatever its shares.
- CPU shares only work if tasks yield. A task that never awaits keeps the
  CPU whatever its group, and its group is charged for it afterwards.
- CPU time is divided per shard. A group busy on one shard and idle on
  another is not compensated across them.
- I/O shares apply to file reads, writes and syncs waiting for a device.
  They have no effect on the network.
- Shares are clamped to 1 to 10,000. There is no way to stop a group
  altogether; one share is as low as it goes.
- A group gives priority, not a limit. To cap what a class of work may
  use when nothing else wants the capacity, use a
  [`Semaphore`](crate::sync::Semaphore).
- Two groups may have the same name; the name is only a label for
  statistics.

# The disk I/O scheduler and device capacity

Reads, writes and syncs on [`File`](crate::fs::File) and
[`DmaFile`](crate::fs::DmaFile) pass through a scheduler that each shard
keeps per device. By default a shard limits each device only by
[`io_depth`](crate::Runtime::io_depth), the number of its requests in the
kernel at once. If more is sent than the device can do, the surplus queues
up inside the kernel and the device, where group shares have no say.

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

`examples/iotune.rs` is the first half as a program.

**Caveats**

- Without `io_properties`, the limit is `io_depth` *per shard*. With 24
  shards and the default of 64, a device may be sent 1536 requests at
  once, far more than most can serve promptly.
- A device is identified by the filesystem a path is on. Two filesystems
  on one physical disk are treated as two devices and each gets the full
  capacity; a filesystem spanning several disks is treated as one.
- The path given to `io_properties` must exist when the runtime starts,
  or `run` fails. Files on other filesystems are not affected by it.
- The figures describe the device as it was when measured: idle, at that
  fill level, through that filesystem. A device that is nearly full, or
  shared with other processes, sustains less, and the scheduler will
  then send it too much.
- `measure` needs a filesystem with `O_DIRECT`, a gigabyte of free space
  by default, and an otherwise idle machine. It loads the device as hard
  as it can; do not run it next to a live service.
- Short measurements of small files are unreliable: a device's rates
  depend on what it was doing a moment before. Keep the default phase
  length and file size, or raise them, and measure more than once.
- Only data requests are scheduled. Opening, closing, renaming and
  the other management calls go straight to the kernel.
- A request that has been handed to the kernel can no longer be
  overtaken. A higher-priority request waits behind what is already
  inside the device, about as long as
  [`io_latency_goal`](crate::Runtime::io_latency_goal) with configured
  capacity, and as long as `io_depth` requests take without.
- Dropping the future of a request that is still waiting in the scheduler
  withdraws it cleanly. Once it is in the kernel, see
  [Cancellation](#cancellation).

# Memory

[`ShardAlloc`](crate::alloc::ShardAlloc) is an optional global allocator
that gives every shard its own heap, with no locks on the fast path. Memory
freed on a different shard than it was allocated on finds its way back to
its owner.

```rust,no_run
#[global_allocator]
static GLOBAL: starfish::alloc::ShardAlloc = starfish::alloc::ShardAlloc;
```

With it installed, [`Runtime::memory_limit`](crate::Runtime::memory_limit)
sets a size per shard above which the runtime asks the application to give
memory back, by calling the functions registered with
[`alloc::on_pressure`](crate::alloc::on_pressure) on that shard. A
function can be taken out again with the hook that `on_pressure` returns.

```rust,no_run
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

fn evict_under_pressure(cache: Rc<RefCell<HashMap<u64, Vec<u8>>>>) -> starfish::Result<()> {
    let hook = starfish::alloc::on_pressure(move || cache.borrow_mut().clear())?;
    // Kept only if the function should be taken out again later, with
    // `hook.remove()`. Dropping it leaves the function registered.
    drop(hook);
    Ok(())
}
```

**Caveats**

- The limit is a request, not a cap. Allocations beyond it still succeed,
  and a shard whose application frees nothing just stays over it.
- The limit is looked at between tasks. A single task that allocates a
  great deal without awaiting goes over it by all of that.
- A pressure function runs on the shard, between tasks, and holds the
  shard up while it runs. Keep it quick.
- If a call leaves the heap over the limit, the next call comes only
  after some turns of the event loop, so a function with nothing to give
  is not called continuously.
- A pressure function belongs to the shard it was registered on, and
  stays registered until the runtime stops unless it is taken out with
  the [`PressureHook`](crate::alloc::PressureHook) that `on_pressure`
  returns. Dropping the hook does not remove the function.
- The limit and [`alloc::stats`](crate::alloc::stats) do nothing unless
  `ShardAlloc` is the program's global allocator.
- Only shard threads use the per-shard heaps. Every other thread, and a
  shard thread before and after `run`, uses the system allocator.
- Memory freed on another shard is not reusable at once: it is handed
  back when its owner next goes round its event loop.
- Each heap has about 32 GiB of address space. Past that, a shard's
  allocations fall back to the system allocator.
- The allocator reserves a large range of address space (not memory) at
  first use. Tools that limit address space (`ulimit -v`, some
  sanitizers) make it settle for less, and with none available it uses
  the system allocator throughout.
- A heap is kept when its runtime stops and reused by a later runtime;
  its memory is not all returned to the system in between.

# Signals

[`signal::termination`](crate::signal::termination) waits for the process
to be asked to stop, by Ctrl-C (`SIGINT`) or `SIGTERM`.
[`Signals`](crate::signal::Signals) listens for any set of signals and
keeps reporting them. A signal is taken over only while something listens
for it; afterwards it does again what it did before.

```rust,no_run
use starfish::signal::Signals;

async fn control() -> std::io::Result<()> {
    let mut signals = Signals::new(&[libc::SIGHUP, libc::SIGTERM])?;
    loop {
        match signals.next().await {
            libc::SIGHUP => println!("reloading configuration"),
            _ => return Ok(()),
        }
    }
}
```

**Caveats**

- Listening for a signal replaces what the process would otherwise do on
  receiving it, for as long as anything listens. While a `Signals` for
  `SIGINT` exists, Ctrl-C does not end the program by itself: a program
  that listens must act on what it hears. When the last listener for a
  signal is dropped, the signal goes back to what it did before.
- [`termination`](crate::signal::termination) stops listening when it
  returns. So during the shutdown that follows, a second Ctrl-C or
  `SIGTERM` does what it would have done had the program never listened,
  which normally is to end it at once, without any code for it.
- "What it did before" is not always "end the program". A shell that
  starts a program in the background, or `nohup`, starts it with some
  signals ignored, `SIGINT` among them. While the program listens it
  receives them all the same; once it stops, they are ignored again.
- Signal handlers are process-wide. If another library installs its own
  handler for the same signal, whichever was installed last wins and the
  other stops hearing it.
- Deliveries of one signal that arrive faster than they are collected are
  merged into one, as the kernel itself does. Signals are not a counter.
- A listener hears only signals that arrive after it was created.
- The first listener starts one helper thread (`starfish-signals`) that
  lives for the rest of the process.
- Signals that report a fault in the program (`SIGSEGV`, `SIGBUS`,
  `SIGFPE`, `SIGILL`) are refused, as are `SIGKILL` and `SIGSTOP`, which
  no program can catch.
- The stall detector's backtraces use the real-time signal
  `SIGRTMIN + 5`; do not listen for that one.
- Signal numbers come from the `libc` crate, as in the example.

# Shutting down

When the future given to `run` completes, all remaining tasks are dropped
where they stand. For an orderly stop, three pieces work together:

1. [`signal::termination`](crate::signal::termination) tells the main task
   that the process was asked to stop.
2. [`JoinHandle::abort`](crate::JoinHandle::abort) stops the accept loops,
   which closes the listeners.
3. A [`Gate`](crate::sync::Gate) that every request entered waits for the
   requests in flight, under a [`timeout`](crate::timeout).

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

**Caveats**

- None of this happens by itself. A program that simply returns from its
  main future cuts every connection off in mid-request.
- The steps have to run on every shard; each shard has its own listener,
  its own gate and its own tasks.
- A connection that is idle, waiting for a request that never comes,
  keeps its guard and holds the gate open until the timeout. Either time
  out idle reads, or abort the connection tasks on shutdown.
- After [`termination`](crate::signal::termination) has returned, a
  second Ctrl-C normally ends the program at once, as for any program:
  nothing listens any more. (Not if the program was started with the
  signal ignored; see [Signals](#signals).) To decide yourself what a second signal does, keep a
  [`Signals`](crate::signal::Signals) and [`select`](crate::select())
  between the shutdown and the next signal, as the `kvdb` example does.
- At the very end the runtime cancels every operation still in the
  kernel and waits until the kernel has let go of all buffers, so that
  none is freed while in use. That is normally immediate.
- Data written to a socket just before returning may not have been sent
  yet. Closing a connection properly means `shutdown` and reading until
  the peer closes too.

# Statistics

[`stats`](crate::stats) describes the current shard: its event loop
([`shard`](crate::stats::shard)), its scheduling groups
([`groups`](crate::stats::groups)) and its device queues
([`devices`](crate::stats::devices)).
[`alloc::stats`](crate::alloc::stats) describes its heap, and
[`stats::all_shards`](crate::stats::all_shards) gathers the event-loop
counters of every shard in one call.

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

**Caveats**

- Every figure is for the shard the call is made on.
  [`stats::all_shards`](crate::stats::all_shards) collects the event-loop
  counters of every shard; for groups and devices, collect them with
  [`submit_to_all`](crate::submit_to_all).
- Counters run from the start of the runtime and are never reset. For a
  rate, take two readings and subtract.
- `busy` is time spent running tasks, `idle` is time asleep and
  `spinning` is time spent looking for work before sleeping. Time the
  event loop spends on its own work is in none of them, so they do not
  add up to the wall clock.
- A group appears in `groups` only on shards where it has run, and a
  device in `devices` only on shards that have used it.
- There is no built-in exporter. Formatting and publishing the numbers
  is the application's job.
- `messages_sent` and `messages_received` count messages between
  shards, not calls. A call is one message out, but its reply is a
  message back only if the caller was already waiting when it came; on a
  busy machine the other shard can finish first, and the caller simply
  finds the result. Do not use these to count calls. (A test in this
  repository did, and failed on every loaded machine.)
- The statistics structures may gain fields in later versions. They are
  marked `#[non_exhaustive]`: read their fields, but do not build one or
  match on all its fields without `..`.

# Stall detection

A *stall* is a time slice that ran past
[`stall_threshold`](crate::Runtime::stall_threshold): some task did not
yield and held up its shard. Stalls are always counted. To find the
culprit, turn on [`stall_backtraces`](crate::Runtime::stall_backtraces) and
print the report. The handler is a closure, so it can also count stalls or
pass them to a logging thread:

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

**Caveats**

- A stall is noticed after it is over, from how long the slice took. A
  shard that never comes back is never reported.
- Backtraces are raw: one line per frame with the module, an offset and
  an address, and a function name only where the dynamic symbol table
  has one. For names, link with `-rdynamic` or translate the addresses
  with `addr2line`.
- With backtraces on, every shard thread is interrupted by a signal
  (`SIGRTMIN + 5`) once per threshold, all the time. That is cheap but
  not free, and it installs a process-wide handler.
- A stall is caught in the act only once it has lasted between one and
  two thresholds. Shorter stalls are counted but come without a stack.
- The stack is of the moment the timer fired, somewhere inside the
  stall. It shows what was running then, which is usually but not
  always the whole story.
- The handler given to `on_stall` is shared by all shards and called on
  whichever stalled, so what it captures must be `Send` and `Sync`: an
  atomic counter or a channel to a logging thread, not an `Rc`. It runs
  on the shard's thread, between tasks: keep it short, and do not block
  in it.
- Time the thread spent descheduled by the operating system counts too.
  On an overcommitted machine, stalls are reported that no task caused.

# Errors and panics

Runtime calls return [`Error`](crate::Error); I/O calls return
`std::io::Error`. Each converts into the other, so `?` works both ways.

The library itself does not panic. A panic in a task is caught: the task
ends, whoever awaits its handle gets
[`Error::Panicked`](crate::Error::Panicked) with the panic's message, and
the shard carries on. The same holds for a closure run on another shard,
on the caller's own shard, or on a blocking thread.

**Caveats**

- This relies on unwinding. Under `panic = "abort"` a panic anywhere ends
  the process.
- The panic's message is kept in the error
  (`Error::Panicked { message }`) when the panic carried text, as
  `panic!("...")` does. It also still goes to the panic hook, by default
  to standard error. A panic with another kind of payload has no
  message.
- A panic leaves the task's shared state as it was at that instant. Data
  in an `Rc<RefCell<..>>` that the task was half-way through changing
  stays half changed, for the other tasks to find.
- A panic in the future given to `run` ends the runtime, and `run`
  returns `Error::Panicked`.
- A panic in a `Drop` during another panic's unwinding aborts the
  process, as everywhere in Rust.
- `Error` is `#[non_exhaustive]`: a `match` on it needs a wildcard arm.
- Every public type implements `Debug`, for logging and for
  `assert!` messages. Types that wrap a resource print a summary, such as
  `Semaphore { available: 1, closed: false, .. }` or a socket's file
  descriptor, never the data passing through them.

# Cancellation

Dropping a future cancels what it was waiting for. That is how
[`timeout`](crate::timeout) and [`abort`](crate::JoinHandle::abort) work,
and it applies to everything in the library.

For waiting on the library's own things (a permit, a channel, a gate, a
timer, a place among cross-shard calls), cancelling is exact: nothing is
lost and nobody else is disturbed.

For I/O it is not exact, because the kernel is involved. The runtime asks
the kernel to cancel the operation and keeps the buffer until the kernel
has let go of it.

**Caveats**

- A cancelled operation may have happened anyway. The kernel may have
  completed a write, or part of it, before the cancellation arrived. A
  cancelled read may have taken bytes out of the socket that are now
  lost with the buffer. (Reading through a
  [`BufReader`](crate::io::BufReader) avoids that: it keeps the read
  alive when its caller gives up.)
- So after a timeout on a socket read or write, the state of the stream
  is unknown and the connection should be closed. After a timeout on a
  file write, the range may hold the old data, the new data or a
  mixture.
- The buffer of a cancelled operation does not come back.
- Cancelling a cross-shard call stops the wait, not the call.
- Operations that create a file descriptor (`accept`, opening a file)
  are safe to cancel: a descriptor that arrives after the cancellation
  is closed.

# Testing code that uses the runtime

A test starts its own runtime with [`Runtime::run`](crate::Runtime::run)
and makes its assertions inside or on what `run` returns. This
repository's `tests/basic.rs` has some fifty tests written that way.

```rust,no_run
#[test]
fn doubles_on_another_shard() {
    let doubled = starfish::Runtime::new()
        .shards(2)
        .pin_threads(false)
        .run(|| async { starfish::submit_to(1, || async { 21 * 2 }).await })
        .unwrap()
        .unwrap();
    assert_eq!(doubled, 42);
}
```

**Caveats**

- Turn pinning off in tests (`pin_threads(false)`). Tests run in parallel
  in one process, and pinned shards from many tests pile onto the same
  few CPUs.
- A test that passes on an idle many-core machine can fail on a loaded
  or small one, because the order in which shards get to run changes.
  Anything that assumes "the other shard has not got there yet" or "has
  got there by now" is such a test. Run the suite with one CPU, `taskset
  -c 0 cargo test`, before trusting it: that is how the fault above in
  this repository's own tests was reproduced, after CI had been failing
  on it.
- Assertions on elapsed time need generous margins, and assertions on
  shares of CPU or disk need a machine that is not doing much else.
- Each test's runtime starts and stops its own threads. That is quick,
  but a test that leaves a task holding a file or a port open keeps it
  until its runtime has stopped, not until the test function's variables
  go out of scope.
- Wait for work to finish by awaiting it (a `JoinHandle`, a channel, a
  gate), not by sleeping for a time that seems long enough.

# A complete example

`examples/kvdb.rs` puts most of this together in about 650 lines: a
key-value server that survives being killed. It is the place to see how
the pieces fit.

- Data is split across shards by a hash of the key
  ([`Sharded`](crate::Sharded)); a request that arrives on another shard
  goes to the owner with `invoke_on`.
- Each shard has a write-ahead log on direct I/O
  ([`DmaStreamWriter`](crate::fs::DmaStreamWriter)) written by a single
  task. Requests hand it their change over a
  [`channel`](crate::sync::channel) and wait for a
  [`oneshot`](crate::sync::oneshot) answer; it writes whatever has
  queued up, syncs once and answers them all, so many clients share one
  sync.
- On start, each shard replays its log
  ([`DmaStreamReader`](crate::fs::DmaStreamReader) with read-ahead) up to
  the first record whose checksum fails.
- A log that has grown large is compacted: a background task in its own
  [`SchedulingGroup`](crate::SchedulingGroup) writes the live data to a
  new file, which is then renamed over the old one
  ([`rename`](crate::fs::rename), then a sync of the directory).
- Connections are limited by a [`Semaphore`](crate::sync::Semaphore) and
  framed with [`BufReader`](crate::io::BufReader) and
  [`BufWriter`](crate::io::BufWriter).
- On Ctrl-C it stops accepting, waits for connections through a
  [`Gate`](crate::sync::Gate), and only then closes the logs, because a
  connection on one shard may be writing to another shard's log.

**What building it showed**

- The data's layout depends on the number of shards, since that decides
  which shard owns a key. The server records the number and refuses to
  start with another. Any design that splits data by shard needs this.
- A map in a `RefCell` cannot stay borrowed across an await, so the
  compaction copies the live data before writing it out. The copy holds
  the shard up for as long as it takes; a large database would want a
  map that can be walked in steps.
- A change must be applied to the map by the task that writes the log,
  after the sync and before the answer. Applying it in the request's own
  task, after being answered, lets a compaction in between lose it.
- Compaction is split in two so that writes do not wait for it. A task
  in a low-priority [scheduling group](#scheduling-groups) writes a
  snapshot of the data to a new file, while the log's writer carries on
  and remembers what it writes; the writer then adds that to the new
  file and switches. The finished file is handed back by sending the
  writer a message on its own channel, which keeps the writer a plain
  loop over one queue.
- The slowest writes take tens of milliseconds, and it is the device:
  now and then a sync takes 10 to 30 ms, on every shard at the same
  moment. Moving compaction to the background did not change that. A
  guess about where time goes is worth less than a measurement.
- Waiting for another task's single answer came up at once, and the
  first version used a [`channel`](crate::sync::channel) of capacity one
  per request. That is what [`oneshot`](crate::sync::oneshot) is for.

# What is not there

- Networking uses the kernel's stack. There is no userspace TCP.
- No TLS, no HTTP, no DNS resolution: addresses are `SocketAddr`s.
- No calls for links or for changing permissions.
- No work stealing or load balancing between shards.
- Only Linux on x86-64.
