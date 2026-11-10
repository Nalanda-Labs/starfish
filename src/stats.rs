//! What a shard has been doing.
//!
//! Every function here reports on the shard it is called on. To see the
//! whole runtime, collect them with [`submit_to_all`](crate::submit_to_all):
//!
//! ```no_run
//! # async fn example() -> starfish::Result<()> {
//! let per_shard = starfish::submit_to_all(|| async { starfish::stats::shard() }).await?;
//! # Ok(()) }
//! ```

use std::cell::Cell;
use std::time::Duration;

use crate::error::Result;
use crate::runtime::current;

/// Counters kept by one shard's event loop.
#[derive(Default)]
pub(crate) struct Counters {
    pub(crate) loops: Cell<u64>,
    pub(crate) sleeps: Cell<u64>,
    pub(crate) tasks_polled: Cell<u64>,
    pub(crate) busy_nanos: Cell<u64>,
    pub(crate) idle_nanos: Cell<u64>,
    pub(crate) messages_sent: Cell<u64>,
    pub(crate) messages_received: Cell<u64>,
    pub(crate) stalls: Cell<u64>,
    pub(crate) longest_stall_nanos: Cell<u64>,
}

pub(crate) fn add(counter: &Cell<u64>, amount: u64) {
    counter.set(counter.get().saturating_add(amount));
}

pub(crate) fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// The event loop of one shard.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardStats {
    /// The shard's index.
    pub shard: usize,
    /// Tasks that exist on the shard, runnable or waiting.
    pub tasks: usize,
    /// How many times a task was run.
    pub tasks_polled: u64,
    /// Turns of the event loop.
    pub loops: u64,
    /// How many times the shard went to sleep for lack of work.
    pub sleeps: u64,
    /// Time spent running tasks.
    pub busy: Duration,
    /// Time spent asleep.
    pub idle: Duration,
    /// Messages sent to other shards.
    pub messages_sent: u64,
    /// Messages received from other shards and from threads outside the
    /// runtime.
    pub messages_received: u64,
    /// How many time slices ran past the stall threshold
    /// ([`Runtime::stall_threshold`](crate::Runtime::stall_threshold)).
    pub stalls: u64,
    /// The longest of those slices.
    pub longest_stall: Duration,
}

/// One scheduling group on one shard.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupStats {
    /// The name the group was created with.
    pub name: String,
    /// The group's CPU weight.
    pub cpu_shares: u32,
    /// The group's disk weight.
    pub io_shares: u32,
    /// CPU time the group's tasks have used on this shard.
    pub cpu_time: Duration,
    /// How many times one of its tasks was run.
    pub tasks_polled: u64,
    /// Tasks of the group that are waiting for the CPU now.
    pub runnable: usize,
}

/// One device's queue on one shard.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeviceStats {
    /// The device number of the filesystem.
    pub device: u64,
    /// Requests waiting in the shard's I/O scheduler now.
    pub queued: usize,
    /// Requests the shard has in the kernel now.
    pub in_flight: usize,
    /// Requests sent to the kernel so far.
    pub dispatched: u64,
    /// The bytes those requests covered.
    pub bytes: u64,
    /// Time requests spent waiting in the scheduler before being sent,
    /// summed over all requests.
    pub total_queue_time: Duration,
    /// The longest any one request waited in the scheduler.
    pub longest_queue_time: Duration,
}

/// The current shard's event loop counters.
pub fn shard() -> Result<ShardStats> {
    let shard = current()?;
    let counters = &shard.stats;
    Ok(ShardStats {
        shard: shard.shared.id,
        tasks: shard.task_count(),
        tasks_polled: counters.tasks_polled.get(),
        loops: counters.loops.get(),
        sleeps: counters.sleeps.get(),
        busy: Duration::from_nanos(counters.busy_nanos.get()),
        idle: Duration::from_nanos(counters.idle_nanos.get()),
        messages_sent: counters.messages_sent.get(),
        messages_received: counters.messages_received.get(),
        stalls: counters.stalls.get(),
        longest_stall: Duration::from_nanos(counters.longest_stall_nanos.get()),
    })
}

/// Every scheduling group that has run on the current shard, in creation
/// order.
pub fn groups() -> Result<Vec<GroupStats>> {
    let shard = current()?;
    Ok(shard
        .run_queue
        .with(|queue| queue.stats())
        .unwrap_or_default())
}

/// Every device the current shard has done file I/O on.
pub fn devices() -> Result<Vec<DeviceStats>> {
    let shard = current()?;
    Ok(shard
        .reactor
        .with(|reactor| reactor.device_stats())
        .unwrap_or_default())
}

/// What the runtime tells [`Runtime::on_stall`](crate::Runtime::on_stall)
/// about a task that did not yield.
#[derive(Clone, Debug)]
pub struct StallReport {
    /// The index of the shard that stalled.
    pub shard: usize,
    /// How long the slice ran.
    pub duration: Duration,
    /// The scheduling group whose task was running.
    pub group: String,
    /// The call stack while the shard was stalled, one line per frame, as
    /// `module(function+offset) [address]`. Empty unless
    /// [`Runtime::stall_backtraces`](crate::Runtime::stall_backtraces) is
    /// on and the stall lasted long enough to be caught in the act.
    pub backtrace: Vec<String>,
}
