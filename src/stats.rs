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

/// One shard's running totals, from which [`ShardStats`] is made.
///
/// Plain cells: only the shard's own thread touches them, and they are
/// updated from the event loop, where an atomic operation per update would
/// cost something for nothing.
#[derive(Default)]
pub(crate) struct Counters {
    pub(crate) loops: Cell<u64>,
    pub(crate) sleeps: Cell<u64>,
    pub(crate) tasks_polled: Cell<u64>,
    pub(crate) busy_nanos: Cell<u64>,
    pub(crate) idle_nanos: Cell<u64>,
    pub(crate) spin_nanos: Cell<u64>,
    pub(crate) messages_sent: Cell<u64>,
    pub(crate) messages_received: Cell<u64>,
    pub(crate) stalls: Cell<u64>,
    pub(crate) longest_stall_nanos: Cell<u64>,
}

/// Adds to a counter, stopping at the largest value instead of wrapping.
/// A counter that has run for long enough to overflow should read as "very
/// large", not as a small number again.
pub(crate) fn add(counter: &Cell<u64>, amount: u64) {
    counter.set(counter.get().saturating_add(amount));
}

/// A duration as whole nanoseconds, or the largest `u64` if it is longer
/// than that can hold (which takes more than five centuries).
pub(crate) fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// What one shard's event loop has done since the runtime started.
///
/// A snapshot, taken by [`shard`] on the shard it describes. The counters only
/// ever grow, so two snapshots subtract to give what happened in between.
///
/// The three durations account for the shard's time: `busy` running tasks,
/// `idle` asleep in the kernel, and `spinning` looking for work without
/// sleeping. What is left of the wall clock is the event loop's own work,
/// which is small on a healthy shard.
///
/// The structure may gain fields in later versions; read its fields, but do
/// not construct it or match on all of them.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
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
    /// Time spent looking for work without sleeping
    /// ([`Runtime::spin_before_sleep`](crate::Runtime::spin_before_sleep)).
    pub spinning: Duration,
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

/// How one scheduling group has fared on one shard.
///
/// One of these per group that has had a task run on the shard, from
/// [`groups`]. Comparing `cpu_time` between groups shows whether the CPU is
/// being divided as their shares say; `runnable` shows whether a group has a
/// backlog.
///
/// The structure may gain fields in later versions.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
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

/// The state of one storage device's request queue on one shard.
///
/// One of these per device the shard has read or written, from [`devices`]. A
/// device is identified by the number of the filesystem it holds. `queued`
/// counts requests held back by the shard's I/O scheduler, where scheduling
/// groups' I/O shares apply; `in_flight` counts those handed to the kernel. A
/// `total_queue_time` that grows quickly means the device is being asked for
/// more than it, or its configured capacity, allows.
///
/// The structure may gain fields in later versions.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
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
///
/// Reads the shard's own counters, with no message to anyone, so it is cheap
/// enough to call often. For every shard at once, see [`all_shards`].
///
/// # Errors
///
/// [`Error::NoRuntime`](crate::Error::NoRuntime) outside a runtime.
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
        spinning: Duration::from_nanos(counters.spin_nanos.get()),
        messages_sent: counters.messages_sent.get(),
        messages_received: counters.messages_received.get(),
        stalls: counters.stalls.get(),
        longest_stall: Duration::from_nanos(counters.longest_stall_nanos.get()),
    })
}

/// The event loop counters of every shard, in shard order.
///
/// Each shard reads its own counters when asked, so the snapshots are taken
/// at slightly different moments, a call between shards apart.
///
/// # Errors
///
/// Those of [`submit_to_all`](crate::submit_to_all).
pub async fn all_shards() -> Result<Vec<ShardStats>> {
    crate::submit_to_all(|| async { shard() }).await?.into_iter().collect()
}

/// Every scheduling group that has run on the current shard, in the order
/// the groups were created.
///
/// A group that exists but has never had a task on this shard is left out.
///
/// # Errors
///
/// [`Error::NoRuntime`](crate::Error::NoRuntime) outside a runtime.
pub fn groups() -> Result<Vec<GroupStats>> {
    let shard = current()?;
    Ok(shard
        .run_queue
        .with(|queue| queue.stats())
        .unwrap_or_default())
}

/// Every storage device the current shard has done file I/O on.
///
/// A device appears once a `File` or `DmaFile` on it has been read, written or
/// synced from this shard. Other shards keep their own queues for the same
/// device and report them separately.
///
/// # Errors
///
/// [`Error::NoRuntime`](crate::Error::NoRuntime) outside a runtime.
pub fn devices() -> Result<Vec<DeviceStats>> {
    let shard = current()?;
    Ok(shard
        .reactor
        .with(|reactor| reactor.device_stats())
        .unwrap_or_default())
}

/// What the runtime tells [`Runtime::on_stall`](crate::Runtime::on_stall)
/// about a task that did not yield.
///
/// A stall is one time slice that lasted longer than
/// [`Runtime::stall_threshold`](crate::Runtime::stall_threshold): for that
/// long, one task kept the shard and every other task, timer and I/O
/// completion on it waited. The report says where and for how long, and, if
/// backtraces are switched on and the stall was caught in the act, what the
/// shard was executing.
///
/// The structure may gain fields in later versions.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct StallReport {
    /// The index of the shard that stalled.
    pub shard: usize,
    /// How long the slice ran.
    pub duration: Duration,
    /// The scheduling group whose task was running.
    pub group: String,
    /// The call stack while the shard was stalled, innermost function
    /// first, one line per function: its name and, where the program was
    /// built with debug information, `at file:line`. A function inlined
    /// into its caller has a line of its own. Where nothing is known about
    /// an address, or the crate was built without its `symbols` feature,
    /// the line is `module(+offset) [address]` instead.
    ///
    /// Empty unless
    /// [`Runtime::stall_backtraces`](crate::Runtime::stall_backtraces) is
    /// on and the stall lasted long enough to be caught in the act.
    pub backtrace: Vec<String>,
}
