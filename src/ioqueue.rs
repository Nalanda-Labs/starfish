//! Disk I/O scheduling: what a device can take, and whose request goes next.
//!
//! Two mechanisms work together. A [`DeviceGroup`] is shared by all shards
//! and meters the total load on one device against its measured capacity.
//! An [`IoQueue`] is private to a shard and decides, among the requests
//! that shard has waiting for the device, which one to send when the device
//! has room, by scheduling-group I/O shares.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use io_uring::squeue;

use crate::sched::{virtual_cost, GroupCache, DEFAULT_GROUP};
use crate::stats::DeviceStats;

/// Nanoseconds in a second, wide enough for the products formed when a
/// rate is turned into a time.
const NANOS_PER_SEC: u128 = 1_000_000_000;
/// Cost of a request, in bytes on top of its size, on a device with no
/// configured capacity. It only orders requests between groups.
const UNMETERED_REQUEST_OVERHEAD: u64 = 16 * 1024;

/// What a storage device can sustain, as measured by
/// [`iotune::measure`](crate::iotune::measure).
///
/// A request is charged the device time it is expected to take: one
/// request's worth of the IOPS figure plus its size against the bandwidth
/// figure, using the read or the write pair as appropriate. The runtime
/// admits requests, across all shards, at a rate of one second of device
/// time per second. A `None` field leaves that dimension unmetered.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IoProperties {
    /// Small random reads per second.
    pub read_iops: Option<u64>,
    /// Sequential read throughput in bytes per second.
    pub read_bytes_per_sec: Option<u64>,
    /// Small random writes per second.
    pub write_iops: Option<u64>,
    /// Sequential write throughput in bytes per second.
    pub write_bytes_per_sec: Option<u64>,
}

/// Whether a request reads or writes. The two are charged at their own
/// rates, since devices are seldom equally fast at both.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Direction {
    Read,
    Write,
}

/// A duration as whole nanoseconds, or the largest `u64` if it does not
/// fit.
fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// One device's capacity, shared by every shard that uses the device.
///
/// It is a token bucket whose tokens are nanoseconds of device time. The
/// bucket fills at one nanosecond per nanosecond and holds at most the
/// latency goal, so the backlog inside the device stays near that long. A
/// request is admitted while the balance is positive and then subtracts
/// its full cost, which may drive the balance negative; later requests wait
/// until the debt is repaid. This admits requests of any size while keeping
/// the average rate exact.
pub(crate) struct DeviceGroup {
    props: IoProperties,
    metered: bool,
    epoch: Instant,
    burst: i64,
    balance: AtomicI64,
    /// Nanoseconds since `epoch` up to which the bucket has been filled.
    filled_to: AtomicU64,
}

impl DeviceGroup {
    /// The shared state for a device with the given capacity.
    ///
    /// `latency_goal` is how much work, measured in the time the device needs
    /// for it, may be inside the device at once: the size of the bucket. A small
    /// goal keeps the device's own queue short, so that a request sent ahead of
    /// others by priority does not then wait behind them inside the device.
    ///
    /// A capacity with no rate given at all describes an unmetered device, which
    /// admits everything.
    pub(crate) fn new(props: IoProperties, latency_goal: Duration) -> DeviceGroup {
        // A rate of zero cannot be honoured; treat it as not given.
        let rate = |rate: Option<u64>| rate.filter(|&rate| rate > 0);
        let props = IoProperties {
            read_iops: rate(props.read_iops),
            read_bytes_per_sec: rate(props.read_bytes_per_sec),
            write_iops: rate(props.write_iops),
            write_bytes_per_sec: rate(props.write_bytes_per_sec),
        };
        let burst = i64::try_from(nanos(latency_goal)).unwrap_or(i64::MAX).max(1);
        DeviceGroup {
            props,
            metered: props != IoProperties::default(),
            epoch: Instant::now(),
            burst,
            balance: AtomicI64::new(burst),
            filled_to: AtomicU64::new(0),
        }
    }

    /// The state for a device whose capacity was not configured: nothing is
    /// metered and every request is admitted at once.
    pub(crate) fn unmetered() -> DeviceGroup {
        DeviceGroup::new(IoProperties::default(), Duration::ZERO)
    }

    /// Nanoseconds of device time a request is expected to take: one
    /// request's worth at the IOPS rate, plus its bytes at the bandwidth rate,
    /// for its direction. A rate that was not given contributes nothing. At
    /// least one, so that no request is free.
    fn device_time(&self, direction: Direction, bytes: u64) -> u64 {
        let (iops, bandwidth) = match direction {
            Direction::Read => (self.props.read_iops, self.props.read_bytes_per_sec),
            Direction::Write => (self.props.write_iops, self.props.write_bytes_per_sec),
        };
        let per_request = iops
            .and_then(|iops| NANOS_PER_SEC.checked_div(u128::from(iops)))
            .unwrap_or(0);
        let per_bytes = bandwidth
            .and_then(|bandwidth| {
                u128::from(bytes)
                    .saturating_mul(NANOS_PER_SEC)
                    .checked_div(u128::from(bandwidth))
            })
            .unwrap_or(0);
        u64::try_from(per_request.saturating_add(per_bytes))
            .unwrap_or(u64::MAX)
            .max(1)
    }

    /// The cost of a request as `(tokens, fair)`: what it takes from the
    /// device's bucket, and what it counts for when sharing the device between
    /// groups.
    ///
    /// On a metered device both are the device time it needs. On an unmetered
    /// one it takes nothing from the bucket, and for sharing it counts by its
    /// size plus a fixed amount per request, so that many small requests are not
    /// free compared with one large one.
    pub(crate) fn cost(&self, direction: Direction, bytes: u64) -> (u64, u64) {
        if self.metered {
            let time = self.device_time(direction, bytes);
            (time, time)
        } else {
            (0, UNMETERED_REQUEST_OVERHEAD.saturating_add(bytes))
        }
    }

    /// Credits the bucket with the time that has passed since it was last
    /// filled, up to its capacity.
    ///
    /// Several shards call this at once. The one that moves the "filled up to"
    /// mark forward is the one that credits the interval it moved it by, so no
    /// stretch of time is ever credited twice.
    fn refill(&self, now: Instant) {
        let now = nanos(now.saturating_duration_since(self.epoch));
        // Whoever advances the watermark owns the interval it advanced by,
        // so concurrent shards never credit the same time twice.
        let before = self.filled_to.fetch_max(now, Ordering::AcqRel);
        let Some(elapsed) = now.checked_sub(before).filter(|&elapsed| elapsed > 0) else {
            return;
        };
        let elapsed = i64::try_from(elapsed).unwrap_or(i64::MAX);
        let mut balance = self.balance.load(Ordering::Acquire);
        loop {
            let filled = balance.saturating_add(elapsed).min(self.burst);
            match self.balance.compare_exchange_weak(
                balance,
                filled,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(current) => balance = current,
            }
        }
    }

    /// Tries to admit a request costing `tokens`. Returns `None` if it may go
    /// now, or how long to wait before asking again.
    ///
    /// A request is admitted whenever the balance is positive, whatever its
    /// cost, and then takes the balance down by its full cost, possibly below
    /// zero. Later requests wait until time has paid the debt off. That admits
    /// requests larger than the bucket while keeping the long-run rate exact.
    fn acquire(&self, tokens: u64, now: Instant) -> Option<Duration> {
        if !self.metered {
            return None;
        }
        self.refill(now);
        let tokens = i64::try_from(tokens).unwrap_or(i64::MAX);
        let mut balance = self.balance.load(Ordering::Acquire);
        loop {
            if balance <= 0 {
                let debt = balance.unsigned_abs().saturating_add(1);
                return Some(Duration::from_nanos(debt));
            }
            match self.balance.compare_exchange_weak(
                balance,
                balance.saturating_sub(tokens),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return None,
                Err(current) => balance = current,
            }
        }
    }
}

/// The devices whose capacity was configured, by device number. Built once
/// when the runtime starts and shared, read-only, by all shards.
pub(crate) struct DeviceTable {
    devices: HashMap<u64, Arc<DeviceGroup>>,
}

impl DeviceTable {
    /// A table of the configured devices.
    pub(crate) fn new(devices: HashMap<u64, Arc<DeviceGroup>>) -> DeviceTable {
        DeviceTable { devices }
    }

    /// The shared state of a device. A device without configured capacity is
    /// unmetered, which needs no sharing between shards, so each shard simply
    /// gets a fresh unmetered state for it.
    fn get(&self, device: u64) -> Arc<DeviceGroup> {
        match self.devices.get(&device) {
            Some(group) => group.clone(),
            None => Arc::new(DeviceGroup::unmetered()),
        }
    }
}

/// A request waiting for its turn at a device: the prepared io_uring
/// entry, where it belongs, what it costs, and when it started waiting.
///
/// `key` is the reactor's slot number for the operation, which is also how a
/// request is found again if its future is dropped while it still waits.
pub(crate) struct QueuedRequest {
    pub(crate) key: usize,
    pub(crate) device: u64,
    pub(crate) sqe: squeue::Entry,
    tokens: u64,
    fair: u64,
    bytes: u64,
    enqueued: Instant,
}

/// One scheduling group's waiting requests for one device, in the order
/// they were made, with the group's virtual time on that device: the device
/// time it has had, divided by its I/O shares.
#[derive(Default)]
struct IoClass {
    queue: VecDeque<QueuedRequest>,
    vtime: u64,
}

/// One shard's requests for one device: a queue per scheduling group, the
/// order in which the groups are to be served, how many requests are in the
/// kernel, and the running totals for the statistics.
struct IoQueue {
    device: Arc<DeviceGroup>,
    /// Indexed by scheduling group.
    classes: Vec<IoClass>,
    /// The classes with waiting requests, ordered by virtual time. An
    /// entry's first field always equals that class's `vtime`.
    active: BTreeSet<(u64, usize)>,
    /// Virtual time of the class dispatched last; a class that becomes busy
    /// after idling starts from here.
    vclock: u64,
    in_flight: usize,
    /// When the device is expected to have room again, if requests are
    /// waiting on its capacity.
    retry_at: Option<Instant>,
    waiting: usize,
    dispatched: u64,
    bytes: u64,
    queue_nanos: u64,
    longest_queue_nanos: u64,
}

impl IoQueue {
    /// An empty queue for `device`.
    fn new(device: Arc<DeviceGroup>) -> IoQueue {
        IoQueue {
            device,
            classes: Vec::new(),
            active: BTreeSet::new(),
            vclock: 0,
            in_flight: 0,
            retry_at: None,
            waiting: 0,
            dispatched: 0,
            bytes: 0,
            queue_nanos: 0,
            longest_queue_nanos: 0,
        }
    }

    /// Files a request under its scheduling group.
    ///
    /// A group that had nothing waiting starts from the current virtual time,
    /// so that a group cannot save up a claim on the device by staying idle.
    fn enqueue(&mut self, class: usize, request: QueuedRequest) {
        self.waiting = self.waiting.saturating_add(1);
        if class >= self.classes.len() {
            self.classes.resize_with(class.saturating_add(1), IoClass::default);
        }
        let Some(entry) = self.classes.get_mut(class) else { return };
        if entry.queue.is_empty() {
            entry.vtime = entry.vtime.max(self.vclock);
            self.active.insert((entry.vtime, class));
        }
        entry.queue.push_back(request);
    }

    /// Moves to `out` every request that may go to the kernel now: while
    /// the shard is below its queue depth and the device has capacity, the
    /// next request of the class that has had the least device time
    /// relative to its I/O shares.
    fn take_ready(
        &mut self,
        groups: &mut GroupCache,
        depth: usize,
        now: Instant,
        out: &mut Vec<QueuedRequest>,
    ) {
        self.retry_at = None;
        while self.in_flight < depth {
            let Some(&(vtime, class)) = self.active.first() else { break };
            let Some(entry) = self.classes.get_mut(class) else {
                self.active.remove(&(vtime, class));
                continue;
            };
            let Some(front) = entry.queue.front() else {
                self.active.remove(&(vtime, class));
                continue;
            };
            if let Some(wait) = self.device.acquire(front.tokens, now) {
                self.retry_at = now.checked_add(wait);
                break;
            }
            let Some(request) = entry.queue.pop_front() else { break };
            self.active.remove(&(vtime, class));
            self.vclock = vtime;
            entry.vtime = vtime.saturating_add(virtual_cost(request.fair, groups.io_shares(class)));
            if !entry.queue.is_empty() {
                self.active.insert((entry.vtime, class));
            }
            self.in_flight = self.in_flight.saturating_add(1);
            self.waiting = self.waiting.saturating_sub(1);
            self.dispatched = self.dispatched.saturating_add(1);
            self.bytes = self.bytes.saturating_add(request.bytes);
            let waited = crate::stats::nanos(now.saturating_duration_since(request.enqueued));
            self.queue_nanos = self.queue_nanos.saturating_add(waited);
            self.longest_queue_nanos = self.longest_queue_nanos.max(waited);
            out.push(request);
        }
    }

    /// Withdraws a waiting request, by its slot number. Used when the future
    /// that made the request is dropped before the request reaches the kernel.
    fn remove(&mut self, class: usize, key: usize) {
        let Some(entry) = self.classes.get_mut(class) else { return };
        let before = entry.queue.len();
        entry.queue.retain(|request| request.key != key);
        self.waiting = self.waiting.saturating_sub(before.saturating_sub(entry.queue.len()));
        if before != 0 && entry.queue.is_empty() {
            self.active.remove(&(entry.vtime, class));
        }
    }

    /// Forgets every waiting request. Used at shutdown.
    fn clear(&mut self) {
        for entry in &mut self.classes {
            entry.queue.clear();
        }
        self.active.clear();
        self.retry_at = None;
        self.waiting = 0;
    }
}

/// All of one shard's disk queues, one per device it has used, and what
/// they share: the device table, the shard's view of the scheduling groups,
/// and the limit on requests in the kernel per device.
pub(crate) struct IoScheduler {
    devices: Arc<DeviceTable>,
    queues: HashMap<u64, IoQueue>,
    groups: GroupCache,
    /// How many requests per device this shard keeps in the kernel.
    depth: usize,
    /// Requests waiting in any queue.
    waiting: usize,
    /// Earliest `retry_at` of any queue.
    retry_at: Option<Instant>,
}

impl IoScheduler {
    /// A scheduler with no queues yet; a device's queue is made the first
    /// time the shard uses the device. `depth` is how many requests per device
    /// the shard keeps in the kernel, and is at least one.
    pub(crate) fn new(devices: Arc<DeviceTable>, groups: GroupCache, depth: usize) -> IoScheduler {
        IoScheduler {
            devices,
            queues: HashMap::new(),
            groups,
            depth: depth.max(1),
            waiting: 0,
            retry_at: None,
        }
    }

    /// Queues a request and returns the group it was filed under, which is
    /// `class` unless that group does not exist, in which case it is the default
    /// group.
    pub(crate) fn enqueue(
        &mut self,
        key: usize,
        device: u64,
        class: usize,
        direction: Direction,
        bytes: u64,
        sqe: squeue::Entry,
    ) -> usize {
        let class = if self.groups.exists(class) { class } else { DEFAULT_GROUP };
        let queue = self
            .queues
            .entry(device)
            .or_insert_with(|| IoQueue::new(self.devices.get(device)));
        let (tokens, fair) = queue.device.cost(direction, bytes);
        queue.enqueue(
            class,
            QueuedRequest {
                key,
                device,
                sqe,
                tokens,
                fair,
                bytes,
                enqueued: Instant::now(),
            },
        );
        self.waiting = self.waiting.saturating_add(1);
        class
    }

    /// Collects into `out` the requests that may go to the kernel now, from
    /// every device's queue, and notes when to look again if some are held back
    /// only by device capacity.
    pub(crate) fn take_ready(&mut self, out: &mut Vec<QueuedRequest>) {
        if self.waiting == 0 {
            self.retry_at = None;
            return;
        }
        let now = Instant::now();
        let before = out.len();
        let mut retry_at: Option<Instant> = None;
        for queue in self.queues.values_mut() {
            queue.take_ready(&mut self.groups, self.depth, now, out);
            if let Some(at) = queue.retry_at {
                retry_at = Some(retry_at.map_or(at, |earliest| earliest.min(at)));
            }
        }
        self.retry_at = retry_at;
        let taken = out.len().saturating_sub(before);
        self.waiting = self.waiting.saturating_sub(taken);
    }

    /// Records that a request previously taken with `take_ready` has left the
    /// kernel, which makes room for another under the per-device limit.
    pub(crate) fn completed(&mut self, device: u64) {
        if let Some(queue) = self.queues.get_mut(&device) {
            queue.in_flight = queue.in_flight.saturating_sub(1);
        }
    }

    /// Withdraws a request that is still waiting.
    pub(crate) fn remove(&mut self, device: u64, class: usize, key: usize) {
        if let Some(queue) = self.queues.get_mut(&device) {
            queue.remove(class, key);
            self.waiting = self.waiting.saturating_sub(1);
        }
    }

    /// When to try `take_ready` again even if nothing completes: requests
    /// are waiting for device capacity that frees up with time.
    pub(crate) fn retry_at(&self) -> Option<Instant> {
        self.retry_at
    }

    /// Forgets every waiting request, on every device. Used at shutdown;
    /// requests already in the kernel are the reactor's to wait for.
    pub(crate) fn clear(&mut self) {
        for queue in self.queues.values_mut() {
            queue.clear();
        }
        self.waiting = 0;
        self.retry_at = None;
    }

    /// The state of every device queue, ordered by device number.
    pub(crate) fn stats(&self) -> Vec<DeviceStats> {
        let mut stats: Vec<DeviceStats> = self
            .queues
            .iter()
            .map(|(&device, queue)| DeviceStats {
                device,
                queued: queue.waiting,
                in_flight: queue.in_flight,
                dispatched: queue.dispatched,
                bytes: queue.bytes,
                total_queue_time: Duration::from_nanos(queue.queue_nanos),
                longest_queue_time: Duration::from_nanos(queue.longest_queue_nanos),
            })
            .collect();
        stats.sort_by_key(|stats| stats.device);
        stats
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::arithmetic_side_effects, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use crate::sched::GroupTable;

    fn nop() -> squeue::Entry {
        io_uring::opcode::Nop::new().build()
    }

    #[test]
    fn requests_are_charged_device_time() {
        let device = DeviceGroup::new(
            IoProperties {
                read_iops: Some(10_000),
                read_bytes_per_sec: Some(100 << 20),
                write_iops: Some(1_000),
                write_bytes_per_sec: None,
            },
            Duration::from_millis(1),
        );
        // 1/10_000 s per request plus 1 MiB at 100 MiB/s.
        assert_eq!(device.cost(Direction::Read, 1 << 20).0, 100_000 + 10_000_000);
        // Writes: only the per-request part was declared.
        assert_eq!(device.cost(Direction::Write, 1 << 20).0, 1_000_000);
        // Nothing declared at all: no tokens, ordered by size.
        let unmetered = DeviceGroup::unmetered();
        assert_eq!(unmetered.cost(Direction::Read, 4096), (0, 16 * 1024 + 4096));
    }

    #[test]
    fn the_bucket_admits_at_the_declared_rate() {
        let device = DeviceGroup::new(
            IoProperties {
                write_bytes_per_sec: Some(1 << 20),
                ..Default::default()
            },
            Duration::from_millis(2),
        );
        let start = device.epoch;
        let (tokens, _) = device.cost(Direction::Write, 64 << 10); // 62.5 ms of device time
        assert_eq!(tokens, 62_500_000);
        // The first request goes at once and leaves a debt.
        assert_eq!(device.acquire(tokens, start), None);
        let wait = device.acquire(tokens, start).unwrap();
        assert!(wait > Duration::from_millis(60) && wait < Duration::from_millis(61), "{wait:?}");
        // Not yet repaid after 50 ms; repaid after 61.
        assert!(device.acquire(tokens, start + Duration::from_millis(50)).is_some());
        assert_eq!(device.acquire(tokens, start + Duration::from_millis(61)), None);
        // Idle time is not saved up beyond the burst: after a long pause
        // only one request goes straight through.
        let later = start + Duration::from_secs(60);
        assert_eq!(device.acquire(tokens, later), None);
        assert!(device.acquire(tokens, later).is_some());
    }

    #[test]
    fn classes_are_served_by_shares_and_withdrawals_are_exact() {
        let table = Arc::new(GroupTable::new());
        let low = table.create_for_test("low", 100);
        let high = table.create_for_test("high", 300);
        let devices = Arc::new(DeviceTable::new(HashMap::new()));
        let mut scheduler = IoScheduler::new(devices, GroupCache::new(table), 1);
        let mut key = 0;
        let mut owner = HashMap::new();
        for _ in 0..40 {
            for class in [low, high] {
                scheduler.enqueue(key, 7, class, Direction::Write, 4096, nop());
                owner.insert(key, class);
                key += 1;
            }
        }
        // Withdraw a few from the middle of the low queue.
        for withdrawn in [10, 12, 14] {
            scheduler.remove(7, low, withdrawn);
            owner.remove(&withdrawn);
        }
        let mut order = Vec::new();
        let mut batch = Vec::new();
        loop {
            scheduler.take_ready(&mut batch);
            let Some(request) = batch.pop() else { break };
            assert!(batch.is_empty(), "depth is one");
            order.push(owner.remove(&request.key).unwrap());
            scheduler.completed(7);
        }
        assert!(owner.is_empty(), "every remaining request was dispatched once");
        assert_eq!(order.len(), 77);
        // While both classes wait, `high` gets three of every four.
        let high_first = order[..40].iter().filter(|&&class| class == high).count();
        assert!((29..=31).contains(&high_first), "{high_first}");
        let stats = scheduler.stats();
        assert_eq!((stats[0].dispatched, stats[0].queued, stats[0].in_flight), (77, 0, 0));
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing
)]
mod model_tests {
    use super::*;
    use crate::sched::GroupTable;
    use std::collections::HashSet;

    struct Rng(u64);

    impl Rng {
        fn next(&mut self, below: usize) -> usize {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            (self.0 % below as u64) as usize
        }
    }

    /// Random interleavings of submitting, withdrawing, dispatching and
    /// completing requests on two devices, checked against a plain model
    /// of what is waiting and what is in the kernel.
    #[test]
    fn random_operations_keep_the_books_straight() {
        let seeds = if cfg!(miri) { 3 } else { 300 };
        for seed in 1..=seeds {
            let mut rng = Rng((seed as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
            let table = Arc::new(GroupTable::new());
            let classes: Vec<usize> = (0..4)
                .map(|i| table.create_for_test("class", 1 + rng.next(2000) as u32 * (i + 1)))
                .collect();
            let depth = 1 + rng.next(6);
            // One device is metered so tightly that requests have to wait
            // for capacity as well as for depth.
            let mut devices = HashMap::new();
            let metered = IoProperties {
                write_iops: Some(2_000_000),
                ..Default::default()
            };
            devices.insert(1, Arc::new(DeviceGroup::new(metered, Duration::from_nanos(1))));
            let mut scheduler =
                IoScheduler::new(Arc::new(DeviceTable::new(devices)), GroupCache::new(table), depth);

            let mut waiting: HashMap<usize, (u64, usize)> = HashMap::new();
            let mut in_kernel: HashMap<usize, u64> = HashMap::new();
            let mut finished: HashSet<usize> = HashSet::new();
            let mut next_key = 0;
            let mut batch = Vec::new();

            let mut dispatch = |scheduler: &mut IoScheduler,
                                waiting: &mut HashMap<usize, (u64, usize)>,
                                in_kernel: &mut HashMap<usize, u64>| {
                scheduler.take_ready(&mut batch);
                for request in batch.drain(..) {
                    let (device, _) = waiting.remove(&request.key).expect("dispatched once, and only if waiting");
                    assert_eq!(device, request.device);
                    in_kernel.insert(request.key, device);
                }
                for device in [0u64, 1] {
                    let flying = in_kernel.values().filter(|&&d| d == device).count();
                    assert!(flying <= depth, "seed {seed}: {flying} in flight at depth {depth}");
                }
            };

            for _ in 0..if cfg!(miri) { 60 } else { 600 } {
                match rng.next(10) {
                    0..=4 => {
                        let device = rng.next(2) as u64;
                        // Sometimes a group the scheduler has never heard of.
                        let class = if rng.next(20) == 0 { 99 } else { classes[rng.next(4)] };
                        let filed = scheduler.enqueue(
                            next_key,
                            device,
                            class,
                            Direction::Write,
                            rng.next(1 << 20) as u64,
                            io_uring::opcode::Nop::new().build(),
                        );
                        assert!(filed == class || (class == 99 && filed == DEFAULT_GROUP));
                        waiting.insert(next_key, (device, filed));
                        next_key += 1;
                    }
                    5 => {
                        if let Some((&key, &(device, class))) = waiting.iter().next() {
                            scheduler.remove(device, class, key);
                            waiting.remove(&key);
                            finished.insert(key);
                        }
                    }
                    6 | 7 => dispatch(&mut scheduler, &mut waiting, &mut in_kernel),
                    _ => {
                        if let Some((&key, &device)) = in_kernel.iter().next() {
                            scheduler.completed(device);
                            in_kernel.remove(&key);
                            assert!(finished.insert(key), "completed twice");
                        }
                    }
                }
                let queued: usize = scheduler.stats().iter().map(|stats| stats.queued).sum();
                assert_eq!(queued, waiting.len(), "seed {seed}");
                let flying: usize = scheduler.stats().iter().map(|stats| stats.in_flight).sum();
                assert_eq!(flying, in_kernel.len(), "seed {seed}");
            }

            // Everything left drains; nothing is lost or duplicated.
            let mut rounds = 0;
            while !waiting.is_empty() || !in_kernel.is_empty() {
                dispatch(&mut scheduler, &mut waiting, &mut in_kernel);
                for (key, device) in in_kernel.drain() {
                    scheduler.completed(device);
                    assert!(finished.insert(key));
                }
                rounds += 1;
                assert!(rounds < 1_000_000, "seed {seed}: requests are stuck");
            }
            assert_eq!(finished.len(), next_key);
            assert!(scheduler.retry_at().is_none());
        }
    }
}
