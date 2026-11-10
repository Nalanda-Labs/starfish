//! Scheduling groups: named classes of work that share a shard's CPU time
//! and its disk queues in proportion to their shares.

use std::collections::{BTreeSet, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use crate::error::{Error, Result};
use crate::reactor::completions_pending;
use crate::runtime::current;
use crate::stats::GroupStats;
use crate::task::{spawn_in, yield_now, JoinHandle, Task};

pub(crate) const DEFAULT_GROUP: usize = 0;
const DEFAULT_SHARES: u32 = 1000;
const MAX_SHARES: u32 = 10_000;

fn clamp_shares(shares: u32) -> u32 {
    shares.clamp(1, MAX_SHARES)
}

pub(crate) struct GroupInfo {
    name: String,
    cpu_shares: AtomicU32,
    io_shares: AtomicU32,
}

/// The runtime-wide list of groups. Groups are only ever added.
pub(crate) struct GroupTable {
    groups: Mutex<Vec<Arc<GroupInfo>>>,
    /// Mirrors `groups.len()` so that existence checks need no lock.
    count: AtomicUsize,
}

impl GroupTable {
    pub(crate) fn new() -> GroupTable {
        let main = Arc::new(GroupInfo {
            name: String::from("main"),
            cpu_shares: AtomicU32::new(DEFAULT_SHARES),
            io_shares: AtomicU32::new(DEFAULT_SHARES),
        });
        GroupTable {
            groups: Mutex::new(vec![main]),
            count: AtomicUsize::new(1),
        }
    }

    pub(crate) fn exists(&self, group: usize) -> bool {
        group < self.count.load(Ordering::Acquire)
    }

    fn create(&self, name: &str, cpu_shares: u32, io_shares: u32) -> usize {
        let mut groups = self.groups.lock().unwrap_or_else(PoisonError::into_inner);
        let id = groups.len();
        groups.push(Arc::new(GroupInfo {
            name: name.to_owned(),
            cpu_shares: AtomicU32::new(clamp_shares(cpu_shares)),
            io_shares: AtomicU32::new(clamp_shares(io_shares)),
        }));
        self.count.store(groups.len(), Ordering::Release);
        id
    }

    #[cfg(test)]
    pub(crate) fn create_for_test(&self, name: &str, shares: u32) -> usize {
        self.create(name, shares, shares)
    }

    fn get(&self, group: usize) -> Option<Arc<GroupInfo>> {
        let groups = self.groups.lock().unwrap_or_else(PoisonError::into_inner);
        groups.get(group).cloned()
    }

    /// Appends the groups from index `from` on to `out`.
    fn copy_from(&self, from: usize, out: &mut Vec<Arc<GroupInfo>>) {
        let groups = self.groups.lock().unwrap_or_else(PoisonError::into_inner);
        out.extend(groups.iter().skip(from).cloned());
    }
}

/// A shard's private view of the group table, so that reading shares on
/// every scheduling decision takes no lock. It catches up with the shared
/// table the first time it is asked about a group it has not seen.
pub(crate) struct GroupCache {
    table: Arc<GroupTable>,
    local: Vec<Arc<GroupInfo>>,
}

impl GroupCache {
    pub(crate) fn new(table: Arc<GroupTable>) -> GroupCache {
        GroupCache {
            table,
            local: Vec::new(),
        }
    }

    fn get(&mut self, group: usize) -> Option<&GroupInfo> {
        if group >= self.local.len() && self.table.exists(group) {
            self.table.copy_from(self.local.len(), &mut self.local);
        }
        self.local.get(group).map(|info| &**info)
    }

    pub(crate) fn exists(&mut self, group: usize) -> bool {
        self.get(group).is_some()
    }

    fn cpu_shares(&mut self, group: usize) -> u32 {
        self.get(group)
            .map_or(DEFAULT_SHARES, |info| info.cpu_shares.load(Ordering::Relaxed))
    }

    pub(crate) fn io_shares(&mut self, group: usize) -> u32 {
        self.get(group)
            .map_or(DEFAULT_SHARES, |info| info.io_shares.load(Ordering::Relaxed))
    }
}

/// Virtual time advanced by `work` units done by a group holding `shares`.
/// A group with twice the shares ages half as fast, so it is picked twice
/// as often.
pub(crate) fn virtual_cost(work: u64, shares: u32) -> u64 {
    work.checked_div(u64::from(shares)).unwrap_or(work).max(1)
}

#[derive(Default)]
struct GroupQueue {
    tasks: VecDeque<Task>,
    vtime: u64,
    cpu_nanos: u64,
    polled: u64,
}

/// The runnable tasks of one shard, one FIFO per group.
pub(crate) struct RunQueues {
    cache: GroupCache,
    groups: Vec<GroupQueue>,
    /// The groups that have runnable tasks, ordered by virtual time. An
    /// entry's first field always equals that group's `vtime`.
    active: BTreeSet<(u64, usize)>,
    /// Virtual time of the most recently picked group. A group that wakes
    /// up after being idle starts from here, so idling earns no credit.
    vclock: u64,
}

impl RunQueues {
    pub(crate) fn new(table: Arc<GroupTable>) -> RunQueues {
        RunQueues {
            cache: GroupCache::new(table),
            groups: Vec::new(),
            active: BTreeSet::new(),
            vclock: 0,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.active.is_empty()
    }

    pub(crate) fn push(&mut self, task: Task) {
        let index = if self.cache.exists(task.group()) {
            task.group()
        } else {
            DEFAULT_GROUP
        };
        if index >= self.groups.len() {
            self.groups.resize_with(index.saturating_add(1), GroupQueue::default);
        }
        let Some(group) = self.groups.get_mut(index) else { return };
        if group.tasks.is_empty() {
            group.vtime = group.vtime.max(self.vclock);
            self.active.insert((group.vtime, index));
        }
        group.tasks.push_back(task);
    }

    /// Chooses the group to run next: the one that has had the least CPU
    /// time relative to its shares. Returns it with its current length.
    pub(crate) fn pick(&mut self) -> Option<(usize, usize)> {
        let &(vtime, index) = self.active.first()?;
        self.vclock = vtime;
        let len = self.groups.get(index).map_or(0, |group| group.tasks.len());
        Some((index, len))
    }

    pub(crate) fn pop(&mut self, index: usize) -> Option<Task> {
        let group = self.groups.get_mut(index)?;
        let task = group.tasks.pop_front()?;
        if group.tasks.is_empty() {
            self.active.remove(&(group.vtime, index));
        }
        Some(task)
    }

    /// Accounts `nanos` of CPU time, spent running `polled` tasks, to a
    /// group.
    pub(crate) fn charge(&mut self, index: usize, nanos: u64, polled: u64) {
        let shares = self.cache.cpu_shares(index);
        let Some(group) = self.groups.get_mut(index) else { return };
        group.cpu_nanos = group.cpu_nanos.saturating_add(nanos);
        group.polled = group.polled.saturating_add(polled);
        let was_active = self.active.remove(&(group.vtime, index));
        // Scaled so that small slices are not rounded away.
        let work = nanos.saturating_mul(1024);
        group.vtime = group.vtime.saturating_add(virtual_cost(work, shares));
        if was_active {
            self.active.insert((group.vtime, index));
        }
    }

    pub(crate) fn clear(&mut self) {
        for group in &mut self.groups {
            group.tasks.clear();
        }
        self.active.clear();
    }

    pub(crate) fn group_name(&mut self, index: usize) -> String {
        self.cache.get(index).map(|info| info.name.clone()).unwrap_or_default()
    }

    pub(crate) fn stats(&mut self) -> Vec<GroupStats> {
        let mut stats = Vec::with_capacity(self.groups.len());
        for (index, group) in self.groups.iter().enumerate() {
            let Some(info) = self.cache.get(index) else { continue };
            stats.push(GroupStats {
                name: info.name.clone(),
                cpu_shares: info.cpu_shares.load(Ordering::Relaxed),
                io_shares: info.io_shares.load(Ordering::Relaxed),
                cpu_time: Duration::from_nanos(group.cpu_nanos),
                tasks_polled: group.polled,
                runnable: group.tasks.len(),
            });
        }
        stats
    }
}

/// A class of work with relative weights.
///
/// A group has two weights. When several groups have runnable tasks on a
/// shard, each gets CPU time in proportion to its *CPU shares*. When
/// several groups have file requests waiting for a device, each gets device
/// time in proportion to its *I/O shares*. A group with nothing to do costs
/// nothing and saves up nothing.
///
/// A task belongs to the group it was spawned in. [`spawn`](crate::spawn)
/// and [`submit_to`](crate::submit_to) inherit the caller's group; use
/// [`SchedulingGroup::spawn`] to start work in another one. Groups exist on
/// every shard of the runtime that created them, and a runtime can have any
/// number of them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SchedulingGroup {
    id: usize,
}

impl SchedulingGroup {
    /// The group that `main` and everything it spawns run in, with 1000
    /// shares of each kind.
    pub const DEFAULT: SchedulingGroup = SchedulingGroup { id: DEFAULT_GROUP };

    /// Creates a group with the same number of CPU and I/O shares.
    /// Shares are clamped to `1..=10_000`.
    pub fn create(name: &str, shares: u32) -> Result<SchedulingGroup> {
        SchedulingGroup::create_with(name, shares, shares)
    }

    /// Creates a group with separate CPU and I/O shares, each clamped to
    /// `1..=10_000`.
    pub fn create_with(name: &str, cpu_shares: u32, io_shares: u32) -> Result<SchedulingGroup> {
        let id = current()?.runtime.groups.create(name, cpu_shares, io_shares);
        Ok(SchedulingGroup { id })
    }

    /// The group of the task that is running now.
    pub fn current() -> Result<SchedulingGroup> {
        Ok(SchedulingGroup {
            id: current()?.current_group.get(),
        })
    }

    fn info(&self) -> Result<Arc<GroupInfo>> {
        current()?.runtime.groups.get(self.id).ok_or(Error::InvalidGroup)
    }

    /// Changes the group's CPU weight on every shard.
    pub fn set_cpu_shares(&self, shares: u32) -> Result<()> {
        self.info()?
            .cpu_shares
            .store(clamp_shares(shares), Ordering::Relaxed);
        Ok(())
    }

    /// Changes the group's disk weight on every shard.
    pub fn set_io_shares(&self, shares: u32) -> Result<()> {
        self.info()?
            .io_shares
            .store(clamp_shares(shares), Ordering::Relaxed);
        Ok(())
    }

    pub fn cpu_shares(&self) -> Result<u32> {
        Ok(self.info()?.cpu_shares.load(Ordering::Relaxed))
    }

    pub fn io_shares(&self) -> Result<u32> {
        Ok(self.info()?.io_shares.load(Ordering::Relaxed))
    }

    pub fn name(&self) -> Result<String> {
        Ok(self.info()?.name.clone())
    }

    /// Spawns a task on the current shard in this group. The handle
    /// resolves to [`Error::InvalidGroup`] if the group does not belong to
    /// the running runtime.
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        spawn_in(Some(self.id), future)
    }
}

/// Whether the running task should yield because its time slice (see
/// [`Runtime::task_quota`](crate::Runtime::task_quota)) has run out.
///
/// The runtime cannot interrupt a task, so loops that may run long should
/// check this, or simply await [`maybe_yield`]. The check is two memory
/// reads, cheap enough for the inside of a loop.
pub fn need_preempt() -> bool {
    completions_pending()
}

/// Yields to the scheduler if the time slice is used up; otherwise returns
/// immediately.
pub async fn maybe_yield() {
    if need_preempt() {
        yield_now().await;
    }
}
