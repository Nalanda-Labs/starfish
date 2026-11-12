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

/// The index of the group that exists from the start: the one `main` and
/// everything it spawns run in.
pub(crate) const DEFAULT_GROUP: usize = 0;
/// The shares of the default group, and the scale against which other
/// groups' shares are judged: a group with 500 gets half as much as it.
const DEFAULT_SHARES: u32 = 1000;
/// The most shares a group may have. With the least being 1, the ratio
/// between two groups is at most ten thousand to one.
const MAX_SHARES: u32 = 10_000;

/// Shares brought into the permitted range.
fn clamp_shares(shares: u32) -> u32 {
    shares.clamp(1, MAX_SHARES)
}

/// What the runtime knows about one scheduling group: its name and its
/// two weights.
///
/// The weights are atomic because they can be changed at any time from any
/// shard and are read by every shard on every scheduling decision.
pub(crate) struct GroupInfo {
    name: String,
    cpu_shares: AtomicU32,
    io_shares: AtomicU32,
}

/// The runtime-wide list of groups, shared by all shards.
///
/// Groups are only ever added, so an index, once handed out, stays valid and
/// always means the same group.
pub(crate) struct GroupTable {
    groups: Mutex<Vec<Arc<GroupInfo>>>,
    /// Mirrors `groups.len()` so that existence checks need no lock.
    count: AtomicUsize,
}

impl GroupTable {
    /// A table holding only the default group.
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

    /// Whether `group` is the index of a group that exists. A single atomic
    /// read, with no lock.
    pub(crate) fn exists(&self, group: usize) -> bool {
        group < self.count.load(Ordering::Acquire)
    }

    /// Adds a group and returns its index.
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

    /// Adds a group with the same shares of both kinds; for the unit tests of
    /// other modules.
    #[cfg(test)]
    pub(crate) fn create_for_test(&self, name: &str, shares: u32) -> usize {
        self.create(name, shares, shares)
    }

    /// One group's record, or `None` for an index that is not a group's.
    fn get(&self, group: usize) -> Option<Arc<GroupInfo>> {
        let groups = self.groups.lock().unwrap_or_else(PoisonError::into_inner);
        groups.get(group).cloned()
    }

    /// Appends the groups from index `from` on to `out`: how a shard's cache
    /// catches up with groups made since it last looked.
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
    /// An empty cache over `table`. It fills on first use.
    pub(crate) fn new(table: Arc<GroupTable>) -> GroupCache {
        GroupCache {
            table,
            local: Vec::new(),
        }
    }

    /// One group's record, from the cache, after catching up with the shared
    /// table if the group is newer than the cache.
    fn get(&mut self, group: usize) -> Option<&GroupInfo> {
        if group >= self.local.len() && self.table.exists(group) {
            self.table.copy_from(self.local.len(), &mut self.local);
        }
        self.local.get(group).map(|info| &**info)
    }

    /// Whether `group` exists.
    pub(crate) fn exists(&mut self, group: usize) -> bool {
        self.get(group).is_some()
    }

    /// A group's CPU weight, or the default weight for a group that does not
    /// exist.
    fn cpu_shares(&mut self, group: usize) -> u32 {
        self.get(group)
            .map_or(DEFAULT_SHARES, |info| info.cpu_shares.load(Ordering::Relaxed))
    }

    /// A group's disk weight, or the default weight for a group that does not
    /// exist.
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

/// One group's runnable tasks on one shard, with the group's virtual time
/// and its running totals.
///
/// Virtual time is the CPU time the group has used, divided by its shares.
/// The group with the least virtual time runs next, which is what gives each
/// group CPU time in proportion to its shares.
#[derive(Default)]
struct GroupQueue {
    tasks: VecDeque<Task>,
    vtime: u64,
    cpu_nanos: u64,
    polled: u64,
}

/// The runnable tasks of one shard, one first-in-first-out queue per
/// group, with the bookkeeping that decides which group runs next.
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
    /// Empty run queues over `table`.
    pub(crate) fn new(table: Arc<GroupTable>) -> RunQueues {
        RunQueues {
            cache: GroupCache::new(table),
            groups: Vec::new(),
            active: BTreeSet::new(),
            vclock: 0,
        }
    }

    /// Whether no group has a runnable task.
    pub(crate) fn is_empty(&self) -> bool {
        self.active.is_empty()
    }

    /// Makes a task runnable, at the back of its group's queue.
    ///
    /// A group that was idle starts from the current virtual time, not from
    /// where it left off, so that a group cannot save up a claim on the CPU by
    /// doing nothing. A task whose group does not exist runs in the default
    /// group.
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

    /// Takes the next task of group `index`, or `None` if it has none.
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

    /// Forgets every runnable task. Used at shutdown, when the tasks are about
    /// to be dropped through the task table.
    pub(crate) fn clear(&mut self) {
        for group in &mut self.groups {
            group.tasks.clear();
        }
        self.active.clear();
    }

    /// A group's name, or an empty string for an index that is not a group's.
    pub(crate) fn group_name(&mut self, index: usize) -> String {
        self.cache.get(index).map(|info| info.name.clone()).unwrap_or_default()
    }

    /// One [`GroupStats`] per group that has had a task on this shard.
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

    /// Creates a group with the same number of CPU and I/O shares, clamped to
    /// `1..=10_000`.
    ///
    /// The name is a label for statistics and stall reports; it need not be
    /// unique. The group exists on every shard from now on.
    ///
    /// # Errors
    ///
    /// [`Error::NoRuntime`] outside a runtime.
    pub fn create(name: &str, shares: u32) -> Result<SchedulingGroup> {
        SchedulingGroup::create_with(name, shares, shares)
    }

    /// Creates a group with separate CPU and I/O shares, each clamped to
    /// `1..=10_000`.
    ///
    /// There is no limit on the number of groups, but a group cannot be
    /// removed and every shard keeps a little state for each. Groups are
    /// classes of work, to be made once at start-up, not one per request
    /// or per connection.
    pub fn create_with(name: &str, cpu_shares: u32, io_shares: u32) -> Result<SchedulingGroup> {
        let id = current()?.runtime.groups.create(name, cpu_shares, io_shares);
        Ok(SchedulingGroup { id })
    }

    /// The group of the task that is running now. This is the group a task
    /// spawned here would inherit.
    ///
    /// # Errors
    ///
    /// [`Error::NoRuntime`] outside a runtime.
    pub fn current() -> Result<SchedulingGroup> {
        Ok(SchedulingGroup {
            id: current()?.current_group.get(),
        })
    }

    /// The group's record in the running runtime's table.
    fn info(&self) -> Result<Arc<GroupInfo>> {
        current()?.runtime.groups.get(self.id).ok_or(Error::InvalidGroup)
    }

    /// Changes the group's CPU weight on every shard, clamped to
    /// `1..=10_000`. It takes effect at each shard's next scheduling decision.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidGroup`] if the group does not belong to the running
    /// runtime, [`Error::NoRuntime`] outside one.
    pub fn set_cpu_shares(&self, shares: u32) -> Result<()> {
        self.info()?
            .cpu_shares
            .store(clamp_shares(shares), Ordering::Relaxed);
        Ok(())
    }

    /// Changes the group's disk weight on every shard, clamped to
    /// `1..=10_000`. It applies to requests not yet handed to the kernel.
    ///
    /// # Errors
    ///
    /// As for [`set_cpu_shares`](Self::set_cpu_shares).
    pub fn set_io_shares(&self, shares: u32) -> Result<()> {
        self.info()?
            .io_shares
            .store(clamp_shares(shares), Ordering::Relaxed);
        Ok(())
    }

    /// The group's CPU weight, as last set with
    /// [`set_cpu_shares`](Self::set_cpu_shares) or at creation.
    ///
    /// Fails with [`Error::InvalidGroup`] if the group does not belong to the
    /// running runtime, [`Error::NoRuntime`] outside one.
    pub fn cpu_shares(&self) -> Result<u32> {
        Ok(self.info()?.cpu_shares.load(Ordering::Relaxed))
    }

    /// The group's disk weight, as last set with
    /// [`set_io_shares`](Self::set_io_shares) or at creation.
    ///
    /// Fails with [`Error::InvalidGroup`] if the group does not belong to the
    /// running runtime, [`Error::NoRuntime`] outside one.
    pub fn io_shares(&self) -> Result<u32> {
        Ok(self.info()?.io_shares.load(Ordering::Relaxed))
    }

    /// The name the group was created with, as it appears in statistics.
    ///
    /// Fails with [`Error::InvalidGroup`] if the group does not belong to the
    /// running runtime, [`Error::NoRuntime`] outside one.
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
pub fn should_yield() -> bool {
    completions_pending()
}

/// Yields to the scheduler if the time slice is used up; otherwise returns
/// immediately.
///
/// This is what a long loop should call on every pass. It costs two memory
/// reads when the slice still has time, and a trip round the run queue, as
/// [`yield_now`](crate::yield_now) makes, when it has not.
///
/// ```no_run
/// # async fn example(items: &[u64]) -> u64 {
/// let mut total = 0u64;
/// for item in items {
///     total = total.wrapping_add(item.wrapping_mul(31));
///     starfish::maybe_yield().await;
/// }
/// total
/// # }
/// ```
pub async fn maybe_yield() {
    if should_yield() {
        yield_now().await;
    }
}
