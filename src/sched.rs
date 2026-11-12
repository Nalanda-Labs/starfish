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

// A group is known by a number that packs two things: the index of its
// slot in the group table (the low half) and the slot's generation at the
// time the group was made (the high half). Removing a group frees its slot
// for the next group and raises the slot's generation, so a number that
// outlives its group never matches whatever takes the slot next. The
// default group is slot 0 at generation 0, which makes its number 0.
const _: () = assert!(usize::BITS == 64);

/// Bits of a group's number that hold its slot.
const SLOT_BITS: u32 = 32;
/// The most slots the table can have, which is the most groups that can
/// exist at one time.
const MAX_SLOTS: usize = 1 << SLOT_BITS;

/// The number of the group in `slot` at `generation`.
fn pack(slot: usize, generation: u32) -> usize {
    (slot & (MAX_SLOTS - 1)) | ((generation as usize) << SLOT_BITS)
}

/// The slot half of a group's number.
fn slot_of(group: usize) -> usize {
    group & (MAX_SLOTS - 1)
}

/// The generation half of a group's number.
fn generation_of(group: usize) -> u32 {
    (group >> SLOT_BITS) as u32
}

/// What the runtime knows about one scheduling group: its name, its two
/// weights and the generation of its slot.
///
/// The weights are atomic because they can be changed at any time from any
/// shard and are read by every shard on every scheduling decision.
pub(crate) struct GroupInfo {
    name: String,
    generation: u32,
    cpu_shares: AtomicU32,
    io_shares: AtomicU32,
}

/// One place in the group table: the group in it, if any, and how many
/// groups have been removed from it.
struct Slot {
    info: Option<Arc<GroupInfo>>,
    generation: u32,
}

/// The table proper, behind the lock.
struct Slots {
    slots: Vec<Slot>,
    /// Indexes of the slots with no group, to be used before the table grows.
    free: Vec<usize>,
}

/// The runtime-wide list of groups, shared by all shards.
///
/// A group keeps its slot for as long as it exists. Removing it empties the
/// slot, which the next group to be created takes over; the table therefore
/// grows only as far as the most groups that ever existed at once.
pub(crate) struct GroupTable {
    slots: Mutex<Slots>,
    /// Counts the changes to which groups exist. A shard compares it with the
    /// count it last saw to know whether its copy of the table is current,
    /// which costs one load and no lock.
    epoch: AtomicUsize,
}

impl GroupTable {
    /// A table holding only the default group.
    pub(crate) fn new() -> GroupTable {
        let main = Arc::new(GroupInfo {
            name: String::from("main"),
            generation: 0,
            cpu_shares: AtomicU32::new(DEFAULT_SHARES),
            io_shares: AtomicU32::new(DEFAULT_SHARES),
        });
        GroupTable {
            slots: Mutex::new(Slots {
                slots: vec![Slot {
                    info: Some(main),
                    generation: 0,
                }],
                free: Vec::new(),
            }),
            epoch: AtomicUsize::new(0),
        }
    }

    /// Adds a group and returns its number, or `None` if the table is full.
    fn create(&self, name: &str, cpu_shares: u32, io_shares: u32) -> Option<usize> {
        let mut table = self.slots.lock().unwrap_or_else(PoisonError::into_inner);
        let index = match table.free.pop() {
            Some(index) => index,
            None => {
                let index = table.slots.len();
                if index >= MAX_SLOTS {
                    return None;
                }
                table.slots.push(Slot {
                    info: None,
                    generation: 0,
                });
                index
            }
        };
        let slot = table.slots.get_mut(index)?;
        let generation = slot.generation;
        slot.info = Some(Arc::new(GroupInfo {
            name: name.to_owned(),
            generation,
            cpu_shares: AtomicU32::new(clamp_shares(cpu_shares)),
            io_shares: AtomicU32::new(clamp_shares(io_shares)),
        }));
        self.epoch.fetch_add(1, Ordering::Release);
        Some(pack(index, generation))
    }

    /// Adds a group with the same shares of both kinds; for the unit tests of
    /// other modules.
    #[cfg(test)]
    pub(crate) fn create_for_test(&self, name: &str, shares: u32) -> usize {
        self.create(name, shares, shares).unwrap_or(DEFAULT_GROUP)
    }

    /// Removes a group and frees its slot. Returns false if `group` is not
    /// the number of a group that exists, or is the default group, which
    /// cannot be removed.
    fn remove(&self, group: usize) -> bool {
        let index = slot_of(group);
        if index == DEFAULT_GROUP {
            return false;
        }
        let mut table = self.slots.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(slot) = table.slots.get_mut(index) else {
            return false;
        };
        if slot.info.is_none() || slot.generation != generation_of(group) {
            return false;
        }
        slot.info = None;
        slot.generation = slot.generation.wrapping_add(1);
        table.free.push(index);
        self.epoch.fetch_add(1, Ordering::Release);
        true
    }

    /// One group's record, or `None` for a number that is not a group's.
    fn get(&self, group: usize) -> Option<Arc<GroupInfo>> {
        let table = self.slots.lock().unwrap_or_else(PoisonError::into_inner);
        let info = table.slots.get(slot_of(group))?.info.as_ref()?;
        (info.generation == generation_of(group)).then(|| info.clone())
    }

    /// Replaces `out` with the table as it stands, slot by slot, and returns
    /// the epoch that copy belongs to: how a shard's cache catches up.
    fn copy_to(&self, out: &mut Vec<Option<Arc<GroupInfo>>>) -> usize {
        let table = self.slots.lock().unwrap_or_else(PoisonError::into_inner);
        out.clear();
        out.extend(table.slots.iter().map(|slot| slot.info.clone()));
        // Read under the lock, which every change holds, so it is exact.
        self.epoch.load(Ordering::Acquire)
    }
}

/// A shard's private copy of the group table, so that reading shares on
/// every scheduling decision takes no lock. Before every use it compares
/// the table's epoch with the one its copy was made at, and copies the table
/// again if groups have been created or removed since.
pub(crate) struct GroupCache {
    table: Arc<GroupTable>,
    local: Vec<Option<Arc<GroupInfo>>>,
    /// The table's epoch when `local` was copied; `usize::MAX` before the
    /// first copy, which no table reaches.
    epoch: usize,
}

impl GroupCache {
    /// An empty cache over `table`. It fills on first use.
    pub(crate) fn new(table: Arc<GroupTable>) -> GroupCache {
        GroupCache {
            table,
            local: Vec::new(),
            epoch: usize::MAX,
        }
    }

    /// Brings the copy up to date if the table has changed.
    fn refresh(&mut self) {
        if self.table.epoch.load(Ordering::Acquire) != self.epoch {
            self.epoch = self.table.copy_to(&mut self.local);
        }
    }

    /// The group now in slot `index`, if there is one.
    fn in_slot(&mut self, index: usize) -> Option<&GroupInfo> {
        self.refresh();
        self.local.get(index)?.as_deref()
    }

    /// The slot of the group numbered `group`, or `None` if no such group
    /// exists: it never did, or it has been removed.
    pub(crate) fn slot(&mut self, group: usize) -> Option<usize> {
        let index = slot_of(group);
        let info = self.in_slot(index)?;
        (info.generation == generation_of(group)).then_some(index)
    }

    /// Whether the group numbered `group` exists.
    pub(crate) fn exists(&mut self, group: usize) -> bool {
        self.slot(group).is_some()
    }

    /// The CPU weight of the group in slot `index`, or the default weight
    /// for an empty slot.
    fn cpu_shares(&mut self, index: usize) -> u32 {
        self.in_slot(index)
            .map_or(DEFAULT_SHARES, |info| info.cpu_shares.load(Ordering::Relaxed))
    }

    /// The disk weight of the group in slot `index`, or the default weight
    /// for an empty slot.
    pub(crate) fn io_shares(&mut self, index: usize) -> u32 {
        self.in_slot(index)
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
    /// Generation of the group the totals belong to. When another group has
    /// taken the slot, the totals start again from zero.
    generation: u32,
}

/// The runnable tasks of one shard, one first-in-first-out queue per
/// group, with the bookkeeping that decides which group runs next.
pub(crate) struct RunQueues {
    cache: GroupCache,
    /// Indexed by slot in the group table.
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
    /// doing nothing. A task whose group does not exist, which includes one
    /// whose group has been removed, runs in the default group.
    pub(crate) fn push(&mut self, task: Task) {
        // The default group always exists, so its tasks, which are most
        // tasks, skip the lookup.
        let index = match task.group() {
            DEFAULT_GROUP => DEFAULT_GROUP,
            group => self.cache.slot(group).unwrap_or(DEFAULT_GROUP),
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

    /// Whether the group numbered `group` exists.
    pub(crate) fn group_exists(&mut self, group: usize) -> bool {
        self.cache.exists(group)
    }

    /// Chooses the group to run next: the one that has had the least CPU
    /// time relative to its shares. Returns its slot with its current
    /// length.
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
        let generation = self.cache.in_slot(index).map(|info| info.generation);
        let Some(group) = self.groups.get_mut(index) else { return };
        if let Some(generation) = generation {
            if group.generation != generation {
                // Another group has taken the slot since the last charge.
                group.generation = generation;
                group.cpu_nanos = 0;
                group.polled = 0;
            }
        }
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

    /// The name of the group in slot `index`, or an empty string for an empty
    /// slot.
    pub(crate) fn group_name(&mut self, index: usize) -> String {
        self.cache.in_slot(index).map(|info| info.name.clone()).unwrap_or_default()
    }

    /// One [`GroupStats`] per group that has had a task on this shard.
    pub(crate) fn stats(&mut self) -> Vec<GroupStats> {
        let mut stats = Vec::with_capacity(self.groups.len());
        for (index, group) in self.groups.iter().enumerate() {
            let Some(info) = self.cache.in_slot(index) else { continue };
            // Totals left by a removed group that had this slot before are
            // not this group's.
            let (cpu_nanos, polled) = if group.generation == info.generation {
                (group.cpu_nanos, group.polled)
            } else {
                (0, 0)
            };
            stats.push(GroupStats {
                name: info.name.clone(),
                cpu_shares: info.cpu_shares.load(Ordering::Relaxed),
                io_shares: info.io_shares.load(Ordering::Relaxed),
                cpu_time: Duration::from_nanos(cpu_nanos),
                tasks_polled: polled,
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
/// every shard of the runtime that created them, from
/// [`create`](Self::create) until [`remove`](Self::remove), and a runtime
/// can have any number of them.
///
/// The value itself is a small `Copy` handle that can be sent to other
/// shards and kept anywhere. Once the group is removed, every copy of the
/// handle is dead, and stays dead even after other groups are created.
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
    /// Every shard keeps a little state for each group that exists, and
    /// creating one takes a lock shared by all shards. Groups are classes
    /// of work, such as a tenant or a kind of job: made when the class
    /// appears and [removed](Self::remove) when it goes, not one per request
    /// or per connection.
    ///
    /// # Errors
    ///
    /// [`Error::NoRuntime`] outside a runtime. [`Error::InvalidGroup`] if
    /// four thousand million groups exist already.
    pub fn create_with(name: &str, cpu_shares: u32, io_shares: u32) -> Result<SchedulingGroup> {
        let id = current()?
            .runtime
            .groups
            .create(name, cpu_shares, io_shares)
            .ok_or(Error::InvalidGroup)?;
        Ok(SchedulingGroup { id })
    }

    /// Removes the group from every shard, freeing what the runtime keeps
    /// for it. Every copy of this handle stops working: calls on them fail
    /// with [`Error::InvalidGroup`], and so does spawning into the group.
    ///
    /// Tasks still in the group are not stopped. They carry on in the
    /// default group, with its shares, from the next time they are woken,
    /// and so do the tasks they spawn and the file requests they make. To
    /// end a group's work rather than reclassify it, abort or await its
    /// tasks first.
    ///
    /// ```no_run
    /// use starfish::SchedulingGroup;
    ///
    /// # async fn example() -> starfish::Result<()> {
    /// let job = SchedulingGroup::create("import", 200)?;
    /// job.spawn(async { /* the work */ }).await?;
    /// job.remove()?;
    /// # Ok(()) }
    /// ```
    ///
    /// # Errors
    ///
    /// [`Error::InvalidGroup`] if the group has been removed already, does
    /// not belong to the running runtime, or is
    /// [`DEFAULT`](Self::DEFAULT), which cannot be removed.
    /// [`Error::NoRuntime`] outside a runtime.
    pub fn remove(self) -> Result<()> {
        if current()?.runtime.groups.remove(self.id) {
            Ok(())
        } else {
            Err(Error::InvalidGroup)
        }
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
