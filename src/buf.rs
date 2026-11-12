//! Buffers for I/O.
//!
//! An I/O call takes its buffer by value and returns it with the result,
//! because the kernel may keep using the memory after the future is
//! dropped. [`IoBuf`] and [`IoBufMut`] describe what such a buffer must
//! provide; they are implemented for the usual owned byte containers, for
//! [`AlignedBuf`] and for [`FixedBuf`].
//!
//! A [`FixedBuf`] comes from a pool that each shard registers with its
//! io_uring at start-up ([`Runtime::fixed_buffers`](crate::Runtime::fixed_buffers)).
//! The kernel maps and pins registered memory once instead of on every
//! request, and sends from it are zero-copy: the network stack transmits
//! straight from the buffer.
//!
//! A [`RecvBuf`] goes the other way: it is a buffer the kernel chose, from
//! a pool each shard offers it
//! ([`Runtime::recv_buffers`](crate::Runtime::recv_buffers)), and filled
//! with data received on a [`RecvStream`](crate::net::RecvStream).

use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::collections::BTreeMap;
use std::future::Future;
use std::io;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::atomic::{AtomicU16, Ordering};
use std::task::{Context, Poll, Waker};

use crate::error::{Error, Result};
use crate::fs::AlignedBuf;
use crate::runtime::{try_current, wake_quietly};

/// A buffer the kernel can read from.
///
/// # Safety
///
/// `as_ptr` must return a pointer that is valid for reads of `len` bytes,
/// and that stays valid and unchanged when the value is moved, for as long
/// as the value is alive and not mutated.
pub unsafe trait IoBuf: Unpin + 'static {
    /// Start of the buffer's memory. The kernel reads [`len`](Self::len) bytes
    /// from here.
    fn as_ptr(&self) -> *const u8;

    /// Number of initialised bytes: how much a write sends.
    fn len(&self) -> usize;

    /// Whether the buffer holds no initialised bytes. A write of such a buffer
    /// sends nothing.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The buffer's slot in the shard's registered-buffer table, if it is
    /// registered memory, in which case I/O uses the kernel's faster path for
    /// such buffers. `None`, the default, for ordinary memory.
    fn fixed_index(&self) -> Option<u16> {
        None
    }
}

/// A buffer the kernel can write into.
///
/// # Safety
///
/// In addition to the [`IoBuf`] contract, `as_mut_ptr` must return a
/// pointer valid for writes of `capacity` bytes under the same conditions.
pub unsafe trait IoBufMut: IoBuf {
    /// Start of the buffer's memory, for writing. The kernel writes at most
    /// [`capacity`](Self::capacity) bytes from here.
    fn as_mut_ptr(&mut self) -> *mut u8;

    /// Number of bytes the buffer has room for: how much a read may bring. A
    /// buffer with no capacity cannot be read into.
    fn capacity(&self) -> usize;

    /// Declares the first `len` bytes initialised.
    ///
    /// # Safety
    ///
    /// The first `len` bytes must be initialised and `len` must not exceed
    /// the capacity.
    unsafe fn set_len(&mut self, len: usize);
}

// SAFETY: a Vec's heap allocation does not move with the Vec.
unsafe impl IoBuf for Vec<u8> {
    /// The start of the vector's heap allocation.
    fn as_ptr(&self) -> *const u8 {
        Vec::as_ptr(self)
    }

    /// The vector's length.
    fn len(&self) -> usize {
        Vec::len(self)
    }
}

// SAFETY: as above; `Vec::as_mut_ptr` covers the whole capacity.
unsafe impl IoBufMut for Vec<u8> {
    /// The start of the vector's heap allocation, for writing.
    fn as_mut_ptr(&mut self) -> *mut u8 {
        Vec::as_mut_ptr(self)
    }

    /// The vector's capacity, which may be more than was asked for when it
    /// was made.
    fn capacity(&self) -> usize {
        Vec::capacity(self)
    }

    /// Sets the vector's length to what a read brought.
    unsafe fn set_len(&mut self, len: usize) {
        Vec::set_len(self, len);
    }
}

// SAFETY: a boxed slice's allocation does not move with the Box.
unsafe impl IoBuf for Box<[u8]> {
    /// The start of the boxed slice.
    fn as_ptr(&self) -> *const u8 {
        <[u8]>::as_ptr(self)
    }

    /// The slice's length.
    fn len(&self) -> usize {
        <[u8]>::len(self)
    }
}

// SAFETY: a String's heap allocation does not move with the String.
unsafe impl IoBuf for String {
    /// The start of the string's bytes.
    fn as_ptr(&self) -> *const u8 {
        str::as_ptr(self)
    }

    /// The string's length in bytes.
    fn len(&self) -> usize {
        str::len(self)
    }
}

// SAFETY: static memory never moves or goes away.
unsafe impl IoBuf for &'static [u8] {
    /// The start of the static slice.
    fn as_ptr(&self) -> *const u8 {
        <[u8]>::as_ptr(self)
    }

    /// The slice's length.
    fn len(&self) -> usize {
        <[u8]>::len(self)
    }
}

// SAFETY: static memory never moves or goes away.
unsafe impl IoBuf for &'static str {
    /// The start of the static string's bytes.
    fn as_ptr(&self) -> *const u8 {
        str::as_ptr(self)
    }

    /// The string's length in bytes.
    fn len(&self) -> usize {
        str::len(self)
    }
}

// SAFETY: the buffer's allocation does not move with the handle.
unsafe impl IoBuf for AlignedBuf {
    /// The start of the aligned allocation.
    fn as_ptr(&self) -> *const u8 {
        self.raw_ptr()
    }

    /// The buffer's length.
    fn len(&self) -> usize {
        AlignedBuf::len(self)
    }
}

// SAFETY: as above; the allocation spans the whole capacity.
unsafe impl IoBufMut for AlignedBuf {
    /// The start of the aligned allocation, for writing.
    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.raw_ptr()
    }

    /// The buffer's capacity, a whole number of blocks.
    fn capacity(&self) -> usize {
        AlignedBuf::capacity(self)
    }

    /// Sets the buffer's length to what a read brought.
    unsafe fn set_len(&mut self, len: usize) {
        AlignedBuf::set_len(self, len);
    }
}

/// The memory behind one shard's registered buffers: `count` buffers of
/// `size` bytes in one page-aligned allocation.
///
/// It is allocated once, when the runtime starts, and registered with the
/// shard's io_uring, which maps and pins it. It must therefore outlive the
/// ring; the shard keeps it for as long as the reactor exists, and every
/// [`FixedBuf`] holds a reference too.
pub(crate) struct Region {
    ptr: NonNull<u8>,
    layout: Layout,
    count: u16,
    size: usize,
}

// SAFETY: the region exclusively owns its allocation. It is created on the
// thread that starts the runtime and moved to its shard before use.
unsafe impl Send for Region {}

/// Alignment of the region and of each buffer in it: one page. That is
/// what registration wants, and it also satisfies direct I/O.
const REGION_ALIGN: usize = 4096;

impl Region {
    /// Allocates zeroed memory for `count` buffers of at least `size`
    /// bytes each, rounded up to whole pages so that the buffers also suit
    /// direct I/O.
    pub(crate) fn new(count: u16, size: usize) -> io::Result<Region> {
        let invalid = || io::Error::new(io::ErrorKind::InvalidInput, "fixed buffer pool is too large");
        let size = size.max(1).checked_next_multiple_of(REGION_ALIGN).ok_or_else(invalid)?;
        let total = size.checked_mul(usize::from(count.max(1))).ok_or_else(invalid)?;
        let layout = Layout::from_size_align(total, REGION_ALIGN).map_err(|_| invalid())?;
        // SAFETY: the layout has a non-zero size.
        let ptr = unsafe { alloc_zeroed(layout) };
        match NonNull::new(ptr) {
            Some(ptr) => Ok(Region {
                ptr,
                layout,
                count,
                size,
            }),
            None => Err(io::ErrorKind::OutOfMemory.into()),
        }
    }

    /// One entry per buffer, in index order, in the form the kernel takes for
    /// registration. A buffer's index in this list is its index in the kernel's
    /// table, which is what a fixed read or write names.
    pub(crate) fn iovecs(&self) -> Vec<libc::iovec> {
        (0..self.count)
            .map(|index| libc::iovec {
                iov_base: self.buffer(index).cast(),
                iov_len: self.size,
            })
            .collect()
    }

    /// The address of buffer `index`. An index past the end is taken as the
    /// last buffer, so the result always lies inside the region.
    fn buffer(&self, index: u16) -> *mut u8 {
        let offset = usize::from(index.min(self.count.saturating_sub(1))).wrapping_mul(self.size);
        // SAFETY: the index is clamped to the buffers the region holds, so
        // the offset stays inside the allocation.
        unsafe { self.ptr.as_ptr().add(offset) }
    }
}

impl Drop for Region {
    /// Frees the region's memory. By now the ring it was registered with is
    /// closed and no buffer refers to it.
    fn drop(&mut self) {
        // SAFETY: allocated in `new` with this layout.
        unsafe { dealloc(self.ptr.as_ptr(), self.layout) };
    }
}

/// One shard's registered buffers: which are free, and which tasks are
/// waiting for one.
///
/// Waiting tasks are numbered as they arrive, so that the one that has
/// waited longest gets the next buffer to come back.
#[derive(Default)]
pub(crate) struct FixedPool {
    region: Option<Rc<Region>>,
    free: Vec<u16>,
    /// Tasks waiting for a buffer, oldest first.
    waiters: BTreeMap<u64, Waker>,
    next_waiter: u64,
}

impl FixedPool {
    /// A pool over `region`, with every buffer free; or an empty pool, which
    /// never has a buffer, if the runtime was started without one.
    pub(crate) fn new(region: Option<Region>) -> FixedPool {
        let free = region
            .as_ref()
            .map(|region| (0..region.count).rev().collect())
            .unwrap_or_default();
        FixedPool {
            region: region.map(Rc::new),
            free,
            waiters: BTreeMap::new(),
            next_waiter: 0,
        }
    }

    /// Takes a free buffer, if there is one. Buffers are handed out
    /// lowest-index first, so a lightly used pool keeps touching the same
    /// memory.
    fn take(&mut self) -> Option<FixedBuf> {
        let region = self.region.as_ref()?;
        let index = self.free.pop()?;
        Some(FixedBuf {
            region: region.clone(),
            index,
            len: 0,
        })
    }

    /// Puts a buffer back and returns the waker of the task that has waited
    /// longest, for the caller to wake once the pool is no longer borrowed.
    ///
    /// A buffer from some other pool, which can only be one from a runtime that
    /// has since stopped, is ignored.
    fn release(&mut self, region: &Rc<Region>, index: u16) -> Option<Waker> {
        let ours = self.region.as_ref().is_some_and(|own| Rc::ptr_eq(own, region));
        if !ours {
            return None;
        }
        self.free.push(index);
        self.waiters.pop_first().map(|(_, waker)| waker)
    }
}

/// A buffer from the shard's registered pool.
///
/// It works like a fixed-capacity byte vector. I/O with it uses the
/// kernel's registered-buffer path, and socket writes from it are
/// zero-copy. Dropping it returns it to the pool. It cannot leave the shard
/// it was taken on.
pub struct FixedBuf {
    region: Rc<Region>,
    index: u16,
    len: usize,
}

impl FixedBuf {
    /// Takes a buffer from the current shard's pool, or `None` if all are in
    /// use, no pool was configured, or the caller is not on a shard.
    ///
    /// It does not wait and does not join the queue of waiting tasks, so with
    /// tasks waiting in [`acquire`](Self::acquire) it can take a buffer ahead of
    /// them.
    pub fn try_acquire() -> Option<FixedBuf> {
        try_current()?.fixed.with(FixedPool::take).flatten()
    }

    /// Takes a buffer from the current shard's pool, waiting for one to be
    /// returned if all are in use.
    ///
    /// # Errors
    ///
    /// [`Error::NoFixedBuffers`] if the runtime was started without
    /// [`fixed_buffers`](crate::Runtime::fixed_buffers), and
    /// [`Error::NoRuntime`] outside a runtime.
    pub fn acquire() -> Acquire {
        Acquire { waiter: None }
    }

    /// Number of bytes in the buffer: how much a write of it sends.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the buffer holds no bytes.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Number of bytes the buffer has room for: the size given to
    /// [`Runtime::fixed_buffers`](crate::Runtime::fixed_buffers), rounded
    /// up to whole pages.
    pub fn capacity(&self) -> usize {
        self.region.size
    }

    /// Sets the length, clamped to the capacity. Bytes that come into view
    /// keep whatever was last stored there, possibly by an earlier user of
    /// the buffer (zeros in a pool that has not been used).
    pub fn set_len(&mut self, len: usize) {
        self.len = len.min(self.region.size);
    }

    /// Replaces the contents with a copy of `data`, as much as fits, and sets
    /// the length to match. Returns how many bytes were copied, which is less
    /// than `data.len()` if the buffer is smaller.
    pub fn fill_from(&mut self, data: &[u8]) -> usize {
        self.len = data.len().min(self.region.size);
        if let Some(source) = data.get(..self.len) {
            self.copy_from_slice(source);
        }
        self.len
    }
}

impl Deref for FixedBuf {
    /// A fixed buffer reads as a slice of its bytes.
    type Target = [u8];

    /// The buffer's bytes, up to its length.
    fn deref(&self) -> &[u8] {
        // SAFETY: the buffer lies inside the region, which the `Rc` keeps
        // alive; `len` is within its size and the memory is initialised
        // (zeroed at allocation).
        unsafe { std::slice::from_raw_parts(self.region.buffer(self.index), self.len) }
    }
}

impl DerefMut for FixedBuf {
    /// The buffer's bytes, up to its length, for changing in place. To write
    /// beyond the current length, raise it first with
    /// [`set_len`](FixedBuf::set_len).
    fn deref_mut(&mut self) -> &mut [u8] {
        // SAFETY: as in `deref`; each index is handed out to one `FixedBuf`
        // at a time, so access is exclusive.
        unsafe { std::slice::from_raw_parts_mut(self.region.buffer(self.index), self.len) }
    }
}

impl Drop for FixedBuf {
    /// Returns the buffer to its pool and wakes the task that has waited
    /// longest for one. Outside a shard there is no pool to return it to.
    fn drop(&mut self) {
        let Some(shard) = try_current() else { return };
        let waker = shard
            .fixed
            .with(|pool| pool.release(&self.region, self.index))
            .flatten();
        if let Some(waker) = waker {
            wake_quietly(waker);
        }
    }
}

// SAFETY: the buffer's memory belongs to the region, which does not move.
unsafe impl IoBuf for FixedBuf {
    /// The start of the buffer's registered memory.
    fn as_ptr(&self) -> *const u8 {
        self.region.buffer(self.index)
    }

    /// The buffer's length.
    fn len(&self) -> usize {
        self.len
    }

    /// The buffer's index in the kernel's table of registered buffers.
    fn fixed_index(&self) -> Option<u16> {
        Some(self.index)
    }
}

// SAFETY: as above; the buffer spans `region.size` bytes.
unsafe impl IoBufMut for FixedBuf {
    /// The start of the buffer's registered memory, for writing.
    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.region.buffer(self.index)
    }

    /// The buffer's capacity: the pool's buffer size.
    fn capacity(&self) -> usize {
        self.region.size
    }

    /// Sets the buffer's length to what a read brought.
    unsafe fn set_len(&mut self, len: usize) {
        FixedBuf::set_len(self, len);
    }
}

/// Future returned by [`FixedBuf::acquire`]: completes with a buffer as
/// soon as one is free and it is this task's turn.
///
/// Dropping it gives up the place in the queue.
#[must_use = "no buffer is taken unless this is awaited"]
pub struct Acquire {
    /// This task's place in the queue, once it had to wait.
    waiter: Option<u64>,
}

impl Future for Acquire {
    /// The buffer, or why there will never be one.
    type Output = Result<FixedBuf>;

    /// Takes a buffer if one is free; otherwise joins the queue of waiting
    /// tasks, keeping its place if it is already in it.
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(shard) = try_current() else {
            return Poll::Ready(Err(Error::NoRuntime));
        };
        let mut waker = Some(cx.waker().clone());
        let waiter = self.waiter;
        let outcome = shard.fixed.with(|pool| {
            if pool.region.is_none() {
                return Err(Error::NoFixedBuffers);
            }
            if let Some(buf) = pool.take() {
                let previous = waiter.and_then(|waiter| pool.waiters.remove(&waiter));
                return Ok((Some(buf), None, previous));
            }
            let id = waiter.unwrap_or_else(|| {
                let id = pool.next_waiter;
                pool.next_waiter = pool.next_waiter.wrapping_add(1);
                id
            });
            let previous = waker.take().and_then(|waker| pool.waiters.insert(id, waker));
            Ok((None, Some(id), previous))
        });
        match outcome {
            Some(Ok((Some(buf), _, previous))) => {
                drop(previous);
                self.waiter = None;
                Poll::Ready(Ok(buf))
            }
            Some(Ok((None, id, previous))) => {
                drop(previous);
                self.waiter = id;
                Poll::Pending
            }
            Some(Err(err)) => Poll::Ready(Err(err)),
            None => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

impl Drop for Acquire {
    /// Leaves the queue of tasks waiting for a buffer.
    fn drop(&mut self) {
        let (Some(waiter), Some(shard)) = (self.waiter, try_current()) else {
            return;
        };
        let waker = shard.fixed.with(|pool| pool.waiters.remove(&waiter));
        drop(waker);
    }
}

debug_impl!(FixedBuf, "index" => |buf| buf.index, "len" => |buf| buf.len);

/// The memory of a ring through which one shard hands receive buffers to
/// the kernel: an array of entries, each naming one buffer, that both
/// sides read and only this side writes.
///
/// The kernel takes buffers from the front as data arrives; the shard adds
/// buffers at the back as the application returns them. Unlike the
/// registered buffers of a [`FixedBuf`], which the application picks, these
/// are picked by the kernel at the moment data arrives, which is what lets
/// one request receive again and again.
struct RingMemory {
    ptr: NonNull<u8>,
    layout: Layout,
    /// Number of entries; a power of two.
    entries: u16,
}

// SAFETY: the memory is exclusively owned. It is allocated on the thread
// that starts the runtime and moved to its shard before use.
unsafe impl Send for RingMemory {}

impl Drop for RingMemory {
    /// Frees the ring's memory. By now the io_uring it was registered with
    /// is closed.
    fn drop(&mut self) {
        // SAFETY: allocated in `RecvParts::new` with this layout.
        unsafe { dealloc(self.ptr.as_ptr(), self.layout) };
    }
}

/// What a shard's receive-buffer pool is made of, before the shard's
/// thread takes it over: the buffers and the ring that offers them to the
/// kernel.
pub(crate) struct RecvParts {
    region: Region,
    ring: RingMemory,
}

/// The most buffers a receive pool can have: the largest ring the kernel
/// accepts.
const MAX_RECV_BUFFERS: u16 = 1 << 15;

impl RecvParts {
    /// Allocates `count` buffers of at least `size` bytes (rounded up to
    /// whole pages) and a ring with room to offer all of them.
    pub(crate) fn new(count: u16, size: usize) -> io::Result<RecvParts> {
        let invalid = || io::Error::new(io::ErrorKind::InvalidInput, "receive buffer pool is too large");
        if count > MAX_RECV_BUFFERS {
            return Err(invalid());
        }
        let region = Region::new(count, size)?;
        if u32::try_from(region.size).is_err() {
            return Err(invalid());
        }
        let entries = count.max(1).checked_next_power_of_two().ok_or_else(invalid)?;
        let bytes = usize::from(entries)
            .checked_mul(std::mem::size_of::<io_uring::types::BufRingEntry>())
            .and_then(|bytes| bytes.checked_next_multiple_of(REGION_ALIGN))
            .ok_or_else(invalid)?;
        let layout = Layout::from_size_align(bytes, REGION_ALIGN).map_err(|_| invalid())?;
        // SAFETY: the layout has a non-zero size.
        let ptr = unsafe { alloc_zeroed(layout) };
        let ptr = NonNull::new(ptr).ok_or(io::ErrorKind::OutOfMemory)?;
        Ok(RecvParts {
            region,
            ring: RingMemory { ptr, layout, entries },
        })
    }

    /// The ring's address and its number of entries, as the kernel wants
    /// them for registration.
    pub(crate) fn ring(&self) -> (u64, u16) {
        (self.ring.ptr.as_ptr() as u64, self.ring.entries)
    }
}

/// One shard's receive buffers and the ring that offers them to the
/// kernel.
///
/// Every buffer is in one of three places: on offer in the ring, lent to
/// the application as a [`RecvBuf`] (or waiting in a receive stream's queue
/// to become one), or, briefly, in the kernel's hands on its way from the
/// first to the second. The pool counts the lent ones, because when all are
/// lent the kernel has nothing to receive into and receive streams have to
/// wait for one to come back.
pub(crate) struct RecvRing {
    region: Rc<Region>,
    ring: RingMemory,
    /// Where the next buffer goes in the ring; only ever counts up, wrapping.
    tail: u16,
    /// Buffers the kernel has filled that have not been given back yet.
    lent: usize,
    /// Receive streams that found every buffer lent.
    waiters: Vec<Waker>,
}

impl RecvRing {
    /// Takes over the pool's memory and offers every buffer to the kernel.
    pub(crate) fn new(parts: RecvParts) -> RecvRing {
        let RecvParts { region, ring } = parts;
        let count = region.count;
        let mut pool = RecvRing {
            region: Rc::new(region),
            ring,
            tail: 0,
            lent: 0,
            waiters: Vec::new(),
        };
        for index in 0..count {
            pool.offer(index);
        }
        pool
    }

    /// Puts buffer `index` at the back of the ring, where the kernel will
    /// find it.
    pub(crate) fn offer(&mut self, index: u16) {
        let mask = self.ring.entries.wrapping_sub(1);
        let base = self.ring.ptr.as_ptr().cast::<io_uring::types::BufRingEntry>();
        // SAFETY: the slot is inside the ring's allocation, since the mask
        // keeps it below the number of entries. The kernel does not read a
        // slot until the tail has moved past it, and nothing else writes
        // the ring, so the write races with nothing. The tail is a field
        // of the first entry that the kernel only reads; it is written
        // atomically because the kernel may read it at any moment.
        unsafe {
            let entry = base.add(usize::from(self.tail & mask));
            (*entry).set_addr(self.region.buffer(index) as u64);
            (*entry).set_len(self.region.size as u32);
            (*entry).set_bid(index);
            self.tail = self.tail.wrapping_add(1);
            let tail = io_uring::types::BufRingEntry::tail(base).cast::<AtomicU16>();
            (*tail).store(self.tail, Ordering::Release);
        }
    }

    /// Notes that the kernel has filled a buffer and handed it over.
    pub(crate) fn lend(&mut self) {
        self.lent = self.lent.saturating_add(1);
    }

    /// Takes back a buffer that was lent and offers it to the kernel again.
    /// Appends to `wake` the receive streams that were waiting for one.
    ///
    /// A buffer of some other pool, which can only be one from a runtime
    /// that has since stopped, is ignored.
    pub(crate) fn give_back(&mut self, region: &Rc<Region>, index: u16, wake: &mut Vec<Waker>) {
        if !Rc::ptr_eq(&self.region, region) || index >= self.region.count {
            return;
        }
        self.lent = self.lent.saturating_sub(1);
        self.offer(index);
        wake.append(&mut self.waiters);
    }

    /// Whether the kernel has, or is about to have, a buffer to receive
    /// into: not every buffer is lent.
    pub(crate) fn available(&self) -> bool {
        self.lent < usize::from(self.region.count)
    }

    /// Asks for `waker` to be woken when a buffer comes back.
    pub(crate) fn wait(&mut self, waker: Waker) {
        self.waiters.push(waker);
    }

    /// The buffers' memory, for making a [`RecvBuf`].
    pub(crate) fn region(&self) -> &Rc<Region> {
        &self.region
    }
}

/// Data received through a [`RecvStream`](crate::net::RecvStream): one of
/// the shard's receive buffers, holding what one receive brought.
///
/// It reads as a byte slice, and can be changed in place. Dropping it gives
/// the buffer back to the pool, which the kernel then receives into again,
/// so hold on to these no longer than the data is needed: while every
/// buffer of the pool is held, nothing is received on the shard's receive
/// streams. To keep the bytes, copy them out.
///
/// It can be sent as it is, without copying, by passing it to a write call
/// such as [`TcpStream::write_all`](crate::net::TcpStream::write_all). It
/// cannot leave the shard it was received on.
pub struct RecvBuf {
    region: Rc<Region>,
    index: u16,
    len: usize,
}

impl RecvBuf {
    /// Wraps buffer `index` of `region`, of which the kernel filled the
    /// first `len` bytes.
    pub(crate) fn new(region: Rc<Region>, index: u16, len: usize) -> RecvBuf {
        let len = len.min(region.size);
        RecvBuf { region, index, len }
    }

    /// Number of bytes received.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the buffer holds no bytes. A receive stream never returns
    /// such a buffer: the end of the stream is `None`.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Shortens the buffer to its first `len` bytes; no effect if it is
    /// already that short. Useful before sending part of it back.
    pub fn truncate(&mut self, len: usize) {
        self.len = self.len.min(len);
    }
}

impl Deref for RecvBuf {
    /// A receive buffer reads as a slice of the bytes received.
    type Target = [u8];

    /// The bytes received.
    fn deref(&self) -> &[u8] {
        // SAFETY: the buffer lies inside the region, which the `Rc` keeps
        // alive; `len` is within its size and the memory is initialised
        // (zeroed at allocation). The kernel does not touch a buffer
        // between handing it over and being offered it again, which
        // happens when this value is dropped.
        unsafe { std::slice::from_raw_parts(self.region.buffer(self.index), self.len) }
    }
}

impl DerefMut for RecvBuf {
    /// The bytes received, for changing in place.
    fn deref_mut(&mut self) -> &mut [u8] {
        // SAFETY: as in `deref`; a buffer is lent to one `RecvBuf` at a
        // time, so access is exclusive.
        unsafe { std::slice::from_raw_parts_mut(self.region.buffer(self.index), self.len) }
    }
}

impl Drop for RecvBuf {
    /// Gives the buffer back to the shard's pool, for the kernel to receive
    /// into again, and wakes the receive streams that were waiting for a
    /// buffer. Outside a shard there is no pool to give it to.
    fn drop(&mut self) {
        let Some(shard) = try_current() else { return };
        let wakers = shard
            .reactor
            .with(|reactor| reactor.recv_give_back(&self.region, self.index));
        for waker in wakers.unwrap_or_default() {
            wake_quietly(waker);
        }
    }
}

// SAFETY: the buffer's memory belongs to the region, which does not move
// and which the value keeps alive.
unsafe impl IoBuf for RecvBuf {
    /// The start of the received bytes.
    fn as_ptr(&self) -> *const u8 {
        self.region.buffer(self.index)
    }

    /// The number of bytes received.
    fn len(&self) -> usize {
        self.len
    }
}

debug_impl!(RecvBuf, "index" => |buf| buf.index, "len" => |buf| buf.len);

debug_impl!(Acquire);