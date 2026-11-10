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

use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::collections::BTreeMap;
use std::future::Future;
use std::io;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::ptr::NonNull;
use std::rc::Rc;
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
    /// Start of the buffer's memory.
    fn as_ptr(&self) -> *const u8;

    /// Number of initialised bytes.
    fn len(&self) -> usize;

    /// Whether the buffer holds no initialised bytes.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The buffer's slot in the shard's registered-buffer table, if it is
    /// registered memory.
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
    /// Start of the buffer's memory.
    fn as_mut_ptr(&mut self) -> *mut u8;

    /// Number of bytes the buffer has room for.
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
    fn as_ptr(&self) -> *const u8 {
        Vec::as_ptr(self)
    }

    fn len(&self) -> usize {
        Vec::len(self)
    }
}

// SAFETY: as above; `Vec::as_mut_ptr` covers the whole capacity.
unsafe impl IoBufMut for Vec<u8> {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        Vec::as_mut_ptr(self)
    }

    fn capacity(&self) -> usize {
        Vec::capacity(self)
    }

    unsafe fn set_len(&mut self, len: usize) {
        Vec::set_len(self, len);
    }
}

// SAFETY: a boxed slice's allocation does not move with the Box.
unsafe impl IoBuf for Box<[u8]> {
    fn as_ptr(&self) -> *const u8 {
        <[u8]>::as_ptr(self)
    }

    fn len(&self) -> usize {
        <[u8]>::len(self)
    }
}

// SAFETY: a String's heap allocation does not move with the String.
unsafe impl IoBuf for String {
    fn as_ptr(&self) -> *const u8 {
        str::as_ptr(self)
    }

    fn len(&self) -> usize {
        str::len(self)
    }
}

// SAFETY: static memory never moves or goes away.
unsafe impl IoBuf for &'static [u8] {
    fn as_ptr(&self) -> *const u8 {
        <[u8]>::as_ptr(self)
    }

    fn len(&self) -> usize {
        <[u8]>::len(self)
    }
}

// SAFETY: static memory never moves or goes away.
unsafe impl IoBuf for &'static str {
    fn as_ptr(&self) -> *const u8 {
        str::as_ptr(self)
    }

    fn len(&self) -> usize {
        str::len(self)
    }
}

// SAFETY: the buffer's allocation does not move with the handle.
unsafe impl IoBuf for AlignedBuf {
    fn as_ptr(&self) -> *const u8 {
        self.raw_ptr()
    }

    fn len(&self) -> usize {
        AlignedBuf::len(self)
    }
}

// SAFETY: as above; the allocation spans the whole capacity.
unsafe impl IoBufMut for AlignedBuf {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.raw_ptr()
    }

    fn capacity(&self) -> usize {
        AlignedBuf::capacity(self)
    }

    unsafe fn set_len(&mut self, len: usize) {
        AlignedBuf::set_len(self, len);
    }
}

/// The memory behind one shard's registered buffers: `count` buffers of
/// `size` bytes in one page-aligned allocation.
pub(crate) struct Region {
    ptr: NonNull<u8>,
    layout: Layout,
    count: u16,
    size: usize,
}

// SAFETY: the region exclusively owns its allocation. It is created on the
// thread that starts the runtime and moved to its shard before use.
unsafe impl Send for Region {}

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

    /// One entry per buffer, in index order, for registration.
    pub(crate) fn iovecs(&self) -> Vec<libc::iovec> {
        (0..self.count)
            .map(|index| libc::iovec {
                iov_base: self.buffer(index).cast(),
                iov_len: self.size,
            })
            .collect()
    }

    fn buffer(&self, index: u16) -> *mut u8 {
        let offset = usize::from(index.min(self.count.saturating_sub(1))).wrapping_mul(self.size);
        // SAFETY: the index is clamped to the buffers the region holds, so
        // the offset stays inside the allocation.
        unsafe { self.ptr.as_ptr().add(offset) }
    }
}

impl Drop for Region {
    fn drop(&mut self) {
        // SAFETY: allocated in `new` with this layout.
        unsafe { dealloc(self.ptr.as_ptr(), self.layout) };
    }
}

/// One shard's registered buffers.
#[derive(Default)]
pub(crate) struct FixedPool {
    region: Option<Rc<Region>>,
    free: Vec<u16>,
    /// Tasks waiting for a buffer, oldest first.
    waiters: BTreeMap<u64, Waker>,
    next_waiter: u64,
}

impl FixedPool {
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

    fn take(&mut self) -> Option<FixedBuf> {
        let region = self.region.as_ref()?;
        let index = self.free.pop()?;
        Some(FixedBuf {
            region: region.clone(),
            index,
            len: 0,
        })
    }

    /// Puts a buffer back and returns the waker of the task that has
    /// waited longest, for the caller to wake.
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
    /// Takes a buffer from the current shard's pool, or `None` if all are
    /// in use, no pool was configured, or the caller is not on a shard.
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

    /// Number of bytes in the buffer.
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

    /// Replaces the contents with a copy of `data`, as much as fits.
    /// Returns how many bytes were copied.
    pub fn fill_from(&mut self, data: &[u8]) -> usize {
        self.len = data.len().min(self.region.size);
        if let Some(source) = data.get(..self.len) {
            self.copy_from_slice(source);
        }
        self.len
    }
}

impl Deref for FixedBuf {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        // SAFETY: the buffer lies inside the region, which the `Rc` keeps
        // alive; `len` is within its size and the memory is initialised
        // (zeroed at allocation).
        unsafe { std::slice::from_raw_parts(self.region.buffer(self.index), self.len) }
    }
}

impl DerefMut for FixedBuf {
    fn deref_mut(&mut self) -> &mut [u8] {
        // SAFETY: as in `deref`; each index is handed out to one `FixedBuf`
        // at a time, so access is exclusive.
        unsafe { std::slice::from_raw_parts_mut(self.region.buffer(self.index), self.len) }
    }
}

impl Drop for FixedBuf {
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
    fn as_ptr(&self) -> *const u8 {
        self.region.buffer(self.index)
    }

    fn len(&self) -> usize {
        self.len
    }

    fn fixed_index(&self) -> Option<u16> {
        Some(self.index)
    }
}

// SAFETY: as above; the buffer spans `region.size` bytes.
unsafe impl IoBufMut for FixedBuf {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.region.buffer(self.index)
    }

    fn capacity(&self) -> usize {
        self.region.size
    }

    unsafe fn set_len(&mut self, len: usize) {
        FixedBuf::set_len(self, len);
    }
}

/// Future returned by [`FixedBuf::acquire`].
#[must_use = "no buffer is taken unless this is awaited"]
pub struct Acquire {
    /// This task's place in the queue, once it had to wait.
    waiter: Option<u64>,
}

impl Future for Acquire {
    type Output = Result<FixedBuf>;

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
    fn drop(&mut self) {
        let (Some(waiter), Some(shard)) = (self.waiter, try_current()) else {
            return;
        };
        let waker = shard.fixed.with(|pool| pool.waiters.remove(&waiter));
        drop(waker);
    }
}

debug_impl!(FixedBuf, "index" => |buf| buf.index, "len" => |buf| buf.len);
debug_impl!(Acquire);