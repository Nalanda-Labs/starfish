//! Files, read and written at explicit offsets.
//!
//! [`File`] goes through the kernel page cache. [`DmaFile`] bypasses it
//! (`O_DIRECT`) and reads and writes whole blocks through an
//! [`AlignedBuf`], which is what a storage engine that manages its own
//! cache wants.
//!
//! Reads, writes and syncs on both types pass through the shard's disk I/O
//! scheduler. Each device has its own queue. A shard keeps a bounded number
//! of requests per device in the kernel
//! ([`Runtime::io_depth`](crate::Runtime::io_depth)); if the device's
//! capacity was configured
//! ([`Runtime::io_properties`](crate::Runtime::io_properties)), all shards
//! together send it no more than it can sustain. Requests that have to wait
//! are served by [scheduling group](crate::SchedulingGroup) I/O shares.

use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::ffi::{CStr, CString};
use std::io;
use std::ops::{Deref, DerefMut};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::ptr::NonNull;

use io_uring::{opcode, types};

use crate::buf::{IoBuf, IoBufMut};
use crate::ioqueue::Direction;
use crate::reactor::Op;

/// The alignment [`AlignedBuf::new`] uses, and the one assumed for a file
/// whose filesystem does not report its direct-I/O requirements. It
/// satisfies devices with 512-byte and 4096-byte logical blocks.
pub const DMA_ALIGNMENT: usize = 4096;

const STATX_DIOALIGN: u32 = 0x2000;
static EMPTY_PATH: &CStr = c"";

fn clamp_len(len: usize) -> u32 {
    u32::try_from(len).unwrap_or(u32::MAX)
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn is_multiple(value: u64, of: usize) -> bool {
    value.checked_rem(of as u64) == Some(0)
}

fn c_path(path: &Path) -> io::Result<CString> {
    CString::new(path.as_os_str().as_bytes()).map_err(|_| invalid("path contains a NUL byte"))
}

async fn open_fd(path: &Path, flags: i32, mode: u32) -> io::Result<OwnedFd> {
    let path = c_path(path)?;
    let sqe = opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), path.as_ptr())
        .flags(flags | libc::O_CLOEXEC)
        .mode(mode)
        .build();
    // SAFETY: the pointer targets the CString's heap allocation.
    let (result, _path) = unsafe { Op::new(sqe, path) }.result_is_fd().await;
    let fd = RawFd::try_from(result?).map_err(|_| io::Error::other("invalid file descriptor"))?;
    // SAFETY: the kernel returned a new fd that nothing else owns.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// What the kernel says about an open file.
struct FileInfo {
    /// Device number of the filesystem the file is on.
    device: u64,
    /// Required alignment of buffer memory for direct I/O.
    memory_alignment: usize,
    /// Required alignment of offsets and lengths for direct I/O.
    disk_alignment: usize,
}

async fn stat_fd(fd: &OwnedFd) -> io::Result<FileInfo> {
    // A Vec gives a heap slot whose address survives being moved into the
    // operation; the kernel fills it in.
    let mut buf: Vec<libc::statx> = Vec::with_capacity(1);
    let sqe = opcode::Statx::new(
        types::Fd(fd.as_raw_fd()),
        EMPTY_PATH.as_ptr(),
        buf.as_mut_ptr().cast(),
    )
    .flags(libc::AT_EMPTY_PATH)
    .mask(libc::STATX_BASIC_STATS | STATX_DIOALIGN)
    .build();
    // SAFETY: one pointer targets the Vec's heap allocation, which the op
    // owns; the other targets a static string.
    let (result, mut buf) = unsafe { Op::new(sqe, buf) }.await;
    result?;
    // SAFETY: on success the kernel initialised the one element.
    unsafe { buf.set_len(1) };
    let stat = buf.first().ok_or_else(|| io::Error::other("statx returned nothing"))?;

    #[allow(unused_unsafe)]
    // SAFETY: pure arithmetic on two integers.
    let device = unsafe { libc::makedev(stat.stx_dev_major, stat.stx_dev_minor) };
    let reported = |value: u32| {
        let known = stat.stx_mask & STATX_DIOALIGN != 0 && value != 0;
        if known {
            usize::try_from(value).unwrap_or(DMA_ALIGNMENT)
        } else {
            DMA_ALIGNMENT
        }
    };
    Ok(FileInfo {
        device,
        memory_alignment: reported(stat.stx_dio_mem_align),
        disk_alignment: reported(stat.stx_dio_offset_align),
    })
}

async fn fsync(fd: &OwnedFd, device: u64) -> io::Result<()> {
    let sqe = opcode::Fsync::new(types::Fd(fd.as_raw_fd())).build();
    // SAFETY: the entry carries no pointers.
    let (result, ()) = unsafe { Op::new_disk(sqe, (), device, Direction::Write, 0) }.await;
    result.map(|_| ())
}

/// Reads up to `len` bytes at `offset` into the start of `buf`. With a
/// device the request goes through the I/O scheduler; without one it goes
/// straight to the kernel.
async fn read_into<B: IoBufMut>(
    fd: &OwnedFd,
    device: Option<u64>,
    mut buf: B,
    len: u32,
    offset: u64,
) -> (io::Result<usize>, B) {
    let target = types::Fd(fd.as_raw_fd());
    let ptr = buf.as_mut_ptr();
    let sqe = match buf.fixed_index() {
        Some(index) => opcode::ReadFixed::new(target, ptr, len, index).offset(offset).build(),
        None => opcode::Read::new(target, ptr, len).offset(offset).build(),
    };
    // SAFETY: the pointer targets memory the buffer keeps valid and in place
    // for as long as the op holds the buffer (`IoBufMut`), and `len` is
    // within its capacity.
    let op = unsafe {
        match device {
            Some(device) => Op::new_disk(sqe, buf, device, Direction::Read, u64::from(len)),
            None => Op::new(sqe, buf),
        }
    };
    let (result, mut buf) = op.await;
    match result {
        Ok(n) => {
            let n = (n as usize).min(buf.capacity());
            // SAFETY: the kernel initialised the first `n` bytes, and `n` is
            // within the capacity.
            unsafe { buf.set_len(n) };
            (Ok(n), buf)
        }
        Err(err) => (Err(err), buf),
    }
}

/// Writes `len` bytes of `buf`, starting `start` bytes in, at `offset`.
async fn write_from<B: IoBuf>(
    fd: &OwnedFd,
    device: Option<u64>,
    buf: B,
    start: usize,
    len: u32,
    offset: u64,
) -> (io::Result<usize>, B) {
    let target = types::Fd(fd.as_raw_fd());
    let start = start.min(buf.len());
    let len = len.min(clamp_len(buf.len().saturating_sub(start)));
    // Stays inside the buffer: `start <= len`.
    let ptr = buf.as_ptr().wrapping_add(start);
    let sqe = match buf.fixed_index() {
        Some(index) => opcode::WriteFixed::new(target, ptr, len, index).offset(offset).build(),
        None => opcode::Write::new(target, ptr, len).offset(offset).build(),
    };
    // SAFETY: the pointer targets memory the buffer keeps valid and in place
    // for as long as the op holds the buffer (`IoBuf`), and the range is
    // within its length.
    let op = unsafe {
        match device {
            Some(device) => Op::new_disk(sqe, buf, device, Direction::Write, u64::from(len)),
            None => Op::new(sqe, buf),
        }
    };
    let (result, buf) = op.await;
    (result.map(|n| n as usize), buf)
}

/// Deletes a file.
pub async fn remove_file(path: impl AsRef<Path>) -> io::Result<()> {
    let path = c_path(path.as_ref())?;
    let sqe = opcode::UnlinkAt::new(types::Fd(libc::AT_FDCWD), path.as_ptr()).build();
    // SAFETY: the pointer targets the CString's heap allocation.
    let (result, _path) = unsafe { Op::new(sqe, path) }.await;
    result.map(|_| ())
}

/// An open file. There is no cursor: every read and write names its offset,
/// so concurrent operations on one file do not interfere.
pub struct File {
    fd: OwnedFd,
    device: u64,
}

impl File {
    /// Opens an existing file for reading.
    pub async fn open(path: impl AsRef<Path>) -> io::Result<File> {
        File::open_with(path, libc::O_RDONLY, 0).await
    }

    /// Opens a file for reading and writing, creating or truncating it.
    pub async fn create(path: impl AsRef<Path>) -> io::Result<File> {
        File::open_with(path, libc::O_RDWR | libc::O_CREAT | libc::O_TRUNC, 0o644).await
    }

    /// Opens a file with raw `open(2)` flags and creation mode.
    pub async fn open_with(path: impl AsRef<Path>, flags: i32, mode: u32) -> io::Result<File> {
        let fd = open_fd(path.as_ref(), flags, mode).await?;
        let info = stat_fd(&fd).await?;
        Ok(File {
            fd,
            device: info.device,
        })
    }

    /// Reads at `offset` into `buf` from its start, replacing its contents:
    /// on success the buffer holds exactly the bytes read, at most its
    /// capacity. `Ok(0)` means end of file.
    pub async fn read_at<B: IoBufMut>(&self, buf: B, offset: u64) -> (io::Result<usize>, B) {
        let len = clamp_len(buf.capacity());
        read_into(&self.fd, Some(self.device), buf, len, offset).await
    }

    /// Writes some prefix of `buf` at `offset` and returns how many bytes
    /// were written.
    pub async fn write_at<B: IoBuf>(&self, buf: B, offset: u64) -> (io::Result<usize>, B) {
        let len = clamp_len(buf.len());
        write_from(&self.fd, Some(self.device), buf, 0, len, offset).await
    }

    /// Writes all of `buf` at `offset`.
    pub async fn write_all_at<B: IoBuf>(&self, mut buf: B, offset: u64) -> (io::Result<()>, B) {
        let mut written = 0usize;
        while written < buf.len() {
            let at = offset.saturating_add(written as u64);
            let len = clamp_len(buf.len().saturating_sub(written));
            let (result, returned) =
                write_from(&self.fd, Some(self.device), buf, written, len, at).await;
            buf = returned;
            match result {
                Ok(0) => return (Err(io::ErrorKind::WriteZero.into()), buf),
                Ok(n) => written = written.saturating_add(n),
                Err(err) => return (Err(err), buf),
            }
        }
        (Ok(()), buf)
    }

    /// Flushes file data and metadata to the device.
    pub async fn sync_all(&self) -> io::Result<()> {
        fsync(&self.fd, self.device).await
    }
}

/// A heap buffer with aligned memory and a capacity that is a whole number
/// of blocks, as direct I/O requires.
///
/// It has a fixed capacity and a length within it, like a `Vec<u8>` that
/// never reallocates. The memory starts out zeroed.
pub struct AlignedBuf {
    ptr: NonNull<u8>,
    capacity: usize,
    len: usize,
    alignment: usize,
}

// SAFETY: the buffer exclusively owns its allocation, like a `Vec<u8>`.
unsafe impl Send for AlignedBuf {}
// SAFETY: shared access only reads the bytes.
unsafe impl Sync for AlignedBuf {}

impl AlignedBuf {
    /// Allocates an empty buffer aligned to [`DMA_ALIGNMENT`]. To match a
    /// particular file exactly, use [`DmaFile::alloc`].
    pub fn new(capacity: usize) -> io::Result<AlignedBuf> {
        AlignedBuf::with_alignment(capacity, DMA_ALIGNMENT)
    }

    /// Allocates an empty buffer whose memory is aligned to `alignment` and
    /// whose capacity is `capacity` rounded up to a multiple of `alignment`
    /// (and at least one such block).
    ///
    /// # Errors
    ///
    /// `InvalidInput` if `alignment` is not a power of two or the size is
    /// not representable, `OutOfMemory` if the allocation fails.
    pub fn with_alignment(capacity: usize, alignment: usize) -> io::Result<AlignedBuf> {
        if !alignment.is_power_of_two() {
            return Err(invalid("alignment is not a power of two"));
        }
        let capacity = capacity
            .max(1)
            .checked_next_multiple_of(alignment)
            .ok_or_else(|| invalid("buffer capacity is too large"))?;
        let layout = Layout::from_size_align(capacity, alignment)
            .map_err(|_| invalid("buffer capacity is too large"))?;
        // SAFETY: the layout has a non-zero size.
        let ptr = unsafe { alloc_zeroed(layout) };
        match NonNull::new(ptr) {
            Some(ptr) => Ok(AlignedBuf {
                ptr,
                capacity,
                len: 0,
                alignment,
            }),
            None => Err(io::ErrorKind::OutOfMemory.into()),
        }
    }

    /// Allocates a buffer holding a copy of `data`, zero-padded to a whole
    /// number of [`DMA_ALIGNMENT`] blocks.
    pub fn padded_from(data: &[u8]) -> io::Result<AlignedBuf> {
        let mut buf = AlignedBuf::new(data.len())?;
        buf.fill_padded(data);
        Ok(buf)
    }

    /// Replaces the contents with `data` (as much as fits), zero-padded to
    /// the whole capacity.
    pub fn fill_padded(&mut self, data: &[u8]) {
        self.len = self.capacity;
        let split = data.len().min(self.capacity);
        let (head, tail) = self.split_at_mut(split);
        if let Some(source) = data.get(..head.len()) {
            head.copy_from_slice(source);
        }
        tail.fill(0);
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// The alignment of the buffer's memory.
    pub fn alignment(&self) -> usize {
        self.alignment
    }

    /// Start of the allocation, valid for the whole capacity.
    pub(crate) fn raw_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    /// Sets the length, clamped to the capacity. Bytes that come into view
    /// keep whatever was last stored there (zeros in a fresh buffer).
    pub fn set_len(&mut self, len: usize) {
        self.len = len.min(self.capacity);
    }
}

impl Deref for AlignedBuf {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        // SAFETY: `len <= capacity`, and all `capacity` bytes are allocated
        // and initialised (zeroed at allocation, only overwritten since).
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl DerefMut for AlignedBuf {
    fn deref_mut(&mut self) -> &mut [u8] {
        // SAFETY: as in `deref`, and `&mut self` gives exclusive access.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        if let Ok(layout) = Layout::from_size_align(self.capacity, self.alignment) {
            // SAFETY: allocated in `with_alignment` with this exact layout.
            unsafe { dealloc(self.ptr.as_ptr(), layout) };
        }
    }
}

/// A file opened for direct I/O: reads and writes go straight to the
/// device, bypassing the kernel page cache.
///
/// The device dictates how buffers, offsets and lengths must be aligned.
/// The file asks the kernel for these requirements when it is opened and
/// reports them as [`memory_alignment`](Self::memory_alignment) and
/// [`disk_alignment`](Self::disk_alignment); [`alloc`](Self::alloc) returns
/// a buffer that satisfies both. A call that breaks them fails with
/// `InvalidInput` before reaching the kernel.
pub struct DmaFile {
    fd: OwnedFd,
    device: u64,
    memory_alignment: usize,
    disk_alignment: usize,
}

impl DmaFile {
    /// Opens an existing file for reading.
    pub async fn open(path: impl AsRef<Path>) -> io::Result<DmaFile> {
        DmaFile::open_with(path, libc::O_RDONLY, 0).await
    }

    /// Opens a file for reading and writing, creating or truncating it.
    pub async fn create(path: impl AsRef<Path>) -> io::Result<DmaFile> {
        DmaFile::open_with(path, libc::O_RDWR | libc::O_CREAT | libc::O_TRUNC, 0o644).await
    }

    /// Opens a file with raw `open(2)` flags (plus `O_DIRECT`) and creation
    /// mode. Fails with `EINVAL` on filesystems without direct I/O.
    pub async fn open_with(path: impl AsRef<Path>, flags: i32, mode: u32) -> io::Result<DmaFile> {
        let fd = open_fd(path.as_ref(), flags | libc::O_DIRECT, mode).await?;
        let info = stat_fd(&fd).await?;
        Ok(DmaFile {
            fd,
            device: info.device,
            memory_alignment: info.memory_alignment,
            disk_alignment: info.disk_alignment,
        })
    }

    /// The alignment buffer memory must have. Taken from the kernel
    /// (`STATX_DIOALIGN`); [`DMA_ALIGNMENT`] if the filesystem does not
    /// report it.
    pub fn memory_alignment(&self) -> usize {
        self.memory_alignment
    }

    /// The block size that offsets and lengths must be multiples of. Taken
    /// from the kernel (`STATX_DIOALIGN`); [`DMA_ALIGNMENT`] if the
    /// filesystem does not report it.
    pub fn disk_alignment(&self) -> usize {
        self.disk_alignment
    }

    /// Allocates an empty buffer suitable for this file, with room for at
    /// least `capacity` bytes rounded up to whole blocks.
    pub fn alloc(&self, capacity: usize) -> io::Result<AlignedBuf> {
        AlignedBuf::with_alignment(capacity, self.memory_alignment.max(self.disk_alignment))
    }

    fn check(&self, ptr: *const u8, offset: u64, len: usize) -> io::Result<u32> {
        if !is_multiple(ptr as usize as u64, self.memory_alignment) {
            return Err(invalid("buffer memory is not aligned for this file"));
        }
        if !is_multiple(offset, self.disk_alignment) {
            return Err(invalid("offset is not block-aligned"));
        }
        if !is_multiple(len as u64, self.disk_alignment) {
            return Err(invalid("length is not a whole number of blocks"));
        }
        u32::try_from(len).map_err(|_| invalid("buffer is larger than one request allows"))
    }

    /// Reads at `offset`, replacing the buffer's contents, as many whole
    /// blocks as fit in its capacity. A short read means end of file.
    ///
    /// Any buffer with suitably aligned memory works: an [`AlignedBuf`] or
    /// a [`FixedBuf`](crate::buf::FixedBuf).
    pub async fn read_at<B: IoBufMut>(&self, buf: B, offset: u64) -> (io::Result<usize>, B) {
        self.read_impl(buf, offset, true).await
    }

    /// Writes the buffer's contents at `offset`. The buffer's length must be
    /// a whole number of blocks. Returns how many bytes were written, which
    /// can be short only if the device runs out of space.
    pub async fn write_at<B: IoBuf>(&self, buf: B, offset: u64) -> (io::Result<usize>, B) {
        self.write_impl(buf, offset, true).await
    }

    /// `scheduled` is false only for capacity measurement, which must see
    /// the device itself, not the scheduler.
    pub(crate) async fn read_impl<B: IoBufMut>(
        &self,
        mut buf: B,
        offset: u64,
        scheduled: bool,
    ) -> (io::Result<usize>, B) {
        // The largest whole number of blocks that fits.
        let spare = buf.capacity().checked_rem(self.disk_alignment).unwrap_or(0);
        let len = buf.capacity().saturating_sub(spare);
        match self.check(buf.as_mut_ptr(), offset, len) {
            Ok(len) => read_into(&self.fd, scheduled.then_some(self.device), buf, len, offset).await,
            Err(err) => (Err(err), buf),
        }
    }

    pub(crate) async fn write_impl<B: IoBuf>(
        &self,
        buf: B,
        offset: u64,
        scheduled: bool,
    ) -> (io::Result<usize>, B) {
        match self.check(buf.as_ptr(), offset, buf.len()) {
            Ok(len) => {
                write_from(&self.fd, scheduled.then_some(self.device), buf, 0, len, offset).await
            }
            Err(err) => (Err(err), buf),
        }
    }

    /// Flushes file data and metadata to the device.
    pub async fn sync_all(&self) -> io::Result<()> {
        fsync(&self.fd, self.device).await
    }
}
