//! Files, read and written at explicit offsets, and the directories they
//! live in.
//!
//! [`File`] goes through the kernel page cache. [`DmaFile`] bypasses it
//! (`O_DIRECT`) and reads and writes whole blocks through an
//! [`AlignedBuf`], which is what a storage engine that manages its own
//! cache wants. [`DmaStreamWriter`] and [`DmaStreamReader`] do the
//! blocking-up for data that is written and read from front to back.
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
use std::ffi::{CStr, CString, OsStr};
use std::io;
use std::ops::{Deref, DerefMut};
use std::ffi::OsString;
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::ptr::NonNull;

use io_uring::{opcode, types};

use crate::buf::{IoBuf, IoBufMut};
use crate::ioqueue::Direction;
use crate::reactor::Op;

/// The alignment [`AlignedBuf::new`] uses, and the one assumed for a file
/// whose filesystem does not report its direct-I/O requirements. It
/// satisfies devices with 512-byte and 4096-byte logical blocks.
pub const DMA_ALIGNMENT: usize = 4096;

/// The `statx` mask bit that asks for a file's direct-I/O alignment
/// requirements. Kernels from 6.1 answer it; older ones leave it unset in
/// the reply.
const STATX_DIOALIGN: u32 = 0x2000;
/// An empty path, for `statx` on an open descriptor.
static EMPTY_PATH: &CStr = c"";

/// A length as the 32-bit number one io_uring operation takes, clamped to
/// the largest it can express.
fn clamp_len(len: usize) -> u32 {
    u32::try_from(len).unwrap_or(u32::MAX)
}

/// An `InvalidInput` error with the given message: the caller asked for
/// something that cannot be done, and the kernel was not troubled with it.
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

/// Whether `value` is a whole multiple of `of`. False if `of` is zero.
fn is_multiple(value: u64, of: usize) -> bool {
    value.checked_rem(of as u64) == Some(0)
}

/// A path as the NUL-terminated string the kernel takes. Fails for a path
/// with a NUL byte inside, which no file can have.
fn c_path(path: &Path) -> io::Result<CString> {
    CString::new(path.as_os_str().as_bytes()).map_err(|_| invalid("path contains a NUL byte"))
}

/// An open file's descriptor.
///
/// Closing a file can block: on some filesystems it writes data out or
/// waits for a server. So a dropped descriptor is closed through the
/// shard's io_uring, with nobody waiting for the result, and only closed
/// on the spot when there is no shard to do it.
struct FileFd(RawFd);

impl AsRawFd for FileFd {
    /// The descriptor's number.
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}

impl Drop for FileFd {
    /// Closes the descriptor through the shard's ring, without waiting.
    fn drop(&mut self) {
        crate::reactor::close_fd(self.0);
    }
}

/// Closes a file and reports what the kernel said about it.
///
/// The descriptor is taken out of its owner first, so that it is closed
/// exactly once, by the operation here. If the operation never reaches the
/// kernel, the descriptor is closed directly instead.
async fn close_fd(fd: FileFd) -> io::Result<()> {
    let raw = fd.0;
    // From here on the descriptor is closed by the operation below.
    std::mem::forget(fd);
    let sqe = opcode::Close::new(types::Fd(raw)).build();
    // SAFETY: the entry carries no pointers.
    let (result, ()) = unsafe { Op::new(sqe, ()) }.await;
    match result {
        Ok(_) => Ok(()),
        // An error with no error number is the runtime's own: the request
        // never reached the kernel, so the descriptor is still open.
        Err(err) if err.raw_os_error().is_none() => {
            // SAFETY: the descriptor is ours and nothing else closes it.
            match unsafe { libc::close(raw) } {
                0 => Ok(()),
                _ => Err(io::Error::last_os_error()),
            }
        }
        // The kernel closed the descriptor even though it reports an
        // error, typically that data written earlier could not be stored.
        Err(err) => Err(err),
    }
}

/// Opens `path` with the given flags and mode, close-on-exec, and returns
/// its descriptor. If the future is dropped as the open completes, the
/// reactor closes the descriptor.
async fn open_fd(path: &Path, flags: i32, mode: u32) -> io::Result<FileFd> {
    let path = c_path(path)?;
    let sqe = opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), path.as_ptr())
        .flags(flags | libc::O_CLOEXEC)
        .mode(mode)
        .build();
    // SAFETY: the pointer targets the CString's heap allocation.
    let (result, _path) = unsafe { Op::new(sqe, path) }.result_is_fd().await;
    let fd = RawFd::try_from(result?).map_err(|_| io::Error::other("invalid file descriptor"))?;
    Ok(FileFd(fd))
}

/// What the kernel says about an open file: which device it is on, how
/// direct I/O on it must be aligned, and how long it is.
struct FileInfo {
    /// Device number of the filesystem the file is on.
    device: u64,
    /// Required alignment of buffer memory for direct I/O.
    memory_alignment: usize,
    /// Required alignment of offsets and lengths for direct I/O.
    disk_alignment: usize,
    /// Length of the file in bytes.
    size: u64,
}

/// Asks the kernel about an open file.
///
/// Where the filesystem does not report its direct-I/O requirements, which
/// is every filesystem on kernels before 6.1 and some on later ones,
/// [`DMA_ALIGNMENT`] is assumed for both.
async fn stat_fd(fd: &FileFd) -> io::Result<FileInfo> {
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
        size: stat.stx_size,
    })
}

/// Flushes a file's data and metadata to its device. Goes through the I/O
/// scheduler as a write of no bytes, so it takes its turn among the device's
/// other requests.
async fn fsync(fd: &FileFd, device: u64) -> io::Result<()> {
    let sqe = opcode::Fsync::new(types::Fd(fd.as_raw_fd())).build();
    // SAFETY: the entry carries no pointers.
    let (result, ()) = unsafe { Op::new_disk(sqe, (), device, Direction::Write, 0) }.await;
    result.map(|_| ())
}

/// Flushes a file's data to its device, with only the metadata needed to
/// read it back. Scheduled like [`fsync`].
async fn fdatasync(fd: &FileFd, device: u64) -> io::Result<()> {
    let sqe = opcode::Fsync::new(types::Fd(fd.as_raw_fd()))
        .flags(types::FsyncFlags::DATASYNC)
        .build();
    // SAFETY: the entry carries no pointers.
    let (result, ()) = unsafe { Op::new_disk(sqe, (), device, Direction::Write, 0) }.await;
    result.map(|_| ())
}

/// Sets a file's length, through io_uring where the kernel can and on a
/// thread outside the runtime where it cannot.
async fn set_len(fd: &FileFd, len: u64) -> io::Result<()> {
    let raw = fd.as_raw_fd();
    let sqe = opcode::Ftruncate::new(types::Fd(raw), len).build();
    // SAFETY: the entry carries no pointers.
    let (result, ()) = unsafe { Op::new(sqe, ()) }.await;
    match result {
        Ok(_) => Ok(()),
        // Kernels before 6.9 do not know the operation. Fall back to the
        // system call, on a thread of its own since it may block.
        Err(err) if err.raw_os_error() == Some(libc::EINVAL) => truncate_on_thread(raw, len).await,
        Err(err) => Err(err),
    }
}

/// Sets a file's length with the blocking system call, off the shard.
async fn truncate_on_thread(fd: RawFd, len: u64) -> io::Result<()> {
    let len = libc::off64_t::try_from(len).map_err(|_| invalid("length is too large"))?;
    // The call is made on a duplicate of the descriptor. It runs on
    // another thread and goes on if this future is dropped, by when the
    // original may be closed and its number in use for another file.
    // SAFETY: plain system call; the result is checked.
    let duplicate = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) };
    if duplicate < 0 {
        return Err(io::Error::last_os_error());
    }
    crate::run_blocking(move || {
        // SAFETY: plain system calls on a descriptor only this closure
        // has, which it closes.
        unsafe {
            let outcome = match libc::ftruncate64(duplicate, len) {
                0 => Ok(()),
                _ => Err(io::Error::last_os_error()),
            };
            libc::close(duplicate);
            outcome
        }
    })
    .await?
}

/// Reserves disk space for a range of a file, extending the file if the
/// range ends beyond it.
async fn allocate(fd: &FileFd, offset: u64, len: u64) -> io::Result<()> {
    let sqe = opcode::Fallocate::new(types::Fd(fd.as_raw_fd()), len).offset(offset).build();
    // SAFETY: the entry carries no pointers.
    let (result, ()) = unsafe { Op::new(sqe, ()) }.await;
    result.map(|_| ())
}

/// Reads up to `len` bytes at `offset` into the start of `buf`. With a
/// device the request goes through the I/O scheduler; without one it goes
/// straight to the kernel.
async fn read_into<B: IoBufMut>(
    fd: &FileFd,
    device: Option<u64>,
    mut buf: B,
    len: u32,
    offset: u64,
) -> (io::Result<usize>, B) {
    if len == 0 {
        // A read of nothing would return `Ok(0)`, which means "end of
        // file" to every caller.
        return (Err(invalid("read buffer has no room")), buf);
    }
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
/// With a device the request goes through the I/O scheduler; without one it
/// goes straight to the kernel. Returns how many bytes were written and the
/// buffer.
async fn write_from<B: IoBuf>(
    fd: &FileFd,
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
///
/// The name goes at once; the file's contents are freed when the last open
/// descriptor to it is closed. To make the removal survive a crash, sync the
/// directory ([`sync_dir`]).
///
/// # Errors
///
/// `NotFound` if there is no such file; the kernel's error if `path` is a
/// directory or may not be removed.
pub async fn remove_file(path: impl AsRef<Path>) -> io::Result<()> {
    let path = c_path(path.as_ref())?;
    let sqe = opcode::UnlinkAt::new(types::Fd(libc::AT_FDCWD), path.as_ptr()).build();
    // SAFETY: the pointer targets the CString's heap allocation.
    let (result, _path) = unsafe { Op::new(sqe, path) }.await;
    result.map(|_| ())
}

/// Deletes an empty directory.
///
/// # Errors
///
/// `DirectoryNotEmpty` if it has anything in it (see [`remove_dir_all`]),
/// `NotFound` if it does not exist.
pub async fn remove_dir(path: impl AsRef<Path>) -> io::Result<()> {
    let path = c_path(path.as_ref())?;
    let sqe = opcode::UnlinkAt::new(types::Fd(libc::AT_FDCWD), path.as_ptr())
        .flags(libc::AT_REMOVEDIR)
        .build();
    // SAFETY: the pointer targets the CString's heap allocation.
    let (result, _path) = unsafe { Op::new(sqe, path) }.await;
    result.map(|_| ())
}

/// Deletes a directory and everything in it.
///
/// This is many system calls, and none of them has an asynchronous form
/// that would help, so it runs on a thread outside the runtime
/// ([`run_blocking`](crate::run_blocking)) while the task waits.
///
/// It is not one step: if it fails or the program stops part-way, some of
/// the contents are gone and some are not. Symbolic links inside are
/// removed, not followed.
pub async fn remove_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref().to_path_buf();
    crate::run_blocking(move || std::fs::remove_dir_all(path)).await?
}

/// Renames a file or directory, replacing `to` if it exists. Both names
/// must be on the same filesystem.
///
/// The replacement is atomic: anyone opening `to` gets either the old
/// file or the new one, complete. That makes "write a new file, sync it,
/// rename it over the old one" the way to update a file safely. For the
/// rename itself to survive a crash, sync the directory as well.
pub async fn rename(from: impl AsRef<Path>, to: impl AsRef<Path>) -> io::Result<()> {
    let from = c_path(from.as_ref())?;
    let to = c_path(to.as_ref())?;
    let sqe = opcode::RenameAt::new(
        types::Fd(libc::AT_FDCWD),
        from.as_ptr(),
        types::Fd(libc::AT_FDCWD),
        to.as_ptr(),
    )
    .build();
    // SAFETY: the pointers target the CStrings' heap allocations.
    let (result, _paths) = unsafe { Op::new(sqe, (from, to)) }.await;
    result.map(|_| ())
}

/// Creates a directory. Its parent must exist and it must not.
///
/// The directory gets mode `0777` less the process's umask.
///
/// # Errors
///
/// `AlreadyExists` if something is at the path, `NotFound` if the parent is
/// missing (see [`create_dir_all`]).
pub async fn create_dir(path: impl AsRef<Path>) -> io::Result<()> {
    let path = c_path(path.as_ref())?;
    let sqe = opcode::MkDirAt::new(types::Fd(libc::AT_FDCWD), path.as_ptr())
        .mode(0o777)
        .build();
    // SAFETY: the pointer targets the CString's heap allocation.
    let (result, _path) = unsafe { Op::new(sqe, path) }.await;
    result.map(|_| ())
}

/// Whether `path` names a directory, following symbolic links.
async fn is_dir(path: &Path) -> io::Result<bool> {
    let path = c_path(path)?;
    let mut buf: Vec<libc::statx> = Vec::with_capacity(1);
    let sqe = opcode::Statx::new(types::Fd(libc::AT_FDCWD), path.as_ptr(), buf.as_mut_ptr().cast())
        .mask(libc::STATX_TYPE)
        .build();
    // SAFETY: the pointers target the heap allocations of the CString and
    // the Vec, both owned by the op.
    let (result, (_path, mut buf)) = unsafe { Op::new(sqe, (path, buf)) }.await;
    result?;
    // SAFETY: on success the kernel initialised the one element.
    unsafe { buf.set_len(1) };
    let mode = buf.first().map_or(0, |stat| u32::from(stat.stx_mode));
    Ok(mode & libc::S_IFMT == libc::S_IFDIR)
}

/// Creates a directory and every missing directory above it. Succeeds if
/// the directory is already there.
///
/// The directories are made one at a time from the outermost in. It is not
/// one step: stopped part-way, the outer ones exist and the inner ones do
/// not.
///
/// # Errors
///
/// `AlreadyExists` if some part of the path exists and is not a directory.
pub async fn create_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref();
    // Outermost first.
    let mut missing: Vec<&Path> = path
        .ancestors()
        .filter(|ancestor| !ancestor.as_os_str().is_empty())
        .collect();
    missing.reverse();
    for directory in missing {
        match create_dir(directory).await {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
                // Something is there; it has to be a directory.
                if !is_dir(directory).await? {
                    return Err(err);
                }
            }
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

/// One name in a directory, as returned by [`read_dir`].
///
/// It holds the name and, where the filesystem gave it for free, what kind
/// of thing the name refers to. It is a snapshot: by the time it is used, the
/// file may have been renamed or removed.
#[derive(Clone, Debug)]
pub struct DirEntry {
    directory: PathBuf,
    name: OsString,
    file_type: Option<std::fs::FileType>,
}

impl DirEntry {
    /// The entry's name, without the directory. Not necessarily valid UTF-8.
    pub fn file_name(&self) -> &OsStr {
        &self.name
    }

    /// The path given to [`read_dir`] followed by the entry's name: what to
    /// pass to `File::open` and the like.
    pub fn path(&self) -> PathBuf {
        self.directory.join(&self.name)
    }

    /// What kind of thing the entry is, without following a symbolic
    /// link. `None` if the filesystem would not say.
    pub fn file_type(&self) -> Option<std::fs::FileType> {
        self.file_type
    }

    /// Whether the entry is a directory. A symbolic link to a directory is
    /// not one; neither is an entry whose kind the filesystem did not say.
    pub fn is_dir(&self) -> bool {
        self.file_type.is_some_and(|kind| kind.is_dir())
    }

    /// Whether the entry is a regular file. A symbolic link to a file is not
    /// one; neither is an entry whose kind the filesystem did not say.
    pub fn is_file(&self) -> bool {
        self.file_type.is_some_and(|kind| kind.is_file())
    }
}

/// Lists a directory. `.` and `..` are left out, and the order is whatever
/// the filesystem returns.
///
/// The kernel has no asynchronous way to do this, so the listing is made
/// on a thread outside the runtime ([`run_blocking`](crate::run_blocking))
/// while the task waits. That costs more than other file calls and the
/// whole listing is held in memory: fine for finding one's files at
/// start-up, not for a hot path or for directories with millions of
/// entries.
pub async fn read_dir(path: impl AsRef<Path>) -> io::Result<Vec<DirEntry>> {
    let directory = path.as_ref().to_path_buf();
    crate::run_blocking(move || {
        let mut entries = Vec::new();
        for entry in std::fs::read_dir(&directory)? {
            let entry = entry?;
            entries.push(DirEntry {
                directory: directory.clone(),
                name: entry.file_name(),
                file_type: entry.file_type().ok(),
            });
        }
        Ok(entries)
    })
    .await?
}

/// Makes a change to a directory durable: a file created, removed or
/// renamed in it survives a crash only once this has returned.
///
/// A directory is itself a file, of names, and like any file it has to be
/// synced. Syncing the file whose name changed is not enough.
pub async fn sync_dir(path: impl AsRef<Path>) -> io::Result<()> {
    let directory = File::open_with(path, libc::O_RDONLY | libc::O_DIRECTORY, 0).await?;
    directory.sync_all().await?;
    directory.close().await
}

/// How a file is opened: for reading or writing, and what happens if it
/// does or does not exist. It works like `std::fs::OpenOptions`.
///
/// ```no_run
/// use starfish::fs::OpenOptions;
///
/// # async fn example() -> std::io::Result<()> {
/// // Read and write an existing file, without emptying it.
/// let file = OpenOptions::new().read(true).write(true).open("/var/data/table").await?;
///
/// // A new file that must not exist yet.
/// let lock = OpenOptions::new().write(true).create_new(true).open("/var/data/LOCK").await?;
///
/// // A log that every write is added to the end of.
/// let log = OpenOptions::new().append(true).create(true).open("/var/log/events").await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OpenOptions {
    read: bool,
    write: bool,
    append: bool,
    create: bool,
    create_new: bool,
    truncate: bool,
    mode: u32,
}

impl Default for OpenOptions {
    /// Nothing set, as [`OpenOptions::new`].
    fn default() -> Self {
        OpenOptions::new()
    }
}

impl OpenOptions {
    /// Nothing set: add at least [`read`](Self::read),
    /// [`write`](Self::write) or [`append`](Self::append).
    pub fn new() -> OpenOptions {
        OpenOptions {
            read: false,
            write: false,
            append: false,
            create: false,
            create_new: false,
            truncate: false,
            mode: 0o644,
        }
    }

    /// Open for reading. Without this, reads on the file fail.
    pub fn read(mut self, read: bool) -> OpenOptions {
        self.read = read;
        self
    }

    /// Open for writing. Without this or [`append`](Self::append), writes on
    /// the file fail.
    pub fn write(mut self, write: bool) -> OpenOptions {
        self.write = write;
        self
    }

    /// Open for writing, with every write going to the end of the file
    /// whatever offset it names. The kernel places each write, so several
    /// writers, even several processes, do not overwrite each other.
    pub fn append(mut self, append: bool) -> OpenOptions {
        self.append = append;
        self
    }

    /// Create the file if it does not exist; open it as it is if it does.
    /// Needs the file opened for writing.
    pub fn create(mut self, create: bool) -> OpenOptions {
        self.create = create;
        self
    }

    /// Create the file, and fail with `AlreadyExists` if it exists. The
    /// check and the creation are one step, so of several programs trying
    /// at once exactly one succeeds.
    pub fn create_new(mut self, create_new: bool) -> OpenOptions {
        self.create_new = create_new;
        self
    }

    /// Empty the file if it exists, as the first thing done on opening it.
    /// Needs the file opened for writing.
    pub fn truncate(mut self, truncate: bool) -> OpenOptions {
        self.truncate = truncate;
        self
    }

    /// The permission bits a newly created file gets, before the
    /// process's umask is taken off. Default `0o644`.
    pub fn mode(mut self, mode: u32) -> OpenOptions {
        self.mode = mode;
        self
    }

    /// The `open(2)` flags these options stand for.
    ///
    /// Rejects the combinations that could only be mistakes: neither reading nor
    /// writing, and creating or emptying a file that is not opened for writing.
    fn flags(&self) -> io::Result<i32> {
        let writing = self.write || self.append;
        let access = match (self.read, writing) {
            (true, true) => libc::O_RDWR,
            (true, false) => libc::O_RDONLY,
            (false, true) => libc::O_WRONLY,
            (false, false) => return Err(invalid("a file is opened for reading, writing or both")),
        };
        if !writing && (self.create || self.create_new || self.truncate) {
            return Err(invalid("creating or emptying a file needs it opened for writing"));
        }
        let mut flags = access;
        if self.append {
            flags |= libc::O_APPEND;
        }
        if self.create_new {
            flags |= libc::O_CREAT | libc::O_EXCL;
        } else if self.create {
            flags |= libc::O_CREAT;
        }
        if self.truncate {
            flags |= libc::O_TRUNC;
        }
        Ok(flags)
    }

    /// Opens a file through the page cache, as these options say.
    ///
    /// # Errors
    ///
    /// `InvalidInput` for a combination of options that makes no sense, before
    /// the kernel is asked. Otherwise the kernel's error: `NotFound`,
    /// `AlreadyExists` with [`create_new`](Self::create_new), `PermissionDenied`.
    pub async fn open(&self, path: impl AsRef<Path>) -> io::Result<File> {
        File::open_with(path, self.flags()?, self.mode).await
    }

    /// Opens a file for direct I/O.
    ///
    /// Appending does not go with direct I/O's whole-block writes at
    /// stated offsets; use a
    /// [`DmaStreamWriter`] for that.
    pub async fn open_direct(&self, path: impl AsRef<Path>) -> io::Result<DmaFile> {
        if self.append {
            return Err(invalid("a file opened for direct I/O cannot be in append mode"));
        }
        DmaFile::open_with(path, self.flags()?, self.mode).await
    }
}

/// An open file, read and written through the kernel's page cache.
///
/// There is no cursor: every read and write names its offset, so concurrent
/// operations on one file do not interfere, and the methods take `&self`.
/// For reading or writing in order, wrap the file in a [`Cursor`].
///
/// Dropping it closes the file without waiting and without reporting
/// failure; [`close`](Self::close) reports.
pub struct File {
    fd: FileFd,
    device: u64,
}

impl File {
    /// Opens an existing file for reading only.
    ///
    /// # Errors
    ///
    /// `NotFound` if there is no such file; the kernel's error otherwise.
    pub async fn open(path: impl AsRef<Path>) -> io::Result<File> {
        File::open_with(path, libc::O_RDONLY, 0).await
    }

    /// Opens a file for reading and writing, creating it if it does not exist
    /// and emptying it if it does. New files get mode `0644` less the umask.
    ///
    /// For any other combination, such as keeping the contents or refusing to
    /// replace an existing file, use [`OpenOptions`].
    pub async fn create(path: impl AsRef<Path>) -> io::Result<File> {
        File::open_with(path, libc::O_RDWR | libc::O_CREAT | libc::O_TRUNC, 0o644).await
    }

    /// Opens a file with raw `open(2)` flags and creation mode. For the
    /// usual combinations, [`OpenOptions`] says the same in words.
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
    ///
    /// # Errors
    ///
    /// `InvalidInput` if the buffer has no capacity, as `Vec::new()` has
    /// none: there would be no telling that from the end of the file.
    pub async fn read_at<B: IoBufMut>(&self, buf: B, offset: u64) -> (io::Result<usize>, B) {
        let len = clamp_len(buf.capacity());
        read_into(&self.fd, Some(self.device), buf, len, offset).await
    }

    /// Writes some prefix of `buf` at `offset` and returns how many bytes were
    /// written, which is normally all of them. Writing past the end extends the
    /// file; a gap left before `offset` reads back as zeros.
    ///
    /// The data is in the page cache when this returns, not yet on the device.
    pub async fn write_at<B: IoBuf>(&self, buf: B, offset: u64) -> (io::Result<usize>, B) {
        let len = clamp_len(buf.len());
        write_from(&self.fd, Some(self.device), buf, 0, len, offset).await
    }

    /// Writes all of `buf` at `offset`, repeating the write from where the
    /// last one stopped until nothing is left.
    ///
    /// # Errors
    ///
    /// The first failed write's error, or `WriteZero` if a write took nothing,
    /// which is what a full device looks like.
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

    /// Flushes file data and metadata to the device. When this returns, the
    /// file's contents and its length, times and permissions survive a crash.
    /// The file's name in its directory is another matter; see [`sync_dir`].
    pub async fn sync_all(&self) -> io::Result<()> {
        fsync(&self.fd, self.device).await
    }

    /// Flushes file data to the device, and of the metadata only what is
    /// needed to read the data back (the file's length, not its
    /// timestamps). Often cheaper than [`sync_all`](Self::sync_all).
    pub async fn sync_data(&self) -> io::Result<()> {
        fdatasync(&self.fd, self.device).await
    }

    /// The file's length in bytes, asked of the kernel on every call. Another
    /// process, or another task, may have changed it since the last one.
    pub async fn size(&self) -> io::Result<u64> {
        Ok(stat_fd(&self.fd).await?.size)
    }

    /// Sets the file's length. Shortening discards the rest; lengthening adds
    /// zeros, without allocating space for them.
    ///
    /// On kernels before 6.9 this runs on a thread outside the runtime, which
    /// is slower but does not hold the shard up.
    pub async fn set_len(&self, len: u64) -> io::Result<()> {
        set_len(&self.fd, len).await
    }

    /// Reserves disk space for `len` bytes from `offset`, growing the file
    /// if the range ends beyond it. Writes into the range then cannot fail
    /// for lack of space, and need not allocate.
    pub async fn allocate(&self, offset: u64, len: u64) -> io::Result<()> {
        allocate(&self.fd, offset, len).await
    }

    /// Closes the file and reports the outcome.
    ///
    /// Dropping a file closes it too, without waiting and without telling
    /// whether it worked. Some filesystems, network ones above all, report
    /// at close that earlier writes could not be stored; call this to find
    /// out. The file is closed even if an error is returned.
    pub async fn close(self) -> io::Result<()> {
        close_fd(self.fd).await
    }
}

/// Reads or writes a [`File`] in order, keeping the position that the file
/// itself does not have.
///
/// It makes a file usable with [`BufReader`](crate::io::BufReader) and
/// [`BufWriter`](crate::io::BufWriter). The file may be borrowed, owned or
/// in an `Rc`; several cursors on one file each have their own position.
///
/// ```no_run
/// use starfish::fs::{Cursor, File};
/// use starfish::io::BufWriter;
///
/// # async fn example() -> std::io::Result<()> {
/// let file = File::create("/tmp/report").await?;
/// let mut out = BufWriter::new(Cursor::new(&file));
/// for n in 0..1000 {
///     out.write_all(format!("line {n}\n").as_bytes()).await?;
/// }
/// out.flush().await?;
/// file.sync_data().await?;
/// # Ok(())
/// # }
/// ```
pub struct Cursor<F> {
    file: F,
    position: u64,
}

impl<F: std::borrow::Borrow<File>> Cursor<F> {
    /// A cursor at the start of the file.
    pub fn new(file: F) -> Cursor<F> {
        Cursor { file, position: 0 }
    }

    /// A cursor at `position`, which need not be inside the file. To append,
    /// start at the file's [`size`](File::size).
    pub fn at(file: F, position: u64) -> Cursor<F> {
        Cursor { file, position }
    }

    /// Where the next read or write will take place.
    ///
    /// Under a [`BufReader`](crate::io::BufReader) this runs ahead of what
    /// has been read from the reader, by the bytes still in its buffer;
    /// under a [`BufWriter`](crate::io::BufWriter) it lags behind by the
    /// bytes not yet flushed.
    pub fn position(&self) -> u64 {
        self.position
    }

    /// Moves the cursor: the next read or write takes place at `position`.
    /// Under a buffered reader or writer, flush or discard its buffer first.
    pub fn set_position(&mut self, position: u64) {
        self.position = position;
    }

    /// The file the cursor reads and writes.
    pub fn get_ref(&self) -> &File {
        self.file.borrow()
    }

    /// Gives the file back, ending the cursor.
    pub fn into_inner(self) -> F {
        self.file
    }
}

impl<F: std::borrow::Borrow<File>> crate::io::Read for Cursor<F> {
    /// Reads at the cursor's position and moves it past what was read.
    async fn read_some(&mut self, buf: Vec<u8>) -> (io::Result<usize>, Vec<u8>) {
        let (result, buf) = self.file.borrow().read_at(buf, self.position).await;
        if let Ok(read) = &result {
            self.position = self.position.saturating_add(*read as u64);
        }
        (result, buf)
    }
}

impl<F: std::borrow::Borrow<File>> crate::io::Write for Cursor<F> {
    /// Writes all of `buf` at the cursor's position and moves it past what
    /// was written. After a failed write the position is unchanged, though part
    /// of the buffer may have been written.
    async fn write_whole(&mut self, buf: Vec<u8>) -> (io::Result<()>, Vec<u8>) {
        let (result, buf) = self.file.borrow().write_all_at(buf, self.position).await;
        if result.is_ok() {
            self.position = self.position.saturating_add(buf.len() as u64);
        }
        (result, buf)
    }
}

/// What the filesystem records about a file or directory, as returned by
/// [`metadata`]: its length, its kind, its permission bits and when it was
/// last changed. A snapshot from the moment of the call.
#[derive(Clone, Copy, Debug)]
pub struct Metadata {
    len: u64,
    mode: u32,
    modified: std::time::SystemTime,
}

impl Metadata {
    /// Length in bytes. For a directory, whatever the filesystem uses to
    /// store its names.
    pub fn len(&self) -> u64 {
        self.len
    }

    /// Whether the length is zero.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Whether this is a regular file, as opposed to a directory, a device,
    /// a socket and so on.
    pub fn is_file(&self) -> bool {
        self.mode & libc::S_IFMT == libc::S_IFREG
    }

    /// Whether this is a directory.
    pub fn is_dir(&self) -> bool {
        self.mode & libc::S_IFMT == libc::S_IFDIR
    }

    /// The permission bits, as in `chmod`: `0o644` and the like, including
    /// the set-user-id, set-group-id and sticky bits.
    pub fn permissions(&self) -> u32 {
        self.mode & 0o7777
    }

    /// When the contents were last changed, as the filesystem recorded it.
    pub fn modified(&self) -> std::time::SystemTime {
        self.modified
    }
}

/// Looks up a path, following symbolic links. Fails with `NotFound` if
/// nothing is there, which makes it the way to ask whether a file exists.
///
/// The answer may be out of date by the time it is used. To act on a file
/// only if it exists, or only if it does not, open it with the right
/// [`OpenOptions`] instead of checking first.
pub async fn metadata(path: impl AsRef<Path>) -> io::Result<Metadata> {
    let path = c_path(path.as_ref())?;
    let mut buf: Vec<libc::statx> = Vec::with_capacity(1);
    let sqe = opcode::Statx::new(types::Fd(libc::AT_FDCWD), path.as_ptr(), buf.as_mut_ptr().cast())
        .mask(libc::STATX_BASIC_STATS)
        .build();
    // SAFETY: the pointers target the heap allocations of the CString and
    // the Vec, both owned by the op.
    let (result, (_path, mut buf)) = unsafe { Op::new(sqe, (path, buf)) }.await;
    result?;
    // SAFETY: on success the kernel initialised the one element.
    unsafe { buf.set_len(1) };
    let stat = buf.first().ok_or_else(|| io::Error::other("statx returned nothing"))?;
    let since_epoch = std::time::Duration::new(
        u64::try_from(stat.stx_mtime.tv_sec).unwrap_or(0),
        stat.stx_mtime.tv_nsec,
    );
    Ok(Metadata {
        len: stat.stx_size,
        mode: u32::from(stat.stx_mode),
        modified: std::time::UNIX_EPOCH
            .checked_add(since_epoch)
            .unwrap_or(std::time::UNIX_EPOCH),
    })
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
    /// Allocates an empty buffer aligned to [`DMA_ALIGNMENT`], with room for
    /// `capacity` bytes rounded up to whole blocks. To match a particular file
    /// exactly, use [`DmaFile::alloc`].
    ///
    /// # Errors
    ///
    /// `OutOfMemory` if the allocation fails, `InvalidInput` for a size that
    /// cannot be represented.
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
    /// number of [`DMA_ALIGNMENT`] blocks: ready to be written with direct I/O.
    pub fn padded_from(data: &[u8]) -> io::Result<AlignedBuf> {
        let mut buf = AlignedBuf::new(data.len())?;
        buf.fill_padded(data);
        Ok(buf)
    }

    /// Replaces the contents with `data` (as much as fits), zero-padded to
    /// the whole capacity, and sets the length to the capacity. Afterwards the
    /// buffer is a whole number of blocks, which is what a direct write needs.
    pub fn fill_padded(&mut self, data: &[u8]) {
        self.len = self.capacity;
        let split = data.len().min(self.capacity);
        let (head, tail) = self.split_at_mut(split);
        if let Some(source) = data.get(..head.len()) {
            head.copy_from_slice(source);
        }
        tail.fill(0);
    }

    /// Number of bytes in the buffer: how much a write of it sends.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the buffer holds no bytes.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Number of bytes the buffer has room for, a whole number of blocks.
    /// A read fills at most this much.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// The alignment of the buffer's memory, as given when it was allocated.
    pub fn alignment(&self) -> usize {
        self.alignment
    }

    /// Start of the allocation, valid for the whole capacity.
    pub(crate) fn raw_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    /// Sets the length, clamped to the capacity. Bytes that come into view
    /// keep whatever was last stored there, which is zeros in a fresh buffer.
    pub fn set_len(&mut self, len: usize) {
        self.len = len.min(self.capacity);
    }
}

impl Deref for AlignedBuf {
    /// An aligned buffer reads as a slice of its bytes.
    type Target = [u8];

    /// The buffer's bytes, up to its length.
    fn deref(&self) -> &[u8] {
        // SAFETY: `len <= capacity`, and all `capacity` bytes are allocated
        // and initialised (zeroed at allocation, only overwritten since).
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl DerefMut for AlignedBuf {
    /// The buffer's bytes, up to its length, for changing in place.
    fn deref_mut(&mut self) -> &mut [u8] {
        // SAFETY: as in `deref`, and `&mut self` gives exclusive access.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl Drop for AlignedBuf {
    /// Frees the allocation.
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
    fd: FileFd,
    device: u64,
    memory_alignment: usize,
    disk_alignment: usize,
}

impl DmaFile {
    /// Opens an existing file for direct reading.
    ///
    /// # Errors
    ///
    /// `NotFound` if there is no such file, and `EINVAL` on a filesystem without
    /// direct I/O.
    pub async fn open(path: impl AsRef<Path>) -> io::Result<DmaFile> {
        DmaFile::open_with(path, libc::O_RDONLY, 0).await
    }

    /// Opens a file for direct reading and writing, creating it if it does
    /// not exist and emptying it if it does.
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
    /// least `capacity` bytes rounded up to whole blocks. Its memory satisfies
    /// both of the file's alignment requirements.
    pub fn alloc(&self, capacity: usize) -> io::Result<AlignedBuf> {
        AlignedBuf::with_alignment(capacity, self.memory_alignment.max(self.disk_alignment))
    }

    /// Starts a read of as many whole blocks as fit in `buf`, at `offset`,
    /// without waiting for it. The request is on its way to the device
    /// (through the I/O scheduler) when this returns.
    fn start_read(&self, buf: AlignedBuf, offset: u64) -> std::result::Result<Op<AlignedBuf>, (io::Error, AlignedBuf)> {
        let spare = buf.capacity().checked_rem(self.disk_alignment).unwrap_or(0);
        let len = buf.capacity().saturating_sub(spare);
        let ptr = buf.raw_ptr();
        match self.check(ptr, offset, len) {
            Ok(len) => {
                let sqe = opcode::Read::new(types::Fd(self.fd.as_raw_fd()), ptr, len)
                    .offset(offset)
                    .build();
                // SAFETY: the pointer targets the buffer's own allocation,
                // which does not move with the buffer and lives as long as
                // the op holds it, and `len` is within its capacity.
                Ok(unsafe { Op::new_disk(sqe, buf, self.device, Direction::Read, u64::from(len)) })
            }
            Err(err) => Err((err, buf)),
        }
    }

    /// Checks a transfer against the file's alignment rules and returns its
    /// length as io_uring takes it.
    ///
    /// The kernel would refuse a misaligned request with a bare `EINVAL`; these
    /// errors say which rule was broken.
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
    /// # Errors
    ///
    /// `InvalidInput` if the buffer breaks the file's alignment rules or
    /// has room for less than one block.
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

    /// Reads at `offset` as many whole blocks as fit in the buffer.
    /// `scheduled` is false only for capacity measurement, which must see the
    /// device itself, not the scheduler.
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

    /// Writes the buffer at `offset`, through the I/O scheduler or, for
    /// capacity measurement only, straight to the kernel.
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

    /// Flushes file data and metadata to the device. Direct I/O bypasses the
    /// page cache but not the device's own write cache, so this is still what
    /// makes written data safe.
    pub async fn sync_all(&self) -> io::Result<()> {
        fsync(&self.fd, self.device).await
    }

    /// Flushes file data to the device, and of the metadata only what is
    /// needed to read the data back. Direct I/O bypasses the page cache
    /// but not the device's own write cache, so data is only safe after a
    /// sync.
    pub async fn sync_data(&self) -> io::Result<()> {
        fdatasync(&self.fd, self.device).await
    }

    /// The file's length in bytes, asked of the kernel on every call.
    pub async fn size(&self) -> io::Result<u64> {
        Ok(stat_fd(&self.fd).await?.size)
    }

    /// Sets the file's length, which need not be a whole number of
    /// blocks. Since writes are whole blocks, this is how a file gets a
    /// length that is not: write the last block padded, then cut the file
    /// to size.
    pub async fn set_len(&self, len: u64) -> io::Result<()> {
        set_len(&self.fd, len).await
    }

    /// Reserves disk space for `len` bytes from `offset`, growing the file
    /// if the range ends beyond it. Writing into space that is already
    /// allocated is what keeps direct I/O from waiting on the filesystem.
    pub async fn allocate(&self, offset: u64, len: u64) -> io::Result<()> {
        allocate(&self.fd, offset, len).await
    }

    /// Closes the file and reports the outcome; see [`File::close`].
    pub async fn close(self) -> io::Result<()> {
        close_fd(self.fd).await
    }
}

/// The error every call on a stream returns once one of its operations
/// has been dropped part-way: the stream's buffer went with that operation,
/// and its position in the file is no longer known.
fn interrupted() -> io::Error {
    io::Error::other("an earlier operation on this stream was dropped before it finished")
}

/// Writes a [`DmaFile`] from start to end, taking data in pieces of any
/// size.
///
/// Direct I/O only transfers whole, aligned blocks. The writer collects
/// what it is given in an aligned buffer and writes the buffer out when it
/// is full, so the caller can write records of any length. This is the
/// tool for write-ahead logs and for files written once from front to
/// back.
///
/// ```no_run
/// use starfish::fs::{DmaFile, DmaStreamWriter};
///
/// # async fn example() -> std::io::Result<()> {
/// let file = DmaFile::create("/var/data/log").await?;
/// let mut log = DmaStreamWriter::new(&file, 128 * 1024)?;
/// for n in 0..1000u32 {
///     log.write_all(&n.to_le_bytes()).await?;
/// }
/// // Pads the last block, writes it, gives the file its true length and
/// // makes it all durable.
/// log.finish().await?;
/// # Ok(())
/// # }
/// ```
///
/// # Durability and length
///
/// Data is on the device only after [`sync`](Self::sync) or
/// [`finish`](Self::finish). Until `finish`, the file's length on disk is
/// a whole number of blocks (or whatever was preallocated), with zeros
/// after the data: only `finish` sets the true length. A program that may
/// be read after a crash must be able to tell where its data ends, for
/// instance by a length or checksum in every record.
///
/// Dropping the writer writes nothing: what is in the buffer is lost.
pub struct DmaStreamWriter<F> {
    file: F,
    /// `None` after an operation was dropped part-way.
    buf: Option<AlignedBuf>,
    /// Where in the file the buffer's first byte goes; a block boundary.
    start: u64,
    /// How much of the buffer holds data.
    filled: usize,
    block: usize,
}

impl<F: std::borrow::Borrow<DmaFile>> DmaStreamWriter<F> {
    /// A writer that starts at the beginning of the file, with a buffer of
    /// at least `buffer_size` bytes (rounded up to whole blocks). Larger
    /// buffers mean fewer, larger writes.
    pub fn new(file: F, buffer_size: usize) -> io::Result<DmaStreamWriter<F>> {
        let block = file.borrow().disk_alignment().max(1);
        let buf = file.borrow().alloc(buffer_size)?;
        Ok(DmaStreamWriter {
            file,
            buf: Some(buf),
            start: 0,
            filled: 0,
            block,
        })
    }

    /// A writer that continues at the end of the file.
    ///
    /// The file must have its true length, as [`finish`](Self::finish)
    /// leaves it. If that is not a whole number of blocks, the last,
    /// partial block is read back so that it can be completed.
    pub async fn append(file: F, buffer_size: usize) -> io::Result<DmaStreamWriter<F>> {
        let mut writer = DmaStreamWriter::new(file, buffer_size)?;
        let size = writer.file.borrow().size().await?;
        let tail = usize::try_from(size.checked_rem(writer.block as u64).unwrap_or(0)).unwrap_or(0);
        writer.start = size.saturating_sub(tail as u64);
        if tail > 0 {
            let buf = writer.buf.take().ok_or_else(interrupted)?;
            let (result, buf) = writer.file.borrow().read_at(buf, writer.start).await;
            writer.buf = Some(buf);
            if result? < tail {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            writer.filled = tail;
        }
        Ok(writer)
    }

    /// How many bytes the stream holds so far: the file offset the next byte
    /// will go to. It counts what is still in the buffer, so it runs ahead of
    /// the file's length until the buffer is written.
    pub fn position(&self) -> u64 {
        self.start.saturating_add(self.filled as u64)
    }

    /// The file being written.
    pub fn get_ref(&self) -> &DmaFile {
        self.file.borrow()
    }

    /// Adds all of `data` to the stream, writing the buffer to the file each
    /// time it fills up. Data that fits in the buffer costs a copy and no I/O.
    ///
    /// # Errors
    ///
    /// The file's error, or `WriteZero` if the device is full. After an error
    /// the stream should be abandoned.
    pub async fn write_all(&mut self, data: &[u8]) -> io::Result<()> {
        let mut rest = data;
        while !rest.is_empty() {
            let buf = self.buf.as_mut().ok_or_else(interrupted)?;
            let capacity = buf.capacity();
            let room = capacity.saturating_sub(self.filled);
            let (now, later) = rest.split_at_checked(room.min(rest.len())).unwrap_or((rest, &[]));
            // Everything up to the capacity is addressable once the
            // length says so; what is beyond `filled` is overwritten here
            // or padded before it is written.
            buf.set_len(capacity);
            let end = self.filled.saturating_add(now.len());
            if let Some(target) = buf.get_mut(self.filled..end) {
                target.copy_from_slice(now);
            }
            self.filled = end;
            rest = later;
            if self.filled >= capacity {
                self.write_buffer(capacity).await?;
                self.start = self.start.saturating_add(capacity as u64);
                self.filled = 0;
            }
        }
        Ok(())
    }

    /// Writes the first `len` bytes of the buffer, a whole number of blocks,
    /// at the buffer's place in the file, and takes the buffer back.
    async fn write_buffer(&mut self, len: usize) -> io::Result<()> {
        let mut buf = self.buf.take().ok_or_else(interrupted)?;
        buf.set_len(len);
        let (result, buf) = self.file.borrow().write_at(buf, self.start).await;
        self.buf = Some(buf);
        if result? != len {
            return Err(io::Error::new(io::ErrorKind::WriteZero, "the device is full"));
        }
        Ok(())
    }

    /// Writes out what is in the buffer, with the last block padded with
    /// zeros. The buffer keeps its contents, so that the padded block is
    /// written again, complete, once more data has arrived.
    async fn write_tail(&mut self) -> io::Result<()> {
        if self.filled == 0 {
            return Ok(());
        }
        let padded = self.filled.checked_next_multiple_of(self.block).unwrap_or(self.filled);
        let buf = self.buf.as_mut().ok_or_else(interrupted)?;
        buf.set_len(padded);
        if let Some(padding) = buf.get_mut(self.filled..) {
            padding.fill(0);
        }
        self.write_buffer(padded).await
    }

    /// Makes everything written so far durable.
    ///
    /// The last block goes to the file padded with zeros, and the file's
    /// length is not adjusted: see the notes on the type. The writer can
    /// be used further afterwards.
    pub async fn sync(&mut self) -> io::Result<()> {
        self.write_tail().await?;
        self.file.borrow().sync_data().await
    }

    /// Ends the stream: writes what is left, sets the file's length to
    /// exactly what was written, makes it durable and gives the file back.
    ///
    /// This is the only call after which the file's length says where the data
    /// ends.
    pub async fn finish(mut self) -> io::Result<F> {
        self.write_tail().await?;
        self.file.borrow().set_len(self.position()).await?;
        self.file.borrow().sync_all().await?;
        Ok(self.file)
    }
}

impl<F: std::borrow::Borrow<DmaFile>> crate::io::Write for DmaStreamWriter<F> {
    /// Adds the buffer's contents to the stream.
    async fn write_whole(&mut self, buf: Vec<u8>) -> (io::Result<()>, Vec<u8>) {
        (self.write_all(&buf).await, buf)
    }
}

/// Reads a [`DmaFile`] from start to end, giving data out in pieces of
/// any size.
///
/// It reads the file a buffer at a time, in whole aligned blocks as direct
/// I/O demands, and copies out of the buffer. It also implements
/// [`io::Read`](crate::io::Read), so a [`BufReader`](crate::io::BufReader)
/// on top of it finds lines in a file read with direct I/O.
///
/// With [`read_ahead`](Self::read_ahead), the reader asks the device for
/// the next buffers while the caller is still working on the current one,
/// so that a scan does not stop and wait at every buffer's end.
///
/// ```no_run
/// use starfish::fs::{DmaFile, DmaStreamReader};
///
/// # async fn example() -> std::io::Result<()> {
/// let file = DmaFile::open("/var/data/log").await?;
/// let mut log = DmaStreamReader::new(&file, 128 * 1024)?;
/// let mut number = [0u8; 4];
/// while log.read_exact(&mut number).await.is_ok() {
///     println!("{}", u32::from_le_bytes(number));
/// }
/// # Ok(())
/// # }
/// ```
pub struct DmaStreamReader<F> {
    file: F,
    /// The buffer being given out. `None` before the first read and while
    /// its memory is with the kernel.
    current: Option<AlignedBuf>,
    /// How much of `current` has been given out.
    consumed: usize,
    /// Reads on their way, oldest first, each with its place in the file.
    pending: std::collections::VecDeque<(u64, Op<AlignedBuf>)>,
    /// Buffers not in use.
    spare: Vec<AlignedBuf>,
    /// Where the next read to be started begins; a block boundary.
    next: u64,
    /// The file offset of the next byte to give out.
    position: u64,
    /// Bytes to pass over in the first buffer, when the stream starts
    /// inside a block.
    skip: usize,
    /// How many reads to keep on their way beyond the one being waited for.
    ahead: usize,
    /// The end of the file has been reached.
    ended: bool,
    /// A read was dropped while it was being waited for.
    broken: bool,
    block: usize,
    buffer_size: usize,
}

impl<F: std::borrow::Borrow<DmaFile>> DmaStreamReader<F> {
    /// A reader that starts at the beginning of the file, with a buffer of at
    /// least `buffer_size` bytes (rounded up to whole blocks). Larger buffers
    /// mean fewer, larger reads.
    pub fn new(file: F, buffer_size: usize) -> io::Result<DmaStreamReader<F>> {
        DmaStreamReader::at(file, 0, buffer_size)
    }

    /// A reader that starts at `position`, which may be anywhere, inside a
    /// block or not. The first read begins at the block boundary before it, and
    /// the bytes up to `position` are passed over.
    pub fn at(file: F, position: u64, buffer_size: usize) -> io::Result<DmaStreamReader<F>> {
        let block = file.borrow().disk_alignment().max(1);
        let first = file.borrow().alloc(buffer_size)?;
        let skip = usize::try_from(position.checked_rem(block as u64).unwrap_or(0)).unwrap_or(0);
        Ok(DmaStreamReader {
            file,
            current: None,
            consumed: 0,
            pending: std::collections::VecDeque::new(),
            spare: vec![first],
            next: position.saturating_sub(skip as u64),
            position,
            skip,
            ahead: 0,
            ended: false,
            broken: false,
            block,
            buffer_size,
        })
    }

    /// Keeps `buffers` further reads on their way to the device while the
    /// current buffer is being used, so that a scan from front to back
    /// rarely has to wait. The reader then holds `buffers + 1` buffers.
    ///
    /// Worth it for long sequential scans. For a reader that takes a
    /// little and stops, it reads data nobody asked for.
    pub fn read_ahead(mut self, buffers: usize) -> io::Result<DmaStreamReader<F>> {
        // Up to 64: beyond that it is the device's queue that is being
        // filled, not time that is being saved.
        self.ahead = buffers.min(64);
        while self.spare.len() <= self.ahead {
            self.spare.push(self.file.borrow().alloc(self.buffer_size)?);
        }
        Ok(self)
    }

    /// The file offset of the next byte the reader will give out.
    pub fn position(&self) -> u64 {
        self.position
    }

    /// The file being read.
    pub fn get_ref(&self) -> &DmaFile {
        self.file.borrow()
    }

    /// Starts reads until as many are on their way as there should be: the
    /// one about to be waited for, plus the configured number ahead of it. Each
    /// takes a free buffer and the next stretch of the file.
    fn start_reads(&mut self) -> io::Result<()> {
        while self.pending.len() <= self.ahead {
            let Some(buf) = self.spare.pop() else { break };
            let step = buf.capacity() as u64;
            match self.file.borrow().start_read(buf, self.next) {
                Ok(op) => {
                    self.pending.push_back((self.next, op));
                    self.next = self.next.saturating_add(step);
                }
                Err((err, buf)) => {
                    self.spare.push(buf);
                    return Err(err);
                }
            }
        }
        Ok(())
    }

    /// Waits for the oldest read on its way. Returns where in the file it was
    /// and how much it brought, with the data in `current`; `None` if no read
    /// was on its way.
    async fn finish_read(&mut self) -> io::Result<Option<(u64, usize)>> {
        let Some((offset, op)) = self.pending.pop_front() else { return Ok(None) };
        // If this future is dropped at the await, the read and its buffer
        // go with it and the stream has a hole.
        self.broken = true;
        let (result, mut buf) = op.await;
        self.broken = false;
        match result {
            Ok(read) => {
                let read = (read as usize).min(buf.capacity());
                buf.set_len(read);
                self.current = Some(buf);
                Ok(Some((offset, read)))
            }
            Err(err) => {
                self.spare.push(buf);
                Err(err)
            }
        }
    }

    /// The bytes read from the file and not yet given out, after reading
    /// more if there are none. Empty at the end of the file.
    async fn fill(&mut self) -> io::Result<&[u8]> {
        loop {
            if self.broken {
                return Err(interrupted());
            }
            let buffered = self.current.as_ref().map_or(0, |buf| buf.len());
            if self.consumed < buffered || self.ended {
                break;
            }
            // The buffer that has been used up is free for the next read.
            if let Some(used) = self.current.take() {
                self.spare.push(used);
            }
            self.consumed = 0;
            self.start_reads()?;
            let Some((offset, read)) = self.finish_read().await? else {
                return Err(interrupted());
            };
            let capacity = self.current.as_ref().map_or(0, |buf| buf.capacity());
            // The first buffer may begin before the stream does.
            self.consumed = self.skip.min(read);
            self.skip = self.skip.saturating_sub(self.consumed);
            if read == 0 || read.checked_rem(self.block) != Some(0) {
                // Short of a block boundary, or nothing at all: the end of
                // the file. The reads beyond it are of no use.
                self.ended = true;
                self.pending.clear();
            } else if read < capacity {
                // A short read that is not the end. The reads already on
                // their way assumed a full one and are in the wrong place:
                // let them finish, discard them, and carry on from here.
                while let Some((_, op)) = self.pending.pop_front() {
                    self.broken = true;
                    let (_, buf) = op.await;
                    self.broken = false;
                    self.spare.push(buf);
                }
                self.next = offset.saturating_add(read as u64);
            }
        }
        let current = self.current.as_ref().map_or(&[][..], |buf| &**buf);
        Ok(current.get(self.consumed..).unwrap_or(&[]))
    }

    /// Marks `count` bytes of the current buffer as given out.
    fn advance(&mut self, count: usize) {
        self.consumed = self.consumed.saturating_add(count);
        self.position = self.position.saturating_add(count as u64);
    }

    /// Reads some bytes into `out` and returns how many: at least one unless
    /// the file has ended. `Ok(0)` means the end of the file (or that `out` is
    /// empty).
    pub async fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let available = self.fill().await?;
        let count = available.len().min(out.len());
        if let (Some(target), Some(source)) = (out.get_mut(..count), available.get(..count)) {
            target.copy_from_slice(source);
        }
        self.advance(count);
        Ok(count)
    }

    /// Fills all of `out`.
    ///
    /// # Errors
    ///
    /// `UnexpectedEof` if the file ends first; the bytes read up to then
    /// are in `out`.
    pub async fn read_exact(&mut self, out: &mut [u8]) -> io::Result<()> {
        let mut rest = out;
        while !rest.is_empty() {
            let count = self.read(rest).await?;
            if count == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            rest = rest.get_mut(count..).unwrap_or_default();
        }
        Ok(())
    }
}

impl<F: std::borrow::Borrow<DmaFile>> crate::io::Read for DmaStreamReader<F> {
    /// Fills the vector with what is buffered, up to its capacity.
    async fn read_some(&mut self, mut buf: Vec<u8>) -> (io::Result<usize>, Vec<u8>) {
        buf.clear();
        let room = buf.capacity();
        let available = match self.fill().await {
            Ok(available) => available,
            Err(err) => return (Err(err), buf),
        };
        let count = available.len().min(room);
        buf.extend_from_slice(available.get(..count).unwrap_or(&[]));
        self.advance(count);
        (Ok(count), buf)
    }
}

debug_impl!(File, "fd" => |file| file.fd.as_raw_fd());
debug_impl!(DmaFile, "fd" => |file| file.fd.as_raw_fd(), "disk_alignment" => |file| file.disk_alignment);
debug_impl!(AlignedBuf, "len" => |buf| buf.len, "capacity" => |buf| buf.capacity);
debug_impl!(<F> Cursor<F>, "position" => |cursor| cursor.position);
debug_impl!(<F> DmaStreamWriter<F>, "position" => |writer| writer.start.saturating_add(writer.filled as u64));
debug_impl!(<F> DmaStreamReader<F>, "position" => |reader| reader.position);

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic, clippy::arithmetic_side_effects)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::pin::pin;
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};

    /// Wakes a thread blocked in `block_on`.
    struct Unpark(std::thread::Thread);

    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
        let mut future = pin!(future);
        loop {
            match future.as_mut().poll(&mut Context::from_waker(&waker)) {
                Poll::Ready(output) => return output,
                Poll::Pending => std::thread::park(),
            }
        }
    }

    /// The path kernels before 6.9 take in `set_len`, which newer ones
    /// never exercise.
    #[test]
    #[cfg_attr(miri, ignore = "makes system calls Miri does not model")]
    fn the_length_can_be_set_off_the_shard() {
        let path = std::env::temp_dir().join(format!("starfish-test-{}-truncate", std::process::id()));
        std::fs::write(&path, vec![7u8; 5000]).unwrap();
        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();

        block_on(truncate_on_thread(file.as_raw_fd(), 1234)).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 1234);
        block_on(truncate_on_thread(file.as_raw_fd(), 9000)).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 9000);

        // The original descriptor is untouched, and errors come through.
        drop(file);
        let read_only = std::fs::File::open(&path).unwrap();
        let refused = block_on(truncate_on_thread(read_only.as_raw_fd(), 1)).unwrap_err();
        assert_eq!(refused.raw_os_error(), Some(libc::EINVAL));
        assert!(block_on(truncate_on_thread(-1, 1)).is_err());
        std::fs::remove_file(&path).unwrap();
    }
}
