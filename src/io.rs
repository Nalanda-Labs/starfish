//! Buffered reading and writing, over TCP streams and files.
//!
//! [`BufReader`] finds lines and pieces of known length in a stream of
//! bytes, and [`BufWriter`] collects small writes into few large ones. They
//! work on anything that implements [`Read`] and [`Write`]:
//!
//! - a [`TcpStream`](crate::net::TcpStream), owned, borrowed or in an `Rc`;
//! - a [`File`](crate::fs::File) wrapped in a [`Cursor`](crate::fs::Cursor),
//!   which supplies the position that files themselves do not have.
//!
//! ```no_run
//! use starfish::fs::{Cursor, File};
//! use starfish::io::BufReader;
//!
//! # async fn example() -> std::io::Result<()> {
//! let file = File::open("/etc/hosts").await?;
//! let mut lines = BufReader::new(Cursor::new(&file));
//! while let Some(line) = lines.next_line().await? {
//!     println!("{line}");
//! }
//! # Ok(())
//! # }
//! ```

use std::future::Future;
use std::io;
use std::pin::Pin;

/// A read that has been started and does not borrow what it reads from;
/// see [`Read::start_read`].
pub type PendingRead = Pin<Box<dyn Future<Output = (io::Result<usize>, Vec<u8>)>>>;

/// A source of bytes that is read in order: a TCP stream, or a file
/// through a [`Cursor`](crate::fs::Cursor).
///
/// This is what [`BufReader`] reads from. It is deliberately small; use
/// the reader's methods, not this one, unless you are implementing a new
/// source.
pub trait Read {
    /// Reads into `buf` from its start, replacing its contents: on success
    /// the buffer holds exactly the bytes read, at most its capacity.
    /// `Ok(0)` means there is nothing more (or the buffer has no
    /// capacity).
    fn read_owned(&mut self, buf: Vec<u8>) -> impl Future<Output = (io::Result<usize>, Vec<u8>)>;

    /// Starts a read whose future does not borrow the source, or hands
    /// the buffer back if this source cannot do that.
    ///
    /// A source whose reads take data away for good, as a socket's do,
    /// should provide this. [`BufReader`] then keeps the read it has
    /// started when its caller stops waiting, and what arrives is there
    /// for the next call, where a read that was dropped would take its
    /// data with it. The default says "cannot".
    fn start_read(&mut self, buf: Vec<u8>) -> std::result::Result<PendingRead, Vec<u8>> {
        Err(buf)
    }
}

/// A destination for bytes that are written in order: a TCP stream, or a
/// file through a [`Cursor`](crate::fs::Cursor).
///
/// This is what [`BufWriter`] writes to.
pub trait Write {
    /// Writes all of `buf`, in as many steps as it takes, and hands the buffer
    /// back with the outcome. On an error, how much was written before it is not
    /// known.
    fn write_all_owned(&mut self, buf: Vec<u8>) -> impl Future<Output = (io::Result<()>, Vec<u8>)>;
}

/// Default size of the buffers of [`BufReader`] and [`BufWriter`]: large
/// enough that small reads and writes are batched well, small enough to be
/// cheap per connection.
const DEFAULT_BUFFER: usize = 8 * 1024;

/// An `InvalidData` error with the given message: the stream held
/// something the reader cannot return, such as a line that is too long or is
/// not text.
fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

/// Reads through a buffer, for data made of lines or of pieces of known
/// length.
///
/// It reads from anything that implements [`Read`]:
///
/// - a TCP stream, owned or shared: `BufReader<TcpStream>`,
///   `BufReader<&TcpStream>` and `BufReader<Rc<TcpStream>>` all work, the
///   latter two so that another task can write to the same connection;
/// - a file, through a [`Cursor`](crate::fs::Cursor) that keeps the
///   position.
///
/// ```no_run
/// use starfish::net::{BufReader, BufWriter, TcpStream};
///
/// # async fn example(stream: TcpStream) -> std::io::Result<()> {
/// let mut reader = BufReader::new(&stream);
/// let mut writer = BufWriter::new(&stream);
/// while let Some(line) = reader.next_line().await? {
///     writer.write_all(line.to_uppercase().as_bytes()).await?;
///     writer.write_all(b"\n").await?;
///     writer.flush().await?;
/// }
/// # Ok(())
/// # }
/// ```
///
/// # Cancellation
///
/// A read that is given up, by [`timeout`](crate::timeout) or
/// [`select`](crate::select()) or an aborted task, loses nothing with
/// [`fill_buf`](Self::fill_buf), [`read`](Self::read),
/// [`read_line`](Self::read_line) and [`next_line`](Self::next_line): the
/// reader keeps the read it had started and the part of a line it had
/// collected, and the next call carries on from there.
///
/// [`read_until`](Self::read_until) appends to a buffer of the caller's,
/// so it carries on only if it is called again with that same buffer.
/// [`read_exact`](Self::read_exact) cannot be resumed: it does not record
/// how much of its output it had filled.
pub struct BufReader<S> {
    stream: S,
    /// What the last read from the stream delivered.
    buf: Vec<u8>,
    /// How much of `buf` has been handed out.
    pos: usize,
    capacity: usize,
    limit: usize,
    /// A read that was started and whose caller may have stopped waiting.
    pending: Option<PendingRead>,
    /// The part of a line collected so far by `read_line`.
    partial: Vec<u8>,
}

impl<S: Read> BufReader<S> {
    /// A reader with an 8 KiB buffer, which suits most line-based protocols.
    ///
    /// Nothing is read until the first call that needs data, and the buffer is
    /// allocated then.
    pub fn new(stream: S) -> BufReader<S> {
        BufReader::with_capacity(DEFAULT_BUFFER, stream)
    }

    /// A reader with a buffer of `capacity` bytes (at least one).
    ///
    /// The capacity is how much is asked of the stream at a time. A larger buffer
    /// means fewer reads for the same data; it does not limit the length of a
    /// line, which may span any number of buffers (see [`limit`](Self::limit) for
    /// that).
    pub fn with_capacity(capacity: usize, stream: S) -> BufReader<S> {
        BufReader {
            stream,
            buf: Vec::new(),
            pos: 0,
            capacity: capacity.max(1),
            limit: usize::MAX,
            pending: None,
            partial: Vec::new(),
        }
    }

    /// Sets the longest line, or other delimited item, the reader accepts,
    /// in bytes including the delimiter. A longer one makes
    /// [`read_until`](Self::read_until), [`read_line`](Self::read_line) and
    /// [`next_line`](Self::next_line) fail with `InvalidData`.
    ///
    /// Without a limit, which is the default, a peer that never sends the
    /// delimiter makes the reader collect data until memory runs out.
    pub fn limit(mut self, bytes: usize) -> BufReader<S> {
        self.limit = bytes;
        self
    }

    /// What is being read from: the stream or cursor the reader was given.
    ///
    /// Reading from it directly while the reader holds buffered data would skip
    /// that data. Use it for things that do not read, such as asking a socket
    /// for its peer's address.
    pub fn get_ref(&self) -> &S {
        &self.stream
    }

    /// What is being read from, for using it in ways that need exclusive
    /// access. This is how to write to a stream that a reader is wrapped
    /// around when the stream does both through `&mut self`, as a
    /// `tls::TlsStream` does:
    /// `reader.get_mut().write_all(reply).await`.
    ///
    /// Writing through it is fine. Reading through it would skip whatever
    /// the reader has buffered.
    pub fn get_mut(&mut self) -> &mut S {
        &mut self.stream
    }

    /// Gives back what was being read from, ending the reader.
    ///
    /// Bytes that were received but not yet read from the reader are lost, as is
    /// a read that was in progress; see [`buffer`](Self::buffer). To switch from
    /// buffered to direct reading without losing data, take what
    /// [`buffer`](Self::buffer) holds first.
    pub fn into_inner(self) -> S {
        self.stream
    }

    /// The bytes received from the stream and not yet read: what the next read
    /// would return without waiting. Empty when the reader has to fetch more.
    pub fn buffer(&self) -> &[u8] {
        self.buf.get(self.pos..).unwrap_or(&[])
    }

    /// Marks the first `count` bytes of [`buffer`](Self::buffer) as read, so
    /// that the next read starts after them. A `count` larger than the buffer
    /// consumes all of it.
    ///
    /// This is the second half of the [`fill_buf`](Self::fill_buf) and `consume`
    /// pair, for parsing in place without copying.
    pub fn consume(&mut self, count: usize) {
        self.pos = self.pos.saturating_add(count).min(self.buf.len());
    }

    /// Returns the bytes not yet read, first receiving more from the stream if
    /// there are none. An empty result means the peer has stopped sending (or the
    /// file has ended).
    ///
    /// The bytes stay in the reader until [`consume`](Self::consume) says how
    /// many were used, so a parser can look at them, find that it needs more, and
    /// call again later. All the other reading methods are built on this one.
    ///
    /// If the returned future is dropped while it waits, a read from a socket is
    /// kept and picked up by the next call; nothing is lost.
    ///
    /// # Errors
    ///
    /// The stream's error, if the read failed.
    pub async fn fill_buf(&mut self) -> io::Result<&[u8]> {
        if self.pos >= self.buf.len() {
            if self.pending.is_none() {
                let mut buf = std::mem::take(&mut self.buf);
                buf.clear();
                self.pos = 0;
                // Also restores the buffer after a read that was dropped.
                if buf.capacity() < self.capacity {
                    buf.reserve_exact(self.capacity);
                }
                match self.stream.start_read(buf) {
                    Ok(pending) => self.pending = Some(pending),
                    Err(buf) => {
                        let (result, buf) = self.stream.read_owned(buf).await;
                        self.buf = buf;
                        result?;
                        return Ok(self.buffer());
                    }
                }
            }
            if let Some(pending) = self.pending.as_mut() {
                // If the caller stops waiting here, the read stays in
                // `self.pending` and the next call picks it up.
                let (result, buf) = pending.as_mut().await;
                self.pending = None;
                self.buf = buf;
                self.pos = 0;
                result?;
            }
        }
        Ok(self.buffer())
    }

    /// Reads some bytes into `out` and returns how many: at least one unless
    /// the stream has ended, and at most `out.len()`. `Ok(0)` means the peer has
    /// stopped sending (or `out` is empty).
    ///
    /// It returns what is buffered, or what one read brings, without waiting for
    /// `out` to be filled; use [`read_exact`](Self::read_exact) for that.
    pub async fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let available = self.fill_buf().await?;
        let count = available.len().min(out.len());
        if let (Some(target), Some(source)) = (out.get_mut(..count), available.get(..count)) {
            target.copy_from_slice(source);
        }
        self.consume(count);
        Ok(count)
    }

    /// Fills all of `out`.
    ///
    /// # Errors
    ///
    /// `UnexpectedEof` if the peer stops sending first; the bytes read up
    /// to then are in `out`.
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

    /// Appends to `out` everything up to and including the next
    /// `delimiter`, and returns the number of bytes appended. If the peer
    /// stops sending first, what there was is appended without a
    /// delimiter; `Ok(0)` means there was nothing more.
    ///
    /// # Errors
    ///
    /// `InvalidData` if the item is longer than the [`limit`](Self::limit).
    pub async fn read_until(&mut self, delimiter: u8, out: &mut Vec<u8>) -> io::Result<usize> {
        let limit = self.limit;
        let mut total = 0usize;
        loop {
            let available = self.fill_buf().await?;
            if available.is_empty() {
                return Ok(total);
            }
            let (count, found) = match available.iter().position(|byte| *byte == delimiter) {
                Some(index) => (index.saturating_add(1), true),
                None => (available.len(), false),
            };
            total = total.saturating_add(count);
            if total > limit {
                return Err(invalid_data("line is longer than the reader's limit"));
            }
            out.extend_from_slice(available.get(..count).unwrap_or(available));
            self.consume(count);
            if found {
                return Ok(total);
            }
        }
    }

    /// Appends the next line to `line`, including its line ending, and
    /// returns the number of bytes appended. `Ok(0)` means the peer has
    /// stopped sending.
    ///
    /// # Errors
    ///
    /// `InvalidData` if the line is not valid UTF-8 or is longer than the
    /// [`limit`](Self::limit). The line is consumed all the same.
    pub async fn read_line(&mut self, line: &mut String) -> io::Result<usize> {
        // Collected in the reader, not in a local variable: if the caller
        // stops waiting part-way, the next call carries on.
        loop {
            self.fill_buf().await?;
            let available = self.buf.get(self.pos..).unwrap_or(&[]);
            if available.is_empty() {
                break; // the peer has stopped sending
            }
            let (count, found) = match available.iter().position(|byte| *byte == b'\n') {
                Some(index) => (index.saturating_add(1), true),
                None => (available.len(), false),
            };
            if self.partial.len().saturating_add(count) > self.limit {
                self.partial.clear();
                return Err(invalid_data("line is longer than the reader's limit"));
            }
            self.partial.extend_from_slice(available.get(..count).unwrap_or(available));
            self.pos = self.pos.saturating_add(count).min(self.buf.len());
            if found {
                break;
            }
        }
        let bytes = std::mem::take(&mut self.partial);
        let count = bytes.len();
        let text = String::from_utf8(bytes).map_err(|_| invalid_data("line is not valid UTF-8"))?;
        line.push_str(&text);
        Ok(count)
    }

    /// Returns the next line without its line ending (`\n` or `\r\n`), or
    /// `None` when the peer has stopped sending. A last line without a
    /// line ending is returned too.
    ///
    /// # Errors
    ///
    /// As for [`read_line`](Self::read_line).
    pub async fn next_line(&mut self) -> io::Result<Option<String>> {
        let mut line = String::new();
        if self.read_line(&mut line).await? == 0 {
            return Ok(None);
        }
        if line.ends_with('\n') {
            line.pop();
            if line.ends_with('\r') {
                line.pop();
            }
        }
        Ok(Some(line))
    }
}

/// Writes through a buffer, so that many small writes become few.
///
/// It writes to anything that implements [`Write`]: a TCP stream, owned
/// or shared, or a file through a [`Cursor`](crate::fs::Cursor).
///
/// Nothing reaches the stream or file until the buffer is full or
/// [`flush`](Self::flush) is called. Dropping the writer does **not**
/// flush: what is still buffered is lost. And flushing a file's writer
/// only hands the data to the kernel; it is on the device after the
/// file's own `sync_data`. See [`BufReader`] for an example.
pub struct BufWriter<S> {
    stream: S,
    buf: Vec<u8>,
    capacity: usize,
}

impl<S: Write> BufWriter<S> {
    /// A writer with an 8 KiB buffer. The buffer is allocated at the first
    /// write.
    pub fn new(stream: S) -> BufWriter<S> {
        BufWriter::with_capacity(DEFAULT_BUFFER, stream)
    }

    /// A writer with a buffer of `capacity` bytes (at least one).
    ///
    /// The capacity is how much collects before it is sent without being asked.
    /// Data longer than the buffer is sent in buffer-sized pieces.
    pub fn with_capacity(capacity: usize, stream: S) -> BufWriter<S> {
        BufWriter {
            stream,
            buf: Vec::new(),
            capacity: capacity.max(1),
        }
    }

    /// What is being written to: the stream or cursor the writer was given.
    ///
    /// Writing to it directly while the writer holds buffered data would put the
    /// new data ahead of the old. Flush first.
    pub fn get_ref(&self) -> &S {
        &self.stream
    }

    /// What is being written to, for using it in ways that need exclusive
    /// access, such as reading from a stream that reads and writes through
    /// `&mut self`.
    ///
    /// Reading through it is fine. Writing through it while the writer holds
    /// buffered data would put the new data ahead of the old; flush first.
    pub fn get_mut(&mut self) -> &mut S {
        &mut self.stream
    }

    /// Gives back what was being written to, ending the writer.
    ///
    /// Bytes still in the buffer are lost; [`flush`](Self::flush) first.
    pub fn into_inner(self) -> S {
        self.stream
    }

    /// The bytes written to the writer and not yet sent.
    pub fn buffer(&self) -> &[u8] {
        &self.buf
    }

    /// Adds all of `data` to the buffer, sending the buffer whenever it is
    /// full.
    ///
    /// If this fails, or its future is dropped while it is sending, part
    /// of what was written so far may have been sent and the rest is lost;
    /// the connection is then of no further use.
    pub async fn write_all(&mut self, data: &[u8]) -> io::Result<()> {
        let mut rest = data;
        while !rest.is_empty() {
            let room = self.capacity.saturating_sub(self.buf.len());
            if room == 0 {
                self.flush().await?;
                continue;
            }
            let (now, later) = rest.split_at_checked(room.min(rest.len())).unwrap_or((rest, &[]));
            self.buf.extend_from_slice(now);
            rest = later;
        }
        Ok(())
    }

    /// Sends everything in the buffer, and returns once the stream has
    /// accepted it all.
    ///
    /// For a socket that means the data is in the kernel's hands, on its way to
    /// the peer. For a file it means the data is in the page cache, not yet on
    /// the device: follow with the file's `sync_data` if it has to survive a
    /// crash. Flushing an empty buffer does nothing.
    ///
    /// # Errors
    ///
    /// The stream's error. The buffer is emptied all the same; how much of it
    /// was sent before the error is not known.
    pub async fn flush(&mut self) -> io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let buf = std::mem::take(&mut self.buf);
        let (result, mut buf) = self.stream.write_all_owned(buf).await;
        buf.clear();
        self.buf = buf;
        result
    }
}

debug_impl!(<S> BufReader<S>, "buffered" => |reader| reader.buf.len().saturating_sub(reader.pos));
debug_impl!(<S> BufWriter<S>, "buffered" => |writer| writer.buf.len());