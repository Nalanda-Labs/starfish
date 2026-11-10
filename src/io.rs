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
    fn read_some(&mut self, buf: Vec<u8>) -> impl Future<Output = (io::Result<usize>, Vec<u8>)>;
}

/// A destination for bytes that are written in order: a TCP stream, or a
/// file through a [`Cursor`](crate::fs::Cursor).
///
/// This is what [`BufWriter`] writes to.
pub trait Write {
    /// Writes all of `buf`.
    fn write_whole(&mut self, buf: Vec<u8>) -> impl Future<Output = (io::Result<()>, Vec<u8>)>;
}

/// Default size of the buffers of [`BufReader`] and [`BufWriter`].
const DEFAULT_BUFFER: usize = 8 * 1024;

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
/// Dropping one of the reading futures while it waits for the network may
/// lose the bytes that were arriving at that moment. After a
/// [`timeout`](crate::timeout) on a read, the position in the stream is
/// unknown and the connection should be closed. On a file, the reader
/// would likewise skip or repeat data: start again with a new cursor.
pub struct BufReader<S> {
    stream: S,
    /// What the last read from the stream delivered.
    buf: Vec<u8>,
    /// How much of `buf` has been handed out.
    pos: usize,
    capacity: usize,
    limit: usize,
}

impl<S: Read> BufReader<S> {
    /// A reader with an 8 KiB buffer.
    pub fn new(stream: S) -> BufReader<S> {
        BufReader::with_capacity(DEFAULT_BUFFER, stream)
    }

    /// A reader with a buffer of `capacity` bytes (at least one).
    pub fn with_capacity(capacity: usize, stream: S) -> BufReader<S> {
        BufReader {
            stream,
            buf: Vec::new(),
            pos: 0,
            capacity: capacity.max(1),
            limit: usize::MAX,
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

    /// What is being read from.
    pub fn get_ref(&self) -> &S {
        &self.stream
    }

    /// Gives back what was being read from. Bytes that were received but
    /// not yet read from the reader are lost; see [`buffer`](Self::buffer).
    pub fn into_inner(self) -> S {
        self.stream
    }

    /// The bytes received from the stream and not yet read.
    pub fn buffer(&self) -> &[u8] {
        self.buf.get(self.pos..).unwrap_or(&[])
    }

    /// Marks the first `count` bytes of [`buffer`](Self::buffer) as read.
    pub fn consume(&mut self, count: usize) {
        self.pos = self.pos.saturating_add(count).min(self.buf.len());
    }

    /// Returns the bytes not yet read, first receiving more from the
    /// stream if there are none. An empty result means the peer has
    /// stopped sending. Follow with [`consume`](Self::consume).
    pub async fn fill_buf(&mut self) -> io::Result<&[u8]> {
        if self.pos >= self.buf.len() {
            let mut buf = std::mem::take(&mut self.buf);
            buf.clear();
            self.pos = 0;
            // Also restores the buffer after a read that was dropped.
            if buf.capacity() < self.capacity {
                buf.reserve_exact(self.capacity);
            }
            let (result, buf) = self.stream.read_some(buf).await;
            self.buf = buf;
            result?;
        }
        Ok(self.buffer())
    }

    /// Reads some bytes into `out` and returns how many. `Ok(0)` means the
    /// peer has stopped sending (or `out` is empty).
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
        let mut bytes = Vec::new();
        let count = self.read_until(b'\n', &mut bytes).await?;
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
    /// A writer with an 8 KiB buffer.
    pub fn new(stream: S) -> BufWriter<S> {
        BufWriter::with_capacity(DEFAULT_BUFFER, stream)
    }

    /// A writer with a buffer of `capacity` bytes (at least one).
    pub fn with_capacity(capacity: usize, stream: S) -> BufWriter<S> {
        BufWriter {
            stream,
            buf: Vec::new(),
            capacity: capacity.max(1),
        }
    }

    /// What is being written to.
    pub fn get_ref(&self) -> &S {
        &self.stream
    }

    /// Gives back what was being written to. Bytes still in the buffer
    /// are lost; [`flush`](Self::flush) first.
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

    /// Sends everything in the buffer.
    pub async fn flush(&mut self) -> io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let buf = std::mem::take(&mut self.buf);
        let (result, mut buf) = self.stream.write_whole(buf).await;
        buf.clear();
        self.buf = buf;
        result
    }
}

debug_impl!(<S> BufReader<S>, "buffered" => |reader| reader.buf.len().saturating_sub(reader.pos));
debug_impl!(<S> BufWriter<S>, "buffered" => |writer| writer.buf.len());