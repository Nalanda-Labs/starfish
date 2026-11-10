//! TCP, UDP and Unix-domain sockets, on the kernel's network stack.
//!
//! A socket is not tied to a shard: each call submits to the ring of the
//! shard it is made on, so a connection can be handed to another shard with
//! [`submit_to`](crate::submit_to).
//!
//! Reads and writes accept any [`IoBufMut`] / [`IoBuf`]. Reads into a
//! [`FixedBuf`] use the shard's registered memory, and
//! [`TcpStream::send_zc`] transmits from one without copying.
//!
//! [`BufReader`] and [`BufWriter`] (from [`io`](crate::io), re-exported
//! here) put a buffer in front of a TCP stream, for protocols made of lines
//! or of many small pieces.

use std::io;
use std::net::{Shutdown, SocketAddr};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::rc::Rc;

use io_uring::{opcode, types};
use socket2::{Domain, Protocol, SockAddr, SockRef, Socket, Type};

use crate::buf::{FixedBuf, IoBuf, IoBufMut};
use crate::reactor::{HeldOp, Op};

pub use crate::io::{BufReader, BufWriter};

fn to_socket_addr(addr: SockAddr) -> io::Result<SocketAddr> {
    addr.as_socket()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "not an IP socket address"))
}

fn clamp_len(len: usize) -> u32 {
    u32::try_from(len).unwrap_or(u32::MAX)
}

/// What a datagram receive brought: its length if it fitted the buffer.
///
/// The receive asks the kernel for the datagram's true length, which may
/// be more than the buffer could hold. The buffer is left holding what did
/// fit either way.
///
/// # Safety
///
/// The kernel must have initialised the first `min(received, capacity)`
/// bytes of `buf`.
unsafe fn datagram_received<B: IoBufMut>(buf: &mut B, received: usize) -> io::Result<usize> {
    let capacity = buf.capacity();
    buf.set_len(received.min(capacity));
    if received > capacity {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("a datagram of {received} bytes was cut to the buffer's {capacity}"),
        ));
    }
    Ok(received)
}

fn no_room() -> io::Error {
    // A read of nothing would return `Ok(0)`, which means "the peer has
    // closed" to every caller.
    io::Error::new(io::ErrorKind::InvalidInput, "read buffer has no room")
}

/// Reads from a stream socket into `buf` from its start.
async fn stream_read<B: IoBufMut>(fd: RawFd, mut buf: B) -> (io::Result<usize>, B) {
    if buf.capacity() == 0 {
        return (Err(no_room()), buf);
    }
    let fd = types::Fd(fd);
    let len = clamp_len(buf.capacity());
    let ptr = buf.as_mut_ptr();
    let sqe = match buf.fixed_index() {
        Some(index) => opcode::ReadFixed::new(fd, ptr, len, index).build(),
        None => opcode::Recv::new(fd, ptr, len).build(),
    };
    // SAFETY: the pointer targets memory the buffer keeps valid and in
    // place for as long as the op holds the buffer (`IoBufMut`).
    let (result, mut buf) = unsafe { Op::new(sqe, buf) }.await;
    match result {
        Ok(n) => {
            let n = (n as usize).min(buf.capacity());
            // SAFETY: the kernel initialised the first `n` bytes, and
            // `n` is within the capacity.
            unsafe { buf.set_len(n) };
            (Ok(n), buf)
        }
        Err(err) => (Err(err), buf),
    }
}

/// Sends some of `buf`, from `offset` on, to a stream socket.
async fn stream_send_from<B: IoBuf>(fd: RawFd, buf: B, offset: usize) -> (io::Result<usize>, B) {
    let offset = offset.min(buf.len());
    let len = clamp_len(buf.len().saturating_sub(offset));
    // Stays inside the buffer: `offset <= len`.
    let ptr = buf.as_ptr().wrapping_add(offset);
    let sqe = opcode::Send::new(types::Fd(fd), ptr, len).flags(libc::MSG_NOSIGNAL).build();
    // SAFETY: the pointer targets memory the buffer keeps valid and in
    // place for as long as the op holds the buffer (`IoBuf`).
    let (result, buf) = unsafe { Op::new(sqe, buf) }.await;
    (result.map(|n| n as usize), buf)
}

/// Sends all of `buf` to a stream socket.
async fn stream_write_all<B: IoBuf>(fd: RawFd, mut buf: B) -> (io::Result<()>, B) {
    let mut written = 0usize;
    while written < buf.len() {
        let (result, returned) = stream_send_from(fd, buf, written).await;
        buf = returned;
        match result {
            Ok(0) => return (Err(io::ErrorKind::WriteZero.into()), buf),
            Ok(n) => written = written.saturating_add(n),
            Err(err) => return (Err(err), buf),
        }
    }
    (Ok(()), buf)
}

/// Waits for the next connection on a listening socket.
async fn accept_fd(listener: RawFd) -> io::Result<OwnedFd> {
    let sqe = opcode::Accept::new(types::Fd(listener), std::ptr::null_mut(), std::ptr::null_mut())
        .flags(libc::SOCK_CLOEXEC)
        .build();
    // SAFETY: the entry carries no pointers.
    let (result, ()) = unsafe { Op::new(sqe, ()) }.result_is_fd().await;
    let fd = RawFd::try_from(result?).map_err(|_| io::Error::other("invalid file descriptor"))?;
    // SAFETY: the kernel returned a new fd that nothing else owns.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// Connects a socket to `addr`.
async fn connect_fd(fd: &OwnedFd, addr: SockAddr) -> io::Result<()> {
    // Boxed so the address the kernel reads does not move with the future.
    let addr = Box::new(addr);
    let sqe = opcode::Connect::new(types::Fd(fd.as_raw_fd()), addr.as_ptr().cast(), addr.len()).build();
    // SAFETY: the pointer targets the boxed address held as op data.
    let (result, _addr) = unsafe { Op::new(sqe, addr) }.await;
    result.map(|_| ())
}

/// Makes the buffered reader and writer work on a kind of stream, held by
/// value, by reference or in an `Rc`.
macro_rules! buffered_stream {
    ($stream:ty) => {
        buffered_stream!(@impl $stream, $stream);
        buffered_stream!(@impl $stream, &$stream);
        buffered_stream!(@impl $stream, std::rc::Rc<$stream>);
    };
    (@impl $stream:ty, $holder:ty) => {
        impl crate::io::Read for $holder {
            async fn read_some(&mut self, buf: Vec<u8>) -> (io::Result<usize>, Vec<u8>) {
                <$stream>::read(self, buf).await
            }

            fn start_read(&mut self, buf: Vec<u8>) -> std::result::Result<crate::io::PendingRead, Vec<u8>> {
                // Holds the descriptor's number, not the stream: whoever
                // keeps this future keeps the stream as well.
                Ok(Box::pin(stream_read(self.fd.as_raw_fd(), buf)))
            }
        }

        impl crate::io::Write for $holder {
            async fn write_whole(&mut self, buf: Vec<u8>) -> (io::Result<()>, Vec<u8>) {
                <$stream>::write_all(self, buf).await
            }
        }
    };
}

buffered_stream!(TcpStream);
buffered_stream!(UnixStream);

/// A listening TCP socket.
pub struct TcpListener {
    fd: OwnedFd,
}

impl TcpListener {
    /// Binds with `SO_REUSEPORT`, so every shard can bind its own listener to
    /// the same address and the kernel spreads connections across them.
    pub fn bind(addr: SocketAddr) -> io::Result<TcpListener> {
        let socket = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
        socket.set_reuse_address(true)?;
        socket.set_reuse_port(true)?;
        socket.bind(&addr.into())?;
        socket.listen(1024)?;
        Ok(TcpListener {
            fd: socket.into(),
        })
    }

    /// The address the listener is bound to. After binding to port 0, this
    /// tells which port the kernel chose.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        to_socket_addr(SockRef::from(&self.fd).local_addr()?)
    }

    /// Waits for the next connection.
    ///
    /// Dropping the future cancels the wait. A connection that arrives at
    /// that very moment is closed, not handed to a later call.
    pub async fn accept(&self) -> io::Result<TcpStream> {
        Ok(TcpStream {
            fd: accept_fd(self.fd.as_raw_fd()).await?,
        })
    }
}

/// A connected TCP socket.
pub struct TcpStream {
    fd: OwnedFd,
}

impl TcpStream {
    /// Opens a connection to `addr`.
    pub async fn connect(addr: SocketAddr) -> io::Result<TcpStream> {
        let socket = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
        let fd: OwnedFd = socket.into();
        connect_fd(&fd, SockAddr::from(addr)).await?;
        Ok(TcpStream { fd })
    }

    /// Reads into `buf` from its start, replacing its contents: on success
    /// the buffer holds exactly the bytes received, at most its capacity.
    ///
    /// `Ok(0)` means the peer closed the connection.
    ///
    /// # Errors
    ///
    /// `InvalidInput` if the buffer has no capacity, as `Vec::new()` has
    /// none: there would be no telling that from a closed connection.
    pub async fn read<B: IoBufMut>(&self, buf: B) -> (io::Result<usize>, B) {
        stream_read(self.fd.as_raw_fd(), buf).await
    }

    /// Writes some prefix of `buf` and returns how many bytes were sent.
    pub async fn write<B: IoBuf>(&self, buf: B) -> (io::Result<usize>, B) {
        stream_send_from(self.fd.as_raw_fd(), buf, 0).await
    }

    /// Writes all of `buf`.
    pub async fn write_all<B: IoBuf>(&self, buf: B) -> (io::Result<()>, B) {
        stream_write_all(self.fd.as_raw_fd(), buf).await
    }

    /// Sends some prefix of `buf` without copying it: the network stack
    /// transmits straight from the registered memory. Returns how many
    /// bytes were accepted.
    ///
    /// The call takes the buffer for good. It returns as soon as the data
    /// is queued, but the kernel keeps reading the buffer until the peer
    /// has acknowledged the data; only then does the buffer go back to the
    /// pool. A sender that outruns its peer therefore runs out of buffers
    /// and waits in [`FixedBuf::acquire`], which is the backpressure.
    ///
    /// Avoiding the copy pays off for large payloads, roughly from 10 KiB
    /// up. For small messages [`write`](Self::write) is faster.
    ///
    /// The kernel may accept less than the whole buffer, and the buffer is
    /// gone either way; [`send_all_zc`](Self::send_all_zc) sends all of
    /// it.
    pub async fn send_zc(&self, buf: FixedBuf) -> io::Result<usize> {
        let fd = types::Fd(self.fd.as_raw_fd());
        let len = clamp_len(buf.len());
        let sqe = opcode::SendZc::new(fd, IoBuf::as_ptr(&buf), len)
            .buf_index(IoBuf::fixed_index(&buf))
            .flags(libc::MSG_NOSIGNAL)
            .build();
        // SAFETY: the pointer targets the buffer's registered memory, which
        // the reactor keeps until the kernel releases it.
        let sent = unsafe { HeldOp::new(sqe, buf) }.await?;
        Ok(sent as usize)
    }

    /// Sends all of `buf` without copying it, in as many steps as it
    /// takes. Like [`send_zc`](Self::send_zc), it returns once the data is
    /// queued, and the buffer goes back to the pool when the peer has
    /// acknowledged all of it.
    pub async fn send_all_zc(&self, buf: FixedBuf) -> io::Result<()> {
        let fd = types::Fd(self.fd.as_raw_fd());
        let index = IoBuf::fixed_index(&buf);
        let start = IoBuf::as_ptr(&buf);
        let total = buf.len();
        // Every step's send keeps the buffer until the kernel is done
        // with that step, so the buffer is shared between them.
        let buf = Rc::new(buf);
        let mut sent = 0usize;
        while sent < total {
            let len = clamp_len(total.saturating_sub(sent));
            // Stays inside the buffer: `sent < total`.
            let sqe = opcode::SendZc::new(fd, start.wrapping_add(sent), len)
                .buf_index(index)
                .flags(libc::MSG_NOSIGNAL)
                .build();
            // SAFETY: the pointer targets the buffer's registered memory,
            // which the reactor keeps, through its share of the buffer,
            // until the kernel releases it.
            match unsafe { HeldOp::new(sqe, buf.clone()) }.await? {
                0 => return Err(io::ErrorKind::WriteZero.into()),
                accepted => sent = sent.saturating_add(accepted as usize),
            }
        }
        Ok(())
    }

    /// The address of the other end of the connection.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        to_socket_addr(SockRef::from(&self.fd).peer_addr()?)
    }

    /// The address of this end of the connection.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        to_socket_addr(SockRef::from(&self.fd).local_addr()?)
    }

    /// Turns `TCP_NODELAY` on or off. On, small writes are sent at once
    /// instead of being held back to be merged with later ones (Nagle's
    /// algorithm), which is what request/response protocols want.
    pub fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        SockRef::from(&self.fd).set_nodelay(nodelay)
    }

    /// Shuts down the read half, the write half or both.
    pub fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        SockRef::from(&self.fd).shutdown(how)
    }
}

/// The kernel-facing description of one datagram: the message header and
/// the two things it points to. It lives in its own heap allocation, at a
/// fixed address, because the header holds pointers into it.
struct Datagram {
    header: libc::msghdr,
    part: libc::iovec,
    address: libc::sockaddr_storage,
}

/// Owns a [`Datagram`] through a raw pointer, so that the pointers inside
/// it stay valid however this handle is moved.
struct DatagramBox(std::ptr::NonNull<Datagram>);

impl DatagramBox {
    /// Describes `len` bytes at `data`, with room for a peer address.
    fn new(data: *mut u8, len: usize) -> DatagramBox {
        // SAFETY: all three are plain C structures for which all-zero is a
        // valid value.
        let datagram: Box<Datagram> = Box::new(unsafe { std::mem::zeroed() });
        let raw = Box::into_non_null(datagram);
        let this = raw.as_ptr();
        // SAFETY: `this` points to the allocation just made; the pointers
        // stored are to its own fields.
        unsafe {
            (*this).part.iov_base = data.cast();
            (*this).part.iov_len = len;
            (*this).header.msg_iov = std::ptr::addr_of_mut!((*this).part);
            (*this).header.msg_iovlen = 1;
            (*this).header.msg_name = std::ptr::addr_of_mut!((*this).address).cast();
            (*this).header.msg_namelen = std::mem::size_of::<libc::sockaddr_storage>() as u32;
        }
        DatagramBox(raw)
    }

    fn header(&self) -> *mut libc::msghdr {
        // SAFETY: the allocation is live for as long as `self`.
        unsafe { std::ptr::addr_of_mut!((*self.0.as_ptr()).header) }
    }

    /// Sets the destination.
    fn set_address(&mut self, addr: &SockAddr) {
        let len = (addr.len() as usize).min(std::mem::size_of::<libc::sockaddr_storage>());
        // SAFETY: copies at most the size of the destination from an
        // address structure of at least `len` bytes.
        unsafe {
            let this = self.0.as_ptr();
            std::ptr::copy_nonoverlapping(
                addr.as_ptr().cast::<u8>(),
                std::ptr::addr_of_mut!((*this).address).cast::<u8>(),
                len,
            );
            (*this).header.msg_namelen = len as u32;
        }
    }

    /// The peer address the kernel filled in.
    fn address(&self) -> io::Result<SocketAddr> {
        // SAFETY: the kernel wrote `msg_namelen` bytes of address into the
        // storage, which is large enough for any address family.
        let addr = unsafe {
            let this = self.0.as_ptr();
            SockAddr::new((*this).address, (*this).header.msg_namelen)
        };
        to_socket_addr(addr)
    }
}

impl Drop for DatagramBox {
    fn drop(&mut self) {
        // SAFETY: made by `Box::into_non_null` in `new`, freed once.
        drop(unsafe { Box::from_raw(self.0.as_ptr()) });
    }
}

/// A UDP socket.
///
/// Like the TCP types it is not tied to a shard, and with `SO_REUSEPORT`
/// every shard can bind its own socket to the same address so that the
/// kernel spreads incoming datagrams across them.
pub struct UdpSocket {
    fd: OwnedFd,
}

impl UdpSocket {
    /// Binds with `SO_REUSEPORT`.
    pub fn bind(addr: SocketAddr) -> io::Result<UdpSocket> {
        let socket = Socket::new(Domain::for_address(addr), Type::DGRAM, Some(Protocol::UDP))?;
        socket.set_reuse_address(true)?;
        socket.set_reuse_port(true)?;
        socket.bind(&addr.into())?;
        Ok(UdpSocket {
            fd: socket.into(),
        })
    }

    /// The address the socket is bound to. After binding to port 0, this
    /// tells which port the kernel chose.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        to_socket_addr(SockRef::from(&self.fd).local_addr()?)
    }

    /// Sets the peer for [`send`](Self::send) and [`recv`](Self::recv);
    /// datagrams from anyone else are then discarded by the kernel.
    pub fn connect(&self, addr: SocketAddr) -> io::Result<()> {
        SockRef::from(&self.fd).connect(&addr.into())
    }

    /// Allows or forbids sending to a broadcast address (`SO_BROADCAST`).
    pub fn set_broadcast(&self, broadcast: bool) -> io::Result<()> {
        SockRef::from(&self.fd).set_broadcast(broadcast)
    }

    /// Sends the contents of `buf` as one datagram to `addr`. Returns how
    /// many bytes were sent, which for UDP is all of them or an error.
    pub async fn send_to<B: IoBuf>(&self, buf: B, addr: SocketAddr) -> (io::Result<usize>, B) {
        let mut datagram = DatagramBox::new(buf.as_ptr().cast_mut(), buf.len());
        datagram.set_address(&SockAddr::from(addr));
        let sqe = opcode::SendMsg::new(types::Fd(self.fd.as_raw_fd()), datagram.header())
            .flags(libc::MSG_NOSIGNAL as u32)
            .build();
        // SAFETY: the header and everything it points to (the datagram
        // allocation and the buffer's memory) are owned by the op data and
        // do not move with it.
        let (result, (buf, _datagram)) = unsafe { Op::new(sqe, (buf, datagram)) }.await;
        (result.map(|n| n as usize), buf)
    }

    /// Receives one datagram into `buf` from its start, replacing its
    /// contents, and reports who sent it.
    ///
    /// # Errors
    ///
    /// `InvalidData` if the datagram was longer than the buffer's
    /// capacity. The buffer comes back holding the part that fitted; the
    /// rest is gone. `InvalidInput` if the buffer has no capacity.
    pub async fn recv_from<B: IoBufMut>(&self, mut buf: B) -> (io::Result<(usize, SocketAddr)>, B) {
        if buf.capacity() == 0 {
            return (Err(no_room()), buf);
        }
        let datagram = DatagramBox::new(buf.as_mut_ptr(), buf.capacity());
        // With this flag the kernel reports the datagram's true length,
        // even when that is more than it could store.
        let sqe = opcode::RecvMsg::new(types::Fd(self.fd.as_raw_fd()), datagram.header())
            .flags(libc::MSG_TRUNC as u32)
            .build();
        // SAFETY: as in `send_to`.
        let (result, (mut buf, datagram)) = unsafe { Op::new(sqe, (buf, datagram)) }.await;
        let received = result.and_then(|n| {
            // SAFETY: the kernel initialised as much of the datagram as
            // fitted.
            let length = unsafe { datagram_received(&mut buf, n as usize) }?;
            Ok((length, datagram.address()?))
        });
        (received, buf)
    }

    /// Sends the contents of `buf` as one datagram to the connected peer.
    pub async fn send<B: IoBuf>(&self, buf: B) -> (io::Result<usize>, B) {
        let len = clamp_len(buf.len());
        let sqe = opcode::Send::new(types::Fd(self.fd.as_raw_fd()), buf.as_ptr(), len)
            .flags(libc::MSG_NOSIGNAL)
            .build();
        // SAFETY: the pointer targets memory the buffer keeps valid and in
        // place for as long as the op holds the buffer (`IoBuf`).
        let (result, buf) = unsafe { Op::new(sqe, buf) }.await;
        (result.map(|n| n as usize), buf)
    }

    /// Receives one datagram from the connected peer into `buf` from its
    /// start, replacing its contents. Fails as
    /// [`recv_from`](Self::recv_from) does if the datagram does not fit.
    pub async fn recv<B: IoBufMut>(&self, mut buf: B) -> (io::Result<usize>, B) {
        if buf.capacity() == 0 {
            return (Err(no_room()), buf);
        }
        let len = clamp_len(buf.capacity());
        let sqe = opcode::Recv::new(types::Fd(self.fd.as_raw_fd()), buf.as_mut_ptr(), len)
            .flags(libc::MSG_TRUNC)
            .build();
        // SAFETY: the pointer targets memory the buffer keeps valid and in
        // place for as long as the op holds the buffer (`IoBufMut`).
        let (result, mut buf) = unsafe { Op::new(sqe, buf) }.await;
        // SAFETY: the kernel initialised as much of the datagram as fitted.
        let received = result.and_then(|n| unsafe { datagram_received(&mut buf, n as usize) });
        (received, buf)
    }
}

/// A listening Unix-domain socket: connections from other processes on
/// the same machine, addressed by a path in the filesystem.
///
/// Unlike a TCP port, a path can have only one listening socket, so the
/// per-shard listeners of a TCP server are not possible. Instead, give
/// every shard a handle to the same socket with
/// [`try_clone`](Self::try_clone); each connection then goes to one of the
/// shards waiting in [`accept`](Self::accept).
pub struct UnixListener {
    fd: OwnedFd,
}

impl UnixListener {
    /// Creates the socket file at `path` and listens on it.
    ///
    /// # Errors
    ///
    /// `AddrInUse` if anything exists at `path`, including the socket file
    /// of an earlier run: the file is not removed when the listener is
    /// dropped. A server normally removes a stale file before binding.
    pub fn bind(path: impl AsRef<std::path::Path>) -> io::Result<UnixListener> {
        let socket = Socket::new(Domain::UNIX, Type::STREAM, None)?;
        socket.set_cloexec(true)?;
        socket.bind(&SockAddr::unix(path)?)?;
        socket.listen(1024)?;
        Ok(UnixListener {
            fd: socket.into(),
        })
    }

    /// Another handle to the same listening socket, for another shard.
    pub fn try_clone(&self) -> io::Result<UnixListener> {
        Ok(UnixListener {
            fd: self.fd.try_clone()?,
        })
    }

    /// Waits for the next connection.
    ///
    /// Dropping the future cancels the wait. A connection that arrives at
    /// that very moment is closed, not handed to a later call.
    pub async fn accept(&self) -> io::Result<UnixStream> {
        Ok(UnixStream {
            fd: accept_fd(self.fd.as_raw_fd()).await?,
        })
    }
}

/// A connected Unix-domain socket. It reads and writes like a
/// [`TcpStream`], works with [`BufReader`] and [`BufWriter`], and can be
/// moved to another shard.
pub struct UnixStream {
    fd: OwnedFd,
}

impl UnixStream {
    /// Connects to the socket at `path`.
    pub async fn connect(path: impl AsRef<std::path::Path>) -> io::Result<UnixStream> {
        let socket = Socket::new(Domain::UNIX, Type::STREAM, None)?;
        socket.set_cloexec(true)?;
        let fd: OwnedFd = socket.into();
        connect_fd(&fd, SockAddr::unix(path)?).await?;
        Ok(UnixStream { fd })
    }

    /// Reads into `buf` from its start, replacing its contents: on success
    /// the buffer holds exactly the bytes received, at most its capacity.
    ///
    /// `Ok(0)` means the peer closed the connection.
    ///
    /// # Errors
    ///
    /// `InvalidInput` if the buffer has no capacity, as `Vec::new()` has
    /// none: there would be no telling that from a closed connection.
    pub async fn read<B: IoBufMut>(&self, buf: B) -> (io::Result<usize>, B) {
        stream_read(self.fd.as_raw_fd(), buf).await
    }

    /// Writes some prefix of `buf` and returns how many bytes were sent.
    pub async fn write<B: IoBuf>(&self, buf: B) -> (io::Result<usize>, B) {
        stream_send_from(self.fd.as_raw_fd(), buf, 0).await
    }

    /// Writes all of `buf`.
    pub async fn write_all<B: IoBuf>(&self, buf: B) -> (io::Result<()>, B) {
        stream_write_all(self.fd.as_raw_fd(), buf).await
    }

    /// Shuts down the read half, the write half or both.
    pub fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        SockRef::from(&self.fd).shutdown(how)
    }

    /// The process on the other end: its process, user and group ids, as
    /// they were when it connected. This is how a server on a Unix socket
    /// knows who is calling, without any password.
    pub fn peer_credentials(&self) -> io::Result<PeerCredentials> {
        let mut credentials = libc::ucred { pid: 0, uid: 0, gid: 0 };
        let mut length = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
        // SAFETY: the kernel writes at most `length` bytes into a live
        // structure of exactly that size.
        let failed = unsafe {
            libc::getsockopt(
                self.fd.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                std::ptr::addr_of_mut!(credentials).cast(),
                &mut length,
            ) != 0
        };
        if failed {
            return Err(io::Error::last_os_error());
        }
        Ok(PeerCredentials {
            process: credentials.pid,
            user: credentials.uid,
            group: credentials.gid,
        })
    }
}

/// Who is on the other end of a [`UnixStream`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct PeerCredentials {
    /// Process id.
    pub process: i32,
    /// User id.
    pub user: u32,
    /// Group id.
    pub group: u32,
}

debug_impl!(TcpListener, "fd" => |socket| socket.fd.as_raw_fd());
debug_impl!(TcpStream, "fd" => |socket| socket.fd.as_raw_fd());
debug_impl!(UdpSocket, "fd" => |socket| socket.fd.as_raw_fd());
debug_impl!(UnixListener, "fd" => |socket| socket.fd.as_raw_fd());
debug_impl!(UnixStream, "fd" => |socket| socket.fd.as_raw_fd());