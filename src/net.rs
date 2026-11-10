//! TCP sockets.
//!
//! A socket is not tied to a shard: each call submits to the ring of the
//! shard it is made on, so a connection can be handed to another shard with
//! [`submit_to`](crate::submit_to).
//!
//! Reads and writes accept any [`IoBufMut`] / [`IoBuf`]. Reads into a
//! [`FixedBuf`] use the shard's registered memory, and
//! [`TcpStream::send_zc`] transmits from one without copying.

use std::io;
use std::net::{Shutdown, SocketAddr};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};

use io_uring::{opcode, types};
use socket2::{Domain, Protocol, SockAddr, SockRef, Socket, Type};

use crate::buf::{FixedBuf, IoBuf, IoBufMut};
use crate::reactor::{HeldOp, Op};

fn to_socket_addr(addr: SockAddr) -> io::Result<SocketAddr> {
    addr.as_socket()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "not an IP socket address"))
}

fn clamp_len(len: usize) -> u32 {
    u32::try_from(len).unwrap_or(u32::MAX)
}

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
        Ok(TcpListener { fd: socket.into() })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        to_socket_addr(SockRef::from(&self.fd).local_addr()?)
    }

    /// Waits for the next connection.
    pub async fn accept(&self) -> io::Result<TcpStream> {
        let sqe = opcode::Accept::new(
            types::Fd(self.fd.as_raw_fd()),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
        .flags(libc::SOCK_CLOEXEC)
        .build();
        // SAFETY: the entry carries no pointers.
        let (result, ()) = unsafe { Op::new(sqe, ()) }.result_is_fd().await;
        let fd = RawFd::try_from(result?).map_err(|_| io::Error::other("invalid file descriptor"))?;
        // SAFETY: the kernel returned a new fd that nothing else owns.
        Ok(TcpStream {
            fd: unsafe { OwnedFd::from_raw_fd(fd) },
        })
    }
}

/// A connected TCP socket.
pub struct TcpStream {
    fd: OwnedFd,
}

impl TcpStream {
    pub async fn connect(addr: SocketAddr) -> io::Result<TcpStream> {
        let socket = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
        let fd: OwnedFd = socket.into();
        // Boxed so the address the kernel reads does not move with the future.
        let addr = Box::new(SockAddr::from(addr));
        let sqe = opcode::Connect::new(types::Fd(fd.as_raw_fd()), addr.as_ptr().cast(), addr.len())
            .build();
        // SAFETY: the pointer targets the boxed address held as op data.
        let (result, _addr) = unsafe { Op::new(sqe, addr) }.await;
        result?;
        Ok(TcpStream { fd })
    }

    /// Reads into `buf` from its start, replacing its contents: on success
    /// the buffer holds exactly the bytes received, at most its capacity.
    ///
    /// `Ok(0)` means the peer closed the connection (or the buffer has no
    /// capacity).
    pub async fn read<B: IoBufMut>(&self, mut buf: B) -> (io::Result<usize>, B) {
        let fd = types::Fd(self.fd.as_raw_fd());
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

    /// Writes some prefix of `buf` and returns how many bytes were sent.
    pub async fn write<B: IoBuf>(&self, buf: B) -> (io::Result<usize>, B) {
        self.send_from(buf, 0).await
    }

    /// Writes all of `buf`.
    pub async fn write_all<B: IoBuf>(&self, mut buf: B) -> (io::Result<()>, B) {
        let mut written = 0usize;
        while written < buf.len() {
            let (result, returned) = self.send_from(buf, written).await;
            buf = returned;
            match result {
                Ok(0) => return (Err(io::ErrorKind::WriteZero.into()), buf),
                Ok(n) => written = written.saturating_add(n),
                Err(err) => return (Err(err), buf),
            }
        }
        (Ok(()), buf)
    }

    async fn send_from<B: IoBuf>(&self, buf: B, offset: usize) -> (io::Result<usize>, B) {
        let fd = types::Fd(self.fd.as_raw_fd());
        let offset = offset.min(buf.len());
        let len = clamp_len(buf.len().saturating_sub(offset));
        // Stays inside the buffer: `offset <= len`.
        let ptr = buf.as_ptr().wrapping_add(offset);
        let sqe = opcode::Send::new(fd, ptr, len).flags(libc::MSG_NOSIGNAL).build();
        // SAFETY: the pointer targets memory the buffer keeps valid and in
        // place for as long as the op holds the buffer (`IoBuf`).
        let (result, buf) = unsafe { Op::new(sqe, buf) }.await;
        (result.map(|n| n as usize), buf)
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

    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        to_socket_addr(SockRef::from(&self.fd).peer_addr()?)
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        to_socket_addr(SockRef::from(&self.fd).local_addr()?)
    }

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
        Ok(UdpSocket { fd: socket.into() })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        to_socket_addr(SockRef::from(&self.fd).local_addr()?)
    }

    /// Sets the peer for [`send`](Self::send) and [`recv`](Self::recv);
    /// datagrams from anyone else are then discarded by the kernel.
    pub fn connect(&self, addr: SocketAddr) -> io::Result<()> {
        SockRef::from(&self.fd).connect(&addr.into())
    }

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
    /// contents, and reports who sent it. A datagram longer than the
    /// buffer's capacity is cut short.
    pub async fn recv_from<B: IoBufMut>(&self, mut buf: B) -> (io::Result<(usize, SocketAddr)>, B) {
        let datagram = DatagramBox::new(buf.as_mut_ptr(), buf.capacity());
        let sqe = opcode::RecvMsg::new(types::Fd(self.fd.as_raw_fd()), datagram.header()).build();
        // SAFETY: as in `send_to`.
        let (result, (mut buf, datagram)) = unsafe { Op::new(sqe, (buf, datagram)) }.await;
        let received = result.and_then(|n| {
            let n = (n as usize).min(buf.capacity());
            // SAFETY: the kernel initialised the first `n` bytes, and `n`
            // is within the capacity.
            unsafe { buf.set_len(n) };
            Ok((n, datagram.address()?))
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
    /// start, replacing its contents.
    pub async fn recv<B: IoBufMut>(&self, mut buf: B) -> (io::Result<usize>, B) {
        let len = clamp_len(buf.capacity());
        let sqe = opcode::Recv::new(types::Fd(self.fd.as_raw_fd()), buf.as_mut_ptr(), len).build();
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
}
