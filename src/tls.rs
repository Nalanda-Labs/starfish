//! TLS over the runtime's streams, with [rustls].
//!
//! This module exists only with the crate's `tls` feature:
//!
//! ```toml
//! starfish = { version = "0.1", features = ["tls"] }
//! ```
//!
//! A [`TlsAcceptor`] turns an accepted connection into a [`TlsStream`], a
//! [`TlsConnector`] does the same for one that was dialled, and the stream
//! then reads and writes plain bytes while TLS travels underneath. What
//! certificates to present and which to trust is decided by a rustls
//! configuration, which the application builds; rustls is re-exported here
//! so that its types are at hand.
//!
//! ```no_run
//! use std::sync::Arc;
//! use starfish::net::TcpListener;
//! use starfish::tls::{rustls, TlsAcceptor};
//!
//! # async fn example(
//! #     certificates: Vec<rustls::pki_types::CertificateDer<'static>>,
//! #     key: rustls::pki_types::PrivateKeyDer<'static>,
//! # ) -> Result<(), Box<dyn std::error::Error>> {
//! let config = rustls::ServerConfig::builder()
//!     .with_no_client_auth()
//!     .with_single_cert(certificates, key)?;
//! let acceptor = TlsAcceptor::new(Arc::new(config));
//!
//! let listener = TcpListener::bind("0.0.0.0:8443".parse()?)?;
//! loop {
//!     let connection = listener.accept().await?;
//!     let acceptor = acceptor.clone();
//!     starfish::spawn(async move {
//!         let mut stream = acceptor.accept(connection).await?;
//!         let mut request = [0u8; 1024];
//!         let length = stream.read(&mut request).await?;
//!         stream.write_all(request.get(..length).unwrap_or_default()).await?;
//!         stream.shutdown().await
//!     });
//! }
//! # }
//! ```
//!
//! The cryptography is rustls's, with the *ring* provider. The acceptor and
//! connector are cheap to clone and can be sent to every shard; a stream
//! stays on the shard that holds its connection.

use std::io::{self, Read as _, Write as _};
use std::sync::Arc;

pub use rustls;

use rustls::pki_types::ServerName;

use crate::io::{Read, Write};

/// How much room the buffer for incoming TLS data starts with: one full
/// TLS record and its framing, so a record usually arrives in one read.
const INCOMING: usize = 16 * 1024 + 512;

/// Makes TLS connections out of accepted ones: the server's side.
///
/// It holds the server's configuration (its certificates and what it
/// requires of clients) and nothing else, so cloning it is cheap and one
/// can be given to every shard and every task.
#[derive(Clone)]
pub struct TlsAcceptor {
    config: Arc<rustls::ServerConfig>,
}

impl TlsAcceptor {
    /// An acceptor that serves with `config`.
    pub fn new(config: Arc<rustls::ServerConfig>) -> TlsAcceptor {
        TlsAcceptor { config }
    }

    /// Performs the server's side of the TLS handshake on `stream` and
    /// returns the encrypted stream once it has succeeded.
    ///
    /// `stream` is anything the buffered reader and writer work on: a
    /// [`TcpStream`](crate::net::TcpStream) or
    /// [`UnixStream`](crate::net::UnixStream), owned, borrowed or in an
    /// `Rc`.
    ///
    /// A client can stall a handshake for as long as it likes. Put the call
    /// under a [`timeout`](crate::timeout); giving it up drops the
    /// connection.
    ///
    /// # Errors
    ///
    /// `InvalidData`, carrying rustls's error, if the client's messages are
    /// not acceptable TLS or no common parameters can be agreed;
    /// `UnexpectedEof` if the client hangs up part-way; and any error of the
    /// stream underneath.
    pub async fn accept<S: Read + Write>(&self, stream: S) -> io::Result<TlsStream<S>> {
        let session = rustls::ServerConnection::new(self.config.clone()).map_err(invalid)?;
        TlsStream::start(stream, session.into()).await
    }
}

/// Makes TLS connections out of dialled ones: the client's side.
///
/// It holds the client's configuration (which certificate authorities it
/// trusts, and its own certificate if it has one) and nothing else, so
/// cloning it is cheap and one can be given to every shard and every task.
#[derive(Clone)]
pub struct TlsConnector {
    config: Arc<rustls::ClientConfig>,
}

impl TlsConnector {
    /// A connector that connects with `config`.
    pub fn new(config: Arc<rustls::ClientConfig>) -> TlsConnector {
        TlsConnector { config }
    }

    /// Performs the client's side of the TLS handshake on `stream`, which
    /// is already connected to the server, and returns the encrypted stream
    /// once it has succeeded.
    ///
    /// `server_name` is the name the server's certificate must be valid
    /// for: a host name such as `"example.org"`, or an IP address in text
    /// form. It is also sent to the server, which may serve several names
    /// on one address, unless it is an IP address.
    ///
    /// # Errors
    ///
    /// `InvalidInput` if `server_name` is neither a host name nor an IP
    /// address; `InvalidData`, carrying rustls's error, if the server's
    /// certificate is not trusted or not valid for the name, or the
    /// handshake fails otherwise; `UnexpectedEof` if the server hangs up
    /// part-way; and any error of the stream underneath.
    pub async fn connect<S: Read + Write>(&self, server_name: &str, stream: S) -> io::Result<TlsStream<S>> {
        let name = ServerName::try_from(server_name.to_owned())
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
        let session = rustls::ClientConnection::new(self.config.clone(), name).map_err(invalid)?;
        TlsStream::start(stream, session.into()).await
    }
}

/// An `InvalidData` error carrying one of rustls's.
fn invalid(err: rustls::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err)
}

/// The error every call on a stream returns once one of its operations
/// has been dropped part-way or the session has failed: encrypted data may
/// have been read or written in part, and the two ends no longer agree on
/// where they are.
fn broken() -> io::Error {
    io::Error::other("the TLS session is unusable: an earlier operation failed or was dropped part-way")
}

/// A TLS connection: plain bytes in and out, encrypted on the stream
/// underneath.
///
/// Made by [`TlsAcceptor::accept`] or [`TlsConnector::connect`], with the
/// handshake already done. [`read`](Self::read) and
/// [`write_all`](Self::write_all) are the whole of using it; it also
/// implements the crate's [`Read`] and [`Write`], so a
/// [`BufReader`](crate::io::BufReader) or
/// [`BufWriter`](crate::io::BufWriter) can be put on top for lines and
/// small writes.
///
/// End a conversation with [`shutdown`](Self::shutdown), which tells the
/// peer that nothing was cut off. Dropping the stream closes the connection
/// without saying so.
///
/// # What to watch for
///
/// - Reading and writing both take `&mut self`: one task uses the stream,
///   in turns. There is no way to read in one task while another writes.
/// - An operation that is dropped before it finishes, by a
///   [`timeout`](crate::timeout) or in a [`select`](crate::select()),
///   leaves the session unusable, and every later call fails. Encrypted
///   data cannot be half read. Put time limits around whole exchanges and
///   drop the connection when one expires.
/// - Each `write_all` is sent as it is, in records of its own. Many small
///   writes are many small records; collect them first, by hand or with a
///   `BufWriter`.
pub struct TlsStream<S> {
    stream: S,
    session: rustls::Connection,
    /// Encrypted data read from the stream. The session has taken the part
    /// before `taken`.
    incoming: Vec<u8>,
    taken: usize,
    /// Reused for encrypted data on its way out.
    outgoing: Vec<u8>,
    /// The stream underneath has ended.
    ended: bool,
    /// The session has been told that it ended.
    end_reported: bool,
    /// An operation on the stream underneath is in progress, or the
    /// session has failed. If it is still set when the next call begins,
    /// that operation never finished.
    broken: bool,
}

impl<S: Read + Write> TlsStream<S> {
    /// Wraps `stream` and performs the handshake.
    async fn start(stream: S, session: rustls::Connection) -> io::Result<TlsStream<S>> {
        let mut stream = TlsStream {
            stream,
            session,
            incoming: Vec::with_capacity(INCOMING),
            taken: 0,
            outgoing: Vec::new(),
            ended: false,
            end_reported: false,
            broken: false,
        };
        while stream.session.is_handshaking() {
            stream.send_pending().await?;
            if stream.session.is_handshaking() && stream.session.wants_read() {
                stream.receive().await?;
            }
        }
        stream.send_pending().await?;
        Ok(stream)
    }

    /// Fails if an earlier operation left the session unusable.
    fn check(&self) -> io::Result<()> {
        if self.broken {
            Err(broken())
        } else {
            Ok(())
        }
    }

    /// Sends everything the session has waiting to go out: handshake
    /// messages, application data that was encrypted, alerts.
    async fn send_pending(&mut self) -> io::Result<()> {
        while self.session.wants_write() {
            let mut out = std::mem::take(&mut self.outgoing);
            out.clear();
            while self.session.wants_write() {
                self.session.write_tls(&mut out)?;
            }
            self.broken = true;
            let (result, out) = self.stream.write_all_owned(out).await;
            self.outgoing = out;
            result?;
            self.broken = false;
        }
        Ok(())
    }

    /// Gives the session more of what the peer sent, reading from the
    /// stream underneath if nothing read earlier is left, and sends
    /// whatever the session has to say in return.
    async fn receive(&mut self) -> io::Result<()> {
        if self.taken >= self.incoming.len() {
            if self.ended {
                if self.end_reported {
                    return Err(io::ErrorKind::UnexpectedEof.into());
                }
                // Reading nothing is how the session learns of the end.
                self.session.read_tls(&mut io::empty())?;
                self.end_reported = true;
                return Ok(());
            }
            let mut buf = std::mem::take(&mut self.incoming);
            buf.clear();
            if buf.capacity() < INCOMING {
                buf.reserve(INCOMING);
            }
            self.taken = 0;
            self.broken = true;
            let (result, buf) = self.stream.read_owned(buf).await;
            self.incoming = buf;
            let read = result?;
            self.broken = false;
            if read == 0 {
                self.ended = true;
                self.incoming.clear();
                return Ok(());
            }
        }
        let mut pending = self.incoming.get(self.taken..).unwrap_or_default();
        let took = self.session.read_tls(&mut pending)?;
        self.taken = self.taken.saturating_add(took);
        if let Err(err) = self.session.process_new_packets() {
            // The session has an alert for the peer saying why; send it if
            // the stream still allows, then give up on the session.
            let _ = self.send_pending().await;
            self.broken = true;
            return Err(invalid(err));
        }
        if took == 0 {
            // The session would not take data it had asked for.
            self.broken = true;
            return Err(io::Error::other("the TLS session stopped accepting data"));
        }
        self.send_pending().await
    }

    /// Reads decrypted bytes into `out` and returns how many: at least one
    /// unless `out` is empty, waiting for data if none has arrived.
    ///
    /// `Ok(0)` means the peer has ended the conversation properly, with a
    /// TLS close; nothing more will come.
    ///
    /// # Errors
    ///
    /// `UnexpectedEof` if the connection was closed without a TLS close,
    /// which may mean it was cut off by someone in between: treat what was
    /// read so far as possibly incomplete. `InvalidData` if what arrived
    /// is not valid TLS, for instance because it was tampered with. Any
    /// error of the stream underneath. After an error other than
    /// `UnexpectedEof` the stream cannot be used further.
    pub async fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        self.check()?;
        if out.is_empty() {
            return Ok(0);
        }
        loop {
            match self.session.reader().read(out) {
                Ok(read) => return Ok(read),
                // Nothing decrypted is waiting: get more from the peer.
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
                Err(err) => return Err(err),
            }
            self.receive().await?;
        }
    }

    /// Fills `out` completely, waiting for as much data as that takes.
    ///
    /// # Errors
    ///
    /// `UnexpectedEof` if the conversation ends first, properly or not;
    /// otherwise as [`read`](Self::read).
    pub async fn read_exact(&mut self, out: &mut [u8]) -> io::Result<()> {
        let mut filled = 0;
        while let Some(rest) = out.get_mut(filled..).filter(|rest| !rest.is_empty()) {
            match self.read(rest).await? {
                0 => return Err(io::ErrorKind::UnexpectedEof.into()),
                read => filled = filled.saturating_add(read),
            }
        }
        Ok(())
    }

    /// Encrypts and sends all of `data`. When it returns, the data has
    /// been handed to the stream underneath.
    ///
    /// # Errors
    ///
    /// Any error of the stream underneath, after which the stream cannot
    /// be used further.
    pub async fn write_all(&mut self, data: &[u8]) -> io::Result<()> {
        self.check()?;
        let mut rest = data;
        while !rest.is_empty() {
            // The session takes as much as its buffer has room for.
            let taken = self.session.writer().write(rest)?;
            rest = rest.get(taken..).unwrap_or_default();
            self.send_pending().await?;
        }
        Ok(())
    }

    /// Ends the conversation: tells the peer, with a TLS close, that
    /// nothing more will be sent and that nothing was cut off. The peer's
    /// `read` then returns `Ok(0)`.
    ///
    /// Reading is still possible afterwards, until the peer does the same.
    /// The connection itself is closed when the stream is dropped.
    pub async fn shutdown(&mut self) -> io::Result<()> {
        self.check()?;
        self.session.send_close_notify();
        self.send_pending().await
    }

    /// The stream underneath. Reading from or writing to it directly would
    /// corrupt the session; this is for asking it things, such as a TCP
    /// stream's peer address.
    pub fn get_ref(&self) -> &S {
        &self.stream
    }

    /// The rustls session, for asking what was agreed: the protocol
    /// version and cipher suite, the certificates the peer presented, and
    /// so on.
    pub fn session(&self) -> &rustls::Connection {
        &self.session
    }

    /// The application protocol agreed during the handshake (ALPN), such
    /// as `b"h2"`, or `None` if none was: either side offered none, or
    /// they had none in common.
    pub fn alpn_protocol(&self) -> Option<&[u8]> {
        self.session.alpn_protocol()
    }
}

impl<S: Read + Write> Read for TlsStream<S> {
    /// Reads decrypted bytes into `buf`, up to its capacity.
    async fn read_owned(&mut self, mut buf: Vec<u8>) -> (io::Result<usize>, Vec<u8>) {
        buf.clear();
        buf.resize(buf.capacity(), 0);
        match self.read(&mut buf).await {
            Ok(read) => {
                buf.truncate(read);
                (Ok(read), buf)
            }
            Err(err) => {
                buf.clear();
                (Err(err), buf)
            }
        }
    }
}

impl<S: Read + Write> Write for TlsStream<S> {
    /// Encrypts and sends all of `buf`.
    async fn write_all_owned(&mut self, buf: Vec<u8>) -> (io::Result<()>, Vec<u8>) {
        let result = self.write_all(&buf).await;
        (result, buf)
    }
}

debug_impl!(TlsAcceptor);
debug_impl!(TlsConnector);
debug_impl!(<S> TlsStream<S>, "handshaking" => |stream| stream.session.is_handshaking());
