//! The error type shared by the runtime's APIs.

use std::fmt;
use std::io;

/// Why a runtime call could not produce a value.
///
/// Every fallible function of the runtime itself returns this type: starting a
/// runtime, spawning and joining tasks, calls to other shards, the
/// coordination primitives. Functions that are plain I/O (sockets, files)
/// return `std::io::Error` instead, and the two convert into each other with
/// `?` in either direction: an `io::Error` becomes [`Error::Io`], and any
/// other variant becomes an `io::Error` of kind `Other` that keeps the
/// original as its source.
///
/// The type is `#[non_exhaustive]`: later versions may add variants, so a
/// `match` on it needs a wildcard arm.
///
/// ```
/// use starfish::Error;
///
/// fn describe(error: &Error) -> &'static str {
///     match error {
///         Error::Io(_) => "the kernel refused",
///         Error::Panicked { .. } => "the task panicked",
///         Error::Canceled => "the task was dropped",
///         _ => "something else",
///     }
/// }
/// assert_eq!(describe(&Error::Canceled), "the task was dropped");
/// ```
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The kernel or the io_uring instance reported an error.
    Io(io::Error),
    /// The call was made on a thread that is not running a shard.
    NoRuntime,
    /// A runtime is already running on this thread.
    AlreadyRunning,
    /// The task (or the closure run elsewhere) panicked. The panic was
    /// caught and the rest of the runtime keeps running.
    Panicked {
        /// What the panic said, if it carried text, as `panic!("...")`
        /// does. `None` for a panic with another kind of payload, or
        /// where the text could not be recovered.
        message: Option<String>,
    },
    /// The task was dropped before it finished, usually because the runtime
    /// or the target shard is shutting down.
    Canceled,
    /// The shard index does not exist.
    InvalidShard {
        /// The index that was asked for.
        shard: usize,
        /// How many shards the runtime has; valid indexes are below this.
        count: usize,
    },
    /// The sharded service has been stopped, or was never started here.
    ServiceStopped,
    /// The scheduling group does not belong to the running runtime.
    InvalidGroup,
    /// The runtime was started without a pool of registered buffers.
    NoFixedBuffers,
    /// The semaphore or gate has been closed.
    Closed,
}

/// `Result` with the runtime's [`Error`] as its error type.
///
/// This is what the runtime's own functions return. It is a plain alias, so
/// `starfish::Result<T>` and `Result<T, starfish::Error>` are the same type
/// and `?` works between them and `std::io::Result` (see [`Error`]).
pub type Result<T> = std::result::Result<T, Error>;

impl fmt::Display for Error {
    /// Writes a one-line, lower-case description of the error, without a
    /// trailing full stop, as is usual for error messages that get embedded in
    /// longer ones. For [`Error::Io`] it includes the kernel's own message, and
    /// for [`Error::Panicked`] the panic's message when there is one.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(err) => write!(f, "I/O error: {err}"),
            Error::NoRuntime => f.write_str("not running inside a starfish runtime"),
            Error::AlreadyRunning => f.write_str("a starfish runtime is already running on this thread"),
            Error::Panicked { message: Some(message) } => write!(f, "task panicked: {message}"),
            Error::Panicked { message: None } => f.write_str("task panicked"),
            Error::Canceled => f.write_str("task was canceled"),
            Error::InvalidShard { shard, count } => {
                write!(f, "shard {shard} does not exist (runtime has {count})")
            }
            Error::ServiceStopped => f.write_str("sharded service is not running on this shard"),
            Error::InvalidGroup => f.write_str("scheduling group does not belong to this runtime"),
            Error::NoFixedBuffers => f.write_str("the runtime has no pool of registered buffers"),
            Error::Closed => f.write_str("closed"),
        }
    }
}

impl std::error::Error for Error {
    /// The underlying `io::Error` for [`Error::Io`], and `None` for every other
    /// variant, which have no lower-level cause.
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    /// Wraps an I/O error as [`Error::Io`], which is what lets `?` turn a failed
    /// socket or file call into a `starfish::Result` error.
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}

impl From<Error> for io::Error {
    /// Turns a runtime error into an I/O error, which is what lets `?` be used
    /// on runtime calls inside functions that return `std::io::Result`.
    ///
    /// [`Error::Io`] gives back the I/O error it held, unchanged. Any other
    /// variant becomes an error of kind `Other` whose source is the original
    /// `Error`, so nothing is lost: `err.get_ref()` and downcasting recover it.
    fn from(err: Error) -> io::Error {
        match err {
            Error::Io(err) => err,
            other => io::Error::other(other),
        }
    }
}
