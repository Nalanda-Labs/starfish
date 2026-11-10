//! The error type shared by the runtime's APIs.

use std::fmt;
use std::io;

/// Why a runtime call could not produce a value.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The kernel or the io_uring instance reported an error.
    Io(io::Error),
    /// The call was made on a thread that is not running a shard.
    NoRuntime,
    /// A runtime is already running on this thread.
    AlreadyRunning,
    /// The task (or the remote closure) panicked. The panic was caught and
    /// the rest of the runtime keeps running.
    Panicked,
    /// The task was dropped before it finished, usually because the runtime
    /// or the target shard is shutting down.
    Canceled,
    /// The shard index does not exist.
    InvalidShard { shard: usize, count: usize },
    /// The sharded service has been stopped, or was never started here.
    ServiceStopped,
    /// The scheduling group does not belong to the running runtime.
    InvalidGroup,
    /// The runtime was started without a pool of registered buffers.
    NoFixedBuffers,
}

/// `Result` with the runtime's [`Error`].
pub type Result<T> = std::result::Result<T, Error>;

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(err) => write!(f, "I/O error: {err}"),
            Error::NoRuntime => f.write_str("not running inside a starfish runtime"),
            Error::AlreadyRunning => f.write_str("a starfish runtime is already running on this thread"),
            Error::Panicked => f.write_str("task panicked"),
            Error::Canceled => f.write_str("task was canceled"),
            Error::InvalidShard { shard, count } => {
                write!(f, "shard {shard} does not exist (runtime has {count})")
            }
            Error::ServiceStopped => f.write_str("sharded service is not running on this shard"),
            Error::InvalidGroup => f.write_str("scheduling group does not belong to this runtime"),
            Error::NoFixedBuffers => f.write_str("the runtime has no pool of registered buffers"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}

impl From<Error> for io::Error {
    fn from(err: Error) -> io::Error {
        match err {
            Error::Io(err) => err,
            other => io::Error::other(other),
        }
    }
}
