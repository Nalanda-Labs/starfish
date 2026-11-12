//! A shared-nothing, thread-per-core async runtime on io_uring, modelled on
//! Seastar.
//!
//! The runtime starts one *shard* per core. Each shard is a pinned thread with
//! its own task queue, io_uring instance and timers. Tasks never migrate, so
//! they do not need to be `Send`, and state owned by a shard needs no locks.
//! Shards talk to each other only by message passing: [`submit_to`] runs a
//! closure on another shard and returns its result, [`Sharded`] keeps one
//! instance of a service on every shard, and a
//! [`shared_spsc`](sync::shared_spsc) or
//! [`shared_mpsc`](sync::shared_mpsc) carries a stream of values from one
//! shard, or from several, to another. All of them slow the sender down
//! when the receiving shard falls behind.
//!
//! ```no_run
//! use starfish::{Runtime, Sharded};
//! use std::cell::Cell;
//!
//! fn main() -> starfish::Result<()> {
//!     let total = Runtime::new().run(|| async {
//!         let counter = Sharded::start(|_shard| Cell::new(0u64)).await?;
//!         counter
//!             .invoke_on_all(|c| async move { c.set(c.get() + 1) })
//!             .await?;
//!         counter
//!             .map_reduce(|c| async move { c.get() }, 0, |a, b| a + b)
//!             .await
//!     })??;
//!     println!("{total} shards counted");
//!     Ok(())
//! }
//! ```
//!
//! Work is divided into [scheduling groups](SchedulingGroup). On each
//! shard, groups share the CPU in proportion to their CPU shares and each
//! storage device in proportion to their I/O shares, so that, say, a
//! compaction job cannot starve request handling. Tell the runtime what a
//! device can sustain ([`iotune::measure`], [`Runtime::io_properties`]) and
//! it keeps the device's own queue short, so that those priorities take
//! effect quickly.
//!
//! Tasks on one shard coordinate through [`sync`]: a
//! [`Semaphore`](sync::Semaphore) to bound what runs at once, a
//! [`channel`](sync::channel) to pass values with backpressure, and a
//! [`Gate`](sync::Gate) to wait for work in progress before shutting down.
//! Code that blocks goes to [`run_blocking`], off the shard.
//! [`select()`] waits for whichever of two things happens first.
//! A task that is no longer wanted is cancelled with [`JoinHandle::abort`],
//! and [`signal::termination`] tells the program when it is asked to stop.
//!
//! Sockets are in [`net`] (TCP, UDP and Unix-domain) and files in [`fs`]
//! (through the page cache or with direct I/O, plus directories). [`io`]
//! has a buffered reader and writer for both.
//!
//! I/O uses owned buffers: a call takes the buffer by value and hands it back
//! with the result, because the kernel keeps writing to it even if the future
//! is dropped.
//!
//! The [guide] walks through all of this with examples.
//!
//! # Panics
//!
//! The runtime itself does not panic: every failure is reported as an
//! [`Error`]. A panic in user code is caught at the task boundary. The task
//! is dropped, whoever awaits it gets [`Error::Panicked`] with the panic's
//! message, and every other
//! task and shard keeps running. (This relies on unwinding, so it does not
//! apply to programs built with `panic = "abort"`.)

// No code path in this crate may panic. These lints keep the usual sources
// out; `RefCell` is only reached through `runtime::Local`, which cannot
// panic either.
#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable,
    clippy::todo,
    clippy::unimplemented,
    clippy::indexing_slicing,
    clippy::string_slice,
    clippy::arithmetic_side_effects,
    clippy::unwrap_in_result,
    clippy::panic_in_result_fn
)]
// Everything public is documented, and can be printed for debugging.
#![warn(missing_docs, missing_debug_implementations)]

/// Implements `Debug` for types whose insides would mean nothing to a
/// reader: prints the type's name and, where given, a few telling values.
///
/// Most of the library's types hold raw pointers, ring state or closures,
/// which either cannot be printed or would bury the useful part. They
/// still implement `Debug`, so that a type holding one can derive it; the
/// `missing_debug_implementations` lint above keeps that true.
///
/// The forms it takes:
///
/// ```ignore
/// debug_impl!(Timer);                              // Timer { .. }
/// debug_impl!(<T> Sender<T>);                      // generic types
/// debug_impl!(File, "fd" => |file| file.fd);       // File { fd: 7, .. }
/// ```
///
/// Each `"name" => |this| value` pair adds a field; `this` is bound to
/// `&self` and the value must itself be `Debug`. The output always ends in
/// `..`, since there is always more inside than is shown. Generic
/// parameters get no `Debug` bound: nothing of theirs is printed unless a
/// field expression does it.
macro_rules! debug_impl {
    ($(<$($generic:ident),+>)? $name:ident $(<$($arg:ident),+>)? $(, $field:literal => |$this:ident| $value:expr)*) => {
        impl$(<$($generic),+>)? std::fmt::Debug for $name$(<$($arg),+>)? {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let mut out = f.debug_struct(stringify!($name));
                $(
                    let $this = self;
                    out.field($field, &$value);
                )*
                out.finish_non_exhaustive()
            }
        }
    };
}

#[cfg(doc)]
#[doc = include_str!("../docs/guide.md")]
pub mod guide {}

mod blocking;
mod error;
mod ioqueue;
mod reactor;
mod runtime;
mod sched;
mod select;
mod shared_channel;
mod sharded;
mod smp;
mod spsc;
mod stall;
mod task;
mod timer;

pub mod alloc;
pub mod buf;
pub mod fs;
pub mod io;
pub mod iotune;
pub mod net;
pub mod signal;
pub mod stats;
pub mod sync;

pub use blocking::run_blocking;
pub use error::{Error, Result};
pub use ioqueue::IoProperties;
pub use runtime::{shard_count, shard_id, Runtime};
pub use sched::{maybe_yield, should_yield, SchedulingGroup};
pub use select::{interval, select, select3, select_all, Either, Either3, Interval};
pub use sharded::Sharded;
pub use smp::{submit_to, submit_to_all, submit_to_each};
pub use task::{spawn, yield_now, JoinHandle};
pub use timer::{sleep, timeout, Elapsed, Sleep};
