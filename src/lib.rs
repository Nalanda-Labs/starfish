//! A shared-nothing, thread-per-core async runtime on io_uring, modelled on
//! Seastar.
//!
//! The runtime starts one *shard* per core. Each shard is a pinned thread with
//! its own task queue, io_uring instance and timers. Tasks never migrate, so
//! they do not need to be `Send`, and state owned by a shard needs no locks.
//! Shards talk to each other only by message passing: [`submit_to`] runs a
//! closure on another shard and returns its result, and [`Sharded`] keeps one
//! instance of a service on every shard.
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
//! A task that is no longer wanted is cancelled with [`JoinHandle::abort`],
//! and [`signal::termination`] tells the program when it is asked to stop.
//!
//! I/O uses owned buffers: a call takes the buffer by value and hands it back
//! with the result, because the kernel keeps writing to it even if the future
//! is dropped.
//!
//! # Panics
//!
//! The runtime itself does not panic: every failure is reported as an
//! [`Error`]. A panic in user code is caught at the task boundary. The task
//! is dropped, whoever awaits it gets [`Error::Panicked`], and every other
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

mod error;
mod ioqueue;
mod reactor;
mod runtime;
mod sched;
mod sharded;
mod smp;
mod spsc;
mod stall;
mod task;
mod timer;

pub mod alloc;
pub mod buf;
pub mod fs;
pub mod iotune;
pub mod net;
pub mod signal;
pub mod stats;
pub mod sync;

pub use error::{Error, Result};
pub use ioqueue::IoProperties;
pub use runtime::{shard_count, shard_id, Runtime};
pub use sched::{maybe_yield, need_preempt, SchedulingGroup};
pub use sharded::Sharded;
pub use smp::{submit_to, submit_to_all};
pub use task::{spawn, yield_now, JoinHandle};
pub use timer::{sleep, timeout, Elapsed, Sleep};
