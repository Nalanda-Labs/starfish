//! Finding out what a shard was doing when it failed to yield.
//!
//! A task that runs for a long time without awaiting stalls every other
//! task on its shard. The event loop notices this after the fact, from how
//! long the slice took, but by then the stack that caused it is gone. With
//! backtraces enabled, each shard thread gets a periodic timer that
//! interrupts it; if the interrupt finds the shard still inside the same
//! slice as on the previous tick, it records the call stack on the spot.

use std::cell::Cell;
use std::ffi::{c_int, c_void, CStr};
use std::io;
use std::sync::Once;
use std::time::Duration;

/// Deepest stack recorded.
const MAX_FRAMES: usize = 48;

thread_local! {
    /// Odd while the shard is running tasks; changes with every slice.
    static SLICE: Cell<u64> = const { Cell::new(0) };
    /// The value of `SLICE` at the previous tick.
    static SEEN: Cell<u64> = const { Cell::new(0) };
    /// The stack recorded for the current slice, and how deep it is.
    static FRAMES: Cell<[*mut c_void; MAX_FRAMES]> =
        const { Cell::new([std::ptr::null_mut(); MAX_FRAMES]) };
    static DEPTH: Cell<c_int> = const { Cell::new(0) };
}

fn signal() -> c_int {
    libc::SIGRTMIN().saturating_add(5)
}

/// Marks the start of a slice.
pub(crate) fn slice_begin() {
    let _ = SLICE.try_with(|slice| slice.set(slice.get().wrapping_add(1) | 1));
}

/// Marks the end of a slice.
pub(crate) fn slice_end() {
    let _ = SLICE.try_with(|slice| slice.set(slice.get().wrapping_add(1) & !1));
}

/// Runs on the shard's own thread, interrupting whatever it was doing.
///
/// It may only do what is safe in a signal handler: it reads and writes
/// plain thread-local cells (which have no lazy initialisation and no
/// destructor) and walks the stack into a local array.
extern "C" fn on_tick(_signal: c_int) {
    // SAFETY: `__errno_location` returns this thread's errno slot.
    let errno = unsafe { *libc::__errno_location() };
    let _ = SLICE.try_with(|slice| {
        let slice = slice.get();
        if slice & 1 == 0 {
            return; // not running tasks
        }
        let unchanged = SEEN.try_with(|seen| seen.replace(slice) == slice).unwrap_or(false);
        let recorded = DEPTH.try_with(Cell::get).unwrap_or(1) != 0;
        if unchanged && !recorded {
            let mut frames = [std::ptr::null_mut(); MAX_FRAMES];
            // SAFETY: the array has room for `MAX_FRAMES` entries. The
            // unwinder was loaded in `StallTimer::start`, so this does not
            // allocate.
            let depth = unsafe { libc::backtrace(frames.as_mut_ptr(), MAX_FRAMES as c_int) };
            let _ = FRAMES.try_with(|slot| slot.set(frames));
            let _ = DEPTH.try_with(|slot| slot.set(depth));
        }
    });
    // SAFETY: as above.
    unsafe { *libc::__errno_location() = errno };
}

/// Returns the stack recorded during the slice that just ended, if the
/// timer caught it stalling, as one line per frame.
pub(crate) fn take_backtrace() -> Vec<String> {
    let depth = DEPTH.try_with(|depth| depth.replace(0)).unwrap_or(0);
    if depth <= 0 {
        return Vec::new();
    }
    let frames = FRAMES.try_with(Cell::get).unwrap_or([std::ptr::null_mut(); MAX_FRAMES]);
    // SAFETY: `frames` holds `depth` addresses written by `backtrace`. The
    // result is one malloc'd block of `depth` C strings, freed below.
    unsafe {
        let symbols = libc::backtrace_symbols(frames.as_ptr(), depth);
        if symbols.is_null() {
            return Vec::new();
        }
        let lines = (0..usize::try_from(depth).unwrap_or(0))
            .map(|index| {
                let line = *symbols.add(index);
                if line.is_null() {
                    String::new()
                } else {
                    CStr::from_ptr(line).to_string_lossy().into_owned()
                }
            })
            .collect();
        libc::free(symbols.cast());
        lines
    }
}

/// The per-thread timer; stopped on drop.
pub(crate) struct StallTimer {
    timer: libc::timer_t,
}

impl StallTimer {
    /// Starts interrupting the current thread every `period`.
    pub(crate) fn start(period: Duration) -> io::Result<StallTimer> {
        static INSTALL: Once = Once::new();
        INSTALL.call_once(|| {
            // SAFETY: installs a handler that keeps to async-signal-safe
            // operations. The first `backtrace` call loads the unwinder,
            // which is not safe to do inside the handler, so do it here.
            unsafe {
                let mut warm = [std::ptr::null_mut(); 1];
                libc::backtrace(warm.as_mut_ptr(), 1);
                let mut action: libc::sigaction = std::mem::zeroed();
                action.sa_sigaction = on_tick as extern "C" fn(c_int) as usize;
                action.sa_flags = libc::SA_RESTART;
                libc::sigemptyset(&mut action.sa_mask);
                libc::sigaction(signal(), &action, std::ptr::null_mut());
            }
        });
        // SAFETY: plain syscalls on zero-initialised structures; the timer
        // id is written by `timer_create` before it is used.
        unsafe {
            let mut event: libc::sigevent = std::mem::zeroed();
            event.sigev_notify = libc::SIGEV_THREAD_ID;
            event.sigev_signo = signal();
            event.sigev_notify_thread_id = libc::gettid();
            let mut timer: libc::timer_t = std::mem::zeroed();
            if libc::timer_create(libc::CLOCK_MONOTONIC, &mut event, &mut timer) != 0 {
                return Err(io::Error::last_os_error());
            }
            let period = libc::timespec {
                tv_sec: libc::time_t::try_from(period.as_secs()).unwrap_or(libc::time_t::MAX),
                tv_nsec: libc::c_long::from(period.subsec_nanos().max(1)),
            };
            let schedule = libc::itimerspec {
                it_interval: period,
                it_value: period,
            };
            if libc::timer_settime(timer, 0, &schedule, std::ptr::null_mut()) != 0 {
                let err = io::Error::last_os_error();
                libc::timer_delete(timer);
                return Err(err);
            }
            Ok(StallTimer { timer })
        }
    }
}

impl Drop for StallTimer {
    fn drop(&mut self) {
        // SAFETY: the timer was created in `start` and is deleted once.
        unsafe {
            libc::timer_delete(self.timer);
        }
    }
}
