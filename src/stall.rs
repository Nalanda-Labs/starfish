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

/// Deepest stack recorded. Frames beyond this are left out; the innermost
/// ones, which say where the shard was stuck, come first and are kept.
const MAX_FRAMES: usize = 48;

thread_local! {
    /// Which time slice the shard is in. Odd while the shard is running tasks,
    /// even between slices, and different for every slice, so the timer can tell
    /// "still in the same slice as last time" from "in a new one".
    static SLICE: Cell<u64> = const { Cell::new(0) };
    /// The value of `SLICE` when the timer last fired. If it is unchanged at
    /// the next tick, the same slice has been running for a whole period.
    static SEEN: Cell<u64> = const { Cell::new(0) };
    /// The stack recorded for the current slice. Written by the signal handler
    /// and read by the shard afterwards, both on the same thread.
    static FRAMES: Cell<[*mut c_void; MAX_FRAMES]> =
        const { Cell::new([std::ptr::null_mut(); MAX_FRAMES]) };
    /// How many frames of `FRAMES` are valid. Zero means no stack has been
    /// recorded for the current slice, which is also what stops the handler from
    /// recording a second one.
    static DEPTH: Cell<c_int> = const { Cell::new(0) };
}

/// The signal the stall timers use: a real-time signal, which no ordinary
/// program gives a meaning to. The same one for every shard; each timer aims
/// it at its own thread.
fn signal() -> c_int {
    libc::SIGRTMIN().saturating_add(5)
}

/// Marks the start of a time slice, by making the slice counter odd. Called
/// by the event loop just before it runs a group's tasks.
pub(crate) fn slice_begin() {
    let _ = SLICE.try_with(|slice| slice.set(slice.get().wrapping_add(1) | 1));
}

/// Marks the end of a time slice, by making the slice counter even. A tick
/// that arrives between slices finds an even value and does nothing.
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
/// timer caught it stalling, as one line per frame, innermost first.
///
/// With the `symbols` feature each line names the function and, where the
/// program has debug information, the file and line; a function inlined
/// into another gets a line of its own. Without it, or for an address
/// nothing is known about, the line is what the C library makes of the
/// address: the module, an offset and the address.
///
/// The frames of the signal handler itself, which are on top of every
/// recorded stack, are left out.
pub(crate) fn take_backtrace() -> Vec<String> {
    let depth = DEPTH.try_with(|depth| depth.replace(0)).unwrap_or(0);
    if depth <= 0 {
        return Vec::new();
    }
    let frames = FRAMES.try_with(Cell::get).unwrap_or([std::ptr::null_mut(); MAX_FRAMES]);
    let depth = usize::try_from(depth).unwrap_or(0).min(MAX_FRAMES);
    let raw = raw_lines(&frames, depth);
    let mut lines = Vec::with_capacity(depth);
    // Where the interrupted code begins. The handler's frames are on top,
    // and the frame after the last of them is the kernel's trampoline into
    // the handler, which has no name to go by.
    let mut handler_frame = None;
    let mut starts = Vec::with_capacity(depth);
    for (index, (address, raw)) in frames.iter().zip(raw).enumerate() {
        starts.push(lines.len());
        describe(*address, raw, &mut lines);
        let from = starts.last().copied().unwrap_or(0);
        let of_handler = lines.iter().skip(from).any(|line| line.contains("stall::on_tick"));
        if of_handler && index < HANDLER_FRAMES {
            handler_frame = Some(index);
        }
    }
    if let Some(frame) = handler_frame {
        let interrupted = starts.get(frame.saturating_add(2)).copied().unwrap_or(lines.len());
        lines.drain(..interrupted);
    }
    lines
}

/// How far down the stack the signal handler's own frames can reach: the
/// handler and whatever of it was not inlined.
const HANDLER_FRAMES: usize = 8;

/// What the C library says about each of the first `depth` addresses: the
/// module, the nearest exported symbol if there is one, and the address.
fn raw_lines(frames: &[*mut c_void; MAX_FRAMES], depth: usize) -> Vec<String> {
    // SAFETY: `frames` holds at least `depth` addresses. The result is one
    // malloc'd block of `depth` C strings, freed below.
    unsafe {
        let symbols = libc::backtrace_symbols(frames.as_ptr(), depth as c_int);
        if symbols.is_null() {
            return vec![String::new(); depth];
        }
        let lines = (0..depth)
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

/// Appends the description of one frame to `lines`: a line per function at
/// that address, the innermost inlined one first, or `raw` if the address
/// cannot be resolved.
#[cfg(feature = "symbols")]
fn describe(address: *mut c_void, raw: String, lines: &mut Vec<String>) {
    let before = lines.len();
    // A frame's address is where its call returns to, which may already be
    // the next line of the source. The byte before it is inside the call.
    let inside = address.wrapping_byte_sub(1);
    backtrace::resolve(inside, |symbol| {
        let Some(name) = symbol.name() else { return };
        // The alternate form leaves the hash off the end of a Rust name.
        let mut line = format!("{name:#}");
        if let (Some(file), Some(number)) = (symbol.filename(), symbol.lineno()) {
            line.push_str(&format!(" at {}:{number}", file.display()));
        }
        lines.push(line);
    });
    if lines.len() == before {
        lines.push(raw);
    }
}

/// Appends the description of one frame to `lines`: what the C library
/// made of it.
#[cfg(not(feature = "symbols"))]
fn describe(_address: *mut c_void, raw: String, lines: &mut Vec<String>) {
    lines.push(raw);
}

/// Loads what resolving addresses needs (the program's symbol tables and
/// debug information), so that the first stall report does not pay for it
/// in the middle of serving requests.
fn warm_symbols() {
    #[cfg(feature = "symbols")]
    backtrace::resolve(on_tick as *mut c_void, |_| {});
}

/// One shard's periodic timer, which interrupts the shard's thread so that
/// a stall can be caught while it is happening. Stopped on drop.
pub(crate) struct StallTimer {
    timer: libc::timer_t,
}

impl StallTimer {
    /// Starts interrupting the current thread every `period`.
    ///
    /// The first call in the process installs the signal handler and, before
    /// that, makes one throwaway call to `backtrace`, which loads the unwinder
    /// library. Loading a library takes locks and allocates, neither of which is
    /// allowed inside a signal handler, so it has to have happened already by
    /// the time the handler first runs.
    ///
    /// # Errors
    ///
    /// The kernel's error if the timer cannot be created or armed.
    pub(crate) fn start(period: Duration) -> io::Result<StallTimer> {
        /// Guards the one-time installation of the signal handler.
        static INSTALL: Once = Once::new();
        INSTALL.call_once(|| {
            // SAFETY: installs a handler that keeps to async-signal-safe
            // operations. The first `backtrace` call loads the unwinder,
            // which is not safe to do inside the handler, so do it here.
            unsafe {
                let mut warm = [std::ptr::null_mut(); 1];
                libc::backtrace(warm.as_mut_ptr(), 1);
                warm_symbols();
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
    /// Deletes the timer, so the thread is no longer interrupted.
    fn drop(&mut self) {
        // SAFETY: the timer was created in `start` and is deleted once.
        unsafe {
            libc::timer_delete(self.timer);
        }
    }
}
