//! Unix signals as events a task can wait for.
//!
//! The usual use is an orderly shutdown: the main task waits for
//! [`termination`], then stops accepting work, waits for what is in
//! progress and returns.
//!
//! ```no_run
//! # async fn example() -> std::io::Result<()> {
//! let signal = starfish::signal::termination().await?;
//! println!("signal {signal}: shutting down");
//! # Ok(())
//! # }
//! ```
//!
//! Listening for a signal replaces what the process would otherwise do on
//! receiving it, for as long as anything listens: while a [`Signals`] for
//! `SIGINT` exists, Ctrl-C does not end the program by itself. When the
//! last listener for a signal is dropped, the signal goes back to what it
//! did before.
//!
//! Signals are process-wide, so a listener works on any shard, and every
//! listener for a signal sees it. Deliveries that arrive faster than they
//! are collected are merged into one, as the kernel itself does.

use std::collections::BTreeMap;
use std::ffi::c_int;
use std::future::poll_fn;
use std::io;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::task::{Poll, Waker};

use crate::runtime::wake_quietly;

/// One more than the highest signal number, so that a signal's number can
/// index an array directly. Linux numbers signals from 1 to 64.
const SLOTS: usize = 65;

/// How often each signal has arrived since the process started, indexed by
/// signal number.
///
/// The signal handler adds to these, which is all a handler may safely do
/// with shared data. Everything else compares a count with the value it last
/// saw: a listener has a signal to report when the count has moved on.
static COUNTS: [AtomicU64; SLOTS] = [const { AtomicU64::new(0) }; SLOTS];
/// The eventfd the handler writes to, to wake the relay thread, or -1
/// before there is one. It is created once and never closed, so the handler
/// can use it without any coordination.
static NOTIFY: AtomicI32 = AtomicI32::new(-1);

/// What the listeners and the relay thread share, behind a mutex.
///
/// The signal handler never touches this: taking a lock in a signal handler
/// could deadlock against the very code it interrupted.
struct Registry {
    /// The relay thread and its eventfd exist.
    started: bool,
    /// For each signal, who is listening and what was there before.
    slots: [Slot; SLOTS],
    /// The tasks waiting in [`Signals::next`].
    listeners: BTreeMap<u64, Waker>,
    next_listener: u64,
}

/// One signal's state: how many listeners it has, and what the process did
/// with it before the first of them arrived.
#[derive(Clone, Copy)]
struct Slot {
    /// How many [`Signals`] listen for it.
    listeners: u32,
    /// What the process did with the signal before our handler went in,
    /// to be put back when the last listener goes.
    previous: Option<libc::sigaction>,
}

/// The one registry of the process. Signals are a process-wide resource,
/// so this is too: it is shared by every shard and every runtime.
static REGISTRY: Mutex<Registry> = Mutex::new(Registry {
    started: false,
    slots: [Slot {
        listeners: 0,
        previous: None,
    }; SLOTS],
    listeners: BTreeMap::new(),
    next_listener: 0,
});

/// Locks the registry, recovering it if a thread panicked while holding
/// the lock. Nothing panics while holding it.
fn registry() -> MutexGuard<'static, Registry> {
    // Nothing panics while holding this lock.
    REGISTRY.lock().unwrap_or_else(PoisonError::into_inner)
}

/// How often `signal` has arrived so far. Zero for a number that is not a
/// signal.
fn count(signal: c_int) -> u64 {
    usize::try_from(signal)
        .ok()
        .and_then(|slot| COUNTS.get(slot))
        .map_or(0, |count| count.load(Ordering::SeqCst))
}

/// The signal handler. Runs on whichever thread the kernel picks,
/// interrupting whatever that thread was doing.
///
/// It keeps to what is safe in that position: it adds one to the signal's
/// count and writes to the eventfd, both of which are single atomic or
/// system-call operations, and it saves and restores `errno` so that the code
/// it interrupted does not see the write's result. It takes no lock and
/// allocates nothing. Waking the listeners is left to the relay thread.
extern "C" fn on_signal(signal: c_int) {
    // SAFETY: `__errno_location` returns this thread's errno slot.
    let errno = unsafe { *libc::__errno_location() };
    if let Some(count) = usize::try_from(signal).ok().and_then(|slot| COUNTS.get(slot)) {
        count.fetch_add(1, Ordering::SeqCst);
    }
    let fd = NOTIFY.load(Ordering::SeqCst);
    if fd >= 0 {
        let one: u64 = 1;
        // SAFETY: writes 8 bytes from a live u64 to an eventfd that is
        // never closed.
        unsafe {
            libc::write(fd, (&one as *const u64).cast(), 8);
        }
    }
    // SAFETY: as above.
    unsafe { *libc::__errno_location() = errno };
}

/// The relay thread: sleeps in a read on the eventfd, and each time the
/// handler writes to it, wakes every waiting listener.
///
/// A handler cannot wake tasks itself, since that takes locks and may
/// allocate. This thread is the bridge from the handler's world to the
/// runtime's. Listeners that are woken check the counts for themselves, so
/// waking all of them for any signal is correct, and there are never many.
///
/// It runs for the rest of the process's life. A read that fails for any
/// reason but an interruption ends it.
fn relay(fd: c_int) {
    loop {
        let mut arrivals: u64 = 0;
        // SAFETY: reads 8 bytes into a live u64 from an fd we own.
        let read = unsafe { libc::read(fd, (&mut arrivals as *mut u64).cast(), 8) };
        if read < 0 && io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
            return;
        }
        // A listener checks the counts and registers under this lock, so it
        // either saw the new count or is in the list taken here.
        let listeners = std::mem::take(&mut registry().listeners);
        for (_, waker) in listeners {
            wake_quietly(waker);
        }
    }
}

/// Creates the eventfd and the relay thread, the first time anything
/// listens. Later calls do nothing.
///
/// The eventfd's number is published for the handler only after the thread
/// exists, so the handler never writes to something nobody reads.
///
/// # Errors
///
/// The kernel's error if the eventfd or the thread cannot be created.
fn start(registry: &mut Registry) -> io::Result<()> {
    if registry.started {
        return Ok(());
    }
    // SAFETY: plain syscall; the returned fd is owned by nobody else.
    let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let spawned = std::thread::Builder::new()
        .name(String::from("starfish-signals"))
        .spawn(move || relay(fd));
    if let Err(err) = spawned {
        // SAFETY: nothing else has seen the fd.
        unsafe {
            libc::close(fd);
        }
        return Err(err);
    }
    NOTIFY.store(fd, Ordering::SeqCst);
    registry.started = true;
    Ok(())
}

/// An `InvalidInput` error with the given message.
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

/// Adds a listener for `signal`, installing the handler if it is the first.
///
/// When the handler is installed, the action the process had for the signal
/// until then is saved, to be restored by [`uninstall`] when the last
/// listener goes.
///
/// # Errors
///
/// `InvalidInput` for a number that is not a signal, or for one of the
/// signals that report a fault in the program itself. Otherwise the kernel's
/// error, which is what `SIGKILL` and `SIGSTOP` get, since they cannot be
/// caught.
fn install(registry: &mut Registry, signal: c_int) -> io::Result<()> {
    // The kernel raises these for a fault in the program itself and runs
    // the faulting instruction again when the handler returns.
    /// The signals the kernel raises for a fault in the program itself. When a
    /// handler for one of these returns, the faulting instruction runs again and
    /// faults again, forever: they cannot be turned into events.
    const FAULTS: [c_int; 4] = [libc::SIGSEGV, libc::SIGBUS, libc::SIGFPE, libc::SIGILL];
    if FAULTS.contains(&signal) {
        return Err(invalid("cannot listen for a signal that reports a program fault"));
    }
    let slot = usize::try_from(signal)
        .ok()
        .filter(|slot| *slot >= 1)
        .and_then(|slot| registry.slots.get_mut(slot))
        .ok_or_else(|| invalid("not a signal number"))?;
    if slot.listeners == 0 {
        // SAFETY: installs a handler that keeps to async-signal-safe
        // operations, on a zero-initialised structure, and has the kernel
        // fill in what was there before.
        let (failed, previous) = unsafe {
            let mut action: libc::sigaction = std::mem::zeroed();
            action.sa_sigaction = on_signal as extern "C" fn(c_int) as usize;
            action.sa_flags = libc::SA_RESTART;
            libc::sigemptyset(&mut action.sa_mask);
            let mut previous: libc::sigaction = std::mem::zeroed();
            (libc::sigaction(signal, &action, &mut previous) != 0, previous)
        };
        if failed {
            return Err(io::Error::last_os_error());
        }
        slot.previous = Some(previous);
    }
    slot.listeners = slot.listeners.saturating_add(1);
    Ok(())
}

/// Takes one listener off `signal`. With the last one, the signal goes
/// back to what the process did with it before anyone listened.
fn uninstall(registry: &mut Registry, signal: c_int) {
    let Some(slot) = usize::try_from(signal).ok().and_then(|slot| registry.slots.get_mut(slot)) else {
        return;
    };
    slot.listeners = slot.listeners.saturating_sub(1);
    if slot.listeners == 0 {
        if let Some(previous) = slot.previous.take() {
            // SAFETY: restores a disposition the kernel itself reported.
            unsafe {
                libc::sigaction(signal, &previous, std::ptr::null_mut());
            }
        }
    }
}

/// A listener for a set of signals.
///
/// While it exists, the signals it names are delivered to it instead of doing
/// what they would otherwise do to the process (ending it, for most).
/// [`next`](Self::next) waits for one and says which.
///
/// It reports signals that arrive from the moment it is created, including
/// those that arrive while nobody is waiting in `next`. Signals of one kind
/// that arrive faster than they are collected are reported once, not once
/// each: a signal says "this happened", not how many times.
///
/// Any number of listeners may exist, on any shards, for the same or
/// different signals; each sees every signal it listens for. When the last
/// listener for a signal is dropped, the signal goes back to what the process
/// did with it before.
///
/// ```no_run
/// use starfish::signal::Signals;
///
/// # async fn example() -> std::io::Result<()> {
/// let mut signals = Signals::new(&[libc::SIGHUP, libc::SIGTERM])?;
/// loop {
///     match signals.next().await {
///         libc::SIGHUP => println!("reload"),
///         _ => break,
///     }
/// }
/// # Ok(())
/// # }
/// ```
pub struct Signals {
    id: u64,
    /// Each signal listened for, with the arrival count already reported.
    seen: Vec<(c_int, u64)>,
}

impl Signals {
    /// Starts listening for `signals`, given by number (`libc::SIGINT` and
    /// so on).
    ///
    /// # Errors
    ///
    /// `InvalidInput` if the list is empty, or names something that is not
    /// a signal or that reports a program fault (`SIGSEGV`, `SIGBUS`,
    /// `SIGFPE`, `SIGILL`). Otherwise the kernel's error, which is what
    /// `SIGKILL` and `SIGSTOP` get.
    pub fn new(signals: &[i32]) -> io::Result<Signals> {
        if signals.is_empty() {
            return Err(invalid("no signals to listen for"));
        }
        let mut registry = registry();
        start(&mut registry)?;
        let mut seen = Vec::with_capacity(signals.len());
        for &signal in signals {
            if let Err(err) = install(&mut registry, signal) {
                // All or nothing: undo the ones that went in.
                for (installed, _) in seen {
                    uninstall(&mut registry, installed);
                }
                return Err(err);
            }
            seen.push((signal, count(signal)));
        }
        let id = registry.next_listener;
        registry.next_listener = registry.next_listener.wrapping_add(1);
        Ok(Signals { id, seen })
    }

    /// Waits for one of the signals and returns its number.
    ///
    /// If one has already arrived since the listener was created or since it was
    /// last reported, this returns at once. If several different signals have
    /// arrived, they are returned one per call, in the order they were given to
    /// [`new`](Self::new), not the order they arrived in.
    ///
    /// Dropping the future loses nothing: a signal that arrives meanwhile is
    /// reported by the next call.
    pub async fn next(&mut self) -> i32 {
        poll_fn(|cx| {
            let waker = cx.waker().clone();
            let mut registry = registry();
            for (signal, seen) in &mut self.seen {
                let count = count(*signal);
                if count != *seen {
                    *seen = count;
                    let stale = registry.listeners.remove(&self.id);
                    drop(registry);
                    drop(stale);
                    return Poll::Ready(*signal);
                }
            }
            let replaced = registry.listeners.insert(self.id, waker);
            drop(registry);
            drop(replaced);
            Poll::Pending
        })
        .await
    }
}

impl Drop for Signals {
    /// Stops listening: forgets this listener's waker and takes it off each of
    /// its signals. A signal that is left with no listener goes back to what the
    /// process did with it before anyone listened.
    fn drop(&mut self) {
        let mut registry = registry();
        let waker = registry.listeners.remove(&self.id);
        for (signal, _) in &self.seen {
            uninstall(&mut registry, *signal);
        }
        drop(registry);
        drop(waker);
    }
}

/// Waits until the process is asked to stop, by `SIGINT` (Ctrl-C) or
/// `SIGTERM`, and returns which of the two it was.
///
/// When this returns it has stopped listening, so the signals do again
/// what they did before: a second Ctrl-C during the shutdown that follows
/// ends the program at once, as it would any other. To handle a second
/// request yourself, keep a [`Signals`] instead.
pub async fn termination() -> io::Result<i32> {
    let mut signals = Signals::new(&[libc::SIGINT, libc::SIGTERM])?;
    Ok(signals.next().await)
}

debug_impl!(Signals, "signals" => |signals| signals.seen.iter().map(|(signal, _)| *signal).collect::<Vec<_>>());