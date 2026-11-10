//! A bounded lock-free queue with one producer and one consumer.
//!
//! Every ordered pair of shards gets one of these, so a shard sending to
//! another never contends with a third. The two ends are separate handles
//! that take `&mut self`, which is what makes "one producer, one consumer"
//! a compile-time fact rather than a convention.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Keeps the two indexes on separate cache lines, so the producer and the
/// consumer do not slow each other down by sharing one.
///
/// Each index is written by one side and only occasionally read by the other.
/// On one cache line, every write by either side would take the line away
/// from the other's processor, though they never touch the same data.
#[repr(align(128))]
struct CachePadded<T>(T);

/// The storage shared by the two ends: a fixed array of slots, used as a
/// ring, and the two positions in it.
///
/// `head` and `tail` count values taken and values put, without wrapping at
/// the array's length; the slot for a count is the count modulo the length,
/// which is a mask since the length is a power of two. The queue is empty
/// when they are equal and full when they differ by the length.
///
/// The slots between `head` and `tail` hold values; the rest are
/// uninitialised.
struct Ring<T> {
    slots: Box<[UnsafeCell<MaybeUninit<T>>]>,
    mask: usize,
    /// Next slot to read. Written only by the consumer.
    head: CachePadded<AtomicUsize>,
    /// Next slot to write. Written only by the producer.
    tail: CachePadded<AtomicUsize>,
}

// SAFETY: the ring hands each value from one thread to another, and the
// handles below ensure a slot is never accessed by both ends at once.
unsafe impl<T: Send> Send for Ring<T> {}
// SAFETY: as above; shared access goes through the atomics.
unsafe impl<T: Send> Sync for Ring<T> {}

impl<T> Drop for Ring<T> {
    /// Drops whatever was sent and never received. Runs when both ends are
    /// gone, on whichever thread let go last, which is why the values must be
    /// `Send`.
    fn drop(&mut self) {
        // Both handles are gone, so whatever was sent but never received is
        // dropped here.
        let tail = *self.tail.0.get_mut();
        let mut head = *self.head.0.get_mut();
        while head != tail {
            if let Some(slot) = self.slots.get_mut(head & self.mask) {
                // SAFETY: slots in `head..tail` hold initialised values that
                // nobody else can reach any more.
                unsafe { slot.get_mut().assume_init_drop() };
            }
            head = head.wrapping_add(1);
        }
    }
}

/// The sending end of the queue. There is exactly one, and `push` takes
/// `&mut self`, so two threads can never put values at the same time: the
/// type system enforces what the algorithm requires.
pub(crate) struct Producer<T> {
    ring: Arc<Ring<T>>,
    tail: usize,
    /// The consumer's position as last seen; refreshed only when the queue
    /// looks full, to avoid touching the consumer's cache line.
    head: usize,
}

/// The receiving end of the queue. There is exactly one, and `pop` takes
/// `&mut self`, for the same reason as [`Producer`].
pub(crate) struct Consumer<T> {
    ring: Arc<Ring<T>>,
    head: usize,
    /// The producer's position as last seen; refreshed only when the queue
    /// looks empty.
    tail: usize,
}

/// Creates a queue holding up to `capacity` values, rounded up to a power
/// of two (and at least 2), and returns its two ends.
///
/// A power of two lets a position be turned into a slot with a mask instead
/// of a division. Each end can be moved to the thread that will use it.
pub(crate) fn channel<T>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    let capacity = capacity.max(2).checked_next_power_of_two().unwrap_or(1 << 16);
    let slots: Box<[UnsafeCell<MaybeUninit<T>>]> = (0..capacity)
        .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
        .collect();
    let ring = Arc::new(Ring {
        slots,
        mask: capacity.wrapping_sub(1),
        head: CachePadded(AtomicUsize::new(0)),
        tail: CachePadded(AtomicUsize::new(0)),
    });
    (
        Producer {
            ring: ring.clone(),
            tail: 0,
            head: 0,
        },
        Consumer {
            ring,
            head: 0,
            tail: 0,
        },
    )
}

impl<T> Producer<T> {
    /// Appends a value, or hands it back if the queue is full.
    ///
    /// Never waits and never fails for any other reason. The producer keeps a
    /// copy of where the consumer was when it last looked, and looks again only
    /// when the queue seems full by that copy, so that in the common case a push
    /// touches none of the consumer's memory.
    pub(crate) fn push(&mut self, value: T) -> Result<(), T> {
        let capacity = self.ring.slots.len();
        if self.tail.wrapping_sub(self.head) >= capacity {
            self.head = self.ring.head.0.load(Ordering::Acquire);
            if self.tail.wrapping_sub(self.head) >= capacity {
                return Err(value);
            }
        }
        let Some(slot) = self.ring.slots.get(self.tail & self.ring.mask) else {
            return Err(value);
        };
        // SAFETY: the slot at `tail` is outside `head..tail`, so the
        // consumer does not touch it, and this is the only producer.
        unsafe { (*slot.get()).write(value) };
        self.tail = self.tail.wrapping_add(1);
        self.ring.tail.0.store(self.tail, Ordering::Release);
        Ok(())
    }
}

impl<T> Consumer<T> {
    /// Removes the oldest value, if there is one.
    ///
    /// Never waits. Like the producer, the consumer works from a copy of the
    /// other side's position and refreshes it only when the queue seems empty.
    pub(crate) fn pop(&mut self) -> Option<T> {
        if self.head == self.tail {
            self.tail = self.ring.tail.0.load(Ordering::Acquire);
            if self.head == self.tail {
                return None;
            }
        }
        let slot = self.ring.slots.get(self.head & self.ring.mask)?;
        // SAFETY: the slot at `head` is inside `head..tail`: the producer
        // initialised it and will not touch it again until `head` moves
        // past it, and this is the only consumer.
        let value = unsafe { (*slot.get()).assume_init_read() };
        self.head = self.head.wrapping_add(1);
        self.ring.head.0.store(self.head, Ordering::Release);
        Some(value)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::arithmetic_side_effects, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn fills_empties_and_wraps() {
        let (mut tx, mut rx) = channel::<String>(4);
        assert!(rx.pop().is_none());
        for round in 0..10 {
            for i in 0..4 {
                tx.push(format!("{round}-{i}")).unwrap();
            }
            // Full: the value comes back.
            assert_eq!(tx.push("extra".into()).unwrap_err(), "extra");
            for i in 0..4 {
                assert_eq!(rx.pop().unwrap(), format!("{round}-{i}"));
            }
            assert!(rx.pop().is_none());
        }
    }

    #[test]
    fn drops_what_was_never_received() {
        static DROPS: AtomicUsize = AtomicUsize::new(0);
        struct Counted;
        impl Drop for Counted {
            fn drop(&mut self) {
                DROPS.fetch_add(1, Ordering::SeqCst);
            }
        }
        let (mut tx, mut rx) = channel::<Counted>(8);
        for _ in 0..6 {
            assert!(tx.push(Counted).is_ok());
        }
        drop(rx.pop());
        drop(rx.pop());
        assert_eq!(DROPS.load(Ordering::SeqCst), 2);
        drop(tx);
        drop(rx);
        assert_eq!(DROPS.load(Ordering::SeqCst), 6);
    }

    #[test]
    fn two_threads_see_every_value_in_order() {
        let count = if cfg!(miri) { 2_000 } else { 200_000 };
        let (mut tx, mut rx) = channel::<Box<usize>>(16);
        let producer = std::thread::spawn(move || {
            for i in 0..count {
                let mut value = Box::new(i);
                loop {
                    match tx.push(value) {
                        Ok(()) => break,
                        Err(back) => {
                            value = back;
                            std::thread::yield_now();
                        }
                    }
                }
            }
        });
        let mut next = 0;
        while next < count {
            match rx.pop() {
                Some(value) => {
                    assert_eq!(*value, next);
                    next += 1;
                }
                None => std::thread::yield_now(),
            }
        }
        producer.join().unwrap();
        assert!(rx.pop().is_none());
    }
}
