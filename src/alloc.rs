//! A per-shard memory allocator.
//!
//! Install it as the program's global allocator:
//!
//! ```no_run
//! #[global_allocator]
//! static GLOBAL: starfish::alloc::ShardAlloc = starfish::alloc::ShardAlloc;
//! ```
//!
//! Every shard then allocates from a heap of its own, with no locks and no
//! atomic operations on the fast path, and memory a shard allocates stays
//! on the pages (and the NUMA node) that shard touches. Memory freed by a
//! different thread than the one that allocated it is handed back to its
//! owner through a lock-free list and reused there. Threads that are not
//! shards use the system allocator.
//!
//! # Layout
//!
//! The allocator reserves one large range of address space (no memory) and
//! divides it into fixed-size *areas*, one per heap, so the heap that owns
//! a pointer follows from the pointer's address alone. An area holds a
//! header, an array with one descriptor per page, and the pages. Pages are
//! committed in 2 MiB steps as the heap grows.
//!
//! Pages are handed out in *spans* of whole pages; free spans are kept in
//! lists by size and merged with free neighbours. Requests up to 16 KiB are
//! served from spans cut into equal objects of one of 36 size classes; a
//! span whose objects have all been freed goes back to the page level.
//! Larger requests get a span of their own, and large spans are returned to
//! the operating system when freed.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};

/// Base-two logarithm of the page size. The allocator works in 4 KiB pages
/// whatever the machine's page size is; every size the kernel is asked to
/// commit is a multiple of 2 MiB, so a larger system page size still divides
/// it.
const PAGE_SHIFT: u32 = 12;
/// Bytes in a page: 4096.
const PAGE: usize = 1 << PAGE_SHIFT;
/// Base-two logarithm of an area's size. An area is the address space of
/// one heap: 32 GiB, which is therefore the most one shard can ever hold in
/// its heap. Because every area has this size and they lie side by side, the
/// heap that owns a pointer is found by shifting the pointer's offset in the
/// region (see [`owner_of`]).
const AREA_SHIFT: u32 = 35;
/// Bytes of address space in one area: 32 GiB.
const AREA: usize = 1 << AREA_SHIFT;
/// Pages in one area: 2^23. Page indexes therefore fit a `u32` with room
/// to spare, which is what [`PageDesc`] stores them in.
const AREA_PAGES: usize = AREA >> PAGE_SHIFT;
/// How many areas the region has room for at most, which is one heap per
/// shard a runtime may have. At 32 GiB each that is 32 TiB of address space;
/// [`reserve_region`] settles for less where the process is not allowed that
/// much.
const MAX_HEAPS: usize = 1024;

/// Bytes set aside at the start of an area for the [`Heap`] header. Far
/// more than the header needs (an assertion below checks it fits); the round
/// figure keeps the descriptor array page-aligned.
const HEADER_BYTES: usize = 64 << 10;
/// Bytes the descriptor array would occupy if every page of the area had
/// its descriptor committed: 2^23 descriptors of 32 bytes, 256 MiB. Only
/// address space; the array is committed as far as the heap has grown.
const DESC_BYTES: usize = AREA_PAGES * std::mem::size_of::<PageDesc>();
/// The heap grows by this many pages at a time (2 MiB), or a multiple of
/// it for a request larger than that. 2 MiB is the size of a huge page on
/// x86-64, so each step can be backed by one.
const GROW_PAGES: usize = 512;
/// Bytes in one growth step: 2 MiB. Also the alignment of the region and
/// of the first data page.
const GROW_BYTES: usize = GROW_PAGES << PAGE_SHIFT;
/// Index of the first page that holds allocations. The pages before it
/// are the header and the descriptor array; the sum of the two is rounded up
/// to 2 MiB so that the data pages start on a huge-page boundary. It comes to
/// about 258 MiB into the area. A page index below this never refers to a
/// span, which is why index 0 can mean "none" in the lists.
const FIRST_DATA_PAGE: usize =
    ((HEADER_BYTES + DESC_BYTES).div_ceil(GROW_BYTES) * GROW_BYTES) >> PAGE_SHIFT;

/// How many size classes small objects come in. See [`class_table`] for
/// the sizes.
const CLASSES: usize = 36;
/// The largest request served from a size class: 16 KiB. Anything larger
/// gets a span of whole pages to itself.
const MAX_SMALL: usize = 16 << 10;
/// How many free lists the page level keeps. List `b` holds the free
/// spans of 2^b to 2^(b+1) - 1 pages; the last one holds only a span that is
/// the whole area, which the assertion below ties to the area's size.
const BUCKETS: usize = 24;
/// Checks that the last bucket is exactly the one for a span as large
/// as the area, so [`bucket_of`] never needs a bucket that does not exist.
const _: () = assert!(AREA_PAGES == 1 << (BUCKETS - 1));
/// How many spans of the first candidate list are examined for a fit.
/// Spans in the bucket a request maps to may be smaller than the request, so
/// that list has to be searched; the search stops after this many, and the
/// request is then served from a larger bucket, where any span fits. This
/// bounds the cost of an allocation at the price of sometimes splitting a
/// larger span than necessary.
const SCAN_LIMIT: usize = 16;
/// Freed spans of at least this many pages (1 MiB) have their memory
/// returned to the operating system with `MADV_DONTNEED`. Smaller ones keep
/// their memory, because they are likely to be reused soon and giving memory
/// back and faulting it in again costs more than holding it.
const RELEASE_PAGES: usize = 256;

// Page states. Zero is what untouched descriptor memory reads as.
/// State of a page that carries no information of its own: the middle of
/// a span, or a page the heap has not used yet. It is zero because that is
/// what freshly committed descriptor memory reads as.
const INTERIOR: u8 = 0;
/// State of the first page of a free span. Its descriptor holds the
/// span's length and the links of the free list it is on.
const FREE_HEAD: u8 = 1;
/// State of the last page of a free span of two or more pages. Its
/// descriptor points back at the span's first page, which is how a span being
/// freed finds a free neighbour that ends just before it.
const FREE_TAIL: u8 = 2;
/// State of every page of a span that is cut into small objects. Each
/// such page points at the span's first page, so the span of an object is
/// found from the object's address in two steps.
const SMALL: u8 = 3;
/// State of the first page of a span allocated whole, for one large
/// request. The other pages of the span are `INTERIOR`.
const LARGE: u8 = 4;

/// One per page. What the fields mean depends on `state`:
///
/// - `FREE_HEAD`: first page of a free span of `span_pages`; `prev`/`next`
///   link the free list of its size bucket.
/// - `FREE_TAIL`: last page of a free span that starts at `span_head`.
/// - `SMALL`: page of a span cut into objects, which starts at `span_head`.
///   The span's first page also holds the size class, the list of freed
///   objects, how many objects were never handed out (`fresh`), how many
///   are available in total (`free_count`), and `prev`/`next` linking the
///   class's list of spans with available objects.
/// - `LARGE`: first page of an allocated span of `span_pages`.
///
/// Page indexes are relative to the area; index 0 is the header, so 0
/// doubles as "none" in links.
#[repr(C)]
struct PageDesc {
    /// One of `INTERIOR`, `FREE_HEAD`, `FREE_TAIL`, `SMALL`, `LARGE`.
    state: u8,
    /// Size class of a `SMALL` span; set on its first page only.
    class: u8,
    /// Objects of a `SMALL` span that can be handed out: those freed and
    /// those never used. Zero means the span is full and off its class's list.
    free_count: u16,
    /// Objects of a `SMALL` span that were never handed out. They lie at
    /// the end of the span, after the ones that were.
    fresh: u16,
    /// Makes the layout explicit; unused.
    _pad: u16,
    /// Length of the span in pages, on the first page of a free, small or
    /// large span.
    span_pages: u32,
    /// Index of the span's first page, on a `FREE_TAIL` page and on every
    /// page of a `SMALL` span.
    span_head: u32,
    /// Previous span in the list this one is on (a free list or a class's
    /// list), or 0 if it is the first.
    prev: u32,
    /// Next span in the list, or 0 if this is the last.
    next: u32,
    /// First freed object of a `SMALL` span, or null. Freed objects are
    /// chained through their first word.
    free_head: *mut u8,
}

/// Checks the descriptor's size: the layout of an area, and the cost of
/// 256 MiB of address space for the array, are computed from it.
const _: () = assert!(std::mem::size_of::<PageDesc>() == 32);

/// The header at the start of an area: everything a heap knows about
/// itself apart from the per-page descriptors. Only the owning thread touches
/// it, except for `remote`, which any thread may push to.
///
/// It lives in memory the allocator commits itself and is never constructed
/// as a value: a freshly committed header is all zeros, which is the empty
/// state of every list and counter, and [`enter`] sets the three fields that
/// are not zero. It is always handled through a raw pointer, because a
/// reference would claim exclusive access that `remote` does not have.
#[repr(C)]
struct Heap {
    /// Objects freed by other threads, linked through their first word.
    remote: AtomicPtr<u8>,
    /// Address of the area, which is the address of this header.
    base: usize,
    /// Pages below this index are committed.
    top: usize,
    /// Bytes of the descriptor array that are committed.
    desc_committed: usize,
    /// Per size class: the first span with available objects.
    pools: [u32; CLASSES],
    /// Per size bucket: the first free span.
    free_spans: [u32; BUCKETS],
    /// Bytes in live allocations, at the size reserved for each.
    allocated_bytes: usize,
    /// Above this many allocated bytes the shard asks for memory back.
    /// Zero means no limit.
    limit: usize,
}

/// Checks that the header fits the space the layout gives it.
const _: () = assert!(std::mem::size_of::<Heap>() <= HEADER_BYTES);

/// What the allocator needs to know about one size class.
#[derive(Clone, Copy)]
struct ClassInfo {
    /// Size of an object of the class in bytes; a multiple of 16.
    size: u32,
    /// Pages in a span of the class.
    pages: u16,
    /// Objects in a span of the class: as many as fit.
    objects: u16,
}

/// Computes the table of size classes, at compile time.
///
/// The sizes are 16 to 128 in steps of 16, then four evenly spaced sizes per
/// doubling up to 16 KiB (160, 192, 224, 256, 320, ...). A request is rounded
/// up to the next class, so at most a quarter of an object is wasted on
/// rounding. Every size is a multiple of 16, and since spans start on a page
/// boundary every object is 16-byte aligned; an object of a power-of-two
/// class is aligned to its own size, up to a page.
///
/// A span of a class has enough pages for at least 16 objects, but never
/// more than 64 pages (256 KiB), which is what the largest class gets.
#[allow(clippy::arithmetic_side_effects, clippy::indexing_slicing)] // compile-time only
const fn class_table() -> [ClassInfo; CLASSES] {
    let mut table = [ClassInfo {
        size: 0,
        pages: 0,
        objects: 0,
    }; CLASSES];
    let mut class = 0;
    while class < CLASSES {
        let size = if class < 8 {
            (class + 1) * 16
        } else {
            let doubling = (class - 8) / 4;
            let step = (class - 8) % 4;
            (128 << doubling) + (step + 1) * (32 << doubling)
        };
        // Enough pages for at least 16 objects, at most 64 pages.
        let mut pages = (size * 16).div_ceil(PAGE);
        if pages > 64 {
            pages = 64;
        }
        table[class] = ClassInfo {
            size: size as u32,
            pages: pages as u16,
            objects: ((pages * PAGE) / size) as u16,
        };
        class += 1;
    }
    table
}

/// The size classes, indexed by class number.
static CLASS_INFO: [ClassInfo; CLASSES] = class_table();

/// Looks up a size class. A class number out of range, which no caller
/// produces, gives the largest class instead of a panic: an allocator cannot
/// panic, because panicking allocates.
fn class_info(class: usize) -> ClassInfo {
    match CLASS_INFO.get(class) {
        Some(info) => *info,
        None => ClassInfo {
            size: MAX_SMALL as u32,
            pages: 64,
            objects: 16,
        },
    }
}

/// The smallest class whose objects hold `size` bytes. `size` must be
/// between 1 and [`MAX_SMALL`].
///
/// Up to 128 bytes the class follows from dividing by 16. Above, the position
/// of the highest bit of `size - 1` gives the doubling and the two bits below
/// it give which of the four steps of that doubling; no table or loop is
/// needed.
fn class_of(size: usize) -> usize {
    if size <= 128 {
        return (size.wrapping_add(15) >> 4).wrapping_sub(1);
    }
    let last = size.wrapping_sub(1);
    let doubling = last.checked_ilog2().unwrap_or(7) as usize;
    let step = (last >> doubling.wrapping_sub(2)) & 3;
    doubling
        .wrapping_sub(7)
        .wrapping_mul(4)
        .wrapping_add(8)
        .wrapping_add(step)
}

/// Whether `ShardAlloc` is the program's allocator. Set by the first
/// allocation that goes through it, which in practice happens before `main`.
/// Shards set up heaps only if it is set, so a program that does not install
/// the allocator reserves nothing.
static INSTALLED: AtomicBool = AtomicBool::new(false);
/// Start of the reserved address range; zero until it is reserved. A
/// pointer is one of ours exactly if it lies in the `REGION_LEN` bytes from
/// here.
static REGION_BASE: AtomicUsize = AtomicUsize::new(0);
/// Length of the reserved range in bytes: the number of heaps it has
/// room for, times the area size. Written before `REGION_BASE`, so a thread
/// that sees the base also sees the length.
static REGION_LEN: AtomicUsize = AtomicUsize::new(0);
/// Serialises the one-time reservation, so two shards starting at once
/// do not both map a region. Taken only in [`reserve_region`], never on an
/// allocation path.
static RESERVING: std::sync::Mutex<()> = std::sync::Mutex::new(());
/// Which areas have an owning thread right now. A thread takes an area
/// by flipping its flag from false to true and gives it up by clearing it.
static CLAIMED: [AtomicBool; MAX_HEAPS] = [const { AtomicBool::new(false) }; MAX_HEAPS];
/// Which areas have an initialised header. Set once, the first time an
/// area is claimed, and never cleared: an area keeps its contents when its
/// owner leaves and is taken over as it is by the next thread to claim it.
static READY: [AtomicBool; MAX_HEAPS] = [const { AtomicBool::new(false) }; MAX_HEAPS];

thread_local! {
    /// The heap this thread owns; null for threads that are not shards,
    /// which allocate from the system. A plain thread-local pointer: reading it
    /// is the only work an allocation does before reaching the heap.
    static HEAP: Cell<*mut Heap> = const { Cell::new(ptr::null_mut()) };
}

/// The calling thread's heap, or null if it has none. Also null while the
/// thread is being torn down and its thread-locals are gone; allocations made
/// then go to the system allocator.
fn current_heap() -> *mut Heap {
    HEAP.try_with(Cell::get).unwrap_or(ptr::null_mut())
}

/// The heap whose area contains `ptr`, or `None` if `ptr` lies outside the
/// region, which means it came from the system allocator.
///
/// This is how a block finds its way home when freed: no header in front of
/// the block and no lookup structure, only a subtraction, a comparison and a
/// shift. It is also what makes it safe to mix the two allocators in one
/// program: every block is freed by the allocator its address says made it.
fn owner_of(ptr: *mut u8) -> Option<*mut Heap> {
    let base = REGION_BASE.load(Ordering::Relaxed);
    let offset = (ptr as usize).wrapping_sub(base);
    if base == 0 || offset >= REGION_LEN.load(Ordering::Relaxed) {
        return None;
    }
    let area = offset >> AREA_SHIFT << AREA_SHIFT;
    Some(base.wrapping_add(area) as *mut Heap)
}

/// Reserves the address range, once. Returns its start, or 0 if no
/// address space could be had.
///
/// It asks for room for `MAX_HEAPS` heaps and settles for half as many,
/// repeatedly, where the process's address space is restricted (a resource
/// limit, or a sanitizer's own layout). The reservation is `PROT_NONE` and
/// `MAP_NORESERVE`: it uses no memory and counts against no memory limit
/// until parts of it are committed. It is never unmapped.
///
/// Each attempt maps 2 MiB more than needed and the region starts at the
/// first 2 MiB boundary inside the mapping, so that every heap's growth steps
/// can be backed by huge pages.
fn reserve_region() -> usize {
    let base = REGION_BASE.load(Ordering::Acquire);
    if base != 0 {
        return base;
    }
    let _guard = RESERVING.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let base = REGION_BASE.load(Ordering::Acquire);
    if base != 0 {
        return base;
    }
    let mut heaps = MAX_HEAPS;
    while heaps > 0 {
        let region = heaps << AREA_SHIFT;
        // A little more than needed, so that the part used can start on a
        // 2 MiB boundary, which is what huge pages need.
        let length = region.wrapping_add(GROW_BYTES);
        // SAFETY: an anonymous mapping at an address of the kernel's
        // choosing. PROT_NONE and NORESERVE make it a pure address-space
        // reservation.
        let mapped = unsafe {
            libc::mmap(
                ptr::null_mut(),
                length,
                libc::PROT_NONE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
                -1,
                0,
            )
        };
        if mapped != libc::MAP_FAILED {
            let aligned = (mapped as usize).wrapping_add(GROW_BYTES - 1) & !(GROW_BYTES - 1);
            // The length first: whoever sees the base must see its extent.
            REGION_LEN.store(region, Ordering::Release);
            REGION_BASE.store(aligned, Ordering::Release);
            return aligned;
        }
        heaps >>= 1;
    }
    0
}

/// Makes `len` bytes at `addr` usable, by giving the pages read and write
/// permission. Returns whether the kernel agreed. The pages read as zeros
/// until written, and take up memory only once touched.
///
/// SAFETY: the range must lie inside the reservation, in the area of a heap
/// the caller owns.
unsafe fn commit(addr: usize, len: usize) -> bool {
    libc::mprotect(addr as *mut libc::c_void, len, libc::PROT_READ | libc::PROT_WRITE) == 0
}

// The functions below take the heap as a raw pointer and never form a
// reference to it, because other threads access `remote` concurrently.
//
// SAFETY contract shared by all of them: `h` points to an initialised heap
// header and the caller is the thread that owns that heap.

/// Pointer to the descriptor of page `page`. The descriptor array
/// follows the header.
///
/// SAFETY: besides the shared contract above, `page` must be below the
/// heap's `top`, or its descriptor may not be committed.
unsafe fn desc(h: *mut Heap, page: usize) -> *mut PageDesc {
    ((*h).base.wrapping_add(HEADER_BYTES) as *mut PageDesc).add(page)
}

/// Address of page `page` of the heap.
unsafe fn page_addr(h: *mut Heap, page: usize) -> usize {
    (*h).base.wrapping_add(page << PAGE_SHIFT)
}

/// Pointer to the head of a class's list of spans with available
/// objects. An out-of-range class is clamped to the last one, so the pointer
/// is always inside the header.
unsafe fn pool_head(h: *mut Heap, class: usize) -> *mut u32 {
    ptr::addr_of_mut!((*h).pools).cast::<u32>().add(class.min(CLASSES - 1))
}

/// The free list a span of `pages` pages belongs on: the base-two
/// logarithm of its length, rounded down.
fn bucket_of(pages: usize) -> usize {
    (pages.checked_ilog2().unwrap_or(0) as usize).min(BUCKETS - 1)
}

/// Pointer to the head of a free list. An out-of-range bucket is clamped
/// to the last one.
unsafe fn bucket_head(h: *mut Heap, bucket: usize) -> *mut u32 {
    ptr::addr_of_mut!((*h).free_spans)
        .cast::<u32>()
        .add(bucket.min(BUCKETS - 1))
}

/// Puts a span on the free list of its size, at the front, and marks its
/// first and last pages so that neighbours can find it.
///
/// The span must not be adjacent to another free span: callers merge first
/// ([`free_pages`]) or know the neighbours are in use ([`alloc_pages`]). That
/// free spans never touch is what keeps the heap from fragmenting into
/// adjacent pieces that could have served a larger request.
unsafe fn insert_free(h: *mut Heap, start: usize, pages: usize) {
    let head = bucket_head(h, bucket_of(pages));
    let d = desc(h, start);
    (*d).state = FREE_HEAD;
    (*d).span_pages = pages as u32;
    (*d).prev = 0;
    (*d).next = *head;
    if *head != 0 {
        (*desc(h, *head as usize)).prev = start as u32;
    }
    *head = start as u32;
    if pages > 1 {
        let tail = desc(h, start.wrapping_add(pages).wrapping_sub(1));
        (*tail).state = FREE_TAIL;
        (*tail).span_head = start as u32;
    }
}

/// Takes the free span that starts at `start` off its list and clears
/// the markers on its first and last pages. Returns its length in pages.
/// Works on a span anywhere in its list, since the lists are doubly
/// linked.
unsafe fn remove_free(h: *mut Heap, start: usize) -> usize {
    let d = desc(h, start);
    let pages = (*d).span_pages as usize;
    let (prev, next) = ((*d).prev, (*d).next);
    if prev != 0 {
        (*desc(h, prev as usize)).next = next;
    } else {
        *bucket_head(h, bucket_of(pages)) = next;
    }
    if next != 0 {
        (*desc(h, next as usize)).prev = prev;
    }
    (*d).state = INTERIOR;
    if pages > 1 {
        (*desc(h, start.wrapping_add(pages).wrapping_sub(1))).state = INTERIOR;
    }
    pages
}

/// Finds and removes a free span of at least `want` pages. Returns its
/// first page and its length, which may be more than was asked for; the
/// caller gives back what it does not use.
///
/// It looks first in the bucket `want` falls in, where spans may be too
/// small, examining at most [`SCAN_LIMIT`] of them, and then takes the first
/// span of the next non-empty larger bucket, where every span is large
/// enough. `None` means the heap has to grow.
unsafe fn take_span(h: *mut Heap, want: usize) -> Option<(usize, usize)> {
    let first = bucket_of(want);
    // Spans in the first bucket may be smaller than wanted; look at a few.
    let mut span = *bucket_head(h, first) as usize;
    let mut scanned = 0;
    while span != 0 && scanned < SCAN_LIMIT {
        let d = desc(h, span);
        if (*d).span_pages as usize >= want {
            return Some((span, remove_free(h, span)));
        }
        span = (*d).next as usize;
        scanned = scanned.wrapping_add(1);
    }
    // Every span in a higher bucket is large enough.
    let mut bucket = first.wrapping_add(1);
    while bucket < BUCKETS {
        let span = *bucket_head(h, bucket) as usize;
        if span != 0 {
            return Some((span, remove_free(h, span)));
        }
        bucket = bucket.wrapping_add(1);
    }
    None
}

/// Returns a span to the free lists, merging it with a free span that
/// starts right after it and with one that ends right before it, so that two
/// free spans are never adjacent. The pages must already be marked
/// `INTERIOR` apart from the first, which this clears.
///
/// The neighbour after is recognised by its `FREE_HEAD` page; the one before
/// by its `FREE_TAIL` page, or by `FREE_HEAD` if it is a single page.
unsafe fn free_pages(h: *mut Heap, start: usize, pages: usize) {
    let mut start = start;
    let mut pages = pages;
    (*desc(h, start)).state = INTERIOR;

    let after = start.wrapping_add(pages);
    if after < (*h).top && (*desc(h, after)).state == FREE_HEAD {
        pages = pages.wrapping_add(remove_free(h, after));
    }
    if start > FIRST_DATA_PAGE {
        let before = start.wrapping_sub(1);
        let d = desc(h, before);
        let head = match (*d).state {
            FREE_TAIL => Some((*d).span_head as usize),
            FREE_HEAD if (*d).span_pages == 1 => Some(before),
            _ => None,
        };
        if let Some(head) = head {
            pages = pages.wrapping_add(remove_free(h, head));
            start = head;
        }
    }
    insert_free(h, start, pages);
}

/// Commits more of the area: enough for a span of `want` pages, rounded
/// up to a multiple of 2 MiB. Returns false if the area is exhausted or the
/// kernel refuses the memory, in which case the heap is unchanged apart from
/// descriptor pages that may have been committed.
///
/// The descriptors of the new pages are committed first, then the pages,
/// which are also marked as candidates for huge pages. The new pages join
/// the free lists as one span, merged with a free span at the old top if
/// there is one.
unsafe fn grow(h: *mut Heap, want: usize) -> bool {
    let Some(pages) = want.max(GROW_PAGES).checked_next_multiple_of(GROW_PAGES) else {
        return false;
    };
    let old_top = (*h).top;
    let new_top = old_top.wrapping_add(pages);
    if pages > AREA_PAGES || new_top > AREA_PAGES {
        return false;
    }
    // Descriptors for the new pages first.
    let desc_size = std::mem::size_of::<PageDesc>();
    let Some(needed) = new_top.wrapping_mul(desc_size).checked_next_multiple_of(PAGE) else {
        return false;
    };
    let committed = (*h).desc_committed;
    if needed > committed {
        let from = (*h).base.wrapping_add(HEADER_BYTES).wrapping_add(committed);
        if !commit(from, needed.wrapping_sub(committed)) {
            return false;
        }
        (*h).desc_committed = needed;
    }
    if !commit(page_addr(h, old_top), pages << PAGE_SHIFT) {
        return false;
    }
    // The chunk is 2 MiB-aligned; let the kernel back it with huge pages.
    // Purely a hint, so the result does not matter.
    libc::madvise(
        page_addr(h, old_top) as *mut libc::c_void,
        pages << PAGE_SHIFT,
        libc::MADV_HUGEPAGE,
    );
    (*h).top = new_top;
    free_pages(h, old_top, pages);
    true
}

/// Allocates a span of `pages` pages whose address is a multiple of
/// `align` (a power of two, at least a page). Returns its first page index,
/// or 0 on failure. The span's first page is marked `LARGE`.
///
/// For an alignment above a page it takes a span longer than needed by the
/// alignment less one page, which always contains a suitably aligned start,
/// and puts the unused pages before and after back on the free lists. The
/// heap grows if no free span is long enough.
unsafe fn alloc_pages(h: *mut Heap, pages: usize, align: usize) -> usize {
    let slack = (align >> PAGE_SHIFT).max(1).wrapping_sub(1);
    let Some(want) = pages.checked_add(slack) else { return 0 };
    if pages == 0 || want > AREA_PAGES {
        return 0;
    }
    let (start, len) = match take_span(h, want) {
        Some(span) => span,
        None => {
            if !grow(h, want) {
                return 0;
            }
            match take_span(h, want) {
                Some(span) => span,
                None => return 0,
            }
        }
    };
    let first = if slack == 0 {
        start
    } else {
        let addr = page_addr(h, start);
        let aligned = addr.wrapping_add(align).wrapping_sub(1) & !align.wrapping_sub(1);
        aligned.wrapping_sub((*h).base) >> PAGE_SHIFT
    };
    // Neither remainder can touch a free span: the span they are cut from
    // had no free neighbours.
    let lead = first.wrapping_sub(start);
    let rest = len.wrapping_sub(lead).wrapping_sub(pages);
    if lead > 0 {
        insert_free(h, start, lead);
    }
    if rest > 0 {
        insert_free(h, first.wrapping_add(pages), rest);
    }
    let d = desc(h, first);
    (*d).state = LARGE;
    (*d).span_pages = pages as u32;
    first
}

/// Puts a span at the front of its class's list of spans with available
/// objects. Done when a span is created and when a full span gets an object
/// back.
unsafe fn pool_insert(h: *mut Heap, class: usize, span: usize) {
    let head = pool_head(h, class);
    let d = desc(h, span);
    (*d).prev = 0;
    (*d).next = *head;
    if *head != 0 {
        (*desc(h, *head as usize)).prev = span as u32;
    }
    *head = span as u32;
}

/// Takes a span off its class's list, either because it is full or
/// because it is being dissolved, and clears its links.
unsafe fn pool_remove(h: *mut Heap, class: usize, span: usize) {
    let d = desc(h, span);
    let (prev, next) = ((*d).prev, (*d).next);
    if prev != 0 {
        (*desc(h, prev as usize)).next = next;
    } else {
        *pool_head(h, class) = next;
    }
    if next != 0 {
        (*desc(h, next as usize)).prev = prev;
    }
    (*d).prev = 0;
    (*d).next = 0;
}

/// Allocates one object of a size class. Returns null if a new span was
/// needed and the heap could not provide one.
///
/// The object comes from the first span on the class's list: a previously
/// freed object if the span has one, since that memory is likely still in
/// the processor's cache, and otherwise the next object never handed out. If
/// the list is empty a new span is cut from the page level. A span that
/// gives out its last object leaves the list.
unsafe fn alloc_small(h: *mut Heap, class: usize) -> *mut u8 {
    let info = class_info(class);
    let mut span = *pool_head(h, class) as usize;
    if span == 0 {
        // No span of this class has room: cut a new one.
        let pages = info.pages as usize;
        span = alloc_pages(h, pages, PAGE);
        if span == 0 {
            return ptr::null_mut();
        }
        let mut page = 0;
        while page < pages {
            let d = desc(h, span.wrapping_add(page));
            (*d).state = SMALL;
            (*d).span_head = span as u32;
            page = page.wrapping_add(1);
        }
        let d = desc(h, span);
        (*d).class = class as u8;
        (*d).span_pages = pages as u32;
        (*d).free_count = info.objects;
        (*d).fresh = info.objects;
        (*d).free_head = ptr::null_mut();
        pool_insert(h, class, span);
    }
    let d = desc(h, span);
    let object = if !(*d).free_head.is_null() {
        let object = (*d).free_head;
        (*d).free_head = object.cast::<*mut u8>().read();
        object
    } else {
        // Objects are first handed out in address order, so a span's
        // memory is only touched as far as it is used.
        let index = info.objects.wrapping_sub((*d).fresh) as usize;
        (*d).fresh = (*d).fresh.wrapping_sub(1);
        page_addr(h, span).wrapping_add(index.wrapping_mul(info.size as usize)) as *mut u8
    };
    (*d).free_count = (*d).free_count.wrapping_sub(1);
    if (*d).free_count == 0 {
        pool_remove(h, class, span);
    }
    (*h).allocated_bytes = (*h).allocated_bytes.wrapping_add(info.size as usize);
    object
}

/// Allocates a span of whole pages for a request too large or too
/// strictly aligned for a size class. Returns null on failure, including a
/// size so large that rounding it up to pages overflows.
unsafe fn alloc_large(h: *mut Heap, size: usize, align: usize) -> *mut u8 {
    let Some(bytes) = size.checked_add(PAGE - 1) else {
        return ptr::null_mut();
    };
    let pages = bytes >> PAGE_SHIFT;
    let span = alloc_pages(h, pages, align.max(PAGE));
    if span == 0 {
        return ptr::null_mut();
    }
    (*h).allocated_bytes = (*h).allocated_bytes.wrapping_add(pages << PAGE_SHIFT);
    page_addr(h, span) as *mut u8
}

/// Allocates `size` bytes aligned to `align` from heap `h`; the entry
/// point for the owning thread. Returns null if the heap cannot serve the
/// request.
///
/// It first takes back anything other threads have freed, if the list is not
/// empty, so that memory freed elsewhere is reused before the heap grows.
/// Then it picks the cheapest path that satisfies the alignment:
///
/// - alignment up to 16 and size up to 16 KiB: the size class for the size;
/// - alignment up to a page: a power-of-two class, whose objects are aligned
///   to their size, if the rounded size is still at most 16 KiB;
/// - anything else: a span of its own.
///
/// A zero-size request is treated as one byte.
unsafe fn heap_alloc(h: *mut Heap, size: usize, align: usize) -> *mut u8 {
    if !(*h).remote.load(Ordering::Relaxed).is_null() {
        drain_remote(h);
    }
    let size = size.max(1);
    // Objects of every class start at multiples of 16.
    if align <= 16 && size <= MAX_SMALL {
        return alloc_small(h, class_of(size));
    }
    // An object of a power-of-two class starts at a multiple of its size
    // (or of the page size, for classes larger than a page).
    if align <= PAGE {
        if let Some(rounded) = size.max(align).checked_next_power_of_two() {
            if rounded <= MAX_SMALL {
                return alloc_small(h, class_of(rounded));
            }
        }
    }
    alloc_large(h, size, align)
}

/// How many bytes the allocation at `ptr` can hold, which is its class's
/// object size or its span's length, and may be more than was asked for.
/// Used by `realloc` to decide whether a block can stay where it is. Returns
/// 0 for an address that is not the start of an allocation's page.
unsafe fn usable_size(h: *mut Heap, ptr: *mut u8) -> usize {
    let page = (ptr as usize).wrapping_sub((*h).base) >> PAGE_SHIFT;
    let d = desc(h, page);
    match (*d).state {
        SMALL => class_info((*desc(h, (*d).span_head as usize)).class as usize).size as usize,
        LARGE => ((*d).span_pages as usize) << PAGE_SHIFT,
        _ => 0,
    }
}

/// Frees an object of this heap, on the owning thread.
///
/// A small object goes on its span's list of freed objects, with the link
/// written into the object itself. If the span was full it goes back on its
/// class's list; if all its objects are now free and the class has another
/// span with room, the span is dissolved and its pages return to the page
/// level. The last span of a class is kept, so a program that allocates and
/// frees one object in a loop does not create and dissolve a span each time.
///
/// A large span returns to the page level, and if it is at least
/// [`RELEASE_PAGES`] long its memory is given back to the operating system
/// first.
///
/// A pointer outside the heap's data pages, or to a page that is not
/// allocated, is ignored; a double free of a small object is not detected.
unsafe fn free_local(h: *mut Heap, ptr: *mut u8) {
    let page = (ptr as usize).wrapping_sub((*h).base) >> PAGE_SHIFT;
    if page < FIRST_DATA_PAGE || page >= (*h).top {
        return;
    }
    let d = desc(h, page);
    match (*d).state {
        SMALL => {
            let span = (*d).span_head as usize;
            let head = desc(h, span);
            let class = (*head).class as usize;
            let info = class_info(class);
            ptr.cast::<*mut u8>().write((*head).free_head);
            (*head).free_head = ptr;
            (*head).free_count = (*head).free_count.wrapping_add(1);
            (*h).allocated_bytes = (*h).allocated_bytes.wrapping_sub(info.size as usize);
            if (*head).free_count == 1 {
                // It was full; it has room again.
                pool_insert(h, class, span);
            } else if (*head).free_count == info.objects
                && ((*head).prev != 0 || (*head).next != 0)
            {
                // Entirely unused, and the class has another span with
                // room: give the pages back.
                pool_remove(h, class, span);
                let pages = (*head).span_pages as usize;
                let mut index = 0;
                while index < pages {
                    (*desc(h, span.wrapping_add(index))).state = INTERIOR;
                    index = index.wrapping_add(1);
                }
                free_pages(h, span, pages);
            }
        }
        LARGE => {
            let pages = (*d).span_pages as usize;
            (*h).allocated_bytes = (*h).allocated_bytes.wrapping_sub(pages << PAGE_SHIFT);
            if pages >= RELEASE_PAGES {
                // Hand the memory back; the address range stays ours and
                // reads as zeros when touched again.
                libc::madvise(
                    ptr.cast::<libc::c_void>(),
                    pages << PAGE_SHIFT,
                    libc::MADV_DONTNEED,
                );
            }
            free_pages(h, page, pages);
        }
        _ => {}
    }
}

/// Frees everything other threads have handed back. The whole list is
/// taken with one atomic swap, after which this thread owns it and walks it
/// without further synchronisation. Called from [`heap_alloc`] and from
/// [`poll`], and only when the list is not empty, so the common case costs
/// one load.
unsafe fn drain_remote(h: *mut Heap) {
    let mut object = (*h).remote.swap(ptr::null_mut(), Ordering::AcqRel);
    while !object.is_null() {
        let next = object.cast::<*mut u8>().read();
        free_local(h, object);
        object = next;
    }
}

/// Hands an object back to the heap that owns it, from any thread, by
/// pushing it onto the owner's `remote` list. The owner frees it for real
/// the next time it allocates or its event loop comes round.
///
/// The push is the usual lock-free stack push, with the link stored in the
/// object's first word. The stack is only ever emptied whole, never popped
/// one element at a time, so the ABA problem does not arise.
///
/// SAFETY: `owner` is an initialised heap and `ptr` a live allocation of
/// at least one word from it.
unsafe fn free_remote(owner: *mut Heap, ptr: *mut u8) {
    let remote = &(*owner).remote;
    let mut head = remote.load(Ordering::Relaxed);
    loop {
        ptr.cast::<*mut u8>().write(head);
        match remote.compare_exchange_weak(head, ptr, Ordering::Release, Ordering::Relaxed) {
            Ok(_) => return,
            Err(current) => head = current,
        }
    }
}

/// The allocator. See the [module documentation](self).
///
/// A unit type with no state of its own: everything lives in the reserved
/// region and a thread-local. It is meant to be used in exactly one way, as
/// the program's `#[global_allocator]`:
///
/// ```no_run
/// #[global_allocator]
/// static GLOBAL: starfish::alloc::ShardAlloc = starfish::alloc::ShardAlloc;
/// ```
///
/// Installing it changes nothing for threads that are not shards, which keep
/// using the system allocator through it. Not installing it is also fine:
/// the runtime works the same on the system allocator, only without per-shard
/// heaps, [`stats`], or the memory limit.
pub struct ShardAlloc;

// SAFETY: memory comes either from the system allocator or from a span or
// object of a heap that nothing else uses until it is freed; a block is
// freed by the allocator that produced it, found from its address.
unsafe impl GlobalAlloc for ShardAlloc {
    /// Allocates from the calling thread's heap if it has one, and from the
    /// system allocator otherwise, or if the heap cannot serve the request (the
    /// area is exhausted, or the kernel refuses to commit more). Also records,
    /// the first time, that this allocator is installed.
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if !INSTALLED.load(Ordering::Relaxed) {
            INSTALLED.store(true, Ordering::Relaxed);
        }
        let heap = current_heap();
        if !heap.is_null() {
            let ptr = heap_alloc(heap, layout.size(), layout.align());
            if !ptr.is_null() {
                return ptr;
            }
        }
        System.alloc(layout)
    }

    /// Frees a block where it came from, which its address tells: the system
    /// allocator for an address outside the region, the calling thread's heap
    /// directly if it owns the block, and otherwise the owning heap's remote
    /// list.
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        match owner_of(ptr) {
            None => System.dealloc(ptr, layout),
            Some(owner) if owner == current_heap() => free_local(owner, ptr),
            Some(owner) => free_remote(owner, ptr),
        }
    }

    /// Resizes a block. One owned by the calling thread stays in place if
    /// the new size fits what the block can already hold and is at least half of
    /// that, so growing within a size class and modest shrinking cost nothing.
    /// Otherwise a new block is allocated, the contents copied and the old block
    /// freed, which also moves a block from another shard's heap to the calling
    /// shard's. Blocks from the system allocator are resized by it.
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let Some(owner) = owner_of(ptr) else {
            return System.realloc(ptr, layout, new_size);
        };
        if owner == current_heap() {
            // Stay in place while the block is neither too small nor more
            // than twice too large.
            let usable = usable_size(owner, ptr);
            if new_size <= usable && new_size >= usable >> 1 {
                return ptr;
            }
        }
        let Ok(new_layout) = Layout::from_size_align(new_size, layout.align()) else {
            return ptr::null_mut();
        };
        let new = self.alloc(new_layout);
        if !new.is_null() {
            ptr::copy_nonoverlapping(ptr, new, layout.size().min(new_size));
            self.dealloc(ptr, layout);
        }
        new
    }
}

/// Ownership of a heap by the current thread; given up on drop. A shard
/// thread holds one for as long as it runs.
pub(crate) struct Entered {
    /// Index of the claimed area.
    slot: usize,
}

/// Gives the current thread a heap, if `ShardAlloc` is installed and one
/// is available. Otherwise returns `None` and the thread keeps using the
/// system allocator, which is not an error: the runtime works either way.
///
/// Called by each shard thread as it starts. It reserves the region if this
/// is the first shard of the process, claims the first unclaimed area, and
/// initialises the area's header if no thread has used it before. An area
/// used by an earlier shard (of a runtime that has since stopped) is taken
/// over with everything in it, so memory that outlived that runtime can
/// still be freed, and its free memory is reused.
///
/// `None` is returned if the allocator is not installed, the thread already
/// has a heap, no address space could be reserved, every area is claimed, or
/// the header could not be committed.
pub(crate) fn enter() -> Option<Entered> {
    if !INSTALLED.load(Ordering::Relaxed) || !current_heap().is_null() {
        return None;
    }
    let base = reserve_region();
    if base == 0 {
        return None;
    }
    let heaps = REGION_LEN.load(Ordering::Acquire) >> AREA_SHIFT;
    let slot = CLAIMED.iter().take(heaps).position(|claimed| {
        claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
    })?;
    let ready = READY.get(slot)?;
    let heap = base.wrapping_add(slot << AREA_SHIFT) as *mut Heap;
    if !ready.load(Ordering::Acquire) {
        // SAFETY: the header lies inside our reservation and this thread
        // holds the claim on the slot, so nothing else touches it. Fresh
        // pages read as zeros, which is the empty state of every field not
        // set here.
        unsafe {
            if !commit(heap as usize, HEADER_BYTES) {
                release_claim(slot);
                return None;
            }
            (*heap).base = heap as usize;
            (*heap).top = FIRST_DATA_PAGE;
            let first_desc = FIRST_DATA_PAGE.wrapping_mul(std::mem::size_of::<PageDesc>());
            (*heap).desc_committed = first_desc & !(PAGE - 1);
        }
        ready.store(true, Ordering::Release);
    }
    if HEAP.try_with(|current| current.set(heap)).is_err() {
        release_claim(slot);
        return None;
    }
    Some(Entered { slot })
}

/// Marks an area as unowned again, so another thread can claim it.
fn release_claim(slot: usize) {
    if let Some(claimed) = CLAIMED.get(slot) {
        claimed.store(false, Ordering::Release);
    }
}

impl Drop for Entered {
    /// Detaches the thread from its heap and releases the area.
    ///
    /// From here on this thread allocates from the system, and frees what it
    /// still holds from the heap through the remote list. The heap keeps its
    /// contents for whichever thread claims it next.
    fn drop(&mut self) {
        // From here on this thread allocates from the system, and frees
        // what it still holds from the heap through the remote list. The
        // heap keeps its contents for whichever thread claims it next.
        let _ = HEAP.try_with(|current| current.set(ptr::null_mut()));
        release_claim(self.slot);
    }
}

/// Reclaims memory that other threads freed back to this shard's heap.
/// Called by the event loop every turn, so that a shard which itself
/// allocates little still takes back, and can return to the page level,
/// what other shards free to it. Does nothing on a thread without a heap, and
/// costs one load when nothing is waiting.
pub(crate) fn poll() {
    let heap = current_heap();
    if heap.is_null() {
        return;
    }
    // SAFETY: a non-null thread-local heap is initialised and owned by
    // this thread.
    unsafe {
        if !(*heap).remote.load(Ordering::Relaxed).is_null() {
            drain_remote(heap);
        }
    }
}

/// Memory use of one shard's heap, as returned by [`stats`].
///
/// Both figures describe the heap, not the process: memory the shard
/// allocated before it had a heap, or that came from the system allocator,
/// is not included.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Stats {
    /// Bytes in live allocations, counted at the size actually reserved
    /// for each (its size class, or whole pages).
    pub allocated_bytes: usize,
    /// Bytes of address space the heap has made usable so far. Parts of it
    /// may have been returned to the operating system.
    pub committed_bytes: usize,
}

/// Memory use of the current shard's heap, or `None` if the calling thread
/// has none (it is not a shard, or `ShardAlloc` is not installed).
///
/// Reading it costs nothing: the heap keeps the totals as it goes. Memory
/// freed by other shards is counted as allocated until this shard takes it
/// back, which it does every turn of its event loop.
///
/// ```no_run
/// # async fn example() -> starfish::Result<()> {
/// // One figure per shard.
/// let per_shard = starfish::submit_to_all(|| async { starfish::alloc::stats() }).await?;
/// for (shard, stats) in per_shard.iter().enumerate() {
///     if let Some(stats) = stats {
///         println!("shard {shard}: {} bytes live", stats.allocated_bytes);
///     }
/// }
/// # Ok(()) }
/// ```
pub fn stats() -> Option<Stats> {
    let heap = current_heap();
    if heap.is_null() {
        return None;
    }
    // SAFETY: as in `poll`.
    unsafe {
        Some(Stats {
            allocated_bytes: (*heap).allocated_bytes,
            committed_bytes: (*heap).top.wrapping_sub(FIRST_DATA_PAGE) << PAGE_SHIFT,
        })
    }
}

/// Sets the current shard's memory limit (see
/// [`Runtime::memory_limit`](crate::Runtime::memory_limit)); zero means none.
/// Called by each shard as it starts. Does nothing on a thread without a
/// heap, which is why the limit only works with `ShardAlloc` installed.
pub(crate) fn set_limit(bytes: usize) {
    let heap = current_heap();
    if !heap.is_null() {
        // SAFETY: as in `poll`.
        unsafe { (*heap).limit = bytes };
    }
}

/// Whether the current shard's heap holds more than its limit. Always
/// false without a limit or without a heap. The event loop asks this every
/// turn; it is two loads and a comparison.
pub(crate) fn over_limit() -> bool {
    let heap = current_heap();
    if heap.is_null() {
        return false;
    }
    // SAFETY: as in `poll`.
    unsafe { (*heap).limit != 0 && (*heap).allocated_bytes > (*heap).limit }
}

/// Registers a function that frees memory, such as evicting from a cache,
/// to be called on the current shard while its heap is over the limit set
/// with [`Runtime::memory_limit`](crate::Runtime::memory_limit). It is
/// called from the event loop, between tasks, as soon as the heap is found
/// over the limit. If that does not bring the heap back under it, the
/// function is called again every so often for as long as the heap stays
/// over.
///
/// The function stays registered until the runtime stops, or until
/// [`remove`](PressureHook::remove) is called on what this returns.
/// Dropping the returned value does not remove it.
///
/// The function is registered for the calling shard only; a cache on every
/// shard needs a registration on every shard. It runs on the shard's thread
/// outside any task, so it cannot await, and should free what it can and
/// return. Several functions may be registered, and all are called each
/// time. A panic in one is caught and discarded.
///
/// # Errors
///
/// [`Error::NoRuntime`](crate::Error::NoRuntime) if the calling thread is
/// not a shard.
///
/// # Example
///
/// ```no_run
/// # use std::{cell::RefCell, collections::HashMap, rc::Rc};
/// # fn example() -> starfish::Result<()> {
/// let cache: Rc<RefCell<HashMap<u64, Vec<u8>>>> = Rc::default();
/// let evict = cache.clone();
/// let hook = starfish::alloc::on_pressure(move || evict.borrow_mut().clear())?;
/// // Later, if the cache goes away before the runtime does:
/// hook.remove();
/// # Ok(()) }
/// ```
pub fn on_pressure(reclaim: impl Fn() + 'static) -> crate::Result<PressureHook> {
    let shard = crate::runtime::current()?;
    let hook: std::rc::Rc<dyn Fn()> = std::rc::Rc::new(reclaim);
    let mut registered = Some(hook.clone());
    shard.reclaimers.with(|reclaimers| {
        if let Some(reclaim) = registered.take() {
            reclaimers.push(reclaim);
        }
    });
    Ok(PressureHook { hook })
}

/// A function registered with [`on_pressure`], for taking it out again.
///
/// It is only a handle: dropping it leaves the function registered, for the
/// rest of the runtime's life. It cannot be sent to another thread.
pub struct PressureHook {
    /// The registered function; compared by address to find it in the
    /// shard's list.
    hook: std::rc::Rc<dyn Fn()>,
}

impl PressureHook {
    /// Stops the function from being called, and drops it. It must be done
    /// on the shard the function was registered on, which is the only place this
    /// value can be. Does nothing if the runtime has already stopped, or if the
    /// function was already removed.
    pub fn remove(self) {
        let Some(shard) = crate::runtime::try_current() else { return };
        // Taken out of the list here and dropped after, outside the
        // borrow: dropping it may run code of the application's.
        let removed = shard.reclaimers.with(|reclaimers| {
            let place = reclaimers
                .iter()
                .position(|registered| std::rc::Rc::ptr_eq(registered, &self.hook))?;
            Some(reclaimers.swap_remove(place))
        });
        drop(removed);
    }
}

debug_impl!(PressureHook);

debug_impl!(ShardAlloc);

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::arithmetic_side_effects, clippy::indexing_slicing)]
mod tests {
    use super::*;

    #[test]
    fn size_classes_are_tight_and_ordered() {
        let mut previous = 0;
        for (class, info) in CLASS_INFO.iter().enumerate() {
            let size = info.size as usize;
            assert!(size > previous && size.is_multiple_of(16), "class {class}");
            assert!(info.objects >= 16 && info.pages <= 64, "class {class}");
            assert!(info.objects as usize * size <= info.pages as usize * PAGE);
            // The class's own size maps to it, and one byte more to the next.
            assert_eq!(class_of(size), class);
            assert_eq!(class_of(previous + 1), class);
            // Rounding wastes at most a quarter (beyond the 16-byte grain).
            assert!(size - (previous + 1) < 16.max(size / 4), "class {class}");
            previous = size;
        }
        assert_eq!(previous, MAX_SMALL);
        // Every power of two up to the limit is a class of its own, which
        // is what aligned requests rely on.
        let mut size = 16;
        while size <= MAX_SMALL {
            assert_eq!(class_info(class_of(size)).size as usize, size);
            size *= 2;
        }
    }

    #[test]
    fn spans_are_filed_by_size() {
        assert_eq!(bucket_of(1), 0);
        assert_eq!(bucket_of(2), 1);
        assert_eq!(bucket_of(3), 1);
        assert_eq!(bucket_of(512), 9);
        assert_eq!(bucket_of(AREA_PAGES), BUCKETS - 1);
        assert_eq!(FIRST_DATA_PAGE % GROW_PAGES, 0);
    }
}
