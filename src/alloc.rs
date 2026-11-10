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

const PAGE_SHIFT: u32 = 12;
const PAGE: usize = 1 << PAGE_SHIFT;
/// Address space per heap: 32 GiB.
const AREA_SHIFT: u32 = 35;
const AREA: usize = 1 << AREA_SHIFT;
const AREA_PAGES: usize = AREA >> PAGE_SHIFT;
/// One heap per shard a runtime may have.
const MAX_HEAPS: usize = 1024;

const HEADER_BYTES: usize = 64 << 10;
const DESC_BYTES: usize = AREA_PAGES * std::mem::size_of::<PageDesc>();
/// The heap grows by this many pages at a time (2 MiB).
const GROW_PAGES: usize = 512;
const GROW_BYTES: usize = GROW_PAGES << PAGE_SHIFT;
const FIRST_DATA_PAGE: usize =
    ((HEADER_BYTES + DESC_BYTES).div_ceil(GROW_BYTES) * GROW_BYTES) >> PAGE_SHIFT;

const CLASSES: usize = 36;
const MAX_SMALL: usize = 16 << 10;
const BUCKETS: usize = 24;
const _: () = assert!(AREA_PAGES == 1 << (BUCKETS - 1));
/// How many spans of the first candidate list are examined for a fit.
const SCAN_LIMIT: usize = 16;
/// Freed spans of at least this many pages (1 MiB) have their memory
/// returned to the operating system.
const RELEASE_PAGES: usize = 256;

// Page states. Zero is what untouched descriptor memory reads as.
const INTERIOR: u8 = 0;
const FREE_HEAD: u8 = 1;
const FREE_TAIL: u8 = 2;
const SMALL: u8 = 3;
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
    state: u8,
    class: u8,
    free_count: u16,
    fresh: u16,
    _pad: u16,
    span_pages: u32,
    span_head: u32,
    prev: u32,
    next: u32,
    free_head: *mut u8,
}

const _: () = assert!(std::mem::size_of::<PageDesc>() == 32);

/// The header at the start of an area. Only the owning thread touches it,
/// except for `remote`.
#[repr(C)]
struct Heap {
    /// Objects freed by other threads, linked through their first word.
    remote: AtomicPtr<u8>,
    base: usize,
    /// Pages below this index are committed.
    top: usize,
    /// Bytes of the descriptor array that are committed.
    desc_committed: usize,
    /// Per size class: the first span with available objects.
    pools: [u32; CLASSES],
    /// Per size bucket: the first free span.
    free_spans: [u32; BUCKETS],
    allocated_bytes: usize,
    /// Above this many allocated bytes the shard asks for memory back.
    /// Zero means no limit.
    limit: usize,
}

const _: () = assert!(std::mem::size_of::<Heap>() <= HEADER_BYTES);

#[derive(Clone, Copy)]
struct ClassInfo {
    size: u32,
    pages: u16,
    objects: u16,
}

/// Sizes 16 to 128 in steps of 16, then four evenly spaced sizes per
/// doubling up to 16 KiB, which bounds the rounding waste at 25%.
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

static CLASS_INFO: [ClassInfo; CLASSES] = class_table();

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

/// The smallest class whose objects hold `size` bytes (`1..=MAX_SMALL`).
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

/// Whether `ShardAlloc` is the program's allocator; shards only set up
/// heaps if so.
static INSTALLED: AtomicBool = AtomicBool::new(false);
/// Start of the reserved address range; zero until reserved.
static REGION_BASE: AtomicUsize = AtomicUsize::new(0);
/// Length of the reserved range in bytes. Written before `REGION_BASE`.
static REGION_LEN: AtomicUsize = AtomicUsize::new(0);
/// Serialises the one-time reservation.
static RESERVING: std::sync::Mutex<()> = std::sync::Mutex::new(());
/// Which areas have an owning thread right now.
static CLAIMED: [AtomicBool; MAX_HEAPS] = [const { AtomicBool::new(false) }; MAX_HEAPS];
/// Which areas have an initialised header.
static READY: [AtomicBool; MAX_HEAPS] = [const { AtomicBool::new(false) }; MAX_HEAPS];

thread_local! {
    /// The heap this thread owns; null for threads that are not shards.
    static HEAP: Cell<*mut Heap> = const { Cell::new(ptr::null_mut()) };
}

fn current_heap() -> *mut Heap {
    HEAP.try_with(Cell::get).unwrap_or(ptr::null_mut())
}

/// The heap whose area contains `ptr`, if it is one of ours.
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
/// limit, or a sanitizer's own layout).
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

/// Makes `len` bytes at `addr` (inside the reservation) usable.
unsafe fn commit(addr: usize, len: usize) -> bool {
    libc::mprotect(addr as *mut libc::c_void, len, libc::PROT_READ | libc::PROT_WRITE) == 0
}

// The functions below take the heap as a raw pointer and never form a
// reference to it, because other threads access `remote` concurrently.
//
// SAFETY contract shared by all of them: `h` points to an initialised heap
// header and the caller is the thread that owns that heap.

unsafe fn desc(h: *mut Heap, page: usize) -> *mut PageDesc {
    ((*h).base.wrapping_add(HEADER_BYTES) as *mut PageDesc).add(page)
}

unsafe fn page_addr(h: *mut Heap, page: usize) -> usize {
    (*h).base.wrapping_add(page << PAGE_SHIFT)
}

unsafe fn pool_head(h: *mut Heap, class: usize) -> *mut u32 {
    ptr::addr_of_mut!((*h).pools).cast::<u32>().add(class.min(CLASSES - 1))
}

fn bucket_of(pages: usize) -> usize {
    (pages.checked_ilog2().unwrap_or(0) as usize).min(BUCKETS - 1)
}

unsafe fn bucket_head(h: *mut Heap, bucket: usize) -> *mut u32 {
    ptr::addr_of_mut!((*h).free_spans)
        .cast::<u32>()
        .add(bucket.min(BUCKETS - 1))
}

/// Puts a span on the free list of its size. The span must not be
/// adjacent to another free span.
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

/// Takes a free span off its list and clears its markers.
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

/// Finds and removes a free span of at least `want` pages.
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

/// Returns a span to the free lists, merging it with free neighbours.
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

/// Commits more of the area, enough for a span of `want` pages.
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
/// `align` (a power of two, at least a page). Returns its first page
/// index, or 0 on failure. The span is marked `LARGE`.
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

/// How many bytes the allocation at `ptr` can hold.
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

/// Frees everything other threads have handed back.
unsafe fn drain_remote(h: *mut Heap) {
    let mut object = (*h).remote.swap(ptr::null_mut(), Ordering::AcqRel);
    while !object.is_null() {
        let next = object.cast::<*mut u8>().read();
        free_local(h, object);
        object = next;
    }
}

/// Hands an object back to the heap that owns it, from any thread.
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
pub struct ShardAlloc;

// SAFETY: memory comes either from the system allocator or from a span or
// object of a heap that nothing else uses until it is freed; a block is
// freed by the allocator that produced it, found from its address.
unsafe impl GlobalAlloc for ShardAlloc {
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

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        match owner_of(ptr) {
            None => System.dealloc(ptr, layout),
            Some(owner) if owner == current_heap() => free_local(owner, ptr),
            Some(owner) => free_remote(owner, ptr),
        }
    }

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

/// Ownership of a heap by the current thread; given up on drop.
pub(crate) struct Entered {
    slot: usize,
}

/// Gives the current thread a heap, if `ShardAlloc` is installed and one
/// is available. Otherwise the thread keeps using the system allocator.
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

fn release_claim(slot: usize) {
    if let Some(claimed) = CLAIMED.get(slot) {
        claimed.store(false, Ordering::Release);
    }
}

impl Drop for Entered {
    fn drop(&mut self) {
        // From here on this thread allocates from the system, and frees
        // what it still holds from the heap through the remote list. The
        // heap keeps its contents for whichever thread claims it next.
        let _ = HEAP.try_with(|current| current.set(ptr::null_mut()));
        release_claim(self.slot);
    }
}

/// Reclaims memory that other threads freed back to this shard's heap.
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

/// Memory use of one shard's heap.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
/// [`Runtime::memory_limit`](crate::Runtime::memory_limit)).
pub(crate) fn set_limit(bytes: usize) {
    let heap = current_heap();
    if !heap.is_null() {
        // SAFETY: as in `poll`.
        unsafe { (*heap).limit = bytes };
    }
}

/// Whether the current shard's heap holds more than its limit.
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
/// called from the event loop, between tasks, and repeatedly for as long
/// as the heap stays over the limit.
pub fn on_pressure(reclaim: impl Fn() + 'static) -> crate::Result<()> {
    let shard = crate::runtime::current()?;
    let mut reclaim: Option<std::rc::Rc<dyn Fn()>> = Some(std::rc::Rc::new(reclaim));
    shard.reclaimers.with(|reclaimers| {
        if let Some(reclaim) = reclaim.take() {
            reclaimers.push(reclaim);
        }
    });
    Ok(())
}

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
