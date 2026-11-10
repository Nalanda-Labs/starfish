//! Drives the per-shard allocator with arbitrary sequences of allocations,
//! reallocations and frees (some from another thread) and checks it against
//! a model: every block is aligned as asked, keeps the bytes written to it
//! until it is freed, and the heap's accounting returns to where it began.
//!
//! `cargo +nightly fuzz run alloc_ops`

#![no_main]

use std::alloc::{GlobalAlloc, Layout};

use libfuzzer_sys::fuzz_target;
use starfish::alloc::{stats, ShardAlloc};
use starfish::{yield_now, Runtime};

#[global_allocator]
static GLOBAL: ShardAlloc = ShardAlloc;

/// Upper bound on live bytes, so that an input cannot exhaust memory.
const BUDGET: usize = 64 << 20;
const MAX_BLOCKS: usize = 1024;

struct Block {
    ptr: *mut u8,
    layout: Layout,
    tag: u8,
}

// The helper thread only frees these.
unsafe impl Send for Block {}

fn fill(block: &Block) {
    unsafe { std::ptr::write_bytes(block.ptr, block.tag, block.layout.size()) };
}

fn check(block: &Block) {
    let bytes = unsafe { std::slice::from_raw_parts(block.ptr, block.layout.size()) };
    assert!(
        bytes.iter().all(|&byte| byte == block.tag),
        "block of {} bytes was overwritten",
        block.layout.size()
    );
}

fn size_for(kind: u8, a: u8, b: u8) -> usize {
    let raw = usize::from(a) << 8 | usize::from(b);
    match kind % 8 {
        // Mostly small, around the size-class boundaries.
        0..=3 => 1 + raw % 512,
        4 | 5 => 1 + raw % 20_000,
        6 => 16_000 + raw,
        _ => 1 + raw * 40,
    }
}

fn align_for(c: u8) -> usize {
    match c % 32 {
        // Usually what ordinary types ask for.
        0..=19 => 1 << (c % 5),
        shift => 1 << (shift - 15),
    }
}

async fn run(data: Vec<u8>) {
    let mut blocks: Vec<Block> = Vec::with_capacity(MAX_BLOCKS);
    let baseline = stats().expect("the shard has a heap").allocated_bytes;
    let mut live = 0usize;
    let mut helpers = 0;

    for op in data.chunks_exact(4) {
        let (kind, a, b, c) = (op[0], op[1], op[2], op[3]);
        match kind % 16 {
            0..=7 => {
                let size = size_for(kind, a, b);
                if blocks.len() == MAX_BLOCKS || live + size > BUDGET {
                    continue;
                }
                let layout = Layout::from_size_align(size, align_for(c)).unwrap();
                let ptr = unsafe { GLOBAL.alloc(layout) };
                assert!(!ptr.is_null());
                assert_eq!(ptr as usize % layout.align(), 0, "{layout:?}");
                let block = Block { ptr, layout, tag: c | 1 };
                fill(&block);
                live += size;
                blocks.push(block);
            }
            8..=11 => {
                if blocks.is_empty() {
                    continue;
                }
                let block = blocks.swap_remove(usize::from(a) % blocks.len());
                check(&block);
                live -= block.layout.size();
                unsafe { GLOBAL.dealloc(block.ptr, block.layout) };
            }
            12 | 13 => {
                if blocks.is_empty() {
                    continue;
                }
                let index = usize::from(a) % blocks.len();
                let new_size = size_for(b, c, kind);
                let block = &mut blocks[index];
                if live - block.layout.size() + new_size > BUDGET {
                    continue;
                }
                check(block);
                let kept = block.layout.size().min(new_size);
                let ptr = unsafe { GLOBAL.realloc(block.ptr, block.layout, new_size) };
                assert!(!ptr.is_null());
                assert_eq!(ptr as usize % block.layout.align(), 0);
                let prefix = unsafe { std::slice::from_raw_parts(ptr, kept) };
                assert!(prefix.iter().all(|&byte| byte == block.tag), "realloc lost data");
                live = live - block.layout.size() + new_size;
                block.ptr = ptr;
                block.layout = Layout::from_size_align(new_size, block.layout.align()).unwrap();
                fill(block);
            }
            14 => {
                // Hand some blocks to another thread to free.
                if helpers == 4 || blocks.is_empty() {
                    continue;
                }
                helpers += 1;
                let count = (1 + usize::from(a) % 32).min(blocks.len());
                let moved: Vec<Block> = blocks.drain(blocks.len() - count..).collect();
                live -= moved.iter().map(|block| block.layout.size()).sum::<usize>();
                std::thread::spawn(move || {
                    for block in moved {
                        check(&block);
                        unsafe { GLOBAL.dealloc(block.ptr, block.layout) };
                    }
                })
                .join()
                .unwrap();
            }
            _ => yield_now().await,
        }
        let allocated = stats().unwrap().allocated_bytes;
        assert!(allocated >= baseline, "accounting went below the baseline");
    }

    for block in blocks.drain(..) {
        check(&block);
        unsafe { GLOBAL.dealloc(block.ptr, block.layout) };
    }
    // Let the event loop collect what the helper threads handed back.
    for _ in 0..4 {
        yield_now().await;
    }
    assert_eq!(stats().unwrap().allocated_bytes, baseline, "memory was lost or double-counted");
}

fuzz_target!(|data: &[u8]| {
    let data = data.to_vec();
    Runtime::new()
        .shards(1)
        .pin_threads(false)
        .run(move || run(data))
        .unwrap();
});
