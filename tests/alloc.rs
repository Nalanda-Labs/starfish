//! The per-shard allocator, exercised as the global allocator.

use std::alloc::{alloc, dealloc, realloc, Layout};

use brine::alloc::{stats, ShardAlloc};
use brine::{shard_id, submit_to, yield_now, Runtime};

#[global_allocator]
static GLOBAL: ShardAlloc = ShardAlloc;

fn rt(shards: usize) -> Runtime {
    Runtime::new().shards(shards).pin_threads(false)
}

/// A small deterministic generator, so failures reproduce.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 as usize
    }
}

struct Block {
    ptr: *mut u8,
    layout: Layout,
    tag: u8,
}

unsafe fn make(size: usize, align: usize, tag: u8) -> Block {
    let layout = Layout::from_size_align(size, align).unwrap();
    let ptr = alloc(layout);
    assert!(!ptr.is_null());
    assert_eq!(ptr as usize % align, 0, "size {size} align {align}");
    std::ptr::write_bytes(ptr, tag, size);
    Block { ptr, layout, tag }
}

unsafe fn check_and_free(block: Block) {
    let bytes = std::slice::from_raw_parts(block.ptr, block.layout.size());
    assert!(bytes.iter().all(|&byte| byte == block.tag), "block was overwritten");
    dealloc(block.ptr, block.layout);
}

#[test]
fn threads_outside_a_runtime_use_the_system_allocator() {
    assert_eq!(stats(), None);
    let data = vec![1u8; 100_000];
    assert_eq!(data.len(), 100_000);
}

#[test]
fn sizes_and_alignments() {
    rt(1)
        .run(|| async {
            let before = stats().expect("a shard has a heap");
            let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
            let mut blocks = Vec::with_capacity(4096);
            let capacity_cost = stats().unwrap().allocated_bytes - before.allocated_bytes;

            unsafe {
                // Every size around every class boundary, at natural alignment.
                for size in (1..=20_000).step_by(7).chain([16, 128, 129, 160, 4096, 16_384, 16_385]) {
                    blocks.push(make(size, 8, size as u8));
                }
                // Every alignment up to 1 MiB against assorted sizes.
                for shift in 0..=20 {
                    for size in [1, 24, 100, 1000, 5000, 70_000, 300_000] {
                        blocks.push(make(size, 1 << shift, shift as u8 + 1));
                    }
                }
                // No two live blocks overlap.
                let mut ranges: Vec<(usize, usize)> = blocks
                    .iter()
                    .map(|block| (block.ptr as usize, block.ptr as usize + block.layout.size()))
                    .collect();
                ranges.sort_unstable();
                assert!(ranges.windows(2).all(|pair| pair[0].1 <= pair[1].0));
                drop(ranges);

                let peak = stats().unwrap();
                assert!(peak.allocated_bytes > before.allocated_bytes + 20_000_000);

                // Free in random order, interleaved with new allocations.
                while !blocks.is_empty() {
                    let index = rng.next() % blocks.len();
                    check_and_free(blocks.swap_remove(index));
                    if rng.next().is_multiple_of(4) {
                        let size = 1 + rng.next() % 3000;
                        check_and_free(make(size, 16, 0xAB));
                    }
                }
                let after = stats().unwrap();
                assert_eq!(after.allocated_bytes, before.allocated_bytes + capacity_cost);

                // The same load again fits in what the heap already has.
                for round in 0..3 {
                    for size in (1..=20_000).step_by(7) {
                        blocks.push(make(size, 8, round));
                    }
                    for block in blocks.drain(..) {
                        check_and_free(block);
                    }
                }
                assert_eq!(stats().unwrap().committed_bytes, peak.committed_bytes);
            }
        })
        .unwrap();
}

#[test]
fn realloc_keeps_contents() {
    rt(1)
        .run(|| async {
            let mut values: Vec<u64> = Vec::new();
            for i in 0..2_000_000u64 {
                values.push(i * 3);
            }
            assert!(values.iter().enumerate().all(|(i, &value)| value == i as u64 * 3));
            values.truncate(1000);
            values.shrink_to_fit();
            assert!(values.iter().enumerate().all(|(i, &value)| value == i as u64 * 3));

            unsafe {
                let layout = Layout::from_size_align(100, 64).unwrap();
                let ptr = alloc(layout);
                std::ptr::write_bytes(ptr, 7, 100);
                let grown = realloc(ptr, layout, 100_000);
                assert_eq!(grown as usize % 64, 0);
                assert!(std::slice::from_raw_parts(grown, 100).iter().all(|&byte| byte == 7));
                dealloc(grown, Layout::from_size_align(100_000, 64).unwrap());
            }
        })
        .unwrap();
}

#[test]
fn memory_freed_on_another_shard_returns_to_its_owner() {
    rt(2)
        .run(|| async {
            assert_eq!(shard_id().unwrap(), 0);
            let before = stats().unwrap().allocated_bytes;
            let payload: Vec<Vec<u8>> = (0..2000).map(|i| vec![i as u8; 1 + i % 5000]).collect();
            let held = stats().unwrap().allocated_bytes;
            assert!(held > before + 2_000_000);

            // Shard 1 checks and drops what shard 0 allocated, and hands
            // back something it allocated itself.
            let reply = submit_to(1, move || async move {
                let other = stats().unwrap().allocated_bytes;
                for (i, block) in payload.iter().enumerate() {
                    assert!(block.iter().all(|&byte| byte == i as u8));
                }
                drop(payload);
                // Dropping foreign memory does not change this shard's heap.
                assert_eq!(stats().unwrap().allocated_bytes, other);
                vec![9u8; 50_000]
            })
            .await
            .unwrap();
            assert!(reply.iter().all(|&byte| byte == 9));
            drop(reply);

            // The event loop collects the returned memory.
            for _ in 0..100 {
                if stats().unwrap().allocated_bytes < before + 100_000 {
                    break;
                }
                yield_now().await;
            }
            let after = stats().unwrap().allocated_bytes;
            assert!(after < before + 100_000, "{after} vs {before}");
        })
        .unwrap();
}

#[test]
fn many_runtimes_reuse_heaps() {
    for round in 0..20u8 {
        let sum = rt(2)
            .run(move || async move {
                let data: Vec<Box<[u8; 512]>> = (0..1000).map(|_| Box::new([round; 512])).collect();
                submit_to(1, move || async move {
                    data.iter().map(|block| block[0] as u64).sum::<u64>()
                })
                .await
                .unwrap()
            })
            .unwrap();
        assert_eq!(sum, 1000 * round as u64);
    }
}
