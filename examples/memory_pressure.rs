//! A cache that shrinks when its shard runs short of memory.
//!
//! With the per-shard allocator installed, a shard knows how much memory
//! it holds. Past the configured limit, the runtime calls the functions
//! registered with `on_pressure`, between tasks, until the shard is back
//! under the limit. The limit is a request, checked every so often, so the
//! heap overshoots it a little before the cache is asked to shrink.
//!
//! Run with `cargo run --release --example memory_pressure`.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::rc::Rc;

use starfish::alloc::{self, ShardAlloc};
use starfish::{yield_now, Runtime};

#[global_allocator]
static GLOBAL: ShardAlloc = ShardAlloc;

const LIMIT: usize = 32 << 20;
const ENTRY: usize = 64 << 10;
const INSERTS: usize = 5000;

async fn app() -> starfish::Result<()> {
    let cache: Rc<RefCell<VecDeque<Vec<u8>>>> = Rc::default();
    let evictions = Rc::new(Cell::new(0usize));

    alloc::on_pressure({
        let cache = cache.clone();
        let evictions = evictions.clone();
        move || {
            // Drop the older half.
            let mut cache = cache.borrow_mut();
            let evict = cache.len() / 2;
            cache.drain(..evict);
            evictions.set(evictions.get() + evict);
        }
    })?;

    let mut largest = 0;
    for _ in 0..INSERTS {
        cache.borrow_mut().push_back(vec![0u8; ENTRY]);
        largest = largest.max(cache.borrow().len());
        yield_now().await;
    }

    println!(
        "inserted {INSERTS} entries of {} KiB ({} MiB in all) with a limit of {} MiB",
        ENTRY >> 10,
        (INSERTS * ENTRY) >> 20,
        LIMIT >> 20
    );
    println!(
        "the cache never held more than {} MiB; {} entries were evicted",
        (largest * ENTRY) >> 20,
        evictions.get()
    );
    if let Some(heap) = alloc::stats() {
        println!("the shard's heap now holds {} MiB", heap.allocated_bytes >> 20);
    }
    Ok(())
}

fn main() -> starfish::Result<()> {
    Runtime::new().shards(1).memory_limit(LIMIT).run(app)?
}
