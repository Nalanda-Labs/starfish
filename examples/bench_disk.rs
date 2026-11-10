//! What the I/O scheduler costs: random 4 KiB direct reads issued straight
//! to the kernel (as `iotune` measures them) against the same load going
//! through the scheduler.
//!
//! `cargo run --release --example bench_disk -- <directory> [seconds]`

use std::cell::Cell;
use std::rc::Rc;
use std::time::{Duration, Instant};

use brine::fs::DmaFile;
use brine::iotune::{measure, Options};
use brine::{shard_count, shard_id, spawn, submit_to_all, Runtime};

#[global_allocator]
static GLOBAL: brine::alloc::ShardAlloc = brine::alloc::ShardAlloc;

const FILE_SIZE: u64 = 256 << 20;
const BLOCK: u64 = 4096;
const DEPTH: usize = 64;

fn main() -> brine::Result<()> {
    let directory = std::env::args().nth(1).unwrap_or_else(|| String::from("."));
    let seconds: u64 = std::env::args().nth(2).and_then(|s| s.parse().ok()).unwrap_or(3);
    let phase = Duration::from_secs(seconds);
    Runtime::new().shards(4).run(move || async move {
        let options = Options {
            file_size: FILE_SIZE,
            phase,
            depth: DEPTH,
            ..Default::default()
        };
        let raw = measure(&directory, options).await?;
        println!("straight to the kernel: {:>8} random reads/s", raw.read_iops.unwrap_or(0));

        // The same shape of load, through `DmaFile::read_at`.
        let path = std::path::Path::new(&directory).join(".brine-bench-disk");
        {
            let file = DmaFile::create(&path).await?;
            let mut offset = 0;
            while offset < FILE_SIZE {
                let mut buf = file.alloc(1 << 20)?;
                buf.set_len(1 << 20);
                buf.fill(0x5a);
                let (result, _) = file.write_at(buf, offset).await;
                offset += result? as u64;
            }
            file.sync_all().await?;
        }
        let shards = shard_count()?;
        let reader_path = path.clone();
        let counts = submit_to_all(move || {
            let path = reader_path.clone();
            async move {
                let file = Rc::new(DmaFile::open(&path).await?);
                let done = Rc::new(Cell::new(0u64));
                let deadline = Instant::now() + phase;
                let mut seed = 0x9E37_79B9_7F4A_7C15u64 ^ (shard_id()? as u64) << 32;
                let workers: Vec<_> = (0..DEPTH / shards)
                    .map(|worker| {
                        let (file, done) = (file.clone(), done.clone());
                        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(worker as u64 + 1);
                        let mut state = seed | 1;
                        spawn(async move {
                            let mut buf = file.alloc(BLOCK as usize)?;
                            while Instant::now() < deadline {
                                state ^= state << 13;
                                state ^= state >> 7;
                                state ^= state << 17;
                                let block = state % (FILE_SIZE / BLOCK);
                                let (result, returned) = file.read_at(buf, block * BLOCK).await;
                                result?;
                                buf = returned;
                                done.set(done.get() + 1);
                            }
                            Ok::<(), std::io::Error>(())
                        })
                    })
                    .collect();
                for worker in workers {
                    worker.await??;
                }
                Ok::<u64, brine::Error>(done.get())
            }
        })
        .await?;
        let mut total = 0;
        for count in counts {
            total += count?;
        }
        brine::fs::remove_file(&path).await?;
        println!("through the scheduler:  {:>8} random reads/s", total / seconds.max(1));
        Ok::<(), brine::Error>(())
    })?
}
