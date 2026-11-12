//! Direct I/O: reading and writing whole blocks, bypassing the page cache.
//!
//! This is what a storage engine with its own cache uses. The device says
//! how buffers, offsets and lengths must be aligned; the file reports it
//! and hands out buffers that fit.
//!
//! Run with `cargo run --example direct_io -- <directory>`; without an
//! argument it uses the current directory. The filesystem must support
//! `O_DIRECT`. The scratch file is removed afterwards.

use starfish::fs::{remove_file, DmaFile};
use starfish::Runtime;

async fn app(dir: std::path::PathBuf) -> starfish::Result<()> {
    let path = dir.join(format!(".starfish-example-{}-direct", std::process::id()));
    let file = DmaFile::create(&path).await?;
    println!(
        "{}: buffers aligned to {} bytes, offsets and lengths to {}",
        path.display(),
        file.memory_alignment(),
        file.disk_alignment()
    );

    // The smallest buffer this file accepts is one block.
    let block = file.alloc_buf(1)?.capacity();
    for n in 0..4u64 {
        let mut buf = file.alloc_buf(block)?;
        // Records rarely fill a block; the rest is zeros.
        buf.fill_padded(format!("record {n}").as_bytes());
        let (result, _) = file.write_at(buf, n * block as u64).await;
        result?;
    }
    file.sync_all().await?;
    println!("wrote 4 blocks of {block} bytes");

    // One read for all four blocks.
    let (result, buf) = file.read_at(file.alloc_buf(4 * block)?, 0).await;
    println!("read {} bytes back", result?);
    for record in buf.chunks(block) {
        let text: Vec<u8> = record.iter().copied().take_while(|byte| *byte != 0).collect();
        println!("  {:?}", String::from_utf8_lossy(&text));
    }

    // Breaking the rules is caught before the kernel sees the request.
    let (result, _) = file.read_at(file.alloc_buf(block)?, 1).await;
    if let Err(err) = result {
        println!("reading at offset 1: {err}");
    }

    remove_file(&path).await?;
    Ok(())
}

fn main() -> starfish::Result<()> {
    let dir = std::env::args_os().nth(1).unwrap_or_else(|| ".".into());
    Runtime::new().shards(1).run(move || app(dir.into()))?
}
