//! Copying a file through the page cache.
//!
//! Files have no cursor: every read and write names its offset.
//!
//! Run with `cargo run --example file_copy`. It works on two scratch
//! files in the system's temporary directory and removes them afterwards.

use starfish::fs::{remove_file, File};
use starfish::Runtime;

/// Copies everything in `from` to `to`, 64 KiB at a time, reusing one
/// buffer. Returns the number of bytes copied.
async fn copy(from: &File, to: &File) -> std::io::Result<u64> {
    let mut offset = 0;
    let mut buf = Vec::with_capacity(64 * 1024);
    loop {
        // A read replaces the buffer's contents, up to its capacity.
        let (result, filled) = from.read_at(buf, offset).await;
        let read = result?;
        if read == 0 {
            return Ok(offset);
        }
        let (result, written) = to.write_all_at(filled, offset).await;
        result?;
        offset += read as u64;
        buf = written;
    }
}

async fn app() -> starfish::Result<()> {
    let dir = std::env::temp_dir();
    let source_path = dir.join(format!("starfish-example-{}-source", std::process::id()));
    let copy_path = dir.join(format!("starfish-example-{}-copy", std::process::id()));

    // Something to copy: a megabyte and a bit, so the last read is short.
    let contents: Vec<u8> = (0..1_050_000u32).map(|n| (n % 251) as u8).collect();
    let source = File::create(&source_path).await?;
    let (result, contents) = source.write_all_at(contents, 0).await;
    result?;
    source.sync_all().await?;

    let source = File::open(&source_path).await?;
    let target = File::create(&copy_path).await?;
    let copied = copy(&source, &target).await?;
    target.sync_all().await?;
    println!("copied {copied} bytes to {}", copy_path.display());

    let (result, check) = target.read_at(Vec::with_capacity(contents.len()), 0).await;
    result?;
    println!("the copy matches: {}", check == contents);

    remove_file(&source_path).await?;
    remove_file(&copy_path).await?;
    Ok(())
}

fn main() -> starfish::Result<()> {
    Runtime::new().shards(1).run(app)?
}
