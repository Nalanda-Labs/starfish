//! Reading a file line by line.
//!
//! A file has no position of its own, so a `Cursor` supplies one, and a
//! `BufReader` on top finds the lines.
//!
//! Run with `cargo run --example line_count -- <file>`; without an
//! argument it reads `Cargo.toml`.

use starfish::fs::{metadata, Cursor, File};
use starfish::io::BufReader;
use starfish::Runtime;

async fn count(path: std::path::PathBuf) -> starfish::Result<()> {
    let size = metadata(&path).await?.len();
    let file = File::open(&path).await?;
    let mut lines = BufReader::with_capacity(256 * 1024, Cursor::new(&file));

    let (mut count, mut longest) = (0u64, 0usize);
    // Bytes, not text: a file may hold anything.
    let mut line = Vec::new();
    loop {
        line.clear();
        if lines.read_until(b'\n', &mut line).await? == 0 {
            break;
        }
        count += 1;
        longest = longest.max(line.len());
    }
    println!("{count} lines, the longest {longest} bytes, {size} bytes in all: {}", path.display());
    Ok(())
}

fn main() -> starfish::Result<()> {
    let path = std::env::args_os().nth(1).unwrap_or_else(|| "Cargo.toml".into());
    Runtime::new().shards(1).run(move || count(path.into()))?
}
