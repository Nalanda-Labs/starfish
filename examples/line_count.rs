//! Reading a file from start to end.
//!
//! Run with `cargo run --example line_count -- <file>`; without an
//! argument it counts the lines of `Cargo.toml`.

use starfish::fs::File;
use starfish::Runtime;

async fn count(path: std::path::PathBuf) -> starfish::Result<()> {
    let file = File::open(&path).await?;
    let (mut lines, mut bytes) = (0u64, 0u64);
    let mut buf = Vec::with_capacity(256 * 1024);
    loop {
        let (result, filled) = file.read_at(buf, bytes).await;
        // `Ok(0)` is the end of the file.
        if result? == 0 {
            break;
        }
        lines += filled.iter().filter(|byte| **byte == b'\n').count() as u64;
        bytes += filled.len() as u64;
        buf = filled;
    }
    println!("{lines} lines, {bytes} bytes: {}", path.display());
    Ok(())
}

fn main() -> starfish::Result<()> {
    let path = std::env::args_os().nth(1).unwrap_or_else(|| "Cargo.toml".into());
    Runtime::new().shards(1).run(move || count(path.into()))?
}
