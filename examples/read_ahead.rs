//! What reading ahead does for a scan of a file with direct I/O.
//!
//! The program writes a 512 MiB file and then reads it from front to
//! back, adding up its bytes, twice: once asking the device for one
//! buffer at a time, once with further buffers already on their way while
//! the current one is being added up.
//!
//! Run with `cargo run --release --example read_ahead -- <directory>`;
//! without an argument it uses the current directory. The filesystem must
//! support `O_DIRECT`. The file is removed afterwards.

use std::time::{Duration, Instant};

use starfish::fs::{remove_file, DmaFile, DmaStreamReader, DmaStreamWriter};
use starfish::Runtime;

const SIZE: usize = 512 << 20;
const BUFFER: usize = 256 << 10;

/// Reads the whole file and returns the sum of its bytes.
async fn scan(file: &DmaFile, ahead: usize) -> std::io::Result<(u64, Duration)> {
    let start = Instant::now();
    let mut reader = DmaStreamReader::new(file, BUFFER)?.read_ahead(ahead)?;
    let mut piece = vec![0u8; BUFFER];
    let mut sum = 0u64;
    loop {
        let count = reader.read(&mut piece).await?;
        if count == 0 {
            return Ok((sum, start.elapsed()));
        }
        // The work done on each buffer, during which reads ahead proceed.
        sum += piece.iter().take(count).map(|byte| u64::from(*byte)).sum::<u64>();
    }
}

async fn app(dir: std::path::PathBuf) -> starfish::Result<()> {
    let path = dir.join(format!(".starfish-example-{}-scan", std::process::id()));
    let file = DmaFile::create(&path).await?;
    file.allocate(0, SIZE as u64).await?;

    let chunk: Vec<u8> = (0..1 << 20).map(|n| (n % 251) as u8).collect();
    let mut writer = DmaStreamWriter::new(&file, 1 << 20)?;
    for _ in 0..SIZE / chunk.len() {
        writer.write_all(&chunk).await?;
    }
    writer.finish().await?;
    println!("wrote {} MiB to {}", SIZE >> 20, path.display());

    for ahead in [0, 1, 4, 16] {
        // Three runs each; the device is not always equally quick.
        let mut times = Vec::new();
        let mut sum = 0;
        for _ in 0..3 {
            let (total, time) = scan(&file, ahead).await?;
            sum = total;
            times.push(time);
        }
        times.sort();
        let megabytes = (SIZE >> 20) as f64 / times[1].as_secs_f64();
        println!(
            "{ahead:>2} buffers ahead: {:>7.1?} for the scan, {megabytes:>5.0} MiB/s (checksum {sum})",
            times[1]
        );
    }

    remove_file(&path).await?;
    Ok(())
}

fn main() -> starfish::Result<()> {
    let dir = std::env::args_os().nth(1).unwrap_or_else(|| ".".into());
    Runtime::new().shards(1).run(move || app(dir.into()))?
}
