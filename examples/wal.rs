//! A write-ahead log on direct I/O.
//!
//! Records of any size are appended through a `DmaStreamWriter`, which
//! turns them into the whole aligned blocks that direct I/O demands. Each
//! record carries its length and a checksum, so that a reader can tell
//! where the valid data ends without trusting the file's length: after a
//! crash the file ends in padding, or in a record that was cut short.
//!
//! Run with `cargo run --example wal -- <directory>`; without an argument
//! it uses the current directory. The filesystem must support `O_DIRECT`.
//! The log file is removed afterwards.

use starfish::fs::{remove_file, DmaFile, DmaStreamReader, DmaStreamWriter};
use starfish::Runtime;

fn checksum(data: &[u8]) -> u32 {
    // FNV-1a: enough to recognise a record that was cut short.
    data.iter().fold(0x811c_9dc5, |hash, byte| (hash ^ u32::from(*byte)).wrapping_mul(0x0100_0193))
}

/// One record: length, checksum, payload.
async fn append(log: &mut DmaStreamWriter<&DmaFile>, payload: &[u8]) -> std::io::Result<()> {
    log.write_all(&(payload.len() as u32).to_le_bytes()).await?;
    log.write_all(&checksum(payload).to_le_bytes()).await?;
    log.write_all(payload).await
}

/// Reads records until the data stops making sense.
async fn replay(file: &DmaFile) -> std::io::Result<Vec<Vec<u8>>> {
    let mut log = DmaStreamReader::new(file, 256 * 1024)?;
    let mut records = Vec::new();
    loop {
        let mut header = [0u8; 8];
        if log.read_exact(&mut header).await.is_err() {
            break; // the end of the file
        }
        let length = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
        let expected = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
        // Padding reads as a record of length zero, which is never written.
        if length == 0 || length > 1 << 20 {
            break;
        }
        let mut payload = vec![0u8; length];
        if log.read_exact(&mut payload).await.is_err() || checksum(&payload) != expected {
            break; // cut short, or never finished
        }
        records.push(payload);
    }
    Ok(records)
}

async fn app(dir: std::path::PathBuf) -> starfish::Result<()> {
    let path = dir.join(format!(".starfish-example-{}-wal", std::process::id()));
    let file = DmaFile::create(&path).await?;
    // Space reserved in advance: appends then never wait for the
    // filesystem to find room.
    file.allocate(0, 16 << 20).await?;

    let mut log = DmaStreamWriter::new(&file, 256 * 1024)?;
    for batch in 0..5 {
        for n in 0..200 {
            let record = format!("batch {batch}, entry {n}: {}", "payload ".repeat(n % 9));
            append(&mut log, record.as_bytes()).await?;
        }
        // The point at which a database would acknowledge the batch.
        log.sync().await?;
    }
    println!("wrote 1000 records, {} bytes, in 5 synced batches", log.position());
    println!("the file is still {} bytes long: the preallocated size", file.size().await?);

    // What a restart after a crash would see: the data, then zeros.
    let recovered = replay(&file).await?;
    println!("replay found {} records before the padding", recovered.len());
    if let Some(last) = recovered.last() {
        println!("the last one: {:?}", String::from_utf8_lossy(last));
    }

    // A clean shutdown gives the file its true length.
    let written = log.position();
    log.finish().await?;
    println!("after finish the file is {} bytes long (wrote {written})", file.size().await?);

    remove_file(&path).await?;
    Ok(())
}

fn main() -> starfish::Result<()> {
    let dir = std::env::args_os().nth(1).unwrap_or_else(|| ".".into());
    Runtime::new().shards(1).run(move || app(dir.into()))?
}
