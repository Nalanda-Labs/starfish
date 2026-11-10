//! Many tasks writing to one file through a single writer task.
//!
//! The file has no cursor, so somebody has to decide where each line goes.
//! Here one task owns the file and the offset, and everyone else sends it
//! lines over a channel. The channel is bounded: if the disk is slow, the
//! tasks that log wait instead of piling lines up in memory.
//!
//! Run with `cargo run --example log_writer`.

use std::path::PathBuf;

use starfish::fs::{remove_file, File};
use starfish::sync::{channel, Receiver};
use starfish::{spawn, yield_now, Runtime};

/// Appends lines until every sender is gone. Returns the bytes written.
async fn write_log(path: PathBuf, mut lines: Receiver<String>) -> std::io::Result<u64> {
    let file = File::create(&path).await?;
    let mut offset = 0;
    while let Some(line) = lines.recv().await {
        let length = line.len() as u64;
        let (result, _) = file.write_all_at(line, offset).await;
        result?;
        offset += length;
    }
    file.sync_all().await?;
    Ok(offset)
}

async fn app() -> starfish::Result<()> {
    let path = std::env::temp_dir().join(format!("starfish-example-{}.log", std::process::id()));
    let (log, lines) = channel(16);
    let writer = spawn(write_log(path.clone(), lines));

    let workers: Vec<_> = (0..4)
        .map(|worker| {
            let log = log.clone();
            spawn(async move {
                for step in 0..25 {
                    if log.send(format!("worker {worker} finished step {step}\n")).await.is_err() {
                        return;
                    }
                    yield_now().await;
                }
            })
        })
        .collect();
    for worker in workers {
        worker.await?;
    }
    // With the last sender gone, the writer finishes and syncs.
    drop(log);
    let bytes = writer.await??;

    let file = File::open(&path).await?;
    let (result, contents) = file.read_at(Vec::with_capacity(bytes as usize), 0).await;
    result?;
    let text = String::from_utf8_lossy(&contents);
    println!("{bytes} bytes, {} lines in {}", text.lines().count(), path.display());
    println!("first: {:?}", text.lines().next().unwrap_or_default());
    println!("last:  {:?}", text.lines().last().unwrap_or_default());

    remove_file(&path).await?;
    Ok(())
}

fn main() -> starfish::Result<()> {
    Runtime::new().shards(1).run(app)?
}
