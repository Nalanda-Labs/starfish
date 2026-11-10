//! Replacing a file so that a crash never leaves it half written.
//!
//! The new contents go to a temporary file, which is synced and then
//! renamed over the old one. A reader, or a restart after a crash, finds
//! either the old version or the new one, complete. Syncing the directory
//! afterwards makes the rename itself survive a crash.
//!
//! Run with `cargo run --example atomic_update -- <directory>`; without an
//! argument it works in a scratch directory under the system's temporary
//! directory, which it removes afterwards.

use std::path::{Path, PathBuf};

use starfish::fs::{create_dir_all, read_dir, remove_dir, remove_file, rename, sync_dir, File};
use starfish::Runtime;

async fn replace(directory: &Path, name: &str, contents: Vec<u8>) -> std::io::Result<()> {
    let target = directory.join(name);
    let temporary = directory.join(format!("{name}.tmp"));

    let file = File::create(&temporary).await?;
    let (result, _) = file.write_all_at(contents, 0).await;
    result?;
    // The data must be on the device before the name points to it.
    file.sync_data().await?;
    // `close` reports failures that a plain drop would swallow.
    file.close().await?;

    rename(&temporary, &target).await?;

    // A directory is a file of names; syncing it stores the rename.
    sync_dir(directory).await
}

async fn read(path: &Path) -> std::io::Result<String> {
    let file = File::open(path).await?;
    let size = file.size().await? as usize;
    let (result, contents) = file.read_at(Vec::with_capacity(size.max(1)), 0).await;
    result?;
    Ok(String::from_utf8_lossy(&contents).into_owned())
}

async fn app(directory: PathBuf, scratch: bool) -> starfish::Result<()> {
    create_dir_all(&directory).await?;
    let config = directory.join("config");

    replace(&directory, "config", b"version = 1\n".to_vec()).await?;
    println!("first write:  {:?}", read(&config).await?);
    replace(&directory, "config", b"version = 2\nlonger = true\n".to_vec()).await?;
    println!("after update: {:?}", read(&config).await?);

    let mut names: Vec<_> = read_dir(&directory)
        .await?
        .iter()
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    println!("{} holds {names:?}: no temporary file is left", directory.display());

    if scratch {
        remove_file(&config).await?;
        remove_dir(&directory).await?;
    }
    Ok(())
}

fn main() -> starfish::Result<()> {
    let (directory, scratch) = match std::env::args_os().nth(1) {
        Some(directory) => (PathBuf::from(directory), false),
        None => {
            let name = format!("starfish-example-{}-atomic", std::process::id());
            (std::env::temp_dir().join(name), true)
        }
    };
    Runtime::new().shards(1).run(move || app(directory, scratch))?
}
