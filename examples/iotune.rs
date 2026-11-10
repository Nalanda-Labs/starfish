//! Measures the device behind a directory and prints the figures to pass to
//! `Runtime::io_properties`.
//!
//! Run with `cargo run --release --example iotune -- /path/to/data/dir`.
//! It writes a 1 GiB scratch file there and takes about 20 seconds.

use starfish::iotune::{measure, Options};
use starfish::Runtime;

fn main() -> starfish::Result<()> {
    let Some(directory) = std::env::args_os().nth(1) else {
        eprintln!("usage: iotune <directory>");
        return Ok(());
    };
    let properties = Runtime::new().run(|| measure(directory, Options::default()))??;
    println!("{properties:#?}");
    Ok(())
}
