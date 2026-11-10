//! Reacting to signals.
//!
//! Run with `cargo run --example signals`. It prints its process id; send
//! it signals from another terminal (`kill -USR1 <pid>`), or press Ctrl-C.
//! The first Ctrl-C asks for confirmation, the second one quits, and
//! `SIGTERM` quits at once.

use starfish::signal::Signals;
use starfish::Runtime;

async fn app() -> starfish::Result<()> {
    // From here on these signals no longer do what they do by default: a
    // Ctrl-C reaches this listener instead of ending the process.
    let mut signals = Signals::new(&[libc::SIGINT, libc::SIGTERM, libc::SIGUSR1])?;
    println!("process {}: waiting for SIGUSR1, SIGINT (Ctrl-C) or SIGTERM", std::process::id());

    let mut interrupts = 0;
    loop {
        match signals.next().await {
            libc::SIGUSR1 => println!("SIGUSR1: a program might reload its configuration here"),
            libc::SIGINT => {
                interrupts += 1;
                if interrupts == 2 {
                    println!("SIGINT again: quitting");
                    return Ok(());
                }
                println!("SIGINT: press Ctrl-C once more to quit");
            }
            signal => {
                println!("signal {signal}: quitting");
                return Ok(());
            }
        }
    }
}

fn main() -> starfish::Result<()> {
    Runtime::new().shards(1).run(app)?
}
