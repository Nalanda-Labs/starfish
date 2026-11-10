//! Splitting a computation over all shards.
//!
//! Counts the primes below a limit. Shard `i` of `n` checks the numbers
//! that leave remainder `i` when divided by `n`, and the counts are added
//! up at the end.
//!
//! Run with `cargo run --release --example parallel_primes`.

use std::time::Instant;

use starfish::{maybe_yield, shard_count, shard_id, submit_to_all, Runtime};

const LIMIT: u64 = 3_000_000;

fn is_prime(n: u64) -> bool {
    if n < 2 {
        return false;
    }
    let mut divisor = 2;
    while divisor * divisor <= n {
        if n.is_multiple_of(divisor) {
            return false;
        }
        divisor += 1;
    }
    true
}

/// Counts the primes in this shard's share of the numbers.
async fn count_share() -> starfish::Result<u64> {
    let shards = shard_count()? as u64;
    let mut n = shard_id()? as u64;
    let mut count = 0;
    while n < LIMIT {
        if is_prime(n) {
            count += 1;
        }
        n += shards;
        // Keep the shard responsive to anything else it has to do.
        maybe_yield().await;
    }
    Ok(count)
}

async fn app() -> starfish::Result<()> {
    let start = Instant::now();
    let mut total = 0;
    for count in submit_to_all(count_share).await? {
        total += count?;
    }
    println!(
        "{total} primes below {LIMIT}, found by {} shards in {:?}",
        shard_count()?,
        start.elapsed()
    );
    Ok(())
}

fn main() -> starfish::Result<()> {
    Runtime::new().run(app)?
}
