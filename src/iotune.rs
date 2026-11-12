//! Measures what a storage device can sustain, producing the
//! [`IoProperties`] that [`Runtime::io_properties`](crate::Runtime::io_properties)
//! takes.
//!
//! The measurement writes a scratch file in the directory given and loads
//! the device from every shard at once with direct I/O, bypassing the I/O
//! scheduler, in four phases: sequential writes and reads for bandwidth,
//! then small random reads and writes for IOPS. Run it once per device on
//! an otherwise idle machine and store the result.

use std::cell::Cell;
use std::io;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::{Duration, Instant};

use crate::fs::{remove_file, DmaFile};
use crate::ioqueue::{Direction, IoProperties};
use crate::{shard_id, spawn, submit_to_all};

/// How to run a measurement: how large a file to work in, how long each
/// phase lasts, how hard to load the device, and the request sizes.
///
/// The defaults suit a first measurement of an ordinary SSD. `Options` is
/// meant to be built from them:
///
/// ```
/// use starfish::iotune::Options;
/// use std::time::Duration;
///
/// let options = Options {
///     phase: Duration::from_secs(10),
///     ..Options::default()
/// };
/// assert_eq!(options.depth, 64);
/// ```
///
/// Shorter phases and smaller files finish sooner and measure worse: what a
/// device can do depends on what it was doing a moment before, and short
/// runs catch it at unrepresentative moments.
#[derive(Clone, Copy, Debug)]
pub struct Options {
    /// Size of the scratch file. It should be much larger than the
    /// device's volatile cache; the default is 1 GiB.
    pub file_size: u64,
    /// How long each of the four phases runs. Default 5 seconds.
    pub phase: Duration,
    /// Requests kept in flight, summed over all shards. Default 64.
    pub depth: usize,
    /// Size of the requests in the random phases. Default 4 KiB.
    pub random_size: usize,
    /// Size of the requests in the sequential phases. Default 1 MiB.
    pub sequential_size: usize,
}

impl Default for Options {
    /// A 1 GiB file, 5 seconds per phase, 64 requests in flight, 4 KiB random
    /// and 1 MiB sequential requests.
    fn default() -> Options {
        Options {
            file_size: 1 << 30,
            phase: Duration::from_secs(5),
            depth: 64,
            random_size: 4 << 10,
            sequential_size: 1 << 20,
        }
    }
}

/// The four parts of a measurement, in the order they give useful
/// figures: fill the file, read it in order, read it at random, write it at
/// random. Reads come before the random writes because a device that has
/// just taken a burst of small writes may be slow for a while afterwards.
#[derive(Clone, Copy)]
enum Phase {
    /// Sequential writes; also covers the whole region at least once, so
    /// that later reads hit written blocks.
    Fill,
    SequentialRead,
    RandomWrite,
    RandomRead,
}

/// The work of one shard, fixed before the first phase: the size of its
/// own stretch of the file, the two request sizes, how many requests it
/// keeps in flight, and how long a phase lasts.
#[derive(Clone, Copy)]
struct Plan {
    /// Bytes of the file each shard works in.
    region: u64,
    sequential: u64,
    random: u64,
    workers: usize,
    phase: Duration,
}

/// What one worker, or one shard, got done in a phase: how many requests,
/// and how many bytes they covered.
#[derive(Clone, Copy, Default)]
struct Tally {
    requests: u64,
    bytes: u64,
}

/// One shard's result for one phase, as rates. Shards' rates add up to
/// the device's.
#[derive(Clone, Copy, Default)]
struct Rates {
    requests_per_sec: u64,
    bytes_per_sec: u64,
}

/// A count over a duration, as a rate per second. A duration of zero is
/// taken as one nanosecond, so the result is defined, if meaningless.
fn per_second(count: u64, elapsed: Duration) -> u64 {
    let nanos = elapsed.as_nanos().max(1);
    let rate = u128::from(count)
        .saturating_mul(1_000_000_000)
        .checked_div(nanos)
        .unwrap_or(0);
    u64::try_from(rate).unwrap_or(u64::MAX)
}

/// `value` rounded up to a multiple of `to`.
fn round_up(value: u64, to: u64) -> u64 {
    value.checked_next_multiple_of(to.max(1)).unwrap_or(value)
}

/// `value` rounded down to a multiple of `to`.
fn round_down(value: u64, to: u64) -> u64 {
    value.saturating_sub(value.checked_rem(to.max(1)).unwrap_or(0))
}

/// xorshift64*: a pseudo-random number generator, cheap and random enough
/// to spread requests over a file. Not for anything that needs
/// unpredictability. Its state must not be zero.
struct Rng(u64);

impl Rng {
    /// The next number in the sequence.
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
}

/// Runs one phase on the current shard: starts the shard's workers, each
/// of which issues requests one after another until the phase's time is up,
/// and returns the shard's rates.
///
/// Each shard works in its own stretch of the file, so the shards do not
/// read or write the same blocks.
async fn run_phase(path: PathBuf, phase: Phase, plan: Plan) -> io::Result<Rates> {
    let shard = shard_id()? as u64;
    let base = shard.saturating_mul(plan.region);
    let file = Rc::new(DmaFile::open_raw(&path, libc::O_RDWR, 0).await?);

    let (direction, size, sequential) = match phase {
        Phase::Fill => (Direction::Write, plan.sequential, true),
        Phase::SequentialRead => (Direction::Read, plan.sequential, true),
        Phase::RandomWrite => (Direction::Write, plan.random, false),
        Phase::RandomRead => (Direction::Read, plan.random, false),
    };
    let slots = plan.region.checked_div(size).unwrap_or(0).max(1);
    // Sequential phases share one cursor, so the workers together walk the
    // region in order.
    let cursor = Rc::new(Cell::new(0u64));
    let must_cover = matches!(phase, Phase::Fill);
    let start = Instant::now();
    let deadline = start.checked_add(plan.phase).unwrap_or(start);

    let mut workers = Vec::with_capacity(plan.workers);
    for worker in 0..plan.workers {
        let file = file.clone();
        let cursor = cursor.clone();
        workers.push(spawn(async move {
            let seed = (start.elapsed().as_nanos() as u64)
                ^ shard.wrapping_shl(32)
                ^ (worker as u64).wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut rng = Rng(seed | 1);
            let size_bytes = usize::try_from(size).map_err(|_| io::ErrorKind::InvalidInput)?;
            let mut buf = file.alloc_buf(size_bytes)?;
            if direction == Direction::Write {
                // Not zeros: some devices and filesystems special-case them.
                buf.set_len(size_bytes);
                for chunk in buf.chunks_mut(8) {
                    let word = rng.next().to_le_bytes();
                    if let Some(source) = word.get(..chunk.len()) {
                        chunk.copy_from_slice(source);
                    }
                }
            }
            let mut tally = Tally::default();
            loop {
                let slot = if sequential {
                    let issued = cursor.get();
                    let covered = issued >= slots;
                    if Instant::now() >= deadline && (covered || !must_cover) {
                        break;
                    }
                    cursor.set(issued.saturating_add(1));
                    issued.checked_rem(slots).unwrap_or(0)
                } else {
                    if Instant::now() >= deadline {
                        break;
                    }
                    rng.next().checked_rem(slots).unwrap_or(0)
                };
                let offset = base.saturating_add(slot.saturating_mul(size));
                let (result, returned) = match direction {
                    Direction::Read => file.read_impl(buf, offset, false).await,
                    Direction::Write => file.write_impl(buf, offset, false).await,
                };
                buf = returned;
                if result? != size_bytes {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "short transfer during I/O measurement",
                    ));
                }
                tally.requests = tally.requests.saturating_add(1);
                tally.bytes = tally.bytes.saturating_add(size);
            }
            Ok::<Tally, io::Error>(tally)
        }));
    }

    let mut total = Tally::default();
    let mut failure = None;
    for worker in workers {
        match worker.await {
            Ok(Ok(tally)) => {
                total.requests = total.requests.saturating_add(tally.requests);
                total.bytes = total.bytes.saturating_add(tally.bytes);
            }
            Ok(Err(err)) => failure = Some(err),
            Err(err) => failure = Some(err.into()),
        }
    }
    if let Some(err) = failure {
        return Err(err);
    }
    if matches!(phase, Phase::Fill | Phase::RandomWrite) {
        // Count the time the device needs to actually persist the data.
        file.sync_all().await?;
    }
    let elapsed = start.elapsed();
    Ok(Rates {
        requests_per_sec: per_second(total.requests, elapsed),
        bytes_per_sec: per_second(total.bytes, elapsed),
    })
}

/// Runs one phase on every shard at once and adds up the rates.
async fn run_everywhere(path: &Path, phase: Phase, plan: Plan) -> io::Result<Rates> {
    let path = path.to_path_buf();
    let results = submit_to_all(move || run_phase(path.clone(), phase, plan)).await?;
    let mut total = Rates::default();
    for result in results {
        let rates = result?;
        total.requests_per_sec = total.requests_per_sec.saturating_add(rates.requests_per_sec);
        total.bytes_per_sec = total.bytes_per_sec.saturating_add(rates.bytes_per_sec);
    }
    Ok(total)
}

/// Runs the four phases in order and puts their figures together: the
/// sequential phases give the bandwidths and the random ones the request
/// rates.
async fn run_phases(path: &Path, plan: Plan) -> io::Result<IoProperties> {
    let write = run_everywhere(path, Phase::Fill, plan).await?;
    let read = run_everywhere(path, Phase::SequentialRead, plan).await?;
    // Both read phases come before the random writes. A device that has
    // just taken a burst of small writes may spend the next seconds
    // tidying up, and reads measured then come out many times too low.
    let random_read = run_everywhere(path, Phase::RandomRead, plan).await?;
    let random_write = run_everywhere(path, Phase::RandomWrite, plan).await?;
    Ok(IoProperties {
        read_iops: Some(random_read.requests_per_sec.max(1)),
        read_bytes_per_sec: Some(read.bytes_per_sec.max(1)),
        write_iops: Some(random_write.requests_per_sec.max(1)),
        write_bytes_per_sec: Some(write.bytes_per_sec.max(1)),
    })
}

/// Measures the device that holds `directory`.
///
/// Must be called inside a runtime; it uses every shard. It needs
/// `options.file_size` of free space in `directory` and removes its scratch
/// file when done. The filesystem must support direct I/O.
pub async fn measure(directory: impl AsRef<Path>, options: Options) -> io::Result<IoProperties> {
    let shards = crate::shard_count()? as u64;
    let path = directory
        .as_ref()
        .join(format!(".starfish-iotune-{}", std::process::id()));

    // Creating the file also tells us the device's block size.
    let block = {
        let file = DmaFile::create(&path).await?;
        file.disk_alignment().max(file.memory_alignment()) as u64
    };
    let random = round_up(options.random_size.max(1) as u64, block);
    let sequential = round_up(options.sequential_size.max(1) as u64, random);
    let per_shard = options.file_size.checked_div(shards.max(1)).unwrap_or(0);
    let plan = Plan {
        region: round_down(per_shard, sequential).max(sequential),
        sequential,
        random,
        workers: options
            .depth
            .checked_div(usize::try_from(shards).unwrap_or(1).max(1))
            .unwrap_or(1)
            .max(1),
        phase: options.phase,
    };

    let measured = run_phases(&path, plan).await;
    let removed = remove_file(&path).await;
    let properties = measured?;
    removed?;
    Ok(properties)
}
