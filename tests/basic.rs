use std::cell::Cell;

// The whole suite runs on the per-shard allocator.
#[global_allocator]
static GLOBAL: starfish::alloc::ShardAlloc = starfish::alloc::ShardAlloc;

use std::net::SocketAddr;
use std::rc::Rc;
use std::time::{Duration, Instant};

use starfish::fs::{AlignedBuf, DmaFile, File, DMA_ALIGNMENT};
use starfish::net::{BufReader, BufWriter, TcpListener, TcpStream};
use starfish::signal::Signals;
use starfish::sync::{channel, shared_spsc, Gate, Semaphore};
use starfish::{shard_count, shard_id, sleep, spawn, submit_to, submit_to_all, timeout, yield_now};
use starfish::{maybe_yield, Error, Runtime, SchedulingGroup, Sharded};
use std::cell::RefCell;

/// Tests run in parallel in one process. Unpinned, so that their shards do
/// not all pile onto the same few CPUs and distort each other's timing.
fn rt(shards: usize) -> Runtime {
    Runtime::new().shards(shards).pin_threads(false)
}

#[test]
fn spawn_and_join() {
    let out = rt(1)
        .run(|| async {
            let local = Rc::new(Cell::new(0));
            let handles: Vec<_> = (0..10)
                .map(|i| {
                    let local = local.clone();
                    spawn(async move {
                        yield_now().await;
                        local.set(local.get() + i);
                        i * 2
                    })
                })
                .collect();
            let mut sum = 0;
            for handle in handles {
                sum += handle.await.unwrap();
            }
            (sum, local.get())
        })
        .unwrap();
    assert_eq!(out, (90, 45));
}

#[test]
fn sleep_wakes_in_deadline_order() {
    let order = rt(1)
        .run(|| async {
            let start = Instant::now();
            let slow = spawn(async move {
                sleep(Duration::from_millis(60)).await;
                ("slow", start.elapsed())
            });
            let fast = spawn(async move {
                sleep(Duration::from_millis(20)).await;
                ("fast", start.elapsed())
            });
            let fast = fast.await.unwrap();
            let slow = slow.await.unwrap();
            vec![fast, slow]
        })
        .unwrap();
    assert!(order[0].1 >= Duration::from_millis(20));
    assert!(order[1].1 >= Duration::from_millis(60));
    assert!(order[0].1 < order[1].1);
}

#[test]
fn submit_to_runs_on_the_target_shard() {
    let ids = rt(4)
        .run(|| async {
            assert_eq!(shard_id().unwrap(), 0);
            assert_eq!(shard_count().unwrap(), 4);
            let mut ids = Vec::new();
            for shard in 0..4 {
                ids.push(submit_to(shard, || async { shard_id().unwrap() }).await.unwrap());
            }
            // A shard can call back into the shard that called it.
            let nested = submit_to(2, || async { submit_to(0, || async { shard_id() }).await }).await;
            assert_eq!(nested.unwrap().unwrap().unwrap(), 0);
            ids
        })
        .unwrap();
    assert_eq!(ids, vec![0, 1, 2, 3]);
}

#[test]
fn many_cross_shard_calls() {
    let total = rt(3)
        .run(|| async {
            let handles: Vec<_> = (0..3000u64)
                .map(|i| spawn(submit_to((i % 3) as usize, move || async move { i })))
                .collect();
            let mut total = 0;
            for handle in handles {
                total += handle.await.unwrap().unwrap();
            }
            total
        })
        .unwrap();
    assert_eq!(total, 3000 * 2999 / 2);
}

#[test]
fn sharded_service() {
    let (sum, on_two, ids) = rt(4)
        .run(|| async {
            // Cell is neither Send nor Sync; each shard owns its own.
            let counters = Sharded::start(|shard| Cell::new(shard as u64 * 100)).await.unwrap();
            counters.invoke_on_all(|c| async move { c.set(c.get() + 1) }).await.unwrap();
            counters.invoke_on(2, |c| async move { c.set(c.get() + 5) }).await.unwrap();
            let sum = counters.map_reduce(|c| async move { c.get() }, 0, |a, b| a + b).await.unwrap();
            let on_two = counters.invoke_on(2, |c| async move { c.get() }).await.unwrap();
            let ids = counters.invoke_on_all(|_| async { shard_id().unwrap() }).await.unwrap();
            assert_eq!(counters.local().unwrap().get(), 1);
            counters.stop().await.unwrap();
            assert!(matches!(counters.local(), Err(Error::ServiceStopped)));
            (sum, on_two, ids)
        })
        .unwrap();
    assert_eq!(sum, 100 + 200 + 300 + 4 + 5);
    assert_eq!(on_two, 206);
    assert_eq!(ids, vec![0, 1, 2, 3]);
}

async fn echo(stream: TcpStream) {
    let mut buf = Vec::with_capacity(4096);
    loop {
        let (result, returned) = stream.read(buf).await;
        match result {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
        let (result, returned) = stream.write_all(returned).await;
        if result.is_err() {
            return;
        }
        buf = returned;
    }
}

#[test]
fn tcp_echo_on_every_shard() {
    let served_by = rt(3)
        .run(|| async {
            // Pick a free port on shard 0, then bind it on every shard.
            let first = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
            let addr: SocketAddr = first.local_addr().unwrap();
            drop(first);

            let hits = Sharded::start(|_| Cell::new(0u32)).await.unwrap();
            let bound = submit_to_all(move || async move {
                let listener = TcpListener::bind(addr)?;
                spawn(async move {
                    while let Ok(stream) = listener.accept().await {
                        let hits = hits.local().unwrap();
                        hits.set(hits.get() + 1);
                        spawn(echo(stream));
                    }
                });
                Ok::<(), std::io::Error>(())
            })
            .await
            .unwrap();
            for result in bound {
                result.unwrap();
            }

            let clients: Vec<_> = (0..24u32)
                .map(|i| {
                    spawn(async move {
                        let stream = TcpStream::connect(addr).await.unwrap();
                        stream.set_nodelay(true).unwrap();
                        let message = format!("hello from client {i}").repeat(50).into_bytes();
                        let (result, sent) = stream.write_all(message).await;
                        result.unwrap();
                        let mut received = Vec::new();
                        let mut buf = Vec::with_capacity(512);
                        while received.len() < sent.len() {
                            let (result, returned) = stream.read(buf).await;
                            assert!(result.unwrap() > 0, "server closed early");
                            received.extend_from_slice(&returned);
                            buf = returned;
                        }
                        assert_eq!(received, sent);
                    })
                })
                .collect();
            for client in clients {
                client.await.unwrap();
            }
            hits.invoke_on_all(|h| async move { h.get() }).await.unwrap()
            // The accept loops are still parked in the kernel here; shutdown
            // has to cancel them.
        })
        .unwrap();
    assert_eq!(served_by.iter().sum::<u32>(), 24);
}

#[test]
fn timeout_cancels_pending_io() {
    rt(2)
        .run(|| async {
            let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
            let addr = listener.local_addr().unwrap();

            // Nobody connects: the accept is cancelled in the kernel.
            let start = Instant::now();
            let result = timeout(Duration::from_millis(30), listener.accept()).await;
            assert!(result.is_err());
            assert!(start.elapsed() >= Duration::from_millis(30));

            // The listener still works afterwards.
            let client = spawn(async move { TcpStream::connect(addr).await.unwrap() });
            let server = timeout(Duration::from_secs(5), listener.accept()).await.unwrap().unwrap();
            let client = client.await.unwrap();
            assert_eq!(server.peer_addr().unwrap(), client.local_addr().unwrap());

            // A read with no data is cancelled too, and the buffer is not lost.
            let read = timeout(Duration::from_millis(30), server.read(Vec::with_capacity(64))).await;
            assert!(read.is_err());
            let (result, _) = client.write_all(b"late".to_vec()).await;
            result.unwrap();
            let (result, buf) = server.read(Vec::with_capacity(64)).await;
            assert_eq!(result.unwrap(), 4);
            assert_eq!(buf, b"late");
        })
        .unwrap();
}

#[test]
fn file_round_trip_on_another_shard() {
    let path = scratch("round-trip.bin");
    let path_in = path.clone();
    let read_back = rt(2)
        .run(move || async move {
            let data: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
            let expected = data.clone();
            let path = path_in.clone();
            submit_to(1, move || async move {
                let file = File::create(&path).await?;
                let (result, _) = file.write_all_at(data, 0).await;
                result?;
                file.sync_all().await
            })
            .await
            .unwrap()
            .unwrap();

            let file = File::open(&path_in).await.unwrap();
            let mut contents = Vec::new();
            let mut buf = Vec::with_capacity(64 * 1024);
            loop {
                let (result, returned) = file.read_at(buf, contents.len() as u64).await;
                if result.unwrap() == 0 {
                    break;
                }
                contents.extend_from_slice(&returned);
                buf = returned;
            }
            assert!(File::open("/definitely/not/here").await.is_err());
            contents == expected
        })
        .unwrap();
    let _ = std::fs::remove_file(&path);
    assert!(read_back);
}

#[test]
fn foreign_thread_can_wake_a_task() {
    let value = rt(1)
        .run(|| async {
            let (tx, rx) = std::sync::mpsc::channel::<std::task::Waker>();
            let flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let setter = flag.clone();
            let thread = std::thread::spawn(move || {
                let waker = rx.recv().unwrap();
                std::thread::sleep(Duration::from_millis(20));
                setter.store(true, std::sync::atomic::Ordering::SeqCst);
                waker.wake();
            });
            let mut sent = false;
            std::future::poll_fn(|cx| {
                if flag.load(std::sync::atomic::Ordering::SeqCst) {
                    return std::task::Poll::Ready(());
                }
                if !sent {
                    tx.send(cx.waker().clone()).unwrap();
                    sent = true;
                }
                std::task::Poll::Pending
            })
            .await;
            thread.join().unwrap();
            7
        })
        .unwrap();
    assert_eq!(value, 7);
}

#[test]
fn task_panic_is_contained() {
    let (panicked, after, pending) = rt(1)
        .run(|| async {
            let bad = spawn(async {
                yield_now().await;
                if shard_id().is_ok() {
                    panic!("boom in a task");
                }
                1
            });
            let panicked = bad.await;
            // The shard keeps running other tasks afterwards.
            let after = spawn(async { 2 }).await;
            // A task that is still pending when the runtime stops is dropped.
            let pending = spawn(std::future::pending::<()>());
            (panicked, after, pending)
        })
        .unwrap();
    assert!(matches!(panicked, Err(Error::Panicked { .. })));
    assert_eq!(after.unwrap(), 2);
    drop(pending);
}

#[test]
fn remote_panic_is_reported_and_the_shard_survives() {
    rt(3)
        .run(|| async {
            let in_future = submit_to(1, || async {
                yield_now().await;
                if shard_id().is_ok() {
                    panic!("boom on shard 1");
                }
                5
            })
            .await;
            assert!(matches!(in_future, Err(Error::Panicked { .. })));

            #[allow(unreachable_code)]
            let in_closure = submit_to(2, || {
                panic!("boom before the future exists");
                async { 5 }
            })
            .await;
            assert!(matches!(in_closure, Err(Error::Panicked { .. })));

            // Both shards still answer.
            assert_eq!(submit_to(1, || async { shard_id().unwrap() }).await.unwrap(), 1);
            assert_eq!(submit_to(2, || async { shard_id().unwrap() }).await.unwrap(), 2);

            // A panicking factory fails the start and leaves nothing behind.
            let service = Sharded::start(|shard| {
                if shard == 1 {
                    panic!("boom in a factory");
                }
                Cell::new(0u8)
            })
            .await;
            assert!(matches!(service, Err(Error::Panicked { .. })));

            // A value whose Drop panics does not take the shard down either.
            struct Bomb;
            impl Drop for Bomb {
                fn drop(&mut self) {
                    panic!("boom in a destructor");
                }
            }
            let bomb = Bomb;
            let held = spawn(async move {
                let _bomb = bomb;
                std::future::pending::<()>().await;
            });
            drop(held);
            yield_now().await;
        })
        .unwrap();
}

#[test]
fn main_panic_becomes_an_error() {
    let in_future: Result<(), Error> = rt(2).run(|| async {
        yield_now().await;
        panic!("boom in main");
    });
    assert!(matches!(in_future, Err(Error::Panicked { .. })));

    #[allow(unreachable_code)]
    let in_closure = rt(2).run(|| {
        panic!("boom before main's future exists");
        async {}
    });
    assert!(matches!(in_closure, Err(Error::Panicked { .. })));

    // The thread is usable for a fresh runtime afterwards.
    assert_eq!(rt(2).run(|| async { 3 }).unwrap(), 3);
}

#[test]
fn misuse_returns_errors() {
    // Outside a runtime.
    assert!(matches!(shard_id(), Err(Error::NoRuntime)));
    assert!(matches!(shard_count(), Err(Error::NoRuntime)));
    let handle = spawn(async { 1 });
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(waker);
    let mut handle = std::pin::pin!(handle);
    assert!(matches!(
        std::future::Future::poll(handle.as_mut(), &mut cx),
        std::task::Poll::Ready(Err(Error::NoRuntime))
    ));
    let mut accept = std::pin::pin!(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        listener.accept().await.map(|_| ())
    });
    assert!(matches!(
        std::future::Future::poll(accept.as_mut(), &mut cx),
        std::task::Poll::Ready(Err(_))
    ));

    rt(2)
        .run(|| async {
            let missing = submit_to(9, || async {}).await;
            assert!(matches!(missing, Err(Error::InvalidShard { shard: 9, count: 2 })));
            // A runtime cannot be started from inside a runtime.
            let nested = Runtime::new().shards(1).run(|| async {});
            assert!(matches!(nested, Err(Error::AlreadyRunning)));
            // An absurd duration sleeps "forever" instead of overflowing.
            let forever = timeout(Duration::from_millis(10), sleep(Duration::MAX)).await;
            assert!(forever.is_err());
        })
        .unwrap();
}

/// A file path on a real filesystem (direct I/O needs one).
fn scratch(name: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("{}-{name}", std::process::id()))
}

#[test]
fn scheduling_groups_are_inherited() {
    rt(2)
        .run(|| async {
            assert_eq!(SchedulingGroup::current().unwrap(), SchedulingGroup::DEFAULT);
            assert_eq!(SchedulingGroup::DEFAULT.cpu_shares().unwrap(), 1000);
            assert_eq!(SchedulingGroup::DEFAULT.name().unwrap(), "main");

            let group = SchedulingGroup::create("background", 50).unwrap();
            assert_eq!(group.name().unwrap(), "background");
            assert_eq!((group.cpu_shares().unwrap(), group.io_shares().unwrap()), (50, 50));
            group.set_cpu_shares(70).unwrap();
            group.set_io_shares(900).unwrap();
            assert_eq!((group.cpu_shares().unwrap(), group.io_shares().unwrap()), (70, 900));
            let split = SchedulingGroup::create_with("split", 10, 20_000).unwrap();
            assert_eq!((split.cpu_shares().unwrap(), split.io_shares().unwrap()), (10, 10_000));

            let direct = group.spawn(async { SchedulingGroup::current() }).await;
            assert_eq!(direct.unwrap().unwrap(), group);
            let nested = group.spawn(async { spawn(async { SchedulingGroup::current() }).await }).await;
            assert_eq!(nested.unwrap().unwrap().unwrap(), group);
            let remote = group
                .spawn(async { submit_to(1, || async { SchedulingGroup::current() }).await })
                .await;
            assert_eq!(remote.unwrap().unwrap().unwrap(), group);
            // The caller itself is still in the default group.
            assert_eq!(SchedulingGroup::current().unwrap(), SchedulingGroup::DEFAULT);

            // There is no limit on the number of groups, and one created
            // late works on a shard that has never heard of it.
            let mut last = group;
            for i in 0..300 {
                last = SchedulingGroup::create(&format!("g{i}"), 100).unwrap();
            }
            assert_eq!(last.name().unwrap(), "g299");
            let remote = last
                .spawn(async { submit_to(1, || async { SchedulingGroup::current() }).await })
                .await;
            assert_eq!(remote.unwrap().unwrap().unwrap(), last);
        })
        .unwrap();
    assert!(matches!(SchedulingGroup::create("outside", 1), Err(Error::NoRuntime)));
}

#[test]
fn cpu_time_follows_shares() {
    let (low, high) = rt(1)
        .run(|| async {
            let low = SchedulingGroup::create("low", 100).unwrap();
            let high = SchedulingGroup::create("high", 300).unwrap();
            let stop = Rc::new(Cell::new(false));
            let spin = |stop: Rc<Cell<bool>>| async move {
                let mut iterations = 0u64;
                while !stop.get() {
                    iterations += 1;
                    maybe_yield().await;
                }
                iterations
            };
            let low = low.spawn(spin(stop.clone()));
            let high = high.spawn(spin(stop.clone()));
            // The sleeper is in the default group and still gets to run.
            let start = Instant::now();
            sleep(Duration::from_millis(300)).await;
            assert!(start.elapsed() < Duration::from_millis(400), "sleeper was starved");
            stop.set(true);
            (low.await.unwrap(), high.await.unwrap())
        })
        .unwrap();
    let ratio = high as f64 / low as f64;
    assert!((2.0..4.5).contains(&ratio), "expected about 3x, got {ratio:.2} ({high} vs {low})");
}

#[test]
fn disk_queue_follows_shares() {
    let path = scratch("disk-queue.bin");
    let path_in = path.clone();
    let order = rt(1)
        .io_depth(1)
        .run(move || async move {
            // Equal CPU shares: only the I/O shares differ.
            let low = SchedulingGroup::create_with("low", 500, 100).unwrap();
            let high = SchedulingGroup::create_with("high", 500, 400).unwrap();
            let file = Rc::new(File::create(&path_in).await.unwrap());
            let order = Rc::new(RefCell::new(Vec::new()));
            let mut handles = Vec::new();
            for (tag, group) in [(b'l', low), (b'h', high)] {
                for i in 0..150u64 {
                    let file = file.clone();
                    let order = order.clone();
                    handles.push(group.spawn(async move {
                        let (result, _) = file.write_all_at(vec![tag; 4096], i * 4096).await;
                        result.unwrap();
                        order.borrow_mut().push(tag);
                    }));
                }
            }
            for handle in handles {
                handle.await.unwrap();
            }
            let order = order.borrow().clone();
            order
        })
        .unwrap();
    let _ = std::fs::remove_file(&path);
    assert_eq!(order.len(), 300);
    // While both groups have requests waiting, `high` gets 4 of every 5.
    let high_first = order[..100].iter().filter(|&&tag| tag == b'h').count();
    assert!((65..=95).contains(&high_first), "high got {high_first} of the first 100");
}

#[test]
fn direct_io_round_trip() {
    let path = scratch("dma.bin");
    let path_in = path.clone();
    rt(1)
        .run(move || async move {
            let data: Vec<u8> = (0..10_000u32).map(|i| (i % 253) as u8).collect();
            let buf = AlignedBuf::padded_from(&data).unwrap();
            assert_eq!(buf.len(), 3 * DMA_ALIGNMENT);

            let file = DmaFile::create(&path_in).await.unwrap();
            // The device's own requirements, as reported by the kernel.
            let (memory, disk) = (file.memory_alignment(), file.disk_alignment());
            assert!(memory.is_power_of_two() && disk.is_power_of_two());
            assert!(memory <= DMA_ALIGNMENT && disk <= DMA_ALIGNMENT, "{memory} {disk}");
            let exact = file.alloc_buf(1).unwrap();
            assert_eq!(exact.capacity(), memory.max(disk));
            assert_eq!(exact.alignment(), memory.max(disk));
            assert!(AlignedBuf::with_alignment(100, 3).is_err());
            let (result, buf) = file.write_at(buf, 0).await;
            assert_eq!(result.unwrap(), 3 * DMA_ALIGNMENT);
            file.sync_all().await.unwrap();

            // Misuse is rejected before it reaches the kernel.
            let (result, mut buf) = file.write_at(buf, 100).await;
            assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);
            buf.set_len(disk + 1);
            let (result, _) = file.write_at(buf, 0).await;
            assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);

            let file = DmaFile::open(&path_in).await.unwrap();
            let (result, buf) = file.read_at(AlignedBuf::new(16 * 1024).unwrap(), 0).await;
            assert_eq!(result.unwrap(), 3 * DMA_ALIGNMENT);
            assert_eq!(&buf[..data.len()], &data[..]);
            assert!(buf[data.len()..].iter().all(|&byte| byte == 0));

            let (result, buf) = file.read_at(buf, 2 * DMA_ALIGNMENT as u64).await;
            assert_eq!(result.unwrap(), DMA_ALIGNMENT);
            assert_eq!(&buf[..], &AlignedBuf::padded_from(&data).unwrap()[2 * DMA_ALIGNMENT..]);
            let (result, _) = file.read_at(buf, 3 * DMA_ALIGNMENT as u64).await;
            assert_eq!(result.unwrap(), 0);
        })
        .unwrap();
    let _ = std::fs::remove_file(&path);
}

#[test]
fn dropped_disk_requests_are_withdrawn() {
    let path = scratch("withdrawn.bin");
    let path_in = path.clone();
    rt(1)
        .io_depth(1)
        .run(move || async move {
            let file = Rc::new(File::create(&path_in).await.unwrap());
            // Far more requests than the queue depth, most dropped while
            // still waiting in the scheduler.
            let handles: Vec<_> = (0..200u64)
                .map(|i| {
                    let file = file.clone();
                    spawn(async move {
                        let write = file.write_all_at(vec![7; 4096], i * 4096);
                        timeout(Duration::from_micros(i * 5), write).await.is_ok()
                    })
                })
                .collect();
            for handle in handles {
                handle.await.unwrap();
            }
            // The queue still works afterwards.
            let (result, _) = file.write_all_at(vec![9; 4096], 0).await;
            result.unwrap();
            let (result, buf) = file.read_at(Vec::with_capacity(4096), 0).await;
            assert_eq!(result.unwrap(), 4096);
            assert!(buf.iter().all(|&byte| byte == 9));
        })
        .unwrap();
    let _ = std::fs::remove_file(&path);
}

#[test]
fn device_capacity_is_shared_by_all_shards() {
    const MIB: u64 = 1 << 20;
    let directory = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"));
    let properties = starfish::IoProperties {
        write_bytes_per_sec: Some(4 * MIB),
        ..Default::default()
    };
    let elapsed = rt(2)
        .io_properties(&directory, properties)
        .run(|| async {
            let start = Instant::now();
            // Each of the two shards writes 1 MiB: 2 MiB against a device
            // declared to take 4 MiB per second.
            let written = submit_to_all(|| async {
                let path = scratch(&format!("capacity-{}.bin", shard_id().unwrap()));
                let file = Rc::new(File::create(&path).await.unwrap());
                let writes: Vec<_> = (0..16u64)
                    .map(|i| {
                        let file = file.clone();
                        spawn(async move {
                            let (result, _) = file.write_all_at(vec![1; 64 << 10], i * (64 << 10)).await;
                            result.unwrap();
                        })
                    })
                    .collect();
                for write in writes {
                    write.await.unwrap();
                }
                // Read rates were not declared, so a read costs next to
                // nothing; it only waits for the device to catch up with
                // the writes already admitted.
                let (result, _) = file.read_at(Vec::with_capacity(64 << 10), 0).await;
                assert_eq!(result.unwrap(), 64 << 10);
                starfish::fs::remove_file(&path).await.unwrap();
            })
            .await
            .unwrap();
            assert_eq!(written.len(), 2);
            start.elapsed()
        })
        .unwrap();
    // 2 MiB at 4 MiB/s is half a second, whichever shard does the writing.
    assert!(elapsed >= Duration::from_millis(400), "finished in {elapsed:?}");
    assert!(elapsed < Duration::from_millis(1500), "took {elapsed:?}");
}

#[test]
fn iotune_measures_all_four_rates() {
    let directory = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"));
    let properties = rt(2)
        .run(move || async move {
            let options = starfish::iotune::Options {
                file_size: 16 << 20,
                phase: Duration::from_millis(60),
                depth: 8,
                ..Default::default()
            };
            starfish::iotune::measure(&directory, options).await
        })
        .unwrap()
        .unwrap();
    for rate in [
        properties.read_iops,
        properties.read_bytes_per_sec,
        properties.write_iops,
        properties.write_bytes_per_sec,
    ] {
        assert!(rate.unwrap() > 1, "{properties:?}");
    }
    // Sequential megabyte requests move more data than random 4 KiB ones.
    assert!(properties.read_bytes_per_sec.unwrap() > properties.read_iops.unwrap() * 4096 / 4);
    let leftovers = std::fs::read_dir(env!("CARGO_TARGET_TMPDIR"))
        .unwrap()
        .filter(|entry| entry.as_ref().unwrap().file_name().to_string_lossy().contains("iotune"))
        .count();
    assert_eq!(leftovers, 0);
}

#[test]
fn registered_buffers_on_sockets_and_files() {
    use starfish::buf::FixedBuf;

    let path = scratch("fixed.bin");
    let path_in = path.clone();
    rt(2)
        .fixed_buffers(4, 10_000)
        .run(move || async move {
            // Sizes are rounded up to whole pages.
            let buf = FixedBuf::try_acquire().unwrap();
            assert_eq!((buf.len(), buf.capacity()), (0, 12_288));
            drop(buf);

            // An echo server and a client that both use registered memory:
            // receives go into it, sends are zero-copy from it.
            let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
            let addr = listener.local_addr().unwrap();
            let server = spawn(async move {
                let stream = listener.accept().await.unwrap();
                let mut buf = FixedBuf::acquire().await.unwrap();
                let mut echoed = 0;
                loop {
                    let (result, returned) = stream.read(buf).await;
                    let n = result.unwrap();
                    if n == 0 {
                        return echoed;
                    }
                    let (result, returned) = stream.write_all(returned).await;
                    result.unwrap();
                    echoed += n;
                    buf = returned;
                }
            });
            let client = TcpStream::connect(addr).await.unwrap();
            let mut expected = Vec::new();
            let mut received = Vec::new();
            let mut buf = FixedBuf::acquire().await.unwrap();
            for round in 0..40u32 {
                let message: Vec<u8> = (0..3000 + round * 100).map(|i| (i + round) as u8).collect();
                assert_eq!(buf.fill_from(&message), message.len());
                let (result, returned) = client.write_all(buf).await;
                result.unwrap();
                expected.extend_from_slice(&message);
                buf = returned;
                while received.len() < expected.len() {
                    let (result, returned) = client.read(buf).await;
                    assert!(result.unwrap() > 0);
                    received.extend_from_slice(&returned);
                    buf = returned;
                }
            }
            assert_eq!(received, expected);
            client.shutdown(std::net::Shutdown::Write).unwrap();
            assert_eq!(server.await.unwrap(), expected.len());

            // The same buffer against files, buffered and direct.
            let file = File::create(&path_in).await.unwrap();
            buf.fill_from(&[5u8; 8192]);
            let (result, buf) = file.write_all_at(buf, 0).await;
            result.unwrap();
            let (result, buf) = file.read_at(buf, 4096).await;
            assert_eq!(result.unwrap(), 4096);
            assert!(buf.iter().all(|&byte| byte == 5));
            drop(file);
            let direct = DmaFile::open(&path_in).await.unwrap();
            let (result, buf) = direct.read_at(buf, 0).await;
            assert_eq!(result.unwrap(), 8192);
            assert!(buf.iter().all(|&byte| byte == 5));
            drop(buf);

            // A pool is per shard and bounded; `acquire` waits its turn.
            let held: Vec<FixedBuf> = std::iter::from_fn(FixedBuf::try_acquire).collect();
            assert_eq!(held.len(), 4);
            assert!(timeout(Duration::from_millis(20), FixedBuf::acquire()).await.is_err());
            let waiter = spawn(async { FixedBuf::acquire().await.map(|buf| buf.capacity()) });
            yield_now().await;
            drop(held);
            assert_eq!(waiter.await.unwrap().unwrap(), 12_288);
            let elsewhere = submit_to(1, || async {
                // Collected, not counted: a dropped buffer goes straight back.
                let all: Vec<FixedBuf> = std::iter::from_fn(FixedBuf::try_acquire).collect();
                all.len()
            })
            .await
            .unwrap();
            assert_eq!(elsewhere, 4);
        })
        .unwrap();
    let _ = std::fs::remove_file(&path);
}

#[test]
fn any_owned_buffer_can_be_written() {
    use starfish::buf::FixedBuf;

    rt(1)
        .run(|| async {
            // No pool was configured.
            assert!(FixedBuf::try_acquire().is_none());
            assert!(matches!(FixedBuf::acquire().await, Err(Error::NoFixedBuffers)));

            let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
            let addr = listener.local_addr().unwrap();
            let client = spawn(async move {
                let stream = TcpStream::connect(addr).await.unwrap();
                let (result, _) = stream.write_all("static str, ").await;
                result.unwrap();
                let (result, _) = stream.write_all(&b"static bytes, "[..]).await;
                result.unwrap();
                let (result, _) = stream.write_all(String::from("string, ")).await;
                result.unwrap();
                let (result, _) = stream.write_all(Vec::from(&b"boxed"[..]).into_boxed_slice()).await;
                result.unwrap();
            });
            let stream = listener.accept().await.unwrap();
            client.await.unwrap();
            let mut received = Vec::new();
            let mut buf = AlignedBuf::new(4096).unwrap();
            loop {
                let (result, returned) = stream.read(buf).await;
                if result.unwrap() == 0 {
                    break;
                }
                received.extend_from_slice(&returned);
                buf = returned;
            }
            assert_eq!(received, b"static str, static bytes, string, boxed");
        })
        .unwrap();
    assert!(FixedBuf::try_acquire().is_none());
}

#[test]
fn zero_copy_send_returns_before_the_buffer_does() {
    use starfish::buf::FixedBuf;

    rt(1)
        .fixed_buffers(4, 64 << 10)
        .run(|| async {
            let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
            let addr = listener.local_addr().unwrap();
            let reader = spawn(async move {
                let stream = listener.accept().await.unwrap();
                let mut total = Vec::new();
                let mut buf = Vec::with_capacity(64 << 10);
                loop {
                    let (result, returned) = stream.read(buf).await;
                    if result.unwrap() == 0 {
                        return total;
                    }
                    total.extend_from_slice(&returned);
                    buf = returned;
                }
            });
            let stream = TcpStream::connect(addr).await.unwrap();
            let mut expected = Vec::new();
            // More sends than the pool has buffers: each one must come back.
            for round in 0..64u32 {
                let mut buf = FixedBuf::acquire().await.unwrap();
                let chunk: Vec<u8> = (0..40_000u32).map(|i| (i ^ round) as u8).collect();
                buf.fill_from(&chunk);
                let mut sent = stream.write_zc(buf).await.unwrap();
                // A short send is finished by copying the rest.
                while sent < chunk.len() {
                    let (result, _) = stream.write(chunk[sent..].to_vec()).await;
                    sent += result.unwrap();
                }
                expected.extend_from_slice(&chunk);
            }
            stream.shutdown(std::net::Shutdown::Write).unwrap();
            assert!(reader.await.unwrap() == expected);
            // Every buffer is back in the pool.
            let mut all = Vec::new();
            for _ in 0..4 {
                all.push(timeout(Duration::from_secs(5), FixedBuf::acquire()).await.unwrap().unwrap());
            }
        })
        .unwrap();
}

#[test]
fn statistics_describe_the_shard() {
    let path = scratch("stats.bin");
    let path_in = path.clone();
    rt(2)
        .run(move || async move {
            let group = SchedulingGroup::create("worker", 200).unwrap();
            let before = starfish::stats::shard().unwrap();
            assert_eq!(before.shard, 0);

            let spinner = group.spawn(async {
                let start = Instant::now();
                while start.elapsed() < Duration::from_millis(30) {
                    maybe_yield().await;
                }
            });
            spinner.await.unwrap();
            // A call there and one back. The call back is what this shard
            // is certain to receive as a message: the reply to a call need
            // not be one, since on a busy machine it can be ready before
            // the caller has started to wait.
            submit_to(1, || async { submit_to(0, || async {}).await.unwrap() })
                .await
                .unwrap();
            sleep(Duration::from_millis(20)).await;
            let file = File::create(&path_in).await.unwrap();
            let (result, _) = file.write_all_at(vec![1u8; 10_000], 0).await;
            result.unwrap();

            let after = starfish::stats::shard().unwrap();
            assert!(after.tasks >= 1);
            assert!(after.tasks_polled > before.tasks_polled);
            assert!(after.loops > before.loops);
            assert!(after.sleeps > before.sleeps);
            assert!(after.busy >= Duration::from_millis(25), "{after:?}");
            assert!(after.idle >= Duration::from_millis(15), "{after:?}");
            assert!(after.messages_sent > before.messages_sent);
            assert!(after.messages_received > before.messages_received);

            let groups = starfish::stats::groups().unwrap();
            let worker = groups.iter().find(|group| group.name == "worker").unwrap();
            assert_eq!((worker.cpu_shares, worker.io_shares), (200, 200));
            assert!(worker.cpu_time >= Duration::from_millis(25), "{worker:?}");
            // The spinner yielded every time its slice ran out.
            assert!(worker.tasks_polled > 10, "{worker:?}");
            assert_eq!(groups[0].name, "main");

            let devices = starfish::stats::devices().unwrap();
            assert_eq!(devices.len(), 1);
            assert_eq!((devices[0].dispatched, devices[0].bytes), (1, 10_000));
            assert_eq!((devices[0].queued, devices[0].in_flight), (0, 0));

            // Other shards keep their own books.
            let other = submit_to(1, || async { starfish::stats::shard().unwrap() }).await.unwrap();
            assert_eq!(other.shard, 1);
            assert!(other.messages_received >= 2);
        })
        .unwrap();
    let _ = std::fs::remove_file(&path);
    assert!(matches!(starfish::stats::shard(), Err(Error::NoRuntime)));
}

static STALLS: std::sync::Mutex<Vec<starfish::stats::StallReport>> = std::sync::Mutex::new(Vec::new());

#[inline(never)]
fn hog_the_cpu_for_a_while(duration: Duration) -> u64 {
    let start = Instant::now();
    let mut spins = 0u64;
    while start.elapsed() < duration {
        spins = std::hint::black_box(spins + 1);
    }
    spins
}

#[test]
fn stalls_are_counted_and_reported_with_a_backtrace() {
    let stats = rt(1)
        .stall_threshold(Duration::from_millis(10))
        .stall_backtraces(true)
        .on_stall(|report| STALLS.lock().unwrap().push(report.clone()))
        .run(|| async {
            let group = SchedulingGroup::create("hog", 100).unwrap();
            // Well-behaved work does not count.
            for _ in 0..100 {
                yield_now().await;
            }
            assert_eq!(starfish::stats::shard().unwrap().stalls, 0);
            group.spawn(async { hog_the_cpu_for_a_while(Duration::from_millis(80)) }).await.unwrap();
            starfish::stats::shard().unwrap()
        })
        .unwrap();
    assert_eq!(stats.stalls, 1);
    assert!(stats.longest_stall >= Duration::from_millis(80));

    let reports = STALLS.lock().unwrap();
    assert_eq!(reports.len(), 1);
    let report = &reports[0];
    assert_eq!((report.shard, report.group.as_str()), (0, "hog"));
    assert!(report.duration >= Duration::from_millis(80));
    // Caught in the act: the stack is the test binary's, several frames deep.
    assert!(report.backtrace.len() > 3, "{report:?}");
    assert!(report.backtrace.iter().any(|frame| frame.contains("basic")), "{report:?}");
}

#[test]
fn memory_pressure_asks_the_application_to_free() {
    rt(1)
        .memory_limit(8 << 20)
        .run(|| async {
            let cache: Rc<RefCell<Vec<Vec<u8>>>> = Rc::new(RefCell::new(Vec::new()));
            let evictions = Rc::new(Cell::new(0u32));
            let (held, count) = (cache.clone(), evictions.clone());
            starfish::alloc::on_pressure(move || {
                held.borrow_mut().clear();
                count.set(count.get() + 1);
            })
            .unwrap();

            // Below the limit nothing happens.
            cache.borrow_mut().push(vec![0u8; 1 << 20]);
            for _ in 0..200 {
                yield_now().await;
            }
            assert_eq!(evictions.get(), 0);

            // Above it the cache is emptied, once.
            for _ in 0..16 {
                cache.borrow_mut().push(vec![0u8; 1 << 20]);
            }
            assert!(starfish::alloc::stats().unwrap().allocated_bytes > 16 << 20);
            for _ in 0..200 {
                yield_now().await;
            }
            assert_eq!(evictions.get(), 1);
            assert!(cache.borrow().is_empty());
            assert!(starfish::alloc::stats().unwrap().allocated_bytes < 8 << 20);
        })
        .unwrap();
}

#[test]
fn udp_datagrams() {
    use starfish::net::UdpSocket;

    rt(2)
        .run(|| async {
            let server = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
            let server_addr = server.local_addr().unwrap();
            let client = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
            let client_addr = client.local_addr().unwrap();

            // Datagrams keep their boundaries and name their sender.
            for message in [&b"first"[..], &b"second, longer"[..], &b""[..]] {
                let (result, _) = client.send_to(message, server_addr).await;
                assert_eq!(result.unwrap(), message.len());
            }
            for message in [&b"first"[..], &b"second, longer"[..], &b""[..]] {
                let (result, buf) = server.recv_from(Vec::with_capacity(1500)).await;
                assert_eq!(result.unwrap(), (message.len(), client_addr));
                assert_eq!(buf, message);
            }

            // One that does not fit is an error, not a silently shorter
            // datagram. The part that fitted comes back with the error.
            let (result, _) = client.send_to(vec![9u8; 3000], server_addr).await;
            assert_eq!(result.unwrap(), 3000);
            let (result, buf) = server.recv_from(Vec::with_capacity(100)).await;
            let cut = result.unwrap_err();
            assert_eq!(cut.kind(), std::io::ErrorKind::InvalidData);
            assert!(cut.to_string().contains("3000"), "{cut}");
            assert!(buf.len() == 100 && buf.iter().all(|&byte| byte == 9));
            // One that fits exactly is not.
            let (result, _) = client.send_to(vec![8u8; 100], server_addr).await;
            result.unwrap();
            let (result, buf) = server.recv_from(Vec::with_capacity(100)).await;
            assert_eq!(result.unwrap().0, 100);
            assert_eq!(buf, vec![8u8; 100]);

            // Connected sockets, with the reply sent from another shard.
            client.connect(server_addr).unwrap();
            let (result, _) = client.send(String::from("ping")).await;
            assert_eq!(result.unwrap(), 4);
            submit_to(1, move || async move {
                let (result, buf) = server.recv_from(Vec::with_capacity(64)).await;
                let (_, from) = result.unwrap();
                assert_eq!(buf, b"ping");
                let (result, _) = server.send_to("pong", from).await;
                result.unwrap();
            })
            .await
            .unwrap();
            let (result, buf) = client.recv(Vec::with_capacity(64)).await;
            assert_eq!(result.unwrap(), 4);
            assert_eq!(buf, b"pong");

            // A receive with nothing coming is cancelled cleanly.
            let waiting = timeout(Duration::from_millis(20), client.recv(Vec::with_capacity(64))).await;
            assert!(waiting.is_err());
        })
        .unwrap();
}

/// Sets its flag when dropped.
struct DropFlag(Rc<Cell<bool>>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.set(true);
    }
}

#[test]
fn abort_cancels_a_task_and_its_io() {
    rt(1)
        .run(|| async {
            let listener = Rc::new(TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap());
            let addr = listener.local_addr().unwrap();
            let dropped = Rc::new(Cell::new(false));

            // Nobody connects: the task sits in the kernel's accept.
            let task = spawn({
                let listener = listener.clone();
                let flag = DropFlag(dropped.clone());
                async move {
                    let _flag = flag;
                    listener.accept().await.map(|_| ())
                }
            });
            sleep(Duration::from_millis(10)).await;
            assert!(!task.is_finished());

            // The future, and what it holds, is gone when the call returns.
            task.abort();
            assert!(dropped.get());
            assert!(task.is_finished());
            assert!(matches!(task.await, Err(Error::Canceled)));

            // The accept was cancelled in the kernel; the listener is intact.
            let client = spawn(async move { TcpStream::connect(addr).await.unwrap() });
            let server = timeout(Duration::from_secs(5), listener.accept()).await.unwrap().unwrap();
            let client = client.await.unwrap();
            assert_eq!(server.peer_addr().unwrap(), client.local_addr().unwrap());

            // A task can be aborted before it has run at all.
            let dropped = Rc::new(Cell::new(false));
            let ran = Rc::new(Cell::new(false));
            let task = spawn({
                let flag = DropFlag(dropped.clone());
                let ran = ran.clone();
                async move {
                    let _flag = flag;
                    ran.set(true);
                }
            });
            task.abort();
            assert!(dropped.get());
            drop(task);
            sleep(Duration::from_millis(5)).await;
            assert!(!ran.get());

            // Too late: the task has finished and keeps its output.
            let task = spawn(async { 3 });
            yield_now().await;
            assert!(task.is_finished());
            task.abort();
            assert_eq!(task.await.unwrap(), 3);
        })
        .unwrap();
}

#[test]
fn semaphore_bounds_what_runs_at_once() {
    let (peak, served) = rt(1)
        .run(|| async {
            let semaphore = Semaphore::new(3);
            let running = Rc::new(Cell::new(0u32));
            let peak = Rc::new(Cell::new(0u32));
            let handles: Vec<_> = (0..20)
                .map(|_| {
                    let semaphore = semaphore.clone();
                    let running = running.clone();
                    let peak = peak.clone();
                    spawn(async move {
                        let _permit = semaphore.acquire(1).await.unwrap();
                        running.set(running.get() + 1);
                        peak.set(peak.get().max(running.get()));
                        sleep(Duration::from_millis(2)).await;
                        running.set(running.get() - 1);
                    })
                })
                .collect();
            let mut served = 0;
            for handle in handles {
                handle.await.unwrap();
                served += 1;
            }
            assert_eq!(semaphore.available(), 3);

            // Giving up on a wait, by timeout or by abort, loses nothing.
            let all = semaphore.acquire(3).await.unwrap();
            assert!(timeout(Duration::from_millis(10), semaphore.acquire(1)).await.is_err());
            let waiting = spawn({
                let semaphore = semaphore.clone();
                async move { semaphore.acquire(2).await.map(|permit| permit.count()) }
            });
            let aborted = spawn({
                let semaphore = semaphore.clone();
                async move { semaphore.acquire(1).await.map(|permit| permit.count()) }
            });
            yield_now().await;
            aborted.abort();
            drop(all);
            assert_eq!(waiting.await.unwrap().unwrap(), 2);
            assert_eq!(semaphore.available(), 3);

            // Closing releases those who wait.
            let all = semaphore.acquire(3).await.unwrap();
            let waiting = spawn({
                let semaphore = semaphore.clone();
                async move { semaphore.acquire(1).await.map(|permit| permit.count()) }
            });
            yield_now().await;
            semaphore.close();
            assert!(matches!(waiting.await.unwrap(), Err(Error::Closed)));
            drop(all);
            (peak.get(), served)
        })
        .unwrap();
    assert_eq!((peak, served), (3, 20));
}

#[test]
fn gate_close_waits_for_work_in_progress() {
    rt(1)
        .run(|| async {
            let gate = Gate::new();
            let finished = Rc::new(Cell::new(0u32));
            for i in 0..5u64 {
                let guard = gate.enter().unwrap();
                let finished = finished.clone();
                spawn(async move {
                    let _guard = guard;
                    sleep(Duration::from_millis(2 * (i + 1))).await;
                    finished.set(finished.get() + 1);
                });
            }
            // One piece of work is aborted; its guard goes with its future.
            let stuck = spawn({
                let guard = gate.enter().unwrap();
                async move {
                    let _guard = guard;
                    std::future::pending::<()>().await;
                }
            });
            assert_eq!(gate.active(), 6);

            let closing = gate.close();
            assert!(matches!(gate.enter(), Err(Error::Closed)));
            assert_eq!(finished.get(), 0);
            stuck.abort();
            assert_eq!(gate.active(), 5);
            closing.await;
            assert_eq!(finished.get(), 5);
            assert_eq!(gate.active(), 0);
        })
        .unwrap();
}

#[test]
fn channel_slows_the_producer_to_the_consumer() {
    let (received, most_queued) = rt(1)
        .run(|| async {
            let (sender, mut receiver) = channel(4);
            let producers: Vec<_> = (0..3u32)
                .map(|producer| {
                    let sender = sender.clone();
                    spawn(async move {
                        for i in 0..50 {
                            sender.send(producer * 1000 + i).await.unwrap();
                        }
                    })
                })
                .collect();
            drop(sender);

            let mut received = Vec::new();
            let mut most_queued = 0;
            // Ends by itself when the last producer has finished.
            while let Some(value) = receiver.recv().await {
                most_queued = most_queued.max(receiver.len() + 1);
                received.push(value);
                if received.len() % 16 == 0 {
                    sleep(Duration::from_millis(1)).await;
                }
            }
            for producer in producers {
                producer.await.unwrap();
            }

            // A producer blocked on a full channel is released by an abort
            // and by the receiver going away.
            let (sender, receiver) = channel(1);
            sender.send(0).await.unwrap();
            let aborted = spawn({
                let sender = sender.clone();
                async move { sender.send(1).await.is_ok() }
            });
            let refused = spawn(async move { sender.send(2).await.is_ok() });
            yield_now().await;
            aborted.abort();
            assert!(matches!(aborted.await, Err(Error::Canceled)));
            drop(receiver);
            assert!(!refused.await.unwrap());
            (received, most_queued)
        })
        .unwrap();
    assert_eq!(received.len(), 150);
    assert!(most_queued <= 4, "queued {most_queued}");
    // Each producer's values arrive in the order it sent them.
    for producer in 0..3 {
        let own: Vec<_> = received.iter().filter(|value| *value / 1000 == producer).collect();
        assert!(own.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(own.len(), 50);
    }
}

fn send_signal_to_self(signal: i32) {
    // SAFETY: plain syscalls. The signal has a listener, so it does not
    // end the process.
    unsafe {
        libc::kill(libc::getpid(), signal);
    }
}

#[test]
fn signals_reach_every_listener_on_any_shard() {
    rt(2)
        .run(|| async {
            let mut here = Signals::new(&[libc::SIGUSR1, libc::SIGUSR2]).unwrap();
            let mut there = Signals::new(&[libc::SIGUSR1]).unwrap();
            let remote = spawn(submit_to(1, move || async move { there.next().await }));

            assert!(timeout(Duration::from_millis(20), here.next()).await.is_err());

            send_signal_to_self(libc::SIGUSR1);
            let signal = timeout(Duration::from_secs(5), here.next()).await.unwrap();
            assert_eq!(signal, libc::SIGUSR1);
            assert_eq!(remote.await.unwrap().unwrap(), libc::SIGUSR1);
            assert!(timeout(Duration::from_millis(20), here.next()).await.is_err());

            // Signals that arrive while nobody is waiting are kept.
            send_signal_to_self(libc::SIGUSR2);
            send_signal_to_self(libc::SIGUSR1);
            sleep(Duration::from_millis(20)).await;
            let mut both = [here.next().await, here.next().await];
            both.sort_unstable();
            let mut expected = [libc::SIGUSR1, libc::SIGUSR2];
            expected.sort_unstable();
            assert_eq!(both, expected);

            // A listener only hears what arrives after it was created.
            let mut late = Signals::new(&[libc::SIGUSR1]).unwrap();
            assert!(timeout(Duration::from_millis(20), late.next()).await.is_err());

            assert!(Signals::new(&[]).is_err());
            assert!(Signals::new(&[0]).is_err());
            assert!(Signals::new(&[1000]).is_err());
            assert!(Signals::new(&[libc::SIGSEGV]).is_err());
            assert!(Signals::new(&[libc::SIGKILL]).is_err());
        })
        .unwrap();
}

#[test]
fn calls_between_shards_are_limited() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// Counts the calls running at once on the far shard.
    #[derive(Default)]
    struct Load {
        running: AtomicUsize,
        peak: AtomicUsize,
        finished: AtomicUsize,
    }

    async fn work(load: Arc<Load>, millis: u64) {
        let running = load.running.fetch_add(1, Ordering::SeqCst) + 1;
        load.peak.fetch_max(running, Ordering::SeqCst);
        sleep(Duration::from_millis(millis)).await;
        load.running.fetch_sub(1, Ordering::SeqCst);
        load.finished.fetch_add(1, Ordering::SeqCst);
    }

    rt(2)
        .submit_limit(4)
        .run(|| async {
            // Far more calls than the limit: they all complete, four at a
            // time.
            let load = Arc::new(Load::default());
            let calls: Vec<_> = (0..40)
                .map(|_| {
                    let load = load.clone();
                    spawn(submit_to(1, move || work(load, 2)))
                })
                .collect();
            for call in calls {
                call.await.unwrap().unwrap();
            }
            assert_eq!(load.finished.load(Ordering::SeqCst), 40);
            assert_eq!(load.peak.load(Ordering::SeqCst), 4);

            // Giving up on a call does not free its place: the far shard
            // is still working on it.
            let load = Arc::new(Load::default());
            let abandoned: Vec<_> = (0..4)
                .map(|_| {
                    let load = load.clone();
                    spawn(submit_to(1, move || work(load, 60)))
                })
                .collect();
            sleep(Duration::from_millis(10)).await;
            assert_eq!(load.running.load(Ordering::SeqCst), 4);
            for call in &abandoned {
                call.abort();
            }
            let start = Instant::now();
            let late = load.clone();
            submit_to(1, move || work(late, 1)).await.unwrap();
            assert!(start.elapsed() >= Duration::from_millis(30), "{:?}", start.elapsed());
            assert_eq!(load.peak.load(Ordering::SeqCst), 4);
            // The abandoned calls ran to the end all the same.
            sleep(Duration::from_millis(50)).await;
            assert_eq!(load.finished.load(Ordering::SeqCst), 5);

            // The limit is per direction and per pair: shard 1 calls back
            // while shard 0's calls to it are all in progress.
            let nested: Vec<_> = (0..8)
                .map(|i| spawn(submit_to(1, move || async move { submit_to(0, move || async move { i }).await })))
                .collect();
            let mut sum = 0;
            for call in nested {
                sum += call.await.unwrap().unwrap().unwrap();
            }
            assert_eq!(sum, 28);

            // A wait for a place can be given up.
            let load = Arc::new(Load::default());
            for _ in 0..4 {
                let load = load.clone();
                spawn(submit_to(1, move || work(load, 30)));
            }
            yield_now().await;
            let waiting = timeout(Duration::from_millis(5), submit_to(1, || async {})).await;
            assert!(waiting.is_err());
            submit_to(1, || async {}).await.unwrap();
        })
        .unwrap();
}

#[test]
fn shared_channel_carries_a_stream_between_shards() {
    const VALUES: u64 = 50_000;
    let (sum, peak) = rt(3)
        .run(|| async {
            // Shard 1 produces as fast as it can; shard 2 consumes, pausing
            // now and then. Four places in the queue are enough.
            let (mut sender, mut receiver) = shared_spsc(4);
            let producer = spawn(submit_to(1, move || async move {
                for value in 0..VALUES {
                    sender.send(value).await.unwrap();
                }
            }));
            let consumer = spawn(submit_to(2, move || async move {
                let mut sum = 0;
                let mut expected = 0;
                while let Some(value) = receiver.recv().await {
                    assert_eq!(value, expected);
                    expected += 1;
                    sum += value;
                    if value % 5000 == 0 {
                        sleep(Duration::from_millis(1)).await;
                    }
                }
                (sum, expected)
            }));
            producer.await.unwrap().unwrap();
            let (sum, count) = consumer.await.unwrap().unwrap();
            assert_eq!(count, VALUES);

            // The receiver goes away while the sender is blocked.
            let (mut sender, receiver) = shared_spsc(2);
            let blocked = spawn(submit_to(1, move || async move {
                let mut sent = 0u32;
                while sender.send(sent).await.is_ok() {
                    sent += 1;
                }
                sent
            }));
            sleep(Duration::from_millis(10)).await;
            drop(receiver);
            let sent = timeout(Duration::from_secs(5), blocked).await.unwrap().unwrap().unwrap();

            // The sender goes away while the receiver waits.
            let (sender, mut receiver) = shared_spsc::<u32>(2);
            let waiting = spawn(async move { receiver.recv().await });
            yield_now().await;
            submit_to(1, move || async move { drop(sender) }).await.unwrap();
            assert_eq!(timeout(Duration::from_secs(5), waiting).await.unwrap().unwrap(), None);
            (sum, sent)
        })
        .unwrap();
    assert_eq!(sum, VALUES * (VALUES - 1) / 2);
    assert!(peak <= 2, "only what fits in the queue can have been sent, not {peak}");
}

#[test]
fn buffered_reader_and_writer_frame_a_stream() {
    rt(1)
        .run(|| async {
            let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
            let addr = listener.local_addr().unwrap();
            let client = spawn(async move { TcpStream::connect(addr).await.unwrap() });
            let server = listener.accept().await.unwrap();
            let client = client.await.unwrap();
            client.set_nodelay(true).unwrap();

            // Lines arrive cut up in ways that have nothing to do with
            // where they end.
            let sender = spawn(async move {
                for piece in ["first li", "ne\nsecond line\r\n\nfour", "th\n", "\x00\x01\x02\x03tail"] {
                    let (result, _) = client.write_all(piece).await;
                    result.unwrap();
                    sleep(Duration::from_millis(2)).await;
                }
                client
            });

            // A buffer smaller than a line, to cross its edge many times.
            let mut reader = BufReader::with_capacity(5, &server);
            assert_eq!(reader.next_line().await.unwrap().as_deref(), Some("first line"));
            assert_eq!(reader.next_line().await.unwrap().as_deref(), Some("second line"));
            assert_eq!(reader.next_line().await.unwrap().as_deref(), Some(""));
            let mut line = String::new();
            assert_eq!(reader.read_line(&mut line).await.unwrap(), 7);
            assert_eq!(line, "fourth\n");

            // Then a piece of known length, and what is left.
            let mut header = [0u8; 4];
            reader.read_exact(&mut header).await.unwrap();
            assert_eq!(header, [0, 1, 2, 3]);
            let client = sender.await.unwrap();
            client.shutdown(std::net::Shutdown::Write).unwrap();
            // A last line without an ending still counts.
            assert_eq!(reader.next_line().await.unwrap().as_deref(), Some("tail"));
            assert_eq!(reader.next_line().await.unwrap(), None);
            let mut more = [0u8; 2];
            let short = reader.read_exact(&mut more).await.unwrap_err();
            assert_eq!(short.kind(), std::io::ErrorKind::UnexpectedEof);

            // The other way: many small writes leave as few sends, and
            // nothing leaves before the buffer is full or flushed.
            let mut writer = BufWriter::with_capacity(64, &server);
            for n in 0..10 {
                writer.write_all(format!("{n},").as_bytes()).await.unwrap();
            }
            assert_eq!(writer.buffer(), b"0,1,2,3,4,5,6,7,8,9,");
            let nothing = timeout(Duration::from_millis(20), client.read(Vec::with_capacity(64))).await;
            assert!(nothing.is_err(), "sent before the flush");
            writer.flush().await.unwrap();
            assert!(writer.buffer().is_empty());
            let (result, buf) = client.read(Vec::with_capacity(64)).await;
            assert_eq!(result.unwrap(), 20);
            assert_eq!(buf, b"0,1,2,3,4,5,6,7,8,9,");

            // More than the buffer holds goes out in buffer-sized pieces.
            let big: Vec<u8> = (0..1000u32).map(|n| (n % 251) as u8).collect();
            writer.write_all(&big).await.unwrap();
            writer.flush().await.unwrap();
            drop(writer);
            server.shutdown(std::net::Shutdown::Write).unwrap();
            let mut reader = BufReader::new(&client);
            let mut received = Vec::new();
            // No such byte follows: reads to the end.
            assert_eq!(reader.read_until(255, &mut received).await.unwrap(), 1000);
            assert_eq!(received, big);
        })
        .unwrap();
}

#[test]
fn buffered_reader_refuses_overlong_lines() {
    rt(1)
        .run(|| async {
            let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
            let addr = listener.local_addr().unwrap();
            let client = spawn(async move { TcpStream::connect(addr).await.unwrap() });
            let server = Rc::new(listener.accept().await.unwrap());
            let client = client.await.unwrap();

            let (result, _) = client.write_all("short\nexactly ten\n".to_owned() + &"x".repeat(5000)).await;
            result.unwrap();
            // A shared stream works too, so another task could write to it.
            let mut reader = BufReader::with_capacity(16, server.clone()).limit(12);
            assert_eq!(reader.next_line().await.unwrap().as_deref(), Some("short"));
            assert_eq!(reader.next_line().await.unwrap().as_deref(), Some("exactly ten"));
            let refused = reader.next_line().await.unwrap_err();
            assert_eq!(refused.kind(), std::io::ErrorKind::InvalidData);

            // Bytes that are not text are refused as a line.
            let (result, _) = client.write_all(vec![b'\n', 0xff, 0xfe, b'\n']).await;
            result.unwrap();
            let mut reader = BufReader::new(reader.into_inner());
            let mut skipped = Vec::new();
            reader.read_until(b'\n', &mut skipped).await.unwrap();
            let refused = reader.next_line().await.unwrap_err();
            assert_eq!(refused.kind(), std::io::ErrorKind::InvalidData);
            assert_eq!(reader.get_ref().peer_addr().unwrap(), client.local_addr().unwrap());
        })
        .unwrap();
}

/// How many descriptors this process has open on `path`. Other tests run
/// in the same process, so only the ones for our own file are counted.
fn open_descriptors(path: &std::path::Path) -> usize {
    std::fs::read_dir("/proc/self/fd")
        .unwrap()
        .filter_map(|entry| std::fs::read_link(entry.ok()?.path()).ok())
        .filter(|target| target == path)
        .count()
}

#[test]
fn files_have_sizes_and_directories() {
    use starfish::fs::{create_dir, create_dir_all, read_dir, remove_dir, remove_file, rename};

    let root = scratch("tree");
    rt(2)
        .run({
            let root = root.clone();
            move || async move {
                // Directories, several levels at once, and again.
                let deep = root.join("a/b/c");
                create_dir_all(&deep).await.unwrap();
                create_dir_all(&deep).await.unwrap();
                let again = create_dir(&deep).await.unwrap_err();
                assert_eq!(again.kind(), std::io::ErrorKind::AlreadyExists);
                let orphan = create_dir(root.join("x/y")).await.unwrap_err();
                assert_eq!(orphan.kind(), std::io::ErrorKind::NotFound);

                // Length: grows with writes, and can be set either way.
                let path = deep.join("data");
                let file = File::create(&path).await.unwrap();
                assert_eq!(file.size().await.unwrap(), 0);
                let (result, _) = file.write_all_at(vec![7u8; 5000], 0).await;
                result.unwrap();
                assert_eq!(file.size().await.unwrap(), 5000);
                file.set_len(100).await.unwrap();
                assert_eq!(file.size().await.unwrap(), 100);
                file.set_len(4096).await.unwrap();
                let (result, buf) = file.read_at(Vec::with_capacity(8192), 0).await;
                assert_eq!(result.unwrap(), 4096);
                assert!(buf[..100].iter().all(|&byte| byte == 7));
                assert!(buf[100..].iter().all(|&byte| byte == 0));

                // Reserved space counts as length, and holds zeros.
                file.allocate(0, 1 << 20).await.unwrap();
                assert_eq!(file.size().await.unwrap(), 1 << 20);
                file.sync_data().await.unwrap();
                file.close().await.unwrap();

                // A path with a file in the way cannot become a directory.
                let blocked = create_dir_all(path.join("below")).await.unwrap_err();
                assert_ne!(blocked.kind(), std::io::ErrorKind::NotFound);
                assert!(create_dir_all(&path).await.is_err());

                // Renaming replaces what is there.
                let other = deep.join("other");
                let (result, _) = File::create(&other).await.unwrap().write_all_at("old", 0).await;
                result.unwrap();
                rename(&path, &other).await.unwrap();
                assert_eq!(File::open(&other).await.unwrap().size().await.unwrap(), 1 << 20);
                assert!(File::open(&path).await.is_err());

                // Listing, from another shard as well.
                File::create(deep.join("second")).await.unwrap();
                create_dir(deep.join("sub")).await.unwrap();
                let listing = {
                    let deep = deep.clone();
                    submit_to(1, move || async move { read_dir(deep).await }).await.unwrap().unwrap()
                };
                let mut names: Vec<_> = listing
                    .iter()
                    .map(|entry| (entry.file_name().to_string_lossy().into_owned(), entry.is_dir(), entry.is_file()))
                    .collect();
                names.sort();
                assert_eq!(
                    names,
                    [
                        ("other".to_owned(), false, true),
                        ("second".to_owned(), false, true),
                        ("sub".to_owned(), true, false),
                    ]
                );
                assert!(listing.iter().all(|entry| entry.path().starts_with(&deep)));
                assert!(listing.iter().all(|entry| entry.file_type().is_some()));
                let missing = read_dir(root.join("nowhere")).await.unwrap_err();
                assert_eq!(missing.kind(), std::io::ErrorKind::NotFound);

                // Only an empty directory can be removed.
                assert!(remove_dir(&deep).await.is_err());
                for entry in listing {
                    if entry.is_dir() {
                        remove_dir(entry.path()).await.unwrap();
                    } else {
                        remove_file(entry.path()).await.unwrap();
                    }
                }
                remove_dir(&deep).await.unwrap();
                remove_dir(root.join("a/b")).await.unwrap();
                remove_dir(root.join("a")).await.unwrap();
                remove_dir(&root).await.unwrap();
            }
        })
        .unwrap();
    assert!(!root.exists());
}

#[test]
fn direct_files_can_end_between_blocks() {
    let path = scratch("exact-length");
    rt(1)
        .run({
            let path = path.clone();
            move || async move {
                let file = DmaFile::create(&path).await.unwrap();
                let block = file.alloc_buf(1).unwrap().capacity();
                file.allocate(0, 4 * block as u64).await.unwrap();

                // Whole blocks go in; the file is then cut to what counts.
                let mut buf = file.alloc_buf(block).unwrap();
                buf.fill_padded(b"twelve bytes");
                let (result, _) = file.write_at(buf, 0).await;
                assert_eq!(result.unwrap(), block);
                file.set_len(12).await.unwrap();
                file.sync_data().await.unwrap();
                assert_eq!(file.size().await.unwrap(), 12);

                // A read of whole blocks stops at the end of the file.
                let (result, buf) = file.read_at(file.alloc_buf(block).unwrap(), 0).await;
                assert_eq!(result.unwrap(), 12);
                assert_eq!(&buf[..], b"twelve bytes");
                file.close().await.unwrap();
                starfish::fs::remove_file(&path).await.unwrap();
            }
        })
        .unwrap();
}

#[test]
fn dropped_files_are_closed_without_being_awaited() {
    let path = scratch("many-opens");
    let (before, during, after) = rt(1)
        .run({
            let path = path.clone();
            move || async move {
                File::create(&path).await.unwrap().close().await.unwrap();
                let before = open_descriptors(&path);
                // Kept open, then all dropped at once.
                let mut files = Vec::new();
                for _ in 0..300 {
                    files.push(File::open(&path).await.unwrap());
                }
                let during = open_descriptors(&path);
                drop(files);
                // The closes are in the ring; give them a turn.
                sleep(Duration::from_millis(20)).await;
                let after = open_descriptors(&path);

                // Dropped one by one, far more often than a process may
                // have files open.
                for _ in 0..3000 {
                    drop(File::open(&path).await.unwrap());
                }
                // And one left open when the runtime stops.
                let leftover = File::open(&path).await.unwrap();
                spawn(async move {
                    let _leftover = leftover;
                    std::future::pending::<()>().await;
                });
                (before, during, after)
            }
        })
        .unwrap();
    // The file left open in a task went with the runtime.
    assert_eq!(open_descriptors(&path), 0);
    std::fs::remove_file(&path).unwrap();
    assert_eq!((before, during, after), (0, 300, 0));
}

#[test]
fn blocking_work_runs_off_the_shard() {
    use starfish::run_blocking;
    use std::collections::HashSet;

    rt(2)
        .run(|| async {
            // The shard keeps going while a thread elsewhere is blocked.
            let ticks = Rc::new(Cell::new(0u32));
            let ticker = spawn({
                let ticks = ticks.clone();
                async move {
                    loop {
                        sleep(Duration::from_millis(2)).await;
                        ticks.set(ticks.get() + 1);
                    }
                }
            });
            let here = std::thread::current().id();
            let (value, there) = run_blocking(move || {
                std::thread::sleep(Duration::from_millis(60));
                (6 * 7, std::thread::current().id())
            })
            .await
            .unwrap();
            ticker.abort();
            assert_eq!(value, 42);
            assert_ne!(there, here);
            assert!(ticks.get() >= 10, "only {} ticks", ticks.get());

            // A panic is reported, and the pool is none the worse.
            let panicked = run_blocking(move || {
                if std::thread::current().id() != here {
                    panic!("boom in blocking work");
                }
            })
            .await;
            assert!(matches!(panicked, Err(Error::Panicked { .. })));

            // Work done one piece after another reuses its thread.
            let mut used = HashSet::new();
            for _ in 0..50 {
                used.insert(run_blocking(|| std::thread::current().id()).await.unwrap());
            }
            // The pool belongs to the whole process, and other tests use
            // it at the same time, so this is not always one thread; it is
            // far from one per piece of work.
            assert!(used.len() <= 10, "{} threads for sequential work", used.len());

            // Many at once, from two shards, each on a thread of its own.
            let burst = |count: u64| async move {
                let jobs: Vec<_> = (0..count)
                    .map(|n| {
                        spawn(run_blocking(move || {
                            std::thread::sleep(Duration::from_millis(20));
                            n
                        }))
                    })
                    .collect();
                let mut sum = 0;
                for job in jobs {
                    sum += job.await.unwrap().unwrap();
                }
                sum
            };
            let start = Instant::now();
            let remote = spawn(submit_to(1, move || burst(40)));
            let local = burst(40).await;
            assert_eq!(local + remote.await.unwrap().unwrap(), 2 * (39 * 40 / 2));
            // 80 sleeps of 20 ms in sequence would take 1.6 s.
            assert!(start.elapsed() < Duration::from_millis(800), "{:?}", start.elapsed());

            // Giving up on the wait leaves the work to finish by itself.
            let (sender, receiver) = std::sync::mpsc::channel();
            let abandoned = timeout(
                Duration::from_millis(5),
                run_blocking(move || {
                    std::thread::sleep(Duration::from_millis(30));
                    sender.send(()).unwrap();
                }),
            )
            .await;
            assert!(abandoned.is_err());
            run_blocking(move || receiver.recv_timeout(Duration::from_secs(5)).unwrap()).await.unwrap();
        })
        .unwrap();
}

#[test]
fn files_are_read_and_written_in_order_through_a_cursor() {
    use starfish::fs::{metadata, remove_file, Cursor};

    let path = scratch("cursor");
    rt(1)
        .run({
            let path = path.clone();
            move || async move {
                let missing = metadata(&path).await.unwrap_err();
                assert_eq!(missing.kind(), std::io::ErrorKind::NotFound);

                // Many small writes, few trips to the kernel.
                let file = File::create(&path).await.unwrap();
                let mut out = BufWriter::with_capacity(4096, Cursor::new(&file));
                let mut expected = String::new();
                for n in 0..2000 {
                    let line = format!("line number {n}\n");
                    out.write_all(line.as_bytes()).await.unwrap();
                    expected.push_str(&line);
                }
                // The cursor is behind by what has not been flushed.
                assert!(out.get_ref().position() < expected.len() as u64);
                assert_eq!(out.get_ref().position() % 4096, 0);
                out.flush().await.unwrap();
                assert_eq!(out.get_ref().position(), expected.len() as u64);
                file.sync_data().await.unwrap();

                let found = metadata(&path).await.unwrap();
                assert_eq!(found.len(), expected.len() as u64);
                assert!(found.is_file() && !found.is_dir() && !found.is_empty());
                assert_eq!(found.mode() & 0o600, 0o600);
                let age = found.modified().elapsed().unwrap_or_default();
                assert!(age < Duration::from_secs(60), "{age:?}");
                assert!(metadata(path.parent().unwrap()).await.unwrap().is_dir());

                // Read back line by line, with a buffer that splits lines.
                let mut lines = BufReader::with_capacity(100, Cursor::new(&file));
                let mut count = 0;
                while let Some(line) = lines.next_line().await.unwrap() {
                    assert_eq!(line, format!("line number {count}"));
                    count += 1;
                }
                assert_eq!(count, 2000);
                // The end of a file stays the end.
                assert_eq!(lines.next_line().await.unwrap(), None);

                // Two cursors on one file keep their own places.
                let mut head = BufReader::new(Cursor::new(&file));
                let mut tail = BufReader::new(Cursor::at(&file, expected.len() as u64 - 17));
                assert_eq!(tail.next_line().await.unwrap().as_deref(), Some("line number 1999"));
                assert_eq!(head.next_line().await.unwrap().as_deref(), Some("line number 0"));
                let mut exact = [0u8; 4];
                head.read_exact(&mut exact).await.unwrap();
                assert_eq!(&exact, b"line");

                // Appending: start where the file ends. An owned file works
                // as well as a borrowed one.
                let size = file.size().await.unwrap();
                drop((head, tail, lines));
                let mut out = BufWriter::new(Cursor::at(file, size));
                out.write_all(b"the end\n").await.unwrap();
                out.flush().await.unwrap();
                let cursor = out.into_inner();
                assert_eq!(cursor.position(), size + 8);
                let file = cursor.into_inner();
                let mut tail = Cursor::new(&file);
                tail.set_position(size);
                let mut tail = BufReader::new(tail);
                assert_eq!(tail.next_line().await.unwrap().as_deref(), Some("the end"));
                assert_eq!(tail.get_ref().get_ref().size().await.unwrap(), size + 8);

                remove_file(&path).await.unwrap();
            }
        })
        .unwrap();
}

#[test]
fn a_directory_can_be_listed_a_part_at_a_time() {
    use starfish::fs::{create_dir, read_dir, remove_dir_all, ReadDir};

    let root = scratch("listing");
    rt(1)
        .run({
            let root = root.clone();
            move || async move {
                create_dir(&root).await.unwrap();
                create_dir(root.join("sub")).await.unwrap();
                for index in 0..1000 {
                    File::create(root.join(format!("file-{index:04}"))).await.unwrap();
                }
                // Batches smaller than, equal to and larger than the
                // directory all give the same entries as the whole listing.
                let mut whole: Vec<_> = read_dir(&root).await.unwrap().iter().map(|e| e.path()).collect();
                whole.sort();
                assert_eq!(whole.len(), 1001);
                for batch in [1, 7, 256, 1001, 5000] {
                    let mut listing = ReadDir::open(&root).await.unwrap().batch_size(batch);
                    let (mut paths, mut directories) = (Vec::new(), 0);
                    while let Some(entry) = listing.next().await.unwrap() {
                        directories += usize::from(entry.is_dir());
                        paths.push(entry.path());
                    }
                    // The end stays the end.
                    assert!(listing.next().await.unwrap().is_none());
                    paths.sort();
                    assert!(paths == whole, "batches of {batch} differ from the whole listing");
                    assert_eq!(directories, 1);
                }
                // Stopping early is allowed, and so is an empty directory.
                let mut listing = ReadDir::open(&root).await.unwrap().batch_size(10);
                for _ in 0..25 {
                    listing.next().await.unwrap().unwrap();
                }
                drop(listing);
                let mut empty = ReadDir::open(root.join("sub")).await.unwrap();
                assert!(empty.next().await.unwrap().is_none());

                let missing = ReadDir::open(root.join("nowhere")).await.err().unwrap();
                assert_eq!(missing.kind(), std::io::ErrorKind::NotFound);
                let not_one = ReadDir::open(root.join("file-0000")).await.err().unwrap();
                assert_eq!(not_one.kind(), std::io::ErrorKind::NotADirectory);
                remove_dir_all(&root).await.unwrap();
            }
        })
        .unwrap();
}

#[test]
fn a_removed_group_frees_its_place_and_its_tasks_carry_on() {
    rt(2)
        .run(|| async {
            let before = starfish::stats::groups().unwrap().len();
            let job = SchedulingGroup::create("job", 200).unwrap();
            let (release, held) = starfish::sync::oneshot::<()>();
            // A task that outlives its group, and spawns once the group is
            // gone.
            let survivor = job.spawn(async move {
                held.await;
                let inherited = SchedulingGroup::current().unwrap();
                let child = spawn(async { 7 }).await.unwrap();
                (inherited.name().is_err(), child)
            });
            yield_now().await;
            assert_eq!(job.name().unwrap(), "job");

            job.remove().unwrap();
            // Every copy of the handle is dead, here and on other shards.
            assert!(matches!(job.name(), Err(Error::InvalidGroup)));
            assert!(matches!(job.set_cpu_shares(5), Err(Error::InvalidGroup)));
            assert!(matches!(job.remove(), Err(Error::InvalidGroup)));
            assert!(matches!(job.spawn(async {}).await, Err(Error::InvalidGroup)));
            let elsewhere = submit_to(1, move || async move { job.spawn(async {}).await }).await;
            assert!(matches!(elsewhere.unwrap(), Err(Error::InvalidGroup)));
            assert!(matches!(SchedulingGroup::DEFAULT.remove(), Err(Error::InvalidGroup)));

            // The next group takes the place over, under its own name and
            // with nothing of the old one's record; the old handle does not
            // reach it.
            let next = SchedulingGroup::create("next", 300).unwrap();
            assert_ne!(next, job);
            assert_eq!(next.name().unwrap(), "next");
            assert!(job.name().is_err());
            next.spawn(async {}).await.unwrap();
            let groups = starfish::stats::groups().unwrap();
            assert!(groups.iter().all(|group| group.name != "job"));
            assert!(groups.iter().any(|group| group.name == "next" && group.cpu_shares == 300));

            // The task left behind finishes, in the default group.
            release.send(()).unwrap();
            assert_eq!(survivor.await.unwrap(), (true, 7));
            next.remove().unwrap();

            // Making and removing groups without end does not accumulate.
            for round in 0..10_000 {
                let group = SchedulingGroup::create("passing", 100).unwrap();
                assert_eq!(group.spawn(async move { round }).await.unwrap(), round);
                group.remove().unwrap();
            }
            assert!(starfish::stats::groups().unwrap().len() <= before + 1);
        })
        .unwrap();
}

/// A connected pair of TCP streams on the loopback interface.
async fn tcp_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = listener.local_addr().unwrap();
    let accepting = spawn(async move { listener.accept().await.unwrap() });
    let client = TcpStream::connect(addr).await.unwrap();
    (client, accepting.await.unwrap())
}

#[test]
fn a_receive_stream_hands_over_what_arrives_until_the_peer_closes() {
    rt(1)
        .recv_buffers(8, 4096)
        .run(|| async {
            let (client, server) = tcp_pair().await;
            // The server echoes each buffer as it is, then reports the end.
            let echo = spawn(async move {
                let mut incoming = server.recv_stream().unwrap();
                let mut pieces = 0;
                while let Some(data) = incoming.next().await.unwrap() {
                    assert!(!data.is_empty() && data.len() <= 4096);
                    pieces += 1;
                    let (result, _) = server.write_all(data).await;
                    result.unwrap();
                }
                // The end stays the end.
                assert!(incoming.next().await.unwrap().is_none());
                pieces
            });

            // Far more than the pool holds at once: 1 MiB through 8 buffers.
            let sent: Vec<u8> = (0..1u32 << 20).map(|i| (i.wrapping_mul(2654435761) >> 24) as u8).collect();
            let writer = {
                let sent = sent.clone();
                let client = Rc::new(client);
                let writing = client.clone();
                let task = spawn(async move {
                    let (result, _) = writing.write_all(sent).await;
                    result.unwrap();
                    writing.shutdown(std::net::Shutdown::Write).unwrap();
                });
                (client, task)
            };
            let (client, writing) = writer;
            let mut back = Vec::new();
            loop {
                let (result, buf) = client.read(Vec::with_capacity(64 << 10)).await;
                if result.unwrap() == 0 {
                    break;
                }
                back.extend_from_slice(&buf);
            }
            writing.await.unwrap();
            assert!(back == sent, "the echo differs from what was sent");
            assert!(echo.await.unwrap() >= 256);
        })
        .unwrap();
}

#[test]
fn receive_streams_wait_while_every_buffer_is_held() {
    rt(1)
        .recv_buffers(2, 4096)
        .run(|| async {
            let (client, server) = tcp_pair().await;
            let (result, _) = client.write_all(vec![7u8; 5 * 4096]).await;
            result.unwrap();
            let mut incoming = server.recv_stream().unwrap();
            let first = incoming.next().await.unwrap().unwrap();
            let second = incoming.next().await.unwrap().unwrap();
            let mut received = first.len() + second.len();
            // Both buffers are held: nothing more can arrive, however long
            // the wait. Giving up on the wait loses nothing.
            assert!(timeout(Duration::from_millis(30), incoming.next()).await.is_err());
            drop(first);
            // One buffer back is enough for the stream to go on.
            let third = incoming.next().await.unwrap().unwrap();
            received += third.len();
            assert!(third.iter().all(|&byte| byte == 7));
            drop(second);
            drop(third);
            while received < 5 * 4096 {
                received += incoming.next().await.unwrap().unwrap().len();
            }
            assert_eq!(received, 5 * 4096);

            // A second stream on another connection shares the pool.
            let (other_client, other_server) = tcp_pair().await;
            let mut other = other_server.recv_stream().unwrap();
            let (result, _) = other_client.write_all("hello").await;
            result.unwrap();
            assert_eq!(&*other.next().await.unwrap().unwrap(), b"hello");

            // Nothing sent, nothing received; then the data, after a wait
            // that was given up.
            assert!(timeout(Duration::from_millis(10), incoming.next()).await.is_err());
            let (result, _) = client.write_all("late").await;
            result.unwrap();
            let mut late = incoming.next().await.unwrap().unwrap();
            assert_eq!(&*late, b"late");
            // A buffer can be changed in place and shortened.
            late.make_ascii_uppercase();
            late.truncate(2);
            assert_eq!(&*late, b"LA");
        })
        .unwrap();
}

#[test]
fn dropping_a_receive_stream_returns_its_buffers() {
    use starfish::net::{UnixListener, UnixStream};

    let path = scratch("recv-socket");
    rt(1)
        .recv_buffers(4, 4096)
        .run({
            let path = path.clone();
            move || async move {
                // Each round fills the whole pool with data nobody collects,
                // then drops the stream. If the buffers did not come back,
                // the second round would have nothing to receive into.
                for round in 0..20 {
                    let (client, server) = tcp_pair().await;
                    let mut incoming = server.recv_stream().unwrap();
                    let (result, _) = client.write_all(vec![round as u8; 8 * 4096]).await;
                    result.unwrap();
                    let one = incoming.next().await.unwrap().unwrap();
                    assert!(one.iter().all(|&byte| byte == round as u8));
                    // Let the kernel fill the rest of the pool.
                    sleep(Duration::from_millis(2)).await;
                    drop(incoming);
                    // The buffer handed over outlives the stream.
                    assert!(one.iter().all(|&byte| byte == round as u8));
                    drop(one);
                    // What the stream had not received is still on the
                    // connection for an ordinary read.
                    let (result, buf) = server.read(Vec::with_capacity(4096)).await;
                    assert!(result.unwrap() > 0);
                    assert!(buf.iter().all(|&byte| byte == round as u8));
                }

                // The same over a Unix-domain socket, to the end.
                let listener = UnixListener::bind(&path).unwrap();
                let accepting = spawn(async move { listener.accept().await.unwrap() });
                let client = UnixStream::connect(&path).await.unwrap();
                let server = accepting.await.unwrap();
                let (result, _) = client.write_all("over a unix socket").await;
                result.unwrap();
                client.shutdown(std::net::Shutdown::Write).unwrap();
                let mut incoming = server.recv_stream().unwrap();
                let mut text = Vec::new();
                while let Some(data) = incoming.next().await.unwrap() {
                    text.extend_from_slice(&data);
                }
                assert_eq!(text, b"over a unix socket");
                starfish::fs::remove_file(&path).await.unwrap();
            }
        })
        .unwrap();
}

#[test]
fn a_receive_stream_needs_receive_buffers_and_reports_errors() {
    rt(1)
        .run(|| async {
            let (_client, server) = tcp_pair().await;
            let refused = server.recv_stream().err().unwrap();
            assert_eq!(refused.kind(), std::io::ErrorKind::Unsupported);
        })
        .unwrap();
    rt(1)
        .recv_buffers(2, 4096)
        .run(|| async {
            let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
            let client = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
            let server = listener.accept().await.unwrap();
            let mut incoming = server.recv_stream().unwrap();
            // A reset connection is an error, not an end.
            socket2::SockRef::from(&client).set_linger(Some(Duration::ZERO)).unwrap();
            drop(client);
            let outcome = incoming.next().await;
            assert_eq!(outcome.err().unwrap().kind(), std::io::ErrorKind::ConnectionReset);
        })
        .unwrap();
}

#[test]
fn a_cloned_tcp_listener_shares_one_queue_between_shards() {
    rt(2)
        .run(|| async {
            let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
            let addr = listener.local_addr().unwrap();
            let other = listener.try_clone().unwrap();
            assert_eq!(other.local_addr().unwrap(), addr);
            // Only the other shard accepts, through its clone.
            let accepted = spawn(submit_to(1, move || async move {
                let stream = other.accept().await.unwrap();
                let (result, buf) = stream.read(Vec::with_capacity(16)).await;
                result.unwrap();
                (shard_id().unwrap(), buf)
            }));
            let stream = TcpStream::connect(addr).await.unwrap();
            let (result, _) = stream.write_all("over there").await;
            result.unwrap();
            assert_eq!(accepted.await.unwrap().unwrap(), (1, b"over there".to_vec()));
            // The original still listens once the clone is gone.
            let pending = spawn(async move { listener.accept().await.map(|_| ()) });
            let _second = TcpStream::connect(addr).await.unwrap();
            pending.await.unwrap().unwrap();
        })
        .unwrap();
}

#[test]
fn unix_sockets_connect_processes_on_one_machine() {
    use starfish::net::{UnixListener, UnixStream};

    let path = scratch("socket");
    rt(3)
        .run({
            let path = path.clone();
            move || async move {
                let listener = UnixListener::bind(&path).unwrap();
                assert_eq!(listener.local_addr().unwrap().as_pathname(), Some(path.as_path()));
                // A path holds one socket; a second bind is refused, and so
                // is one over the file a listener leaves behind.
                let taken = UnixListener::bind(&path).err().unwrap();
                assert_eq!(taken.kind(), std::io::ErrorKind::AddrInUse);
                let nobody = UnixStream::connect(scratch("no-such-socket")).await.err().unwrap();
                assert_eq!(nobody.kind(), std::io::ErrorKind::NotFound);

                // Every shard accepts on the same socket through its own
                // handle, and says which shard it is.
                let mut acceptors = Vec::new();
                for shard in 0..3 {
                    let listener = listener.try_clone().unwrap();
                    acceptors.push(spawn(submit_to(shard, move || async move {
                        let stream = listener.accept().await.unwrap();
                        // The accepted end is at the listener's path; the
                        // client never bound, so it has no name.
                        let here = stream.local_addr().unwrap();
                        assert_eq!(here.as_pathname(), listener.local_addr().unwrap().as_pathname());
                        assert!(stream.peer_addr().unwrap().is_unnamed());
                        let me = stream.peer_credentials().unwrap();
                        // SAFETY: plain system calls.
                        let (pid, uid) = unsafe { (libc::getpid(), libc::getuid()) };
                        assert_eq!((me.process, me.user), (pid, uid));

                        let mut lines = BufReader::new(&stream).limit(1024);
                        let mut out = BufWriter::new(&stream);
                        while let Some(line) = lines.next_line().await.unwrap() {
                            out.write_all(format!("shard {shard}: {line}\n").as_bytes()).await.unwrap();
                            out.flush().await.unwrap();
                        }
                    })));
                }
                drop(listener);
                sleep(Duration::from_millis(10)).await;

                let mut served_by = Vec::new();
                for client in 0..3 {
                    let stream = UnixStream::connect(&path).await.unwrap();
                    assert_eq!(stream.peer_addr().unwrap().as_pathname(), Some(path.as_path()));
                    assert!(stream.local_addr().unwrap().is_unnamed());
                    let (result, _) = stream.write_all(format!("hello {client}\n")).await;
                    result.unwrap();
                    let (result, reply) = stream.read(Vec::with_capacity(64)).await;
                    result.unwrap();
                    let reply = String::from_utf8(reply).unwrap();
                    assert!(reply.ends_with(&format!(": hello {client}\n")), "{reply:?}");
                    served_by.push(reply[..7].to_owned());
                    // A plain write, and the end of the conversation.
                    let (result, _) = stream.write("again\n").await;
                    assert_eq!(result.unwrap(), 6);
                    let (result, reply) = stream.read(Vec::with_capacity(64)).await;
                    result.unwrap();
                    assert!(reply.ends_with(b": again\n"));
                    stream.shutdown(std::net::Shutdown::Write).unwrap();
                    let (result, _) = stream.read(Vec::with_capacity(64)).await;
                    assert_eq!(result.unwrap(), 0);
                }
                for acceptor in acceptors {
                    acceptor.await.unwrap().unwrap();
                }
                // Three connections, three shards, each served once.
                served_by.sort();
                assert_eq!(served_by, ["shard 0", "shard 1", "shard 2"]);

                // The socket file outlives its listeners.
                assert!(starfish::fs::metadata(&path).await.is_ok());
                starfish::fs::remove_file(&path).await.unwrap();
            }
        })
        .unwrap();
}

#[test]
fn direct_io_streams_take_pieces_of_any_size() {
    use starfish::fs::{DmaStreamReader, DmaStreamWriter};

    let path = scratch("dma-stream");
    rt(1)
        .run({
            let path = path.clone();
            move || async move {
                let file = DmaFile::create(&path).await.unwrap();
                let block = file.disk_alignment() as u64;
                // Preallocated, as a log would be.
                file.allocate(0, 1 << 20).await.unwrap();

                // Records of awkward sizes, through a buffer of two blocks.
                let mut writer = DmaStreamWriter::new(&file, 2 * block as usize).unwrap();
                let mut expected = Vec::new();
                for n in 0..3000u32 {
                    let record = format!("record {n} {}\n", "x".repeat((n % 37) as usize));
                    writer.write_all(record.as_bytes()).await.unwrap();
                    expected.extend_from_slice(record.as_bytes());
                    assert_eq!(writer.position(), expected.len() as u64);
                    if n == 1500 {
                        // Durable so far; the length is still the
                        // preallocated one.
                        writer.sync().await.unwrap();
                        assert_eq!(file.size().await.unwrap(), 1 << 20);
                    }
                }
                assert!(!(expected.len() as u64).is_multiple_of(block), "the test wants a partial last block");
                writer.finish().await.unwrap();
                // Only now does the file end where the data does.
                assert_eq!(file.size().await.unwrap(), expected.len() as u64);

                // Read back in pieces that do not match anything.
                let mut reader = DmaStreamReader::new(&file, 3 * block as usize).unwrap();
                let mut read_back = Vec::new();
                let mut piece = vec![0u8; 777];
                loop {
                    assert_eq!(reader.position(), read_back.len() as u64);
                    let count = reader.read(&mut piece).await.unwrap();
                    if count == 0 {
                        break;
                    }
                    read_back.extend_from_slice(&piece[..count]);
                }
                assert!(read_back == expected);
                assert_eq!(reader.read(&mut piece).await.unwrap(), 0);

                // From the middle of a block, and to exactly the end.
                let middle = expected.len() as u64 / 2 + 3;
                let mut reader = DmaStreamReader::at(&file, middle, block as usize).unwrap();
                assert_eq!(reader.position(), middle);
                let mut rest = vec![0u8; expected.len() - middle as usize];
                reader.read_exact(&mut rest).await.unwrap();
                assert!(rest == expected[middle as usize..]);
                let short = reader.read_exact(&mut [0u8; 1]).await.unwrap_err();
                assert_eq!(short.kind(), std::io::ErrorKind::UnexpectedEof);

                // Lines, by putting the line reader on top.
                let mut lines = BufReader::new(DmaStreamReader::new(&file, 4 * block as usize).unwrap());
                let mut count = 0;
                while let Some(line) = lines.next_line().await.unwrap() {
                    assert!(line.starts_with(&format!("record {count} ")), "{line:?}");
                    count += 1;
                }
                assert_eq!(count, 3000);

                // Appending completes the partial last block.
                let mut writer = DmaStreamWriter::append(&file, 2 * block as usize).await.unwrap();
                assert_eq!(writer.position(), expected.len() as u64);
                assert_eq!(writer.get_ref().disk_alignment() as u64, block);
                writer.write_all(b"appended\n").await.unwrap();
                expected.extend_from_slice(b"appended\n");
                writer.finish().await.unwrap();
                assert_eq!(file.size().await.unwrap(), expected.len() as u64);
                let mut reader = DmaStreamReader::new(&file, 64 * 1024).unwrap();
                let mut all = vec![0u8; expected.len()];
                reader.read_exact(&mut all).await.unwrap();
                assert!(all == expected);

                // An empty stream leaves an empty file, and reads as one.
                let empty = DmaFile::create(&path).await.unwrap();
                DmaStreamWriter::new(&empty, 1).unwrap().finish().await.unwrap();
                assert_eq!(empty.size().await.unwrap(), 0);
                let mut reader = DmaStreamReader::new(&empty, 1).unwrap();
                assert_eq!(reader.read(&mut piece).await.unwrap(), 0);
                starfish::fs::remove_file(&path).await.unwrap();
            }
        })
        .unwrap();
}

#[test]
fn several_shards_stream_to_one() {
    use starfish::sync::shared_mpsc;

    const VALUES: u64 = 20_000;
    rt(4)
        .run(|| async {
            // Shards 1 to 3 produce as fast as they can; shard 0 consumes.
            let (sender, mut receiver) = shared_mpsc(8);
            let producers: Vec<_> = (1..4)
                .map(|shard| {
                    let mut sender = sender.clone();
                    spawn(submit_to(shard, move || async move {
                        for value in 0..VALUES {
                            sender.send((shard as u64, value)).await.unwrap();
                        }
                    }))
                })
                .collect();
            // Two tasks on one shard, each with a sender of its own.
            let local: Vec<_> = (4..6u64)
                .map(|id| {
                    let mut sender = sender.clone();
                    spawn(async move {
                        for value in 0..1000 {
                            sender.send((id, value)).await.unwrap();
                        }
                    })
                })
                .collect();
            drop(sender);

            let mut next = [0u64; 6];
            let mut total = 0u64;
            // Ends by itself when the last sender is gone.
            while let Some((producer, value)) = receiver.recv().await {
                assert_eq!(value, next[producer as usize], "out of order from {producer}");
                next[producer as usize] += 1;
                total += 1;
                if total.is_multiple_of(4096) {
                    sleep(Duration::from_millis(1)).await;
                }
            }
            assert_eq!(total, 3 * VALUES + 2000);
            assert_eq!(next, [0, VALUES, VALUES, VALUES, 1000, 1000]);
            for producer in producers {
                producer.await.unwrap().unwrap();
            }
            for task in local {
                task.await.unwrap();
            }

            // The receiver goes away while senders on two shards are blocked.
            let (sender, receiver) = shared_mpsc(2);
            let blocked: Vec<_> = (1..3)
                .map(|shard| {
                    let mut sender = sender.clone();
                    spawn(submit_to(shard, move || async move {
                        let mut sent = 0u32;
                        while sender.send(sent).await.is_ok() {
                            sent += 1;
                        }
                        sent
                    }))
                })
                .collect();
            sleep(Duration::from_millis(10)).await;
            drop(receiver);
            for sender in blocked {
                let sent = timeout(Duration::from_secs(5), sender).await.unwrap().unwrap().unwrap();
                assert!(sent <= 2, "only what fits in a queue can have been sent, not {sent}");
            }
            assert!(sender.is_closed());
        })
        .unwrap();
}

#[test]
fn direct_io_reader_reads_ahead() {
    use starfish::fs::{DmaStreamReader, DmaStreamWriter};

    let path = scratch("read-ahead");
    rt(1)
        .run({
            let path = path.clone();
            move || async move {
                let file = DmaFile::create(&path).await.unwrap();
                let block = file.disk_alignment();
                // Some hundred blocks and a bit, so the end falls inside one.
                let contents: Vec<u8> = (0..block * 300 + 123).map(|n| (n % 251) as u8).collect();
                let mut writer = DmaStreamWriter::new(&file, 16 * block).unwrap();
                writer.write_all(&contents).await.unwrap();
                writer.finish().await.unwrap();

                // More reads on their way than the file has buffers left,
                // in pieces that match nothing.
                for ahead in [1, 4, 1000] {
                    let mut reader = DmaStreamReader::new(&file, 4 * block).unwrap().read_ahead(ahead).unwrap();
                    let mut read_back = Vec::with_capacity(contents.len());
                    let mut piece = vec![0u8; 3001];
                    loop {
                        assert_eq!(reader.position(), read_back.len() as u64);
                        let count = reader.read(&mut piece).await.unwrap();
                        if count == 0 {
                            break;
                        }
                        read_back.extend_from_slice(&piece[..count]);
                        // Let the reads ahead complete out of step with us.
                        if read_back.len().is_multiple_of(7) {
                            yield_now().await;
                        }
                    }
                    assert!(read_back == contents, "with {ahead} reads ahead");
                    assert_eq!(reader.read(&mut piece).await.unwrap(), 0);
                }

                // From inside a block, to the end exactly.
                let middle = (block * 150 + 77) as u64;
                let mut reader = DmaStreamReader::at(&file, middle, 2 * block).unwrap().read_ahead(3).unwrap();
                let mut rest = vec![0u8; contents.len() - middle as usize];
                reader.read_exact(&mut rest).await.unwrap();
                assert!(rest == contents[middle as usize..]);
                assert_eq!(reader.position(), contents.len() as u64);
                assert!(reader.read_exact(&mut [0u8; 1]).await.is_err());

                // A reader dropped with reads on their way leaves nothing
                // behind that matters, and a wait that is given up breaks
                // the stream rather than corrupting it.
                let mut reader = DmaStreamReader::new(&file, block).unwrap().read_ahead(8).unwrap();
                let mut first = [0u8; 10];
                reader.read_exact(&mut first).await.unwrap();
                assert_eq!(&first[..], &contents[..10]);
                drop(reader);

                // Lines through the line reader, reading ahead underneath.
                let text = scratch("read-ahead-lines");
                let lines_file = DmaFile::create(&text).await.unwrap();
                let mut writer = DmaStreamWriter::new(&lines_file, 8 * block).unwrap();
                for n in 0..5000 {
                    writer.write_all(format!("line {n}\n").as_bytes()).await.unwrap();
                }
                writer.finish().await.unwrap();
                let source = DmaStreamReader::new(&lines_file, 2 * block).unwrap().read_ahead(4).unwrap();
                let mut lines = BufReader::new(source);
                let mut count = 0;
                while let Some(line) = lines.next_line().await.unwrap() {
                    assert_eq!(line, format!("line {count}"));
                    count += 1;
                }
                assert_eq!(count, 5000);

                starfish::fs::remove_file(&text).await.unwrap();
                starfish::fs::remove_file(&path).await.unwrap();
            }
        })
        .unwrap();
}

#[test]
fn select_takes_whichever_comes_first() {
    use starfish::sync::oneshot;
    use starfish::{interval, select, Either};

    rt(1)
        .run(|| async {
            // A message or a timeout, whichever is first.
            let (sender, mut inbox) = channel::<u32>(4);
            let quiet = select(inbox.recv(), sleep(Duration::from_millis(10))).await;
            assert_eq!(quiet, Either::Second(()));
            sender.send(7).await.unwrap();
            let busy = select(inbox.recv(), sleep(Duration::from_secs(5))).await;
            assert_eq!(busy, Either::First(Some(7)));

            // Losing a race costs a channel nothing: the next value is
            // still there for the next call.
            for value in 0..50 {
                let waiting = select(inbox.recv(), yield_now()).await;
                assert_eq!(waiting, Either::Second(()));
                sender.send(value).await.unwrap();
                assert_eq!(select(inbox.recv(), sleep(Duration::from_secs(5))).await, Either::First(Some(value)));
            }

            // A long job watched by a ticker: the job is kept across the
            // rounds by passing a reference.
            let job = std::pin::pin!(async {
                sleep(Duration::from_millis(35)).await;
                "done"
            });
            let mut job = job;
            let mut every = interval(Duration::from_millis(10));
            let mut ticks = 0;
            let result = loop {
                match select(job.as_mut(), every.tick()).await {
                    Either::First(result) => break result,
                    Either::Second(()) => ticks += 1,
                }
            };
            assert_eq!(result, "done");
            // Three on an idle machine; fewer where ticks are missed.
            assert!((1..=4).contains(&ticks), "{ticks} ticks");

            // Losing cancels: the accept below never completes, and the
            // listener is free afterwards.
            let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
            let (stop, stopped) = oneshot();
            let accepting = spawn(async move {
                match select(listener.accept(), stopped).await {
                    Either::First(_) => panic!("nobody connects"),
                    Either::Second(reason) => (reason, listener),
                }
            });
            yield_now().await;
            stop.send("shutting down").unwrap();
            let (reason, listener) = accepting.await.unwrap();
            assert_eq!(reason, Some("shutting down"));
            let addr = listener.local_addr().unwrap();
            let client = spawn(async move { TcpStream::connect(addr).await.unwrap() });
            timeout(Duration::from_secs(5), listener.accept()).await.unwrap().unwrap();
            client.await.unwrap();
        })
        .unwrap();
}

#[test]
fn interval_ticks_without_drifting_and_skips_what_it_missed() {
    use starfish::interval;

    rt(1)
        .run(|| async {
            let period = Duration::from_millis(10);
            let start = Instant::now();
            let mut every = interval(period);
            assert_eq!(every.period(), period);
            let mut worst_work = Duration::ZERO;
            // Work that takes most of a period does not push the ticks back.
            for tick in 1..=5u32 {
                every.tick().await;
                let at = start.elapsed();
                assert!(at >= period * tick, "tick {tick} early, at {at:?}");
                let work = Instant::now();
                sleep(Duration::from_millis(6)).await;
                worst_work = worst_work.max(work.elapsed());
            }
            // Five ticks with 6 ms of work after each: 56 ms on schedule,
            // 80 ms had each tick waited a full period after the work.
            // Only meaningful where the work itself took what it should;
            // on a slow or loaded machine nothing is on time.
            let total = start.elapsed();
            if worst_work < Duration::from_millis(8) {
                assert!(total < Duration::from_millis(72), "drifted: {total:?}");
            }
            // After a long pause there is one tick at once, not a burst.
            sleep(Duration::from_millis(45)).await;
            let resumed = Instant::now();
            every.tick().await;
            // Had the missed ticks been made up, four more would follow at
            // once. The next one is a full period away.
            every.tick().await;
            let gap = resumed.elapsed();
            assert!(gap >= Duration::from_millis(9), "{gap:?}");
        })
        .unwrap();
}

#[test]
fn a_shard_can_look_for_work_before_it_sleeps() {
    rt(2)
        .spin_before_sleep(Duration::from_micros(200))
        .run(|| async {
            // Everything still works: calls, timers, I/O and sleeping.
            let mut sum = 0;
            for i in 0..2000u64 {
                sum += submit_to(1, move || async move { i }).await.unwrap();
            }
            assert_eq!(sum, 1999 * 2000 / 2);

            let start = Instant::now();
            sleep(Duration::from_millis(20)).await;
            let slept = start.elapsed();
            assert!(slept >= Duration::from_millis(20) && slept < Duration::from_millis(40), "{slept:?}");

            let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
            let addr = listener.local_addr().unwrap();
            let client = spawn(async move { TcpStream::connect(addr).await.unwrap() });
            let server = listener.accept().await.unwrap();
            let client = client.await.unwrap();
            let (result, _) = client.write_all("ping").await;
            result.unwrap();
            let (result, buf) = server.read(Vec::with_capacity(16)).await;
            assert_eq!(result.unwrap(), 4);
            assert_eq!(buf, b"ping");

            // The time spent looking is accounted for, and a shard that
            // has nothing to do for long still goes to sleep.
            let before = starfish::stats::shard().unwrap();
            sleep(Duration::from_millis(50)).await;
            let after = starfish::stats::shard().unwrap();
            assert!(after.spinning > Duration::ZERO);
            assert!(after.sleeps > before.sleeps);
            let spun = after.spinning - before.spinning;
            assert!(spun < Duration::from_millis(10), "spun {spun:?} of a 50 ms sleep");
        })
        .unwrap();

    // Without it, no time is spent that way.
    let spinning = rt(1)
        .run(|| async {
            sleep(Duration::from_millis(5)).await;
            starfish::stats::shard().unwrap().spinning
        })
        .unwrap();
    assert_eq!(spinning, Duration::ZERO);
}

#[test]
fn public_types_can_be_printed_for_debugging() {
    use starfish::sync::oneshot;

    let runtime = format!("{:?}", rt(2));
    assert!(runtime.starts_with("Runtime {") && runtime.contains("shards: 2"), "{runtime}");
    rt(1)
        .run(|| async {
            let semaphore = Semaphore::new(3);
            let permit = semaphore.acquire(2).await.unwrap();
            assert_eq!(format!("{semaphore:?}"), "Semaphore { available: 1, closed: false, .. }");
            assert_eq!(format!("{permit:?}"), "Permit { permits: 2, .. }");

            let gate = Gate::new();
            let guard = gate.enter().unwrap();
            assert_eq!(format!("{gate:?}"), "Gate { active: 1, closed: false, .. }");
            assert_eq!(format!("{guard:?}"), "GateGuard { .. }");

            let (sender, receiver) = channel::<u8>(4);
            sender.try_send(1).unwrap();
            assert_eq!(format!("{receiver:?}"), "Receiver { queued: 1, .. }");
            assert_eq!(format!("{sender:?}"), "Sender { closed: false, .. }");
            let (answer, waiting) = oneshot::<u8>();
            assert_eq!(format!("{answer:?} {waiting:?}"), "OneshotSender { closed: false, .. } OneshotReceiver { .. }");

            let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
            assert!(format!("{listener:?}").starts_with("TcpListener { fd: "));
            let task = spawn(async {});
            assert_eq!(format!("{task:?}"), "JoinHandle { finished: false, .. }");
            task.await.unwrap();
            assert!(format!("{:?}", starfish::stats::shard().unwrap()).contains("spinning"));
            assert!(format!("{:?}", sleep(Duration::from_secs(1))).starts_with("Sleep { deadline: Some("));
        })
        .unwrap();
}

#[test]
fn a_read_with_no_room_is_an_error_not_an_end() {
    let path = scratch("no-room");
    rt(1)
        .run({
            let path = path.clone();
            move || async move {
                let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
                let addr = listener.local_addr().unwrap();
                let client = spawn(async move { TcpStream::connect(addr).await.unwrap() });
                let server = listener.accept().await.unwrap();
                let client = client.await.unwrap();
                let (result, _) = client.write_all("data").await;
                result.unwrap();

                // `Vec::new()` has no capacity. The connection is open and
                // has data; `Ok(0)` would have said it was closed.
                let (result, _) = server.read(Vec::new()).await;
                assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);
                let (result, buf) = server.read(Vec::with_capacity(16)).await;
                assert_eq!(result.unwrap(), 4);
                assert_eq!(buf, b"data");

                let file = File::create(&path).await.unwrap();
                let (result, _) = file.write_all_at("contents", 0).await;
                result.unwrap();
                let (result, _) = file.read_at(Vec::new(), 0).await;
                assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);
                // The end of a file is still `Ok(0)`.
                let (result, _) = file.read_at(Vec::with_capacity(16), 8).await;
                assert_eq!(result.unwrap(), 0);

                let socket = starfish::net::UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
                let (result, _) = socket.recv_from(Vec::new()).await;
                assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);
                starfish::fs::remove_file(&path).await.unwrap();
            }
        })
        .unwrap();
}

#[test]
fn panics_come_back_as_errors_with_their_message() {
    use starfish::run_blocking;

    let message = |error: Error| match error {
        Error::Panicked { message } => message,
        other => panic!("not a panic: {other}"),
    };
    rt(2)
        .run(move || async move {
            // In a task.
            let task = spawn(async {
                if shard_id().is_ok() {
                    panic!("task went wrong: code {}", 7);
                }
            });
            assert_eq!(message(task.await.unwrap_err()).as_deref(), Some("task went wrong: code 7"));

            // In a call to another shard, in the closure and in its future.
            let remote = submit_to(1, || async {
                yield_now().await;
                if shard_id().is_ok() {
                    panic!("remote future went wrong");
                }
            })
            .await;
            assert_eq!(message(remote.unwrap_err()).as_deref(), Some("remote future went wrong"));
            let remote = submit_to(1, || {
                if shard_id().is_ok() {
                    panic!("remote closure went wrong");
                }
                async {}
            })
            .await;
            assert_eq!(message(remote.unwrap_err()).as_deref(), Some("remote closure went wrong"));

            // In a call to this very shard: the same, and the caller, who
            // would once have gone down with it, carries on.
            let here = shard_id().unwrap();
            let local = submit_to(here, || async {
                yield_now().await;
                if shard_id().is_ok() {
                    panic!("local call went wrong");
                }
                1
            })
            .await;
            assert_eq!(message(local.unwrap_err()).as_deref(), Some("local call went wrong"));
            assert_eq!(submit_to(here, || async { 2 }).await.unwrap(), 2);

            // In blocking work, and with a payload that is not text.
            let blocking = run_blocking(|| {
                if std::env::args().count() > 0 {
                    panic!("blocking work went wrong");
                }
            })
            .await;
            assert_eq!(message(blocking.unwrap_err()).as_deref(), Some("blocking work went wrong"));
            let odd = spawn(async {
                if shard_id().is_ok() {
                    std::panic::panic_any(42u32);
                }
            });
            assert_eq!(message(odd.await.unwrap_err()), None);
        })
        .unwrap();

    // In the main future, where it ends the runtime.
    let ended = rt(1).run(|| async {
        if shard_id().is_ok() {
            panic!("main went wrong");
        }
    });
    let error = ended.unwrap_err();
    assert_eq!(error.to_string(), "task panicked: main went wrong");
    assert_eq!(message(error).as_deref(), Some("main went wrong"));
}

#[test]
fn a_buffered_read_that_is_given_up_loses_nothing() {
    rt(1)
        .run(|| async {
            let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
            let addr = listener.local_addr().unwrap();
            let client = spawn(async move { TcpStream::connect(addr).await.unwrap() });
            let server = listener.accept().await.unwrap();
            let client = client.await.unwrap();
            client.set_nodelay(true).unwrap();
            let mut reader = BufReader::new(&server);

            // Half a line arrives, and the reader gives up waiting for the
            // rest, several times over.
            let (result, _) = client.write_all("first ha").await;
            result.unwrap();
            for _ in 0..3 {
                let waited = timeout(Duration::from_millis(10), reader.next_line()).await;
                assert!(waited.is_err());
            }
            // The rest arrives while nobody is waiting, with more behind it.
            let (result, _) = client.write_all("lf\nsecond\nthi").await;
            result.unwrap();
            sleep(Duration::from_millis(10)).await;
            assert_eq!(reader.next_line().await.unwrap().as_deref(), Some("first half"));
            assert_eq!(reader.next_line().await.unwrap().as_deref(), Some("second"));

            // Given up by `select` this time, in the middle of a line.
            for _ in 0..3 {
                let raced = starfish::select(reader.next_line(), sleep(Duration::from_millis(5))).await;
                assert!(matches!(raced, starfish::Either::Second(())));
            }
            let (result, _) = client.write_all("rd\n").await;
            result.unwrap();
            assert_eq!(reader.next_line().await.unwrap().as_deref(), Some("third"));

            // And a reader dropped with a read under way does no harm:
            // the stream is still good for the next reader.
            let waited = timeout(Duration::from_millis(5), reader.next_line()).await;
            assert!(waited.is_err());
            drop(reader);
            let (result, _) = client.write_all("fourth\n").await;
            result.unwrap();
            client.shutdown(std::net::Shutdown::Write).unwrap();
            let mut reader = BufReader::new(&server);
            let mut rest = Vec::new();
            reader.read_until(0, &mut rest).await.unwrap();
            // Whatever the dropped read had not already taken.
            assert!(b"fourth\n".ends_with(&rest), "{rest:?}");
        })
        .unwrap();
}

#[test]
fn zero_copy_send_of_a_whole_buffer() {
    const SIZE: usize = 64 * 1024;
    rt(1)
        .fixed_buffers(4, SIZE)
        .run(|| async {
            let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
            let addr = listener.local_addr().unwrap();
            let receiver = spawn(async move {
                let stream = listener.accept().await.unwrap();
                let mut received = Vec::new();
                let mut buf = Vec::with_capacity(SIZE);
                loop {
                    // Slowly, so that the sender's buffers stay in use.
                    sleep(Duration::from_micros(200)).await;
                    let (result, filled) = stream.read(buf).await;
                    if result.unwrap() == 0 {
                        return received;
                    }
                    received.extend_from_slice(&filled);
                    buf = filled;
                }
            });
            let stream = TcpStream::connect(addr).await.unwrap();
            let mut expected = Vec::new();
            for round in 0..24u8 {
                let mut buf = starfish::buf::FixedBuf::acquire().await.unwrap();
                buf.set_len(SIZE);
                for (index, byte) in buf.iter_mut().enumerate() {
                    *byte = round.wrapping_add(index as u8);
                }
                expected.extend_from_slice(&buf);
                stream.write_all_zc(buf).await.unwrap();
            }
            stream.shutdown(std::net::Shutdown::Write).unwrap();
            let received = receiver.await.unwrap();
            assert_eq!(received.len(), expected.len());
            assert!(received == expected);
            // Every buffer is back in the pool.
            let mut all = Vec::new();
            for _ in 0..4 {
                all.push(timeout(Duration::from_secs(5), starfish::buf::FixedBuf::acquire()).await.unwrap().unwrap());
            }
        })
        .unwrap();
}

#[test]
fn dropped_sockets_are_closed() {
    let path = scratch("socket-churn");
    rt(1)
        .run({
            let path = path.clone();
            move || async move {
                use starfish::net::{UnixListener, UnixStream};
                let listener = UnixListener::bind(&path).unwrap();
                let server = spawn(async move {
                    let mut served = 0;
                    while let Ok(stream) = listener.accept().await {
                        drop(stream);
                        served += 1;
                        if served == 3000 {
                            break;
                        }
                    }
                    served
                });
                // Far more than a process may have open at once, were any
                // of them left open.
                for _ in 0..3000 {
                    let stream = UnixStream::connect(&path).await.unwrap();
                    // The server's close arrives as the end of the stream.
                    let (result, _) = stream.read(Vec::with_capacity(8)).await;
                    assert_eq!(result.unwrap(), 0);
                }
                assert_eq!(server.await.unwrap(), 3000);
                let sockets = std::fs::read_dir("/proc/self/fd")
                    .unwrap()
                    .filter_map(|entry| std::fs::read_link(entry.ok()?.path()).ok())
                    .filter(|target| target.to_string_lossy().starts_with("socket:"))
                    .count();
                assert!(sockets < 200, "{sockets} sockets still open");
                starfish::fs::remove_file(&path).await.unwrap();
            }
        })
        .unwrap();
}

#[test]
fn a_port_can_be_bound_for_one_listener_only() {
    use starfish::net::{BindOptions, UdpSocket};

    rt(1)
        .run(|| async {
            // By default a port is shared: that is how shards get a
            // listener each.
            let first = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
            let addr = first.local_addr().unwrap();
            let second = TcpListener::bind(addr).unwrap();
            drop((first, second));

            // Asked not to share, the second bind is refused, whichever
            // way round they come.
            let alone = BindOptions::new().reuse_port(false).backlog(16);
            let only = TcpListener::bind_with("127.0.0.1:0".parse().unwrap(), &alone).unwrap();
            let addr = only.local_addr().unwrap();
            let taken = TcpListener::bind(addr).err().unwrap();
            assert_eq!(taken.kind(), std::io::ErrorKind::AddrInUse);
            let taken = TcpListener::bind_with(addr, &alone).err().unwrap();
            assert_eq!(taken.kind(), std::io::ErrorKind::AddrInUse);
            // And it works as a listener.
            let client = spawn(async move { TcpStream::connect(addr).await.unwrap() });
            only.accept().await.unwrap();
            client.await.unwrap();

            let only = UdpSocket::bind_with("127.0.0.1:0".parse().unwrap(), &alone).unwrap();
            let addr = only.local_addr().unwrap();
            assert_eq!(UdpSocket::bind(addr).err().unwrap().kind(), std::io::ErrorKind::AddrInUse);
            let shared = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
            UdpSocket::bind(shared.local_addr().unwrap()).unwrap();
            assert_eq!(BindOptions::default(), BindOptions::new());
        })
        .unwrap();
}

#[test]
fn files_are_opened_the_way_the_options_say() {
    use starfish::fs::{remove_dir, remove_file, sync_dir, OpenOptions};

    let dir = scratch("open-options");
    rt(1)
        .run({
            let dir = dir.clone();
            move || async move {
                starfish::fs::create_dir(&dir).await.unwrap();
                let path = dir.join("file");
                let kind = |result: std::io::Result<File>| result.err().unwrap().kind();

                // Nothing is created unless asked for.
                assert_eq!(kind(OpenOptions::new().write(true).open(&path).await), std::io::ErrorKind::NotFound);
                // Exactly one creator.
                let created = OpenOptions::new().write(true).create_new(true).open(&path).await.unwrap();
                assert_eq!(
                    kind(OpenOptions::new().write(true).create_new(true).open(&path).await),
                    std::io::ErrorKind::AlreadyExists
                );
                let (result, _) = created.write_all_at("0123456789", 0).await;
                result.unwrap();
                sync_dir(&dir).await.unwrap();

                // Read and write without emptying the file.
                let both = OpenOptions::new().read(true).write(true).create(true).open(&path).await.unwrap();
                assert_eq!(both.size().await.unwrap(), 10);
                let (result, _) = both.write_all_at("ab", 2).await;
                result.unwrap();
                let (result, buf) = both.read_at(Vec::with_capacity(16), 0).await;
                result.unwrap();
                assert_eq!(buf, b"01ab456789");

                // Reading only: writes are refused by the kernel.
                let reader = OpenOptions::new().read(true).open(&path).await.unwrap();
                let (result, _) = reader.write_at("x", 0).await;
                assert!(result.is_err());

                // Appending: to the end, whatever offset is named.
                let log = OpenOptions::new().append(true).open(&path).await.unwrap();
                let (result, _) = log.write_all_at("-one", 0).await;
                result.unwrap();
                let (result, _) = log.write_all_at("-two", 3).await;
                result.unwrap();
                let (result, buf) = reader.read_at(Vec::with_capacity(32), 0).await;
                result.unwrap();
                assert_eq!(buf, b"01ab456789-one-two");

                // Emptying.
                OpenOptions::new().write(true).truncate(true).open(&path).await.unwrap();
                assert_eq!(reader.size().await.unwrap(), 0);

                // Combinations that make no sense are refused before the
                // kernel is asked.
                assert_eq!(kind(OpenOptions::new().open(&path).await), std::io::ErrorKind::InvalidInput);
                assert_eq!(
                    kind(OpenOptions::default().read(true).truncate(true).open(&path).await),
                    std::io::ErrorKind::InvalidInput
                );
                let direct = OpenOptions::new().append(true).open_direct(&path).await;
                assert_eq!(direct.err().unwrap().kind(), std::io::ErrorKind::InvalidInput);
                let direct = OpenOptions::new().read(true).write(true).mode(0o600).open_direct(&path).await.unwrap();
                assert!(direct.disk_alignment() > 0);

                // Only a directory can be synced as one.
                assert!(sync_dir(&path).await.is_err());
                remove_file(&path).await.unwrap();
                sync_dir(&dir).await.unwrap();
                remove_dir(&dir).await.unwrap();
            }
        })
        .unwrap();
}

#[test]
fn names_are_resolved_and_trees_removed_off_the_shard() {
    use starfish::fs::{create_dir_all, metadata, remove_dir_all};
    use starfish::net::lookup_host;

    let root = scratch("tree-to-remove");
    rt(1)
        .run({
            let root = root.clone();
            move || async move {
                let found = lookup_host("localhost", 8080).await.unwrap();
                assert!(!found.is_empty());
                assert!(found.iter().all(|addr| addr.ip().is_loopback() && addr.port() == 8080), "{found:?}");
                assert!(lookup_host("no-such-host.invalid", 1).await.is_err());

                create_dir_all(root.join("a/b/c")).await.unwrap();
                for name in ["a/one", "a/b/two", "a/b/c/three"] {
                    let file = File::create(root.join(name)).await.unwrap();
                    let (result, _) = file.write_all_at("x", 0).await;
                    result.unwrap();
                }
                // `remove_dir` would refuse; this takes the lot.
                assert!(starfish::fs::remove_dir(&root).await.is_err());
                remove_dir_all(&root).await.unwrap();
                assert_eq!(metadata(&root).await.unwrap_err().kind(), std::io::ErrorKind::NotFound);
                assert_eq!(remove_dir_all(&root).await.unwrap_err().kind(), std::io::ErrorKind::NotFound);
            }
        })
        .unwrap();
}

#[test]
fn invoke_on_each_reports_every_shard_separately() {
    rt(3)
        .run(|| async {
            let service = Sharded::start(|shard| Cell::new(shard * 10)).await.unwrap();
            let outcomes = service
                .invoke_on_each(|cell| async move {
                    assert!(cell.get() != 10, "the instance on shard 1 refuses");
                    cell.get()
                })
                .await
                .unwrap();
            assert_eq!(outcomes.len(), 3);
            assert!(matches!(outcomes[0], Ok(0)));
            assert!(matches!(outcomes[1], Err(Error::Panicked { .. })));
            assert!(matches!(outcomes[2], Ok(20)));
            // The same call through `invoke_on_all` keeps only the failure.
            let all = service
                .invoke_on_all(|cell| async move {
                    assert!(cell.get() != 10, "the instance on shard 1 refuses");
                })
                .await;
            assert!(matches!(all, Err(Error::Panicked { .. })));
            service.stop().await.unwrap();
        })
        .unwrap();
}

#[test]
fn sharded_services_can_be_built_and_finished_by_futures() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    rt(3)
        .run(|| async {
            // Built by a future, which may wait.
            let service = Sharded::start_with(|shard| async move {
                sleep(Duration::from_millis(1)).await;
                Cell::new(shard * 10)
            })
            .await
            .unwrap();
            let values = service.invoke_on_all(|cell| async move { cell.get() }).await.unwrap();
            assert_eq!(values, [0, 10, 20]);

            // Finished by a future as well, on every shard, before the
            // instances go.
            let finished = Arc::new(AtomicUsize::new(0));
            let counter = finished.clone();
            service
                .stop_with(move |cell| {
                    let counter = counter.clone();
                    async move {
                        yield_now().await;
                        counter.fetch_add(cell.get() + 1, Ordering::SeqCst);
                    }
                })
                .await
                .unwrap();
            assert_eq!(finished.load(Ordering::SeqCst), 1 + 11 + 21);
            assert!(matches!(service.local(), Err(Error::ServiceStopped)));

            // A factory that fails on one shard leaves no instance
            // anywhere.
            let built = Arc::new(AtomicUsize::new(0));
            let counter = built.clone();
            let failed = Sharded::<usize>::try_start_with(move |shard| {
                let counter = counter.clone();
                async move {
                    if shard == 1 {
                        return Err(std::io::Error::other("shard 1 cannot start"));
                    }
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok(shard)
                }
            })
            .await;
            let Err(Error::Io(error)) = failed else { panic!("it started") };
            assert_eq!(error.to_string(), "shard 1 cannot start");

            // Every shard's outcome, where `submit_to_all` gives the first
            // failure only.
            let outcomes = starfish::submit_to_each(|| async {
                let shard = shard_id().unwrap();
                if shard == 1 {
                    panic!("shard 1 fails");
                }
                shard
            })
            .await
            .unwrap();
            assert_eq!(outcomes.len(), 3);
            assert!(matches!(outcomes[0], Ok(0)));
            assert!(matches!(&outcomes[1], Err(Error::Panicked { message: Some(text) }) if text == "shard 1 fails"));
            assert!(matches!(outcomes[2], Ok(2)));

            let stats = starfish::stats::all_shards().await.unwrap();
            let shards: Vec<_> = stats.iter().map(|shard| shard.shard).collect();
            assert_eq!(shards, [0, 1, 2]);
            assert!(stats.iter().all(|shard| shard.loops > 0));
        })
        .unwrap();
}

#[test]
fn select_among_three_and_among_many() {
    use starfish::{select3, select_all, Either3};

    rt(1)
        .run(|| async {
            let (sender, mut inbox) = channel::<u8>(1);
            let nothing = std::future::pending::<()>();
            let first = select3(nothing, inbox.recv(), sleep(Duration::from_millis(10))).await;
            assert_eq!(first, Either3::Third(()));
            sender.send(9).await.unwrap();
            let second = select3(std::future::pending::<()>(), inbox.recv(), sleep(Duration::from_secs(5))).await;
            assert_eq!(second, Either3::Second(Some(9)));
            assert_eq!(select3(async { 1 }, async { 2 }, async { 3 }).await, Either3::First(1));

            // Among many of one kind: the quickest, and which it was.
            let sleeps = [30u64, 5, 20].map(|millis| async move {
                sleep(Duration::from_millis(millis)).await;
                millis
            });
            assert_eq!(select_all(sleeps).await, (1, 5));
            // None is favoured when several are ready time after time.
            let mut wins = [0u32; 3];
            for _ in 0..30 {
                let ready = (0..3).map(|n| async move {
                    yield_now().await;
                    n
                });
                wins[select_all(ready).await.0] += 1;
            }
            assert!(wins.iter().all(|count| *count > 0), "{wins:?}");
            // An empty list never finishes.
            let empty = select_all(Vec::<std::future::Ready<()>>::new());
            assert!(timeout(Duration::from_millis(5), empty).await.is_err());
        })
        .unwrap();
}

#[test]
fn stall_handlers_may_capture_and_pressure_functions_be_removed() {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    let stalled = Arc::new(AtomicU64::new(0));
    let counter = stalled.clone();
    rt(1)
        .stall_threshold(Duration::from_millis(5))
        // A closure with something of its own, where a bare function was
        // all that could be given before.
        .on_stall(move |stall| {
            counter.fetch_add(stall.duration.as_millis() as u64, Ordering::SeqCst);
        })
        .memory_limit(8 << 20)
        .run(|| async {
            hog_the_cpu_for_a_while(Duration::from_millis(20));
            yield_now().await;

            let calls = Rc::new(Cell::new(0u32));
            let hook = starfish::alloc::on_pressure({
                let calls = calls.clone();
                move || calls.set(calls.get() + 1)
            })
            .unwrap();
            // Over the limit: the function is asked to free memory.
            let ballast = vec![1u8; 16 << 20];
            for _ in 0..200 {
                yield_now().await;
            }
            assert!(calls.get() > 0);

            hook.remove();
            let before = calls.get();
            for _ in 0..200 {
                yield_now().await;
            }
            assert_eq!(calls.get(), before);
            drop(ballast);
        })
        .unwrap();
    assert!(stalled.load(Ordering::SeqCst) >= 15, "{stalled:?}");
}

#[test]
fn a_signal_goes_back_to_what_it_did_when_nobody_listens() {
    /// What the process does with a signal right now.
    fn handler(signal: i32) -> usize {
        // SAFETY: asks the kernel for the current action without changing
        // it; the structure is filled in by the call.
        unsafe {
            let mut current: libc::sigaction = std::mem::zeroed();
            libc::sigaction(signal, std::ptr::null(), &mut current);
            current.sa_sigaction
        }
    }

    // A signal no other test uses, and which is ignored by default.
    let signal = libc::SIGWINCH;
    let default = handler(signal);
    rt(1)
        .run(move || async move {
            let first = Signals::new(&[signal]).unwrap();
            let mut second = Signals::new(&[signal, signal]).unwrap();
            assert_ne!(handler(signal), default);
            drop(first);
            // Still listened for.
            assert_ne!(handler(signal), default);
            send_signal_to_self(signal);
            assert_eq!(timeout(Duration::from_secs(5), second.next()).await.unwrap(), signal);
            drop(second);
            // Nobody listens: back to the default.
            assert_eq!(handler(signal), default);

            // A list that cannot all be listened for leaves nothing behind.
            assert!(Signals::new(&[signal, libc::SIGKILL]).is_err());
            assert_eq!(handler(signal), default);
        })
        .unwrap();
}
