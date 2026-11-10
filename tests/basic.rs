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
use starfish::sync::{channel, shared_channel, Gate, Semaphore};
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
    assert!(matches!(panicked, Err(Error::Panicked)));
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
            assert!(matches!(in_future, Err(Error::Panicked)));

            #[allow(unreachable_code)]
            let in_closure = submit_to(2, || {
                panic!("boom before the future exists");
                async { 5 }
            })
            .await;
            assert!(matches!(in_closure, Err(Error::Panicked)));

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
            assert!(matches!(service, Err(Error::Panicked)));

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
    assert!(matches!(in_future, Err(Error::Panicked)));

    #[allow(unreachable_code)]
    let in_closure = rt(2).run(|| {
        panic!("boom before main's future exists");
        async {}
    });
    assert!(matches!(in_closure, Err(Error::Panicked)));

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
            let exact = file.alloc(1).unwrap();
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
                let mut sent = stream.send_zc(buf).await.unwrap();
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
            submit_to(1, || async {}).await.unwrap();
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

            // One that does not fit is cut to the buffer.
            let (result, _) = client.send_to(vec![9u8; 3000], server_addr).await;
            assert_eq!(result.unwrap(), 3000);
            let (result, buf) = server.recv_from(Vec::with_capacity(100)).await;
            assert_eq!(result.unwrap().0, 100);
            assert!(buf.len() == 100 && buf.iter().all(|&byte| byte == 9));

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
            let (mut sender, mut receiver) = shared_channel(4);
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
            let (mut sender, receiver) = shared_channel(2);
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
            let (sender, mut receiver) = shared_channel::<u32>(2);
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
                let block = file.alloc(1).unwrap().capacity();
                file.allocate(0, 4 * block as u64).await.unwrap();

                // Whole blocks go in; the file is then cut to what counts.
                let mut buf = file.alloc(block).unwrap();
                buf.fill_padded(b"twelve bytes");
                let (result, _) = file.write_at(buf, 0).await;
                assert_eq!(result.unwrap(), block);
                file.set_len(12).await.unwrap();
                file.sync_data().await.unwrap();
                assert_eq!(file.size().await.unwrap(), 12);

                // A read of whole blocks stops at the end of the file.
                let (result, buf) = file.read_at(file.alloc(block).unwrap(), 0).await;
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
