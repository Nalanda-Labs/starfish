#!/usr/bin/env python3
"""Soak test for the kvdb example: load, hard kills and verification.

Starts the server, writes to it from several connections, kills it with
SIGKILL at a random moment, starts it again and checks that every write the
server acknowledged is there. Repeats for a number of rounds. Compaction is
made frequent so that kills land in and around it.

    cargo build --release --example kvdb
    python3 scripts/kvdb_soak.py [rounds] [directory]

The directory (default target/kvdb-soak) must be on a filesystem that
supports O_DIRECT; it is emptied first. Exits with status 1 if a write was
lost.
"""
import os, random, shutil, signal, socket, subprocess, sys, threading, time

ROUNDS = int(sys.argv[1]) if len(sys.argv) > 1 else 20
DIRECTORY = sys.argv[2] if len(sys.argv) > 2 else "target/kvdb-soak"
SERVER = "target/release/examples/kvdb"
PORT = 7006
WRITERS = 16
KEYS_PER_WRITER = 300


def connect():
    return socket.create_connection(("127.0.0.1", PORT), timeout=10).makefile("rwb")


def ask(conn, line):
    conn.write(line.encode() + b"\n")
    conn.flush()
    return conn.readline().decode().rstrip("\n")


def start():
    env = dict(os.environ, KVDB_COMPACT_AFTER="150000", KVDB_SPIN=random.choice(["0", "100"]))
    server = subprocess.Popen([SERVER, DIRECTORY], env=env, stdout=subprocess.PIPE, text=True)
    banner = server.stdout.readline().strip()  # printed once it is listening
    return server, banner


def check(conn, acknowledged, in_flight):
    """Compares the server's data with what it acknowledged.

    A write that was in flight when the server stopped may or may not have
    happened; whichever it was is what the server must show from now on.
    Returns the number of keys that hold neither value.
    """
    wrong = 0
    for key in set(acknowledged) | set(in_flight):
        found = ask(conn, "GET " + key)
        if key in in_flight and found == in_flight[key]:
            acknowledged[key] = found
        elif found != acknowledged.get(key, "(none)"):
            wrong += 1
            print(f"  {key}: expected {acknowledged.get(key)!r:.60}, in flight {in_flight.get(key)!r:.60}, found {found!r:.60}")
    in_flight.clear()
    return wrong


def main():
    shutil.rmtree(DIRECTORY, ignore_errors=True)
    random.seed(12345)
    acknowledged = {}  # key -> last value the server said OK to
    in_flight = {}  # key -> value sent but not answered when the kill came
    lost = 0
    for round_number in range(1, ROUNDS + 1):
        server, banner = start()

        # Every acknowledged write must be there. A write that was in
        # flight when the server died may or may not have happened.
        wrong = check(connect(), acknowledged, in_flight)
        lost += wrong

        stop = threading.Event()
        lock = threading.Lock()

        def writer(number):
            try:
                conn = connect()
                count = 0
                while not stop.is_set():
                    key = f"w{number}-{random.randrange(KEYS_PER_WRITER)}"
                    if random.random() < 0.1:
                        request, value = f"DEL {key}", "(none)"
                    else:
                        value = f"r{round_number}-{count}-" + "x" * random.randrange(400)
                        request = f"SET {key} {value}"
                    with lock:
                        in_flight[key] = value
                    answer = ask(conn, request)
                    if answer not in ("OK", "0", "1"):
                        return
                    with lock:
                        acknowledged[key] = value
                        in_flight.pop(key, None)
                    count += 1
            except OSError:
                pass

        threads = [threading.Thread(target=writer, args=(n,)) for n in range(WRITERS)]
        for thread in threads:
            thread.start()
        time.sleep(random.uniform(0.2, 2.0))
        clean = round_number % 5 == 0
        server.send_signal(signal.SIGTERM if clean else signal.SIGKILL)
        stop.set()
        for thread in threads:
            thread.join()
        server.wait()
        print(
            f"round {round_number:>3}: {banner.split(': ', 1)[-1]}; {wrong} wrong; "
            f"{'stopped' if clean else 'killed'} with {len(acknowledged)} keys written, "
            f"{len(in_flight)} in flight"
        )

    # A last start to check the final state.
    server, banner = start()
    wrong = check(connect(), acknowledged, in_flight)
    lost += wrong
    server.send_signal(signal.SIGTERM)
    server.wait()
    print(f"final check: {wrong} wrong of {len(acknowledged)}")
    print("PASSED" if lost == 0 else f"FAILED: {lost} acknowledged writes were lost")
    sys.exit(0 if lost == 0 else 1)


if __name__ == "__main__":
    main()
