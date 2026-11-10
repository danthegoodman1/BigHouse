#!/usr/bin/env python3
"""Benchmarks `inodedb` metadata against `local` metadata on one server over s3proxy.

Three disks take the same work: `local` metadata, `inodedb` metadata, and `inodedb` with inline files off. The
inserts run twice more with `fsync_after_insert` and `fsync_part_directory` on: on the `local` metadata disk, where
they make no fsync, and on the server's own drive, where they sync every file and directory, which is what a durable
insert costs there.

- Inserts: small inserts one after another, then the same number from 8 threads, with merges stopped. Reports
  inserts per second, and object-storage writes, InodeDB commits and fsyncs per insert.
- Cold start: a table of many parts on each disk in turn. Restarts the server with only that table attached and
  reports the seconds until the table answers, and object-storage and InodeDB reads during the load.

The inserts and the cold starts run `--rounds` times, with the disks in a different order each round, and the table
reports each measure's median. Writing the tables of many parts runs once.

Each InodeDB commit waits for its store's sync. `--tmpfs` runs an InodeDB of its own on 127.0.0.1:7071 with its
store in /dev/shm, which stands in for a drive whose syncs take microseconds.

    python3 tests/bench.py [--inserts N] [--parts N] [--rounds N] [--tmpfs]

Prints a Markdown table and writes .dev/bench/results.json (results-tmpfs.json with `--tmpfs`). Needs
`scripts/dev-up` and a built server.
"""

from __future__ import annotations

import argparse
import json
import shutil
import statistics
import socket
import subprocess
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bighouse import stack  # noqa: E402

DISKS = ("local", "inodedb", "inodedb_blobs")
# The inserts' extra rows: the `local` disk and the server's own drive, each with MergeTree's fsync settings on.
FSYNC = "fsync_after_insert = 1, fsync_part_directory = 1"
INSERTS_ONLY = {"local_fsync": f"storage_policy = 'local', {FSYNC}", "default_fsync": f"storage_policy = 'default', {FSYNC}"}
NAMES = {"local_fsync": "`local`, fsync on", "default_fsync": "`default`, fsync on"}
WRITES = ("DiskS3PutObject", "DiskS3CreateMultipartUpload", "DiskS3UploadPart")
READS = ("DiskS3GetObject", "DiskS3HeadObject")
INODEDB_READS = ("InodeDBStat", "InodeDBRead", "InodeDBList")
SYNCS = ("FileSync", "DirectorySync")
# Each insert writes its own parts: the async insert queue would batch them and time its flushes instead.
SYNC = {"async_insert": 0}


def start_tmpfs_inodedb(run: str) -> tuple[subprocess.Popen, Path]:
    """An InodeDB on 127.0.0.1:7071 with its SQLite store in /dev/shm, which `stack` then uses."""
    base = Path("/dev/shm") / f"bighouse-bench-{run}"
    base.mkdir()
    (base / "inodedb.toml").write_text(
        f'listen = "127.0.0.1:7071"\nshallow_idle = false\n\n[storage]\nbackend = "sqlite"\npath = "{base}/db.sqlite"\n')
    process = subprocess.Popen([str(stack.INODEDB), "--config", str(base / "inodedb.toml"), "--init-key", str(base / "admin.key")],
                               stdout=open(base / "server.log", "wb"), stderr=subprocess.STDOUT)
    deadline = time.monotonic() + 30
    while True:
        if process.poll() is not None or time.monotonic() > deadline:
            raise RuntimeError(f"InodeDB on tmpfs did not start; see {base}/server.log")
        try:
            if (base / "admin.key").stat().st_size > 0:
                socket.create_connection(("127.0.0.1", 7071), timeout=1).close()
                break
        except OSError:
            pass
        time.sleep(0.1)
    stack.INODEDB_ENDPOINT = "127.0.0.1:7071"
    stack.INODEDB_ADMIN_KEY = base / "admin.key"
    return process, base


def make_server(run: str) -> stack.Server:
    prefix = f"bench/{run}"
    return stack.Server(
        name=f"bench-{run}",
        tcp_port=19700,
        http_port=18800,
        interserver_port=19709,
        disks=[
            stack.local_metadata_disk_xml("local", s3_prefix=f"{prefix}/local"),
            stack.inodedb_disk_xml("inodedb", s3_prefix=f"{prefix}/inodedb", key=stack.mint_key(f"{prefix}/inodedb", "write")),
            stack.inodedb_disk_xml("inodedb_blobs", s3_prefix=f"{prefix}/inodedb_blobs", key=stack.mint_key(f"{prefix}/inodedb_blobs", "write"),
                                   extra="<inodedb_inline_file_max_bytes>0</inodedb_inline_file_max_bytes>"),
        ],
        policies=[stack.policy_xml(disk, disk) for disk in DISKS],
    )


def events(server: stack.Server, names: tuple[str, ...]) -> int:
    listed = ", ".join(f"'{name}'" for name in names)
    return int(server.value(f"SELECT sum(value) FROM system.events WHERE event IN ({listed})") or 0)


def bench_inserts(server: stack.Server, disk: str, count: int, round_: int) -> dict[str, float]:
    table = f"bench.inserts_{disk}_{round_}"
    placement = INSERTS_ONLY.get(disk, f"storage_policy = '{disk}'")
    server.query(f"CREATE TABLE {table} (k UInt64, v String, t DateTime) ENGINE = MergeTree ORDER BY k "
                 f"SETTINGS {placement}, parts_to_throw_insert = 100000, parts_to_delay_insert = 100000")
    server.query(f"SYSTEM STOP MERGES {table}")
    insert = f"INSERT INTO {table} SELECT number, toString(number), now() FROM numbers(1000)"
    result = {}

    writes, commits, syncs = events(server, WRITES), events(server, ("InodeDBCommit",)), events(server, SYNCS)
    started = time.monotonic()
    for _ in range(count):
        server.query(insert, settings=SYNC)
    result["sequential_inserts_per_s"] = count / (time.monotonic() - started)
    result["writes_per_insert"] = (events(server, WRITES) - writes) / count
    result["commits_per_insert"] = (events(server, ("InodeDBCommit",)) - commits) / count
    result["fsyncs_per_insert"] = (events(server, SYNCS) - syncs) / count

    started = time.monotonic()
    with ThreadPoolExecutor(8) as pool:
        list(pool.map(lambda _: server.query(insert, settings=SYNC), range(count)))
    result["parallel_inserts_per_s"] = count / (time.monotonic() - started)

    assert server.value(f"SELECT count() FROM {table}") == str(2 * count * 1000)
    return result


def fill_parts(server: stack.Server, disk: str, parts: int) -> None:
    """A table of `parts` one-row parts, written by 16 inserts at once."""
    table = f"bench.parts_{disk}"
    server.query(f"CREATE TABLE {table} (k UInt64, v String) ENGINE = MergeTree PARTITION BY k ORDER BY k "
                 f"SETTINGS storage_policy = '{disk}', parts_to_throw_insert = 1000000, parts_to_delay_insert = 1000000")
    server.query(f"SYSTEM STOP MERGES {table}")
    step = -(-parts // 16)

    def insert(start: int) -> None:
        server.query(f"INSERT INTO {table} SELECT number, toString(number) FROM numbers({start}, {min(step, parts - start)})",
                     settings={**SYNC, "max_partitions_per_insert_block": 0, "throw_on_max_partitions_per_insert_block": 0})

    with ThreadPoolExecutor(16) as pool:
        list(pool.map(insert, range(0, parts, step)))
    assert server.value(f"SELECT count() FROM system.parts WHERE database = 'bench' AND table = 'parts_{disk}' AND active") == str(parts)


def bench_cold_start(server: stack.Server, disk: str, parts: int) -> dict[str, float]:
    """Restarts the server with only `bench.parts_<disk>` attached and times its load."""
    for other in DISKS:
        if other != disk:
            server.query(f"DETACH TABLE bench.parts_{other} PERMANENTLY")
    server.stop()
    started = time.monotonic()
    server.start()
    while True:
        try:
            if server.value(f"SELECT count() FROM bench.parts_{disk}") == str(parts):
                break
        except stack.QueryError:
            pass
        time.sleep(0.01)
    result = {"cold_start_s": time.monotonic() - started, "reads_during_load": events(server, READS),
              "inodedb_reads_during_load": events(server, INODEDB_READS)}
    for other in DISKS:
        if other != disk:
            server.query(f"ATTACH TABLE bench.parts_{other}")
    return result


def rotated(disks: tuple[str, ...], round_: int) -> tuple[str, ...]:
    """`disks`, starting from the one `round_` places first."""
    first = round_ % len(disks)
    return disks[first:] + disks[:first]


def medians(samples: list[dict[str, float]]) -> dict[str, float]:
    return {key: statistics.median(sample[key] for sample in samples) for key in samples[0]}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--inserts", type=int, default=200)
    parser.add_argument("--parts", type=int, default=10_000)
    parser.add_argument("--rounds", type=int, default=3, help="rounds of the inserts and of the cold starts")
    parser.add_argument("--tmpfs", action="store_true", help="run InodeDB with its store in /dev/shm")
    args = parser.parse_args()

    run = uuid.uuid4().hex[:8]
    inodedb = start_tmpfs_inodedb(run) if args.tmpfs else None
    server = make_server(run)
    server.start()
    inserts = (*DISKS, *INSERTS_ONLY)
    results: dict[str, dict[str, float]] = {disk: {} for disk in inserts}
    samples: dict[str, list[dict[str, float]]] = {disk: [] for disk in inserts}
    try:
        server.query("CREATE DATABASE bench")
        for round_ in range(args.rounds):
            for disk in rotated(inserts, round_):
                samples[disk].append(bench_inserts(server, disk, args.inserts, round_))
                print(f"{disk}: inserts, round {round_ + 1}: {samples[disk][-1]}", flush=True)
        for disk in DISKS:
            started = time.monotonic()
            fill_parts(server, disk, args.parts)
            results[disk]["fill_parts_s"] = time.monotonic() - started
            print(f"{disk}: {args.parts} parts in {results[disk]['fill_parts_s']:.1f} s", flush=True)
        # Each disk's first cold start follows the writes, so each round puts a different disk first.
        cold_starts: dict[str, list[dict[str, float]]] = {disk: [] for disk in DISKS}
        for round_ in range(args.rounds):
            for disk in rotated(DISKS, round_):
                cold_starts[disk].append(bench_cold_start(server, disk, args.parts))
                print(f"{disk}: cold start, round {round_ + 1}: {cold_starts[disk][-1]}", flush=True)
        for disk in inserts:
            results[disk] |= medians(samples[disk])
        for disk in DISKS:
            results[disk] |= medians(cold_starts[disk])
    finally:
        server.stop()
        server.wipe()
        if inodedb:
            inodedb[0].terminate()
            inodedb[0].wait(30)
            shutil.rmtree(inodedb[1])

    out = stack.DEV / ("bench/results-tmpfs.json" if args.tmpfs else "bench/results.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({"inserts": args.inserts, "parts": args.parts, "rounds": args.rounds, "results": results,
                               "samples": samples, "cold_starts": cold_starts}, indent=1))

    print(f"\n| Metadata | Inserts/s, 1 thread | Inserts/s, 8 threads | S3 writes per insert | InodeDB commits per insert "
          f"| fsyncs per insert | {args.parts:,} parts written (s) | Cold start (s) | S3 reads at load | InodeDB reads at load |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for disk in (*DISKS, *INSERTS_ONLY):
        r = results[disk]
        name = NAMES.get(disk, f"`{disk}`")
        rest = (f"{r['fill_parts_s']:.1f} | {r['cold_start_s']:.2f} | {r['reads_during_load']:,.0f} | {r['inodedb_reads_during_load']:,.0f}"
                if disk in DISKS else "| | |")
        print(f"| {name} | {r['sequential_inserts_per_s']:.0f} | {r['parallel_inserts_per_s']:.0f} | {r['writes_per_insert']:.1f} "
              f"| {r['commits_per_insert']:.1f} | {r['fsyncs_per_insert']:.1f} | {rest} |")
    return 0


if __name__ == "__main__":
    sys.exit(main())
