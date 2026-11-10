#!/usr/bin/env python3
"""Kills a writer with SIGKILL while it inserts, merges, mutates and removes parts on an `inodedb` disk, restarts
it, and checks what it finds:

- the server starts, and `CHECK TABLE` passes;
- every insert acknowledged before the kill is there, whole;
- every other insert is there whole or not at all, since a part publishes in one rename;
- every blob a record names exists in object storage.

    python3 tests/crash.py [rounds]       20 by default
"""

from __future__ import annotations

import itertools
import random
import sys
import threading
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bighouse import stack  # noqa: E402

BATCH = 1000


def make_server(run: str) -> stack.Server:
    key = stack.mint_key(f"crash/{run}", "write")
    return stack.Server(
        name=f"crash-{run}",
        tcp_port=19300,
        http_port=18400,
        interserver_port=19309,
        disks=[stack.inodedb_disk_xml("s3_inodedb", s3_prefix=f"crash/{run}", key=key)],
        policies=[stack.policy_xml("s3_inodedb", "s3_inodedb")],
        extra_config="<merge_tree><storage_policy>s3_inodedb</storage_policy><old_parts_lifetime>1</old_parts_lifetime><merge_tree_clear_old_parts_interval_seconds>1</merge_tree_clear_old_parts_interval_seconds><cleanup_delay_period>1</cleanup_delay_period><cleanup_delay_period_random_add>0</cleanup_delay_period_random_add><max_cleanup_delay_period>1</max_cleanup_delay_period></merge_tree>",
    )


def workload(server: stack.Server, acked: set[int], batches: itertools.count, stop: threading.Event, errors: list[str]) -> None:
    rng = random.Random()
    while not stop.is_set():
        batch = next(batches)
        try:
            action = rng.random()
            if action < 0.75:
                server.query(f"INSERT INTO crash.t (batch, n) SELECT {batch}, number FROM numbers({BATCH})", timeout=30)
                acked.add(batch)
            elif action < 0.85:
                server.query("OPTIMIZE TABLE crash.t", timeout=30)
            elif action < 0.95:
                server.query("ALTER TABLE crash.t UPDATE touched = touched + 1 WHERE n % 7 = 0", timeout=30)
            else:
                server.query(f"INSERT INTO crash.scratch SELECT number FROM numbers({BATCH})", timeout=30)
                server.query("TRUNCATE TABLE crash.scratch", timeout=30)
        except stack.QueryError as error:
            # The kill may cut a query off mid-flight; anything else is a failure.
            if not stop.is_set():
                errors.append(str(error)[:300])
        except OSError:
            return


def check(server: stack.Server, acked: set[int]) -> list[str]:
    problems = []
    check_table = server.query("CHECK TABLE crash.t SETTINGS check_query_single_value_result = 1").strip()
    if check_table != "1":
        problems.append(f"CHECK TABLE says {check_table}")

    counts = {
        int(batch): int(count)
        for batch, count in (line.split("\t") for line in server.query("SELECT batch, count() FROM crash.t GROUP BY batch FORMAT TSV").splitlines())
    }
    missing = sorted(b for b in acked if counts.get(b) != BATCH)
    if missing:
        problems.append(f"acknowledged batches missing or partial: {missing[:10]}")
    partial = sorted(b for b, c in counts.items() if c != BATCH)
    if partial:
        problems.append(f"partial batches: {partial[:10]}")

    paths = server.query(
        "SELECT DISTINCT remote_path FROM system.remote_data_paths WHERE disk_name = 's3_inodedb' FORMAT TSV"
    ).splitlines()
    present = {Path(p).name for p in stack.s3_objects(server.s3_prefix)}
    lost = [p for p in paths if Path(p).name not in present]
    if lost:
        problems.append(f"{len(lost)} blobs that records name are gone, e.g. {lost[:3]}")
    return problems


def main() -> int:
    rounds = int(sys.argv[1]) if len(sys.argv) > 1 else 20
    run = uuid.uuid4().hex[:8]
    server = make_server(run)
    server.s3_prefix = f"crash/{run}"
    server.start()
    server.query("CREATE DATABASE crash")
    server.query("CREATE TABLE crash.t (batch UInt64, n UInt64, touched UInt64 DEFAULT 0) ENGINE = MergeTree ORDER BY (batch, n)")
    server.query("CREATE TABLE crash.scratch (n UInt64) ENGINE = MergeTree ORDER BY n")

    acked: set[int] = set()
    batches = itertools.count()
    failures = 0
    rng = random.Random(run)
    try:
        for round_number in range(rounds):
            stop = threading.Event()
            errors: list[str] = []
            workers = [threading.Thread(target=workload, args=(server, acked, batches, stop, errors)) for _ in range(4)]
            for worker in workers:
                worker.start()
            time.sleep(rng.uniform(0.2, 2.0))
            server.kill()
            stop.set()
            for worker in workers:
                worker.join()

            started = time.monotonic()
            server.start()
            problems = check(server, acked) + [f"query failed before the kill: {e}" for e in errors[:3]]
            failures += bool(problems)
            print(f"{'PASS' if not problems else 'FAIL'} round {round_number}: {len(acked)} batches acknowledged, "
                  f"restart took {time.monotonic() - started:.1f} s" + "".join(f"\n    {p}" for p in problems))
    finally:
        server.stop()

    orphans = len(stack.s3_objects(server.s3_prefix))
    print(f"{failures} failed; {orphans} objects under the disk's prefix")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
