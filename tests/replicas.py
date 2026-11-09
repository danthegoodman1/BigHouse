#!/usr/bin/env python3
"""A writer and a long-lived read replica over one `inodedb` disk.

- The replica sees a new part, a dropped partition and a truncation within a few refreshes.
- A query on the replica keeps reading parts the writer merged away and deleted, because the replica's InodeDB
  lease holds their blobs; once the replica stops, it releases the lease, and the writer deletes them.

    python3 tests/replicas.py
"""

from __future__ import annotations

import sys
import threading
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bighouse import catalog, fleet, stack  # noqa: E402

failures = 0


def check(ok: bool, what: str, detail: str = "") -> None:
    global failures
    failures += not ok
    print(f"{'PASS' if ok else 'FAIL'} {what}" + (f": {detail}" if detail and not ok else ""))


def eventually(fn, want: str, timeout: float = 10) -> tuple[bool, str]:
    deadline = time.monotonic() + timeout
    got = ""
    while time.monotonic() < deadline:
        got = fn()
        if got == want:
            return True, got
        time.sleep(0.2)
    return False, got


def main() -> int:
    run = uuid.uuid4().hex[:8]
    root, prefix = f"replicas/{run}", f"replicas/{run}"
    writer = stack.Server(
        name=f"replicas-{run}-writer", tcp_port=19500, http_port=18600, interserver_port=19509,
        disks=[stack.inodedb_disk_xml(fleet.DISK, s3_prefix=prefix, key=stack.mint_key(root, "write"))],
        policies=[stack.policy_xml(fleet.DISK, fleet.DISK)],
        extra_config="<merge_tree><storage_policy>bighouse</storage_policy><old_parts_lifetime>1</old_parts_lifetime><merge_tree_clear_old_parts_interval_seconds>1</merge_tree_clear_old_parts_interval_seconds><cleanup_delay_period>1</cleanup_delay_period><cleanup_delay_period_random_add>0</cleanup_delay_period_random_add><max_cleanup_delay_period>1</max_cleanup_delay_period></merge_tree>"
                     + fleet.database_disk_xml(),
    )
    replica_key = stack.mint_key(root, "read")
    replica = stack.Server(
        name=f"replicas-{run}-replica", tcp_port=19510, http_port=18610, interserver_port=19519,
        disks=[fleet.replica_disk_xml(s3_prefix=prefix, key=replica_key)],
        policies=[stack.policy_xml(fleet.DISK, fleet.DISK)],
        # Every table the replica attaches refreshes its parts each second.
        extra_config=f"<merge_tree><storage_policy>{fleet.DISK}</storage_policy><refresh_parts_interval>1</refresh_parts_interval></merge_tree>",
    )
    writer.start()
    replica.start()
    try:
        writer.query("CREATE DATABASE r")
        # Random strings keep blobs in every part, beyond what fits inline in InodeDB.
        writer.query("CREATE TABLE r.t (p UInt8, k UInt64, v String DEFAULT randomPrintableASCII(100)) "
                     "ENGINE = MergeTree PARTITION BY p ORDER BY k")
        writer.query("INSERT INTO r.t (p, k) SELECT number % 3, number FROM numbers(3000)")

        catalog.attach(replica, replica_key)
        count = lambda: replica.value("SELECT count() FROM r.t")  # noqa: E731
        check(count() == "3000", "the replica attaches the writer's database from InodeDB")

        writer.query("INSERT INTO r.t (p, k) SELECT 3, number FROM numbers(500)")
        ok, got = eventually(count, "3500")
        check(ok, "the replica sees a new part", got)

        writer.query("ALTER TABLE r.t DROP PARTITION 0")
        ok, got = eventually(count, "2500")
        check(ok, "the replica stops seeing a dropped partition", got)

        writer.query("TRUNCATE TABLE r.t")
        ok, got = eventually(count, "0")
        check(ok, "the replica sees a truncation", got)

        # A slow query on the replica, while the writer merges its parts and deletes the old ones.
        writer.query("INSERT INTO r.t (p, k) SELECT 1, number FROM numbers(2000)")
        writer.query("INSERT INTO r.t (p, k) SELECT 1, number + 2000 FROM numbers(2000)")
        ok, _ = eventually(count, "4000")
        check(ok, "the replica sees two new parts")
        before = set(stack.s3_objects(prefix))
        check(bool(before), "the parts keep blobs", "none")

        answer: dict[str, str] = {}

        def slow_query() -> None:
            try:
                answer["value"] = replica.value(
                    "SELECT count(), sum(k) FROM r.t WHERE sleepEachRow(0.001) = 0",
                    settings={"max_block_size": 100, "max_threads": 1, "max_execution_time": 120},
                )
            except Exception as error:  # noqa: BLE001
                answer["value"] = f"error: {error}"

        reader = threading.Thread(target=slow_query)
        reader.start()
        time.sleep(0.5)
        writer.query("OPTIMIZE TABLE r.t FINAL")
        # Dropping and truncating leave empty covering parts, so wait for the inactive parts to go.
        ok, got = eventually(lambda: writer.value("SELECT countIf(NOT active), countIf(active AND rows > 0) FROM system.parts WHERE database = 'r' AND table = 't'"), "0\t1", timeout=30)
        check(ok, "the writer merges and deletes the old parts", got)
        reader.join()
        check(answer.get("value") == "4000\t7998000", "the replica's slow query reads parts the writer deleted", answer.get("value", ""))

        held = before - set(stack.s3_objects(prefix))
        check(not held, "the writer deletes none of their blobs while the replica's lease holds them", f"{len(held)} gone")

        # A stopping replica releases its lease, so the blobs go well before the lease's 30-second TTL would end it.
        replica.stop()
        stopped_at = time.monotonic()
        ok, _ = eventually(lambda: str(bool(before - set(stack.s3_objects(prefix)))), "True", timeout=20)
        check(ok, "once the replica stops and releases its lease, the writer deletes the merged-away blobs")
        print(f"    deleted {time.monotonic() - stopped_at:.1f} s after the replica stopped")
    finally:
        replica.stop()
        writer.stop()
    print(f"{failures} failed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
