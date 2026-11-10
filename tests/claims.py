#!/usr/bin/env python3
"""A writer killed between claiming freed files and acknowledging them leaks no blob.

The test pauses s3proxy so the writer's deletes hang after it claims a dropped table's freed files, kills the
writer, and resumes s3proxy. The claims expire after InodeDB's claim TTL (60 s by default), and the restarted
writer claims the files again and deletes their blobs.

    python3 tests/claims.py

Pausing s3proxy stalls every other user of the stack, so run this alone.
"""

from __future__ import annotations

import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bighouse import fleet, stack  # noqa: E402

failures = 0


def check(ok: bool, what: str, detail: str = "") -> None:
    global failures
    failures += not ok
    print(f"{'PASS' if ok else 'FAIL'} {what}" + (f": {detail}" if detail and not ok else ""))


def main() -> int:
    run = uuid.uuid4().hex[:8]
    root = prefix = f"claims/{run}"
    server = stack.Server(
        name=f"claims-{run}", tcp_port=19900, http_port=18900, interserver_port=19909,
        disks=[stack.inodedb_disk_xml(fleet.DISK, s3_prefix=prefix, key=stack.mint_key(root, "write"))],
        policies=[stack.policy_xml(fleet.DISK, fleet.DISK)],
        extra_config=fleet.default_policy_xml() + "<database_atomic_delay_before_drop_table_sec>0</database_atomic_delay_before_drop_table_sec>",
    )
    server.start()
    try:
        server.query("CREATE TABLE default.t (k UInt64, v String) ENGINE = MergeTree ORDER BY k")
        for i in range(3):
            # Rows wide enough that each part keeps blobs, not only inline files.
            server.query(f"INSERT INTO default.t SELECT number + {i * 100000}, repeat('x', 100) FROM numbers(100000)")
        blobs = set(stack.s3_objects(prefix))
        check(bool(blobs), "the table has blobs", "none")

        subprocess.run(["docker", "pause", "bighouse-s3proxy"], check=True, capture_output=True)
        try:
            dropper = threading.Thread(target=lambda: server.query("DROP TABLE default.t SYNC", timeout=30), daemon=True)
            dropper.start()
            time.sleep(3)
            server.kill()
        finally:
            subprocess.run(["docker", "unpause", "bighouse-s3proxy"], check=True, capture_output=True)
        left = blobs & set(stack.s3_objects(prefix))
        check(bool(left), "the killed writer deleted none of the claimed blobs", "all gone already")

        killed_at = time.monotonic()
        server.start()
        deadline = killed_at + 180
        while time.monotonic() < deadline and blobs & set(stack.s3_objects(prefix)):
            time.sleep(1)
        left = blobs & set(stack.s3_objects(prefix))
        check(not left, "the restarted writer deletes them once the claims expire", f"{len(left)} of {len(blobs)} left")
        print(f"    deleted {time.monotonic() - killed_at:.0f} s after the kill")
    finally:
        server.stop()
        server.wipe()
    print(f"{failures} failed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
