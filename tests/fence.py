#!/usr/bin/env python3
"""Two writers on one `inodedb` disk: the second takes ownership at startup, and the first can no longer write.

- The first writer's next insert fails with `TABLE_IS_READ_ONLY` and applies nothing.
- The second writer attaches the table, reads every row the first committed, and inserts.
- A read key cannot write to InodeDB.

    python3 tests/fence.py
"""

from __future__ import annotations

import subprocess
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bighouse import fleet, stack  # noqa: E402

failures = 0


def check(ok: bool, what: str, detail: str = "") -> None:
    global failures
    failures += not ok
    print(f"{'PASS' if ok else 'FAIL'} {what}" + (f": {detail}" if detail and not ok else ""))


def writer(name: str, port: int, prefix: str, key: str) -> stack.Server:
    return stack.Server(
        name=name, tcp_port=port, http_port=port - 1000, interserver_port=port + 9,
        disks=[stack.inodedb_disk_xml(fleet.DISK, s3_prefix=prefix, key=key)],
        policies=[stack.policy_xml(fleet.DISK, fleet.DISK)],
        extra_config=fleet.default_policy_xml(),
    )


def main() -> int:
    run = uuid.uuid4().hex[:8]
    root = prefix = f"fence/{run}"
    key = stack.mint_key(root, "write")
    first = writer(f"fence-{run}-first", 19850, prefix, key)
    second = writer(f"fence-{run}-second", 19870, prefix, key)
    first.start()
    try:
        first.query("CREATE DATABASE f")
        first.query("CREATE TABLE f.t (k UInt64) ENGINE = MergeTree ORDER BY k")
        first.query("INSERT INTO f.t SELECT number FROM numbers(1000)")
        ddls = fleet.table_ddls(first)

        second.start()
        try:
            first.query("INSERT INTO f.t SELECT number FROM numbers(1000, 500)")
            check(False, "the first writer's insert fails once the second owns the disk")
        except stack.QueryError as error:
            check("TABLE_IS_READ_ONLY" in str(error) and "lost ownership" in str(error),
                  "the first writer's insert fails once the second owns the disk", str(error)[:300])

        second.query("CREATE DATABASE f")
        for _, ddl in ddls:
            second.query(ddl)
        check(second.value("SELECT count(), max(k) FROM f.t") == "1000\t999", "the second writer reads only what the first committed",
              second.value("SELECT count(), max(k) FROM f.t"))
        second.query("INSERT INTO f.t SELECT number FROM numbers(2000, 10)")
        check(second.value("SELECT count() FROM f.t") == "1010", "the second writer inserts")
    finally:
        second.stop()
        first.stop()

    read_key = stack.mint_key(root, "read")
    try:
        stack.idb("mkdir", "probe", key=read_key)
        check(False, "a read key cannot write")
    except subprocess.CalledProcessError:
        check(True, "a read key cannot write")
    print(f"{failures} failed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
