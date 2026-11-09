#!/usr/bin/env python3
"""Fleets find the writer's databases in InodeDB.

- A fleet serves the writer's tables, views, materialized views and `default` database, on every server.
- It leaves out each database it cannot serve as the writer would, says why, and names them when a query fails:
  a `Memory` table, a table on a local disk, a storage policy the fleet lacks, a refreshable materialized view. A
  fleet that leaves out the writer's `default` keeps an empty one of its own.
- The next fleet sees the writer's `ALTER`, `CREATE`, `DROP` and `RENAME` of tables and databases.
- A fleet answers while the writer is down.

    python3 tests/discovery.py
"""

from __future__ import annotations

import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bighouse import api, fleet, stack  # noqa: E402

failures = 0


def check(ok: bool, what: str, detail: str = "") -> None:
    global failures
    failures += not ok
    print(f"{'PASS' if ok else 'FAIL'} {what}" + (f": {detail}" if detail and not ok else ""))


def main() -> int:
    run = uuid.uuid4().hex[:8]
    root = prefix = f"discovery/{run}"
    writer = stack.Server(
        name=f"discovery-{run}-writer", tcp_port=19400, http_port=18400, interserver_port=19409,
        disks=[fleet.writer_disk_xml(s3_prefix=prefix, key=stack.mint_key(root, "write"))],
        policies=[stack.policy_xml(fleet.DISK, fleet.DISK), stack.policy_xml("local_only", "default")],
        extra_config=fleet.writer_config_xml(),
    )
    query_api = api.QueryApi({"inodedb_root": root, "s3_prefix": prefix}, slots=2)

    def ask(sql: str) -> str:
        return query_api.run(sql, replicas=2)[0]

    def same(sql: str, what: str) -> None:
        want = writer.query(sql)
        got = ask(sql)
        check(got == want, what, f"got {got!r}, want {want!r}")

    writer.start()
    try:
        writer.query("CREATE DATABASE d")
        writer.query("CREATE TABLE d.t (k UInt64, v String) ENGINE = MergeTree ORDER BY k")
        writer.query("CREATE VIEW d.v AS SELECT count() AS c, sum(k) AS s FROM d.t")
        writer.query("CREATE TABLE d.evens (k UInt64) ENGINE = MergeTree ORDER BY k")
        writer.query("CREATE MATERIALIZED VIEW d.to_evens TO d.evens AS SELECT k FROM d.t WHERE k % 2 = 0")
        writer.query("INSERT INTO d.t SELECT number, toString(number) FROM numbers(1000)")
        writer.query("CREATE TABLE default.dt (k UInt64) ENGINE = MergeTree ORDER BY k")
        writer.query("INSERT INTO default.dt SELECT number FROM numbers(7)")
        writer.query("CREATE DATABASE mem")
        writer.query("CREATE TABLE mem.m (k UInt64) ENGINE = Memory")
        writer.query("CREATE DATABASE off")
        writer.query("CREATE TABLE off.t (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS disk = 'default'")
        writer.query("CREATE DATABASE elsewhere")
        writer.query("CREATE TABLE elsewhere.t (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS storage_policy = 'local_only'")
        writer.query("CREATE DATABASE sched")
        writer.query("CREATE TABLE sched.target (c UInt64) ENGINE = MergeTree ORDER BY c")
        writer.query("CREATE MATERIALIZED VIEW sched.mv REFRESH EVERY 1 HOUR TO sched.target AS SELECT count() AS c FROM d.t")

        same("SELECT count(), sum(k) FROM d.t", "a fleet reads a table")
        same("SELECT * FROM d.v", "a fleet reads a view")
        same("SELECT count(), sum(k) FROM d.evens", "a fleet reads a materialized view's target")
        same("SELECT count() FROM dt", "a fleet reads the writer's `default` database")
        same("SELECT name, engine FROM system.tables WHERE database = 'd' ORDER BY name", "a fleet lists the database's tables")

        slots = query_api.pool.acquire(2)
        key = stack.mint_key(root, "read", ttl="15m")
        summoned = fleet.Fleet(slots, s3_prefix=prefix, key=key)
        try:
            summoned.start()
            counts = [server.value("SELECT count() FROM d.t") for server in summoned.servers]
            left_out = summoned.left_out
        finally:
            summoned.stop()
            query_api.pool.release(slots)
            stack.idb("key", "revoke", key)
        check(counts == ["1000", "1000"], "every fleet server attaches the databases", f"counts {counts}")
        check(sorted(left_out) == ["elsewhere", "mem", "off", "sched"], "a fleet leaves out what it cannot serve", str(left_out))
        reasons = {
            "mem": "mem.m has engine Memory",
            "off": "off.t keeps data off the `bighouse` disk",
            "elsewhere": "it fails to attach",
            "sched": "sched.mv refreshes on a schedule",
        }
        for name, reason in reasons.items():
            check(reason in left_out.get(name, ""), f"it says why it leaves out `{name}`", left_out.get(name, ""))
        try:
            ask("SELECT count() FROM mem.m")
            check(False, "a query on a left-out database fails and names it")
        except stack.QueryError as error:
            check("left out" in str(error) and "mem.m has engine Memory" in str(error), "a query on a left-out database fails and names it",
                  str(error)[-300:])

        writer.query("ALTER TABLE d.t ADD COLUMN c UInt8 DEFAULT 7")
        writer.query("INSERT INTO d.t (k, v, c) SELECT number + 1000, 'x', 9 FROM numbers(5)")
        writer.query("ALTER TABLE d.t DROP COLUMN v")
        writer.query("RENAME TABLE d.v TO d.v2")
        writer.query("DROP VIEW d.to_evens SYNC")
        writer.query("DROP TABLE d.evens SYNC")
        writer.query("CREATE TABLE d.n (k UInt64) ENGINE = MergeTree ORDER BY k")
        writer.query("INSERT INTO d.n VALUES (1)")
        writer.query("CREATE DATABASE e")
        writer.query("CREATE TABLE e.t (k UInt64) ENGINE = MergeTree ORDER BY k")
        writer.query("INSERT INTO e.t SELECT number FROM numbers(3)")
        writer.query("DROP DATABASE off SYNC")
        writer.query("RENAME DATABASE mem TO mem2")

        same("DESCRIBE TABLE d.t", "the next fleet sees added and dropped columns")
        same("SELECT k, c FROM d.t ORDER BY k DESC LIMIT 7", "the next fleet reads the new column")
        same("SELECT name FROM system.tables WHERE database = 'd' ORDER BY name", "the next fleet sees created, dropped and renamed tables")
        same("SELECT * FROM d.v2", "the next fleet reads a renamed view")
        same("SELECT count() FROM e.t", "the next fleet reads a new database")
        left_out = query_api.run("SELECT 1", replicas=1)[2]
        check(sorted(left_out) == ["elsewhere", "mem2", "sched"], "the next fleet sees dropped and renamed databases", str(left_out))

        writer.query("CREATE TABLE default.mm (k UInt64) ENGINE = Memory")
        got, _, left_out = query_api.run("SELECT count() FROM d.t", replicas=2)
        check("default" in left_out and got == writer.query("SELECT count() FROM d.t"),
              "a fleet that leaves out the writer's `default` keeps an empty one and answers", f"{got!r}, {left_out}")

        want = writer.query("SELECT count(), sum(k), sum(c) FROM d.t")
        writer.stop()
        got = ask("SELECT count(), sum(k), sum(c) FROM d.t")
        check(got == want, "a fleet answers while the writer is down", f"got {got!r}, want {want!r}")
    finally:
        writer.stop()
    check(not list((stack.DEV / "servers").glob("fleet-*")), "every fleet directory is gone")
    print(f"{failures} failed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
