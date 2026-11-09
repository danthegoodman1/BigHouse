#!/usr/bin/env python3
"""Runs each feature on a disk with `local` metadata and on one with `inodedb` metadata, and requires the same
answers from both. Needs `scripts/dev-up` and a built server.

    python3 tests/features.py [scenario ...]
"""

from __future__ import annotations

import sys
import time
import traceback
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bighouse import stack  # noqa: E402

POLICIES = ("s3_local", "s3_inodedb")
DISKS = {"s3_local": "s3_local", "s3_inodedb": "s3_inodedb"}


def make_server() -> stack.Server:
    run = uuid.uuid4().hex[:8]
    key = stack.mint_key(f"features/{run}", "write")
    server = stack.Server(
        name=f"features-{run}",
        tcp_port=19000,
        http_port=18123,
        interserver_port=19009,
        disks=[
            stack.local_metadata_disk_xml("s3_local", s3_prefix=f"features/{run}/local"),
            stack.inodedb_disk_xml("s3_inodedb", s3_prefix=f"features/{run}/inodedb", key=key),
            f"<backups_disk><type>local</type><path>{stack.DEV}/servers/features-{run}/backups/</path></backups_disk>",
        ],
        policies=[stack.policy_xml(p, DISKS[p]) for p in POLICIES]
        + ["<tiered><volumes><hot><disk>default</disk></hot><cold><disk>s3_inodedb</disk></cold></volumes></tiered>"],
        extra_config="<backups><allowed_disk>backups_disk</allowed_disk><allowed_path>/</allowed_path></backups>",
    )
    server.s3_prefixes = {"s3_local": f"features/{run}/local", "s3_inodedb": f"features/{run}/inodedb"}
    return server


def scenario_insert_merge(s: stack.Server, p: str) -> list[str]:
    s.query(f"CREATE TABLE t (k UInt64, v String) ENGINE = MergeTree ORDER BY k SETTINGS storage_policy = '{p}'")
    for i in range(3):
        s.query(f"INSERT INTO t SELECT number + {i * 1000}, toString(number) FROM numbers(1000)")
    out = [s.query("SELECT count(), sum(k), max(v) FROM t"), s.query("SELECT count() FROM system.parts WHERE table = 't' AND active")]
    s.query("OPTIMIZE TABLE t FINAL")
    out += [s.query("SELECT count(), sum(k) FROM t"), s.query("SELECT count() FROM system.parts WHERE table = 't' AND active")]
    out.append(s.query("CHECK TABLE t"))
    return out


def scenario_mutations(s: stack.Server, p: str) -> list[str]:
    s.query(f"CREATE TABLE t (k UInt64, a UInt64, b String) ENGINE = MergeTree ORDER BY k SETTINGS storage_policy = '{p}'")
    s.query("INSERT INTO t SELECT number, number * 2, toString(number) FROM numbers(10000)")
    s.query("ALTER TABLE t UPDATE a = a + 1 WHERE k % 2 = 0", settings={"mutations_sync": 2})
    out = [s.query("SELECT sum(a), count() FROM t")]
    s.query("ALTER TABLE t DELETE WHERE k % 3 = 0", settings={"mutations_sync": 2})
    out.append(s.query("SELECT sum(a), count() FROM t"))
    s.query("DELETE FROM t WHERE k % 5 = 0")
    out.append(s.query("SELECT sum(a), count() FROM t"))
    s.query("ALTER TABLE t ADD COLUMN c UInt8 DEFAULT 7")
    s.query("ALTER TABLE t MATERIALIZE COLUMN c", settings={"mutations_sync": 2})
    s.query("ALTER TABLE t DROP COLUMN b", settings={"mutations_sync": 2})
    out.append(s.query("SELECT sum(a), sum(c), count() FROM t"))
    out.append(s.query("CHECK TABLE t"))
    return out


def scenario_partitions(s: stack.Server, p: str) -> list[str]:
    for name in ("t", "t2", "t3"):
        s.query(f"CREATE TABLE {name} (d UInt8, k UInt64) ENGINE = MergeTree PARTITION BY d ORDER BY k SETTINGS storage_policy = '{p}'")
    s.query("INSERT INTO t SELECT number % 4, number FROM numbers(4000)")
    s.query("INSERT INTO t2 SELECT 9, number FROM numbers(100)")
    out = [s.query("SELECT d, count() FROM t GROUP BY d ORDER BY d")]
    s.query("ALTER TABLE t DETACH PARTITION 1")
    out.append(s.query("SELECT count() FROM t"))
    s.query("ALTER TABLE t ATTACH PARTITION 1")
    out.append(s.query("SELECT count() FROM t"))
    s.query("ALTER TABLE t DROP PARTITION 2")
    s.query("ALTER TABLE t ATTACH PARTITION 9 FROM t2")
    s.query("ALTER TABLE t3 REPLACE PARTITION 0 FROM t")
    s.query("ALTER TABLE t MOVE PARTITION 3 TO TABLE t3")
    out += [s.query("SELECT d, count() FROM t GROUP BY d ORDER BY d"), s.query("SELECT d, count() FROM t3 GROUP BY d ORDER BY d")]
    s.query("ALTER TABLE t DETACH PARTITION 0")
    s.query("ALTER TABLE t DROP DETACHED PARTITION 0", settings={"allow_drop_detached": 1})
    out.append(s.query("SELECT count() FROM system.detached_parts WHERE table = 't'"))
    return out


def scenario_freeze(s: stack.Server, p: str) -> list[str]:
    s.query(f"CREATE TABLE t (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS storage_policy = '{p}'")
    s.query("INSERT INTO t SELECT number, number FROM numbers(1000)")
    s.query("ALTER TABLE t FREEZE WITH NAME 'snap'")
    s.query("ALTER TABLE t UPDATE v = v + 1 WHERE 1", settings={"mutations_sync": 2})
    s.query("OPTIMIZE TABLE t FINAL")
    out = [s.query("SELECT sum(v) FROM t")]
    s.query("ALTER TABLE t UNFREEZE WITH NAME 'snap'")
    out.append(s.query("SELECT sum(v) FROM t"))
    return out


def scenario_ttl_projection_index(s: stack.Server, p: str) -> list[str]:
    s.query(
        f"""CREATE TABLE t (d DateTime, k UInt64, s String, INDEX idx s TYPE bloom_filter GRANULARITY 1,
            PROJECTION by_s (SELECT s, count() GROUP BY s))
        ENGINE = MergeTree ORDER BY k TTL d + INTERVAL 1 DAY SETTINGS storage_policy = '{p}'"""
    )
    s.query("INSERT INTO t SELECT if(number % 2, now(), now() - INTERVAL 2 DAY), number, toString(number % 10) FROM numbers(5000)")
    s.query("ALTER TABLE t MATERIALIZE TTL", settings={"mutations_sync": 2})
    s.query("OPTIMIZE TABLE t FINAL")
    return [
        s.query("SELECT count() FROM t"),
        s.query("SELECT s, count() FROM t GROUP BY s ORDER BY s"),
        s.query("SELECT count() FROM t WHERE s = '3'"),
        s.query("CHECK TABLE t"),
    ]


def scenario_log_engines(s: stack.Server, p: str) -> list[str]:
    out = []
    for engine in ("TinyLog", "StripeLog", "Log"):
        s.query(f"CREATE TABLE t_{engine} (k UInt64, s String) ENGINE = {engine} SETTINGS disk = '{DISKS[p]}'")
        for i in range(30):
            s.query(f"INSERT INTO t_{engine} SELECT number + {i * 100}, toString(number) FROM numbers(100)")
        out.append(s.query(f"SELECT count(), sum(k), max(s) FROM t_{engine}"))
        s.query(f"TRUNCATE TABLE t_{engine}")
        s.query(f"INSERT INTO t_{engine} VALUES (1, 'x')")
        out.append(s.query(f"SELECT * FROM t_{engine}"))
    return out


def scenario_tables(s: stack.Server, p: str) -> list[str]:
    s.query(f"CREATE TABLE t (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS storage_policy = '{p}'")
    s.query(f"CREATE TABLE u (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS storage_policy = '{p}'")
    s.query("INSERT INTO t SELECT number FROM numbers(10)")
    s.query("INSERT INTO u SELECT number FROM numbers(20)")
    s.query("RENAME TABLE t TO t_renamed")
    s.query("EXCHANGE TABLES t_renamed AND u")
    out = [s.query("SELECT count() FROM t_renamed"), s.query("SELECT count() FROM u")]
    s.query("TRUNCATE TABLE u")
    out.append(s.query("SELECT count() FROM u"))
    s.query("DETACH TABLE t_renamed")
    s.query("ATTACH TABLE t_renamed")
    out.append(s.query("SELECT count() FROM t_renamed"))
    return out


def scenario_backup(s: stack.Server, p: str) -> list[str]:
    s.query(f"CREATE TABLE t (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS storage_policy = '{p}'")
    s.query("INSERT INTO t SELECT number FROM numbers(5000)")
    name = uuid.uuid4().hex
    s.query(f"BACKUP TABLE t TO Disk('backups_disk', '{name}')")
    s.query("DROP TABLE t SYNC")
    s.query(f"RESTORE TABLE t FROM Disk('backups_disk', '{name}')")
    return [s.query("SELECT count(), sum(k) FROM t"), s.query(f"SELECT all(disk_name = '{DISKS[p]}') FROM system.parts WHERE table = 't' AND active")]


def scenario_move_between_disks(s: stack.Server, p: str) -> list[str]:
    if p != "s3_inodedb":
        return ["skipped\n"]
    s.query("CREATE TABLE t (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS storage_policy = 'tiered'")
    s.query("INSERT INTO t SELECT number FROM numbers(1000)")
    s.query("ALTER TABLE t MOVE PARTITION tuple() TO DISK 's3_inodedb'")
    out = [s.query("SELECT disk_name, count() FROM system.parts WHERE table = 't' AND active GROUP BY disk_name")]
    s.query("ALTER TABLE t MOVE PARTITION tuple() TO DISK 'default'")
    out += [s.query("SELECT disk_name FROM system.parts WHERE table = 't' AND active"), s.query("SELECT sum(k) FROM t")]
    return out


SCENARIOS = {name[len("scenario_"):]: fn for name, fn in globals().items() if name.startswith("scenario_")}


def run_scenario(s: stack.Server, name: str) -> tuple[bool, str]:
    results = {}
    for p in POLICIES:
        db = f"{name}_{p}"
        s.query(f"CREATE DATABASE {db}")
        try:
            results[p] = SCENARIOS[name](_InDatabase(s, db), p)
        finally:
            s.query(f"DROP DATABASE {db} SYNC")
    if results["s3_local"] != results["s3_inodedb"] and name != "move_between_disks":
        return False, f"local: {results['s3_local']}\ninodedb: {results['s3_inodedb']}"
    return True, ""


class _InDatabase:
    """Runs every query of a scenario in its own database."""

    def __init__(self, server: stack.Server, database: str):
        self.server = server
        self.database = database

    def query(self, sql: str, settings: dict | None = None) -> str:
        return self.server.query(sql, settings={"database": self.database, **(settings or {})})


def main() -> int:
    names = sys.argv[1:] or list(SCENARIOS)
    server = make_server()
    server.start()
    failures = 0
    try:
        for name in names:
            started = time.monotonic()
            try:
                ok, detail = run_scenario(server, name)
            except Exception:  # noqa: BLE001
                ok, detail = False, traceback.format_exc()
            failures += not ok
            print(f"{'PASS' if ok else 'FAIL'} {name} ({time.monotonic() - started:.1f} s)")
            if detail:
                print(detail)

        # Data on the InodeDB disk survives a restart.
        server.query("CREATE TABLE default.persist (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS storage_policy = 's3_inodedb'")
        server.query("INSERT INTO default.persist SELECT number FROM numbers(12345)")
        server.stop()
        server.start()
        ok = server.value("SELECT count() FROM default.persist") == "12345"
        server.query("DROP TABLE default.persist SYNC")
        failures += not ok
        print(f"{'PASS' if ok else 'FAIL'} restart")

        # Every blob goes once its tables are gone, on both disks.
        for p in POLICIES:
            left = stack.s3_objects(server.s3_prefixes[p])
            ok = not left
            failures += not ok
            print(f"{'PASS' if ok else 'FAIL'} blobs removed on {p}" + ("" if ok else f": {len(left)} left, e.g. {left[:3]}"))
    finally:
        server.stop()
    print(f"{failures} failed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
