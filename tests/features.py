#!/usr/bin/env python3
"""Runs each feature on a disk with `local` metadata and on one with `inodedb` metadata, and requires the same
answers from both. Then checks that data survives a restart, that an insert on the `inodedb` disk makes one InodeDB
commit and a deduplicated one leaves no blob, that a renamed table keeps its tokens through a restart, and that every
blob goes once its tables are gone. Needs `scripts/dev-up` and a built server.

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
            # Each metadata type also under the `cache` and `encrypted` wrappers.
            *(f"""<{d}_cache><type>cache</type><disk>{d}</disk><path>{stack.DEV}/servers/features-{run}/cache_{d}/</path><max_size>1Gi</max_size></{d}_cache>
                <{d}_encrypted><type>encrypted</type><disk>{d}_cache</disk><key>1234567812345678</key><path>encrypted/</path></{d}_encrypted>"""
              for d in DISKS.values()),
        ],
        policies=[stack.policy_xml(p, DISKS[p]) for p in POLICIES]
        + [stack.policy_xml(f"{p}_encrypted", f"{DISKS[p]}_encrypted") for p in POLICIES]
        + ["<tiered><volumes><hot><disk>default</disk></hot><cold><disk>s3_inodedb</disk></cold></volumes></tiered>"],
        extra_config=f"""<backups><allowed_disk>backups_disk</allowed_disk><allowed_path>/</allowed_path></backups>
    <keeper_server>
        <tcp_port>19181</tcp_port>
        <server_id>1</server_id>
        <log_storage_path>{stack.DEV}/servers/features-{run}/coordination/log</log_storage_path>
        <snapshot_storage_path>{stack.DEV}/servers/features-{run}/coordination/snapshots</snapshot_storage_path>
        <raft_configuration><server><id>1</id><hostname>127.0.0.1</hostname><port>19234</port></server></raft_configuration>
    </keeper_server>
    <zookeeper><node><host>127.0.0.1</host><port>19181</port></node></zookeeper>""",
    )
    server.s3_prefixes = {"s3_local": f"features/{run}/local", "s3_inodedb": f"features/{run}/inodedb"}
    server.inodedb_root = f"features/{run}"
    return server


def check_table(s: stack.Server, table: str) -> str:
    """`CHECK TABLE`'s rows, one per part, in a fixed order."""
    return "".join(sorted(s.query(f"CHECK TABLE {table}").splitlines(keepends=True)))


def scenario_insert_merge(s: stack.Server, p: str) -> list[str]:
    s.query(f"CREATE TABLE t (k UInt64, v String) ENGINE = MergeTree ORDER BY k SETTINGS storage_policy = '{p}'")
    for i in range(3):
        s.query(f"INSERT INTO t SELECT number + {i * 1000}, toString(number) FROM numbers(1000)")
    out = [s.query("SELECT count(), sum(k), max(v) FROM t"), s.query("SELECT count() FROM system.parts WHERE table = 't' AND active")]
    s.query("OPTIMIZE TABLE t FINAL")
    out += [s.query("SELECT count(), sum(k) FROM t"), s.query("SELECT count() FROM system.parts WHERE table = 't' AND active")]
    out.append(check_table(s, "t"))
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
    out.append(check_table(s, "t"))
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
        check_table(s, "t"),
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


def scenario_ordinary_rename(s: stack.Server, p: str) -> list[str]:
    """A duplicate insert uses up a block number, a table renamed in an `Ordinary` database keeps its tokens, and a
    new table at the old path starts with none."""
    db = f"{s.database}_ordinary"
    s.query(f"CREATE DATABASE {db} ENGINE = Ordinary", settings={"allow_deprecated_database_ordinary": 1})
    try:
        table = f"(k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS storage_policy = '{p}', non_replicated_deduplication_window = 10"
        s.query(f"CREATE TABLE {db}.t {table}")
        s.query(f"SYSTEM STOP MERGES {db}.t")

        def insert(name: str, k: int, token: str) -> None:
            s.query(f"INSERT INTO {db}.{name} VALUES ({k})", settings={"insert_deduplication_token": token})

        insert("t", 1, "a")
        insert("t", 1, "a")
        insert("t", 2, "b")
        s.query(f"RENAME TABLE {db}.t TO {db}.t2")
        insert("t2", 2, "b")
        insert("t2", 3, "c")
        s.query(f"CREATE TABLE {db}.t {table}")
        insert("t", 1, "a")
        parts = f"SELECT table, name FROM system.parts WHERE database = '{db}' AND active ORDER BY table, name"
        return [s.query(f"SELECT k FROM {db}.t2 ORDER BY k"), s.query(f"SELECT k FROM {db}.t"), s.query(parts)]
    finally:
        s.query(f"DROP DATABASE {db} SYNC")


def scenario_backup(s: stack.Server, p: str) -> list[str]:
    s.query(f"CREATE TABLE t (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS storage_policy = '{p}'")
    s.query("INSERT INTO t SELECT number FROM numbers(5000)")
    name = uuid.uuid4().hex
    s.query(f"BACKUP TABLE t TO Disk('backups_disk', '{name}')")
    s.query("DROP TABLE t SYNC")
    s.query(f"RESTORE TABLE t FROM Disk('backups_disk', '{name}')")
    return [s.query("SELECT count(), sum(k) FROM t"), s.query(f"SELECT all(disk_name = '{DISKS[p]}') FROM system.parts WHERE table = 't' AND active")]


def scenario_cache_and_encryption(s: stack.Server, p: str) -> list[str]:
    s.query(f"CREATE TABLE t (k UInt64, v String) ENGINE = MergeTree ORDER BY k SETTINGS storage_policy = '{p}_encrypted'")
    for i in range(3):
        s.query(f"INSERT INTO t SELECT number + {i * 1000}, toString(number) FROM numbers(1000)")
    s.query("OPTIMIZE TABLE t FINAL")
    s.query("ALTER TABLE t UPDATE v = 'x' WHERE k % 7 = 0", settings={"mutations_sync": 2})
    s.query("SYSTEM DROP FILESYSTEM CACHE")
    return [s.query("SELECT count(), sum(k), countIf(v = 'x') FROM t"), check_table(s, "t")]


def scenario_lightweight_update(s: stack.Server, p: str) -> list[str]:
    s.query(f"""CREATE TABLE t (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
        SETTINGS storage_policy = '{p}', enable_block_number_column = 1, enable_block_offset_column = 1""")
    s.query("INSERT INTO t SELECT number, number FROM numbers(10000)")
    s.query("UPDATE t SET v = v * 10 WHERE k % 3 = 0")
    out = [s.query("SELECT sum(v), count() FROM t")]
    s.query("OPTIMIZE TABLE t FINAL")
    out += [s.query("SELECT sum(v), count() FROM t"), check_table(s, "t")]
    return out


def scenario_backup_to_s3(s: stack.Server, p: str) -> list[str]:
    s.query(f"CREATE TABLE t (k UInt64, v String) ENGINE = MergeTree ORDER BY k SETTINGS storage_policy = '{p}'")
    s.query("INSERT INTO t SELECT number, toString(number) FROM numbers(5000)")
    s.query("INSERT INTO t SELECT number + 5000, toString(number) FROM numbers(5000)")
    url = f"{stack.S3_ENDPOINT}/{stack.S3_BUCKET}/feature-backups/{uuid.uuid4().hex}"
    target = f"S3('{url}', '{stack.S3_ACCESS_KEY}', '{stack.S3_SECRET_KEY}')"
    s.query(f"BACKUP TABLE t TO {target}")
    s.query("DROP TABLE t SYNC")
    s.query(f"RESTORE TABLE t FROM {target}")
    return [s.query("SELECT count(), sum(k), max(v) FROM t"), check_table(s, "t")]


def scenario_replicated(s: stack.Server, p: str) -> list[str]:
    # Two replicas on one server fetch each other's parts over the interserver port. `inodedb` disks fetch whole
    # parts, since zero-copy replication ships local metadata files.
    for replica in ("r1", "r2"):
        s.query(f"""CREATE TABLE t_{replica} (k UInt64, v String)
            ENGINE = ReplicatedMergeTree('/features/{{database}}/t', '{replica}') ORDER BY k
            SETTINGS storage_policy = '{p}', allow_remote_fs_zero_copy_replication = 1""")
    s.query("INSERT INTO t_r1 SELECT number, toString(number) FROM numbers(1000)")
    s.query("INSERT INTO t_r2 SELECT number + 1000, toString(number) FROM numbers(1000)")
    s.query("SYSTEM SYNC REPLICA t_r1")
    s.query("OPTIMIZE TABLE t_r1 FINAL", settings={"alter_sync": 2})
    s.query("SYSTEM SYNC REPLICA t_r2")
    out = []
    for replica in ("r1", "r2"):
        out += [s.query(f"SELECT count(), sum(k), max(v) FROM t_{replica}"),
                s.query(f"SELECT name FROM system.parts WHERE table = 't_{replica}' AND active ORDER BY name"),
                check_table(s, f"t_{replica}")]
    return out


def scenario_move_between_disks(s: stack.Server, p: str) -> list[str]:
    if p != "s3_inodedb":
        return ["skipped\n"]
    s.query("CREATE TABLE t (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS storage_policy = 'tiered'")
    s.query("INSERT INTO t SELECT number FROM numbers(1000)")
    s.query("ALTER TABLE t MOVE PARTITION tuple() TO DISK 's3_inodedb'")
    out = [s.query("SELECT disk_name, count() FROM system.parts WHERE table = 't' AND active GROUP BY disk_name")]
    s.query("ALTER TABLE t MOVE PARTITION tuple() TO DISK 'default'")
    out += [s.query("SELECT disk_name FROM system.parts WHERE table = 't' AND active ORDER BY disk_name"), s.query("SELECT sum(k) FROM t")]
    return out


def scenario_wide_part(s: stack.Server, p: str) -> list[str]:
    # 800 columns of hashes make a Wide part of 1,600 files of 12 KB. Each file is small enough to inline, and
    # together they hold 9.6 MB, more than one InodeDB commit takes.
    columns = 800
    s.query(f"CREATE TABLE t (k UInt64, {', '.join(f'c{i} UInt64 DEFAULT sipHash64(k, {i})' for i in range(columns))}) "
            f"ENGINE = MergeTree ORDER BY k "
            f"SETTINGS storage_policy = '{p}', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0")
    s.query("INSERT INTO t (k) SELECT number FROM numbers(1500)")
    sums = ", ".join(f"sum(c{i})" for i in range(0, columns, 100))
    return [s.query(f"SELECT count(), sum(k), {sums} FROM t"),
            s.query("SELECT part_type, count() FROM system.parts WHERE table = 't' AND active GROUP BY part_type"),
            check_table(s, "t")]


def scenario_sparse_wide_part(s: stack.Server, p: str) -> list[str]:
    # The shape of `system.query_metric_log`: 1,728 counters of long names, each zero in most rows. Each column is
    # sparse and writes 4 files, so the part holds about 6,900 files: more than one InodeDB commit takes beside their
    # inline content.
    columns = 1728
    s.query(f"CREATE TABLE t (k UInt64, "
            f"{', '.join(f'ProfileEvent_CounterInAWideTableOfMetrics{i:04d} UInt64 DEFAULT if(sipHash64(k, {i}) % 32 = 0, sipHash64(k, {i}) % 100000, 0)' for i in range(columns))}) "
            f"ENGINE = MergeTree ORDER BY k "
            f"SETTINGS storage_policy = '{p}', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0")
    s.query("INSERT INTO t (k) SELECT number FROM numbers(16000)")
    sums = ", ".join(f"sum(ProfileEvent_CounterInAWideTableOfMetrics{i:04d})" for i in range(0, columns, 200))
    return [s.query(f"SELECT count(), sum(k), {sums} FROM t"),
            s.query("SELECT part_type, count(), sum(files) > 6000 FROM system.parts WHERE table = 't' AND active GROUP BY part_type"),
            check_table(s, "t")]


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

        # An insert on the InodeDB disk commits its part's files together with the part's rename.
        server.query("CREATE TABLE default.once (k UInt64, v String) ENGINE = MergeTree ORDER BY k "
                     "SETTINGS storage_policy = 's3_inodedb', non_replicated_deduplication_window = 10")
        server.query("SYSTEM STOP MERGES default.once")
        # Hashes compress poorly, so the part's data outgrows the inline limit and goes to blobs.
        insert = "INSERT INTO default.once SELECT number, hex(cityHash64(number)) FROM numbers(100000)"
        commits = "SELECT value FROM system.events WHERE event = 'InodeDBCommit'"
        # The token makes the repeated `INSERT SELECT` below a duplicate of this one.
        token = {"insert_deduplication_token": "once"}
        before = int(server.value(commits))
        server.query(insert, settings=token)
        made = int(server.value(commits)) - before
        ok = made == 1
        failures += not ok
        print(f"{'PASS' if ok else 'FAIL'} an insert makes one InodeDB commit" + ("" if ok else f": it made {made}"))

        # A deduplicated insert undoes its part, which removes the blobs it uploaded.
        blobs = stack.s3_objects(server.s3_prefixes["s3_inodedb"])
        server.query(insert, settings=token)
        rows = server.value("SELECT count() FROM default.once")
        left = sorted(set(stack.s3_objects(server.s3_prefixes["s3_inodedb"])) - set(blobs))
        ok = rows == "100000" and len(blobs) > 0 and not left
        failures += not ok
        print(f"{'PASS' if ok else 'FAIL'} a deduplicated insert leaves no blob"
              + ("" if ok else f": {rows} rows, {len(blobs)} blobs before, {len(left)} new: {left[:3]}"))
        server.query("DROP TABLE default.once SYNC")

        # A table renamed in an `Ordinary` database moves its tokens with its directory and keeps them through a
        # restart. `local` metadata's deduplication log keeps writing under the path it started with, so its restart
        # forgets the tokens of inserts after the rename; this check runs on `inodedb` alone.
        server.query("CREATE DATABASE renamed ENGINE = Ordinary", settings={"allow_deprecated_database_ordinary": 1})
        server.query("CREATE TABLE renamed.t (k UInt64) ENGINE = MergeTree ORDER BY k "
                     "SETTINGS storage_policy = 's3_inodedb', non_replicated_deduplication_window = 10")
        server.query("INSERT INTO renamed.t VALUES (1)", settings={"insert_deduplication_token": "a"})
        server.query("RENAME TABLE renamed.t TO renamed.t2")
        server.query("INSERT INTO renamed.t2 VALUES (2)", settings={"insert_deduplication_token": "b"})
        server.query("DETACH TABLE renamed.t2")
        server.query("ATTACH TABLE renamed.t2")
        for k, token in ((1, "a"), (2, "b")):
            server.query(f"INSERT INTO renamed.t2 VALUES ({k})", settings={"insert_deduplication_token": token})
        rows = server.value("SELECT count() FROM renamed.t2")
        coord = f"{server.inodedb_root}/coord/data/renamed"
        tables = stack.idb("ls", coord).split()
        tokens = stack.idb("ls", f"{coord}/t2/tokens").split()
        server.query("DROP DATABASE renamed SYNC")
        ok = rows == "2" and tables == ["t2/"] and len(tokens) == 2
        failures += not ok
        print(f"{'PASS' if ok else 'FAIL'} a renamed table keeps its tokens through a restart"
              + ("" if ok else f": {rows} rows, {tables} under coord, {len(tokens)} tokens"))

        # Every blob goes once its tables are gone, on both disks. The disk removes a dropped table's blobs in the
        # blob killer's rounds, as it claims their files from InodeDB.
        for p in POLICIES:
            deadline = time.monotonic() + 60
            while (left := stack.s3_objects(server.s3_prefixes[p])) and time.monotonic() < deadline:
                time.sleep(1)
            ok = not left
            failures += not ok
            print(f"{'PASS' if ok else 'FAIL'} blobs removed on {p}" + ("" if ok else f": {len(left)} left, e.g. {left[:3]}"))
    finally:
        server.stop()

    # The disk frees coordination files, which hold plain text, without taking them for values of its own.
    foreign = [line for line in server.log.read_text(errors="replace").splitlines() if "holds no InodeDB metadata value" in line]
    ok = not foreign
    failures += not ok
    print(f"{'PASS' if ok else 'FAIL'} freed coordination files log no error" + ("" if ok else f": {len(foreign)}, e.g. {foreign[0][:200]}"))
    print(f"{failures} failed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
