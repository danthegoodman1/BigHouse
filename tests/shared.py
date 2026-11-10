#!/usr/bin/env python3
"""Shared writers on one `inodedb` disk (PLAN.md, Phase 7).

Each scenario starts its own InodeDB root, runs an exclusive writer for the DDL, then shared writers:

- `modes`: shared writers commit side by side; an exclusive writer fences them; revoking one writer's key fences it alone.
- `refused`: a shared writer refuses DDL, mutations, lightweight updates and deletes, partition commands and
  `TRUNCATE`, naming exclusive mode, and runs `OPTIMIZE`.
- `definition`: once the table's definition changes in InodeDB, a writer that loaded the old one commits nothing.
- `concurrent`: writers insert into one table through a materialized view and a projection: exact counts, unique
  block numbers, and a replica reading whole inserts throughout.
- `exactly_once`: a writer killed before its commit, after its table's commit, and after the insert returns; one
  fenced by an exclusive writer; one whose key is revoked. A retry with the same token leaves each row once in the
  table and once in the view's table.
- `merges`: merging writers beside inserting writers on a table of more than 1,024 parts. Parts never intersect, and
  a fresh exclusive writer loads the disk and passes `CHECK TABLE`.
- `leases`: two writers race to merge the same parts; a killed holder's claims expire and another writer takes over;
  a holder whose claims were taken over commits nothing.
- `mode_changes`: an exclusive writer fences shared writers under load and hands back to them: every acknowledged
  insert lands once, and block numbers stay unique.
- `differential`: one workload through an exclusive writer and through shared writers gives equal answers for six
  engines.
- `kill_loop`: SIGKILL random writers at random points for `--kill-rounds` rounds, then check every invariant.

    python3 tests/shared.py [scenario ...] [--kill-rounds N]
"""

from __future__ import annotations

import argparse
import os
import random
import signal
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Callable
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bighouse import catalog, fleet, stack  # noqa: E402

failures = 0
EXCLUSIVE_HINT = "needs an exclusive writer"


def check(ok: bool, what: str, detail: str = "") -> bool:
    global failures
    failures += not ok
    print(f"{'PASS' if ok else 'FAIL'} {what}" + (f": {detail}" if detail and not ok else ""), flush=True)
    return ok


def wait_for(condition: Callable[[], bool], timeout: float = 30, interval: float = 0.2) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if condition():
                return True
        except stack.QueryError:
            pass
        time.sleep(interval)
    return False


class Run:
    """An InodeDB root with its writers: index 0 is exclusive, the rest shared unless asked otherwise."""

    def __init__(self, name: str, *, claim_ttl_ms: int = 4000, merge_tree: str = "<old_parts_lifetime>10</old_parts_lifetime>"):
        self.id = uuid.uuid4().hex[:8]
        self.root = f"shared/{name}/{self.id}"
        self.key = stack.mint_key(self.root, "write")
        self.claim_ttl_ms = claim_ttl_ms
        self.merge_tree = merge_tree
        self.servers: dict[int, stack.Server] = {}

    def server(self, index: int, *, shared: bool = True, key: str | None = None, merge_tree: str = "") -> stack.Server:
        mode = "<inodedb_writer_mode>shared</inodedb_writer_mode>" if shared else ""
        server = stack.Server(
            name=f"shared-{self.id}-{index}", tcp_port=23000 + index, http_port=24000 + index, interserver_port=25000 + index,
            disks=[stack.inodedb_disk_xml(fleet.DISK, s3_prefix=self.root, key=key or self.key,
                                          extra=f"{mode}<inodedb_claim_ttl_ms>{self.claim_ttl_ms}</inodedb_claim_ttl_ms>")],
            policies=[stack.policy_xml(fleet.DISK, fleet.DISK)],
            extra_config=f"<merge_tree><storage_policy>{fleet.DISK}</storage_policy>{self.merge_tree}{merge_tree}</merge_tree>"
                         + fleet.database_disk_xml(),
        )
        self.servers[index] = server
        return server

    def exclusive(self, sql: str) -> None:
        """Runs DDL on a brief exclusive writer, which fences every shared writer running."""
        server = self.server(0, shared=False)
        server.start()
        try:
            for statement in filter(None, (part.strip() for part in sql.split(";"))):
                server.query(statement)
        finally:
            server.stop()

    def stop_all(self) -> None:
        for server in self.servers.values():
            if server.process and server.process.poll() is None:
                signal_server(server, signal.SIGCONT)
            server.stop()


def signal_server(server: stack.Server, sig: signal.Signals) -> None:
    """Signals the server's process group: `clickhouse server` runs as a watchdog whose child serves."""
    os.killpg(server.process.pid, sig)


def kill(server: stack.Server) -> None:
    """SIGKILL to the watchdog and the server alike, so neither finishes anything."""
    signal_server(server, signal.SIGKILL)
    server.process.wait()
    server.process = None
    server.pidfile.unlink(missing_ok=True)


def events(server: stack.Server, name: str) -> int:
    return int(server.value(f"SELECT value FROM system.events WHERE event = '{name}'") or 0)


def active_ranges(server: stack.Server, table: str) -> list[tuple[str, int, int]]:
    rows = server.query(f"SELECT partition_id, min_block_number, max_block_number FROM system.parts "
                        f"WHERE database = 's' AND table = '{table}' AND active ORDER BY partition_id, min_block_number")
    return [(p, int(lo), int(hi)) for p, lo, hi in (line.split("\t") for line in rows.splitlines())]


def disjoint(ranges: list[tuple[str, int, int]]) -> bool:
    for (p1, _, hi), (p2, lo, _) in zip(ranges, ranges[1:]):
        if p1 == p2 and lo <= hi:
            return False
    return True


def fresh_exclusive_check(run: Run, table: str, expected_rows: int) -> None:
    """Loads the disk on a new exclusive writer: exact rows, `CHECK TABLE`, disjoint parts, no intersection logged."""
    server = run.server(0, shared=False)
    server.start()
    try:
        rows = server.value(f"SELECT count() FROM s.{table}")
        check(rows == str(expected_rows), f"{table}: a fresh exclusive writer reads every row once", f"{rows} rows, want {expected_rows}")
        check(server.value(f"CHECK TABLE s.{table} SETTINGS check_query_single_value_result = 1") == "1", f"{table}: CHECK TABLE passes")
        check(disjoint(active_ranges(server, table)), f"{table}: no two active parts intersect")
        log = server.log.read_text()
        check("intersects" not in log, f"{table}: the fresh writer logs no intersecting parts")
    finally:
        server.stop()


def insert_retrying(writers: list[stack.Server], sql: str, token: str, timeout: float = 120) -> stack.Server:
    """Inserts with a deduplication token, retrying on the writers in turn until one acknowledges it."""
    deadline = time.monotonic() + timeout
    settings = {"insert_deduplication_token": token, "deduplicate_blocks_in_dependent_materialized_views": 1, "async_insert": 0}
    attempt = 0
    while True:
        server = writers[attempt % len(writers)]
        attempt += 1
        try:
            server.query(sql, settings=settings)
            return server
        except (stack.QueryError, OSError):
            if time.monotonic() > deadline:
                raise
            time.sleep(0.2)


def scenario_modes() -> None:
    run = Run("modes")
    run.exclusive("CREATE DATABASE s; CREATE TABLE s.t (k UInt64) ENGINE = MergeTree ORDER BY k")
    keys = [run.key, stack.mint_key(run.root, "write"), stack.mint_key(run.root, "write")]
    writers = [run.server(i + 1, key=keys[i]) for i in range(3)]
    try:
        for server in writers:
            server.start()
        for i, server in enumerate(writers):
            server.query(f"INSERT INTO s.t SELECT number FROM numbers({i * 100}, 100)")
        check(True, "three shared writers commit side by side")
        writers[0].query("SYSTEM START MERGES s.t")
        check(wait_for(lambda: writers[0].value("SELECT count() FROM s.t") == "300"), "a shared writer sees the others' parts after a refresh")

        stack.idb("key", "revoke", keys[2])
        try:
            writers[2].query("INSERT INTO s.t SELECT number FROM numbers(1000, 10)")
            check(False, "revoking one writer's key fences that writer")
        except stack.QueryError as error:
            check(True, "revoking one writer's key fences that writer", str(error)[:200])
        writers[1].query("INSERT INTO s.t SELECT number FROM numbers(2000, 10)")
        check(True, "the other shared writers keep committing")

        exclusive = run.server(0, shared=False)
        exclusive.start()
        try:
            try:
                writers[0].query("INSERT INTO s.t SELECT number FROM numbers(3000, 10)")
                check(False, "an exclusive writer fences every shared writer")
            except stack.QueryError as error:
                check("TABLE_IS_READ_ONLY" in str(error), "an exclusive writer fences every shared writer", str(error)[:200])
            exclusive.query("INSERT INTO s.t SELECT number FROM numbers(4000, 10)")
            check(exclusive.value("SELECT count() FROM s.t") == "320", "the exclusive writer reads what the shared writers committed")
        finally:
            exclusive.stop()

        writers[0].stop()
        writers[0].start()
        writers[0].query("INSERT INTO s.t SELECT number FROM numbers(5000, 10)")
        check(writers[0].value("SELECT count() FROM s.t") == "330", "a restarted shared writer begins a new shared epoch")
    finally:
        run.stop_all()


def scenario_refused() -> None:
    run = Run("refused")
    run.exclusive("CREATE DATABASE s; CREATE TABLE s.t (k UInt64, v String) ENGINE = MergeTree ORDER BY k; "
                  "INSERT INTO s.t SELECT number, 'a' FROM numbers(10)")
    writer = run.server(1)
    try:
        writer.start()
        refused = [
            "ALTER TABLE s.t ADD COLUMN x UInt8",
            "ALTER TABLE s.t DELETE WHERE k = 1",
            "ALTER TABLE s.t UPDATE v = 'b' WHERE k = 1",
            "DELETE FROM s.t WHERE k = 2",
            "ALTER TABLE s.t DROP PARTITION tuple()",
            "ALTER TABLE s.t DETACH PARTITION tuple()",
            "TRUNCATE TABLE s.t",
            "CREATE TABLE s.u (k UInt64) ENGINE = MergeTree ORDER BY k",
            "CREATE VIEW s.v AS SELECT 1",
            "DROP TABLE s.t",
            "RENAME TABLE s.t TO s.t2",
            "CREATE DATABASE s2",
        ]
        for sql in refused:
            try:
                writer.query(sql)
                check(False, f"a shared writer refuses `{sql}`")
            except stack.QueryError as error:
                check(EXCLUSIVE_HINT in str(error), f"a shared writer refuses `{sql}`, naming exclusive mode", str(error)[:300])
        writer.query("INSERT INTO s.t SELECT number, 'c' FROM numbers(10, 10)")
        writer.query("OPTIMIZE TABLE s.t FINAL")
        check(writer.value("SELECT count(), uniqExact(_part) FROM s.t") == "20\t1", "a shared writer inserts and runs OPTIMIZE")
    finally:
        run.stop_all()


def scenario_definition() -> None:
    run = Run("definition")
    run.exclusive("CREATE DATABASE s; CREATE TABLE s.t (k UInt64) ENGINE = MergeTree ORDER BY k")
    writer = run.server(1)
    try:
        writer.start()
        writer.query("INSERT INTO s.t SELECT number FROM numbers(10)")
        metadata_path = writer.value("SELECT metadata_path FROM system.tables WHERE database = 's' AND name = 't'")
        # A control plane rewrites the definition without fencing the writer: the same bytes at a new version.
        path = f"{run.root}/tree/{metadata_path}"
        definition = stack.idb_bytes("cat", path)
        subprocess.run([str(stack.IDB), "--endpoint", f"http://{stack.INODEDB_ENDPOINT}", "--key", stack.admin_key(), "put", path],
                       input=definition, check=True, capture_output=True)
        try:
            writer.query("INSERT INTO s.t SELECT number FROM numbers(10, 10)")
            check(False, "an insert after the definition changed commits nothing")
        except stack.QueryError as error:
            check("definition" in str(error) and "ABORTED" in str(error), "an insert after the definition changed commits nothing", str(error)[:300])
        check(writer.value("SELECT count() FROM s.t") == "10", "the refused insert left no rows")
        writer.stop()
        writer.start()
        writer.query("INSERT INTO s.t SELECT number FROM numbers(10, 10)")
        check(writer.value("SELECT count() FROM s.t") == "20", "a writer that loaded the new definition inserts")
    finally:
        run.stop_all()


VIEW_DDL = """
CREATE DATABASE s;
CREATE TABLE s.src (w UInt8, i UInt64, v String, PROJECTION by_w (SELECT w, count() GROUP BY w))
    ENGINE = MergeTree ORDER BY (w, i) SETTINGS non_replicated_deduplication_window = 10000;
CREATE TABLE s.dst (w UInt8, c UInt64) ENGINE = SummingMergeTree ORDER BY w SETTINGS non_replicated_deduplication_window = 10000;
CREATE MATERIALIZED VIEW s.mv TO s.dst AS SELECT w, count() AS c FROM s.src GROUP BY w
"""


def scenario_concurrent(writers_count: int = 4, inserts: int = 40, rows: int = 1000) -> None:
    run = Run("concurrent")
    run.exclusive(VIEW_DDL)
    writers = [run.server(i + 1) for i in range(writers_count)]
    read_key = stack.mint_key(run.root, "read")
    replica = stack.Server(
        name=f"shared-{run.id}-replica", tcp_port=23099, http_port=24099, interserver_port=25099,
        disks=[fleet.replica_disk_xml(s3_prefix=run.root, key=read_key)],
        policies=[stack.policy_xml(fleet.DISK, fleet.DISK)],
        extra_config=f"<merge_tree><storage_policy>{fleet.DISK}</storage_policy><refresh_parts_interval>1</refresh_parts_interval></merge_tree>",
    )
    run.servers[99] = replica
    try:
        for server in writers:
            server.start()
        replica.start()
        catalog.attach(replica, read_key)

        errors: list[str] = []

        def insert(index: int) -> None:
            for n in range(inserts):
                try:
                    writers[index].query(f"INSERT INTO s.src SELECT {index}, number, 'x' FROM numbers({n * rows}, {rows})")
                except stack.QueryError as error:
                    errors.append(str(error)[:300])

        threads = [threading.Thread(target=insert, args=(i,)) for i in range(writers_count)]
        for thread in threads:
            thread.start()
        seen: list[int] = []
        while any(thread.is_alive() for thread in threads):
            try:
                seen.append(int(replica.value("SELECT count() FROM s.src")))
            except stack.QueryError:
                pass
            time.sleep(0.2)
        for thread in threads:
            thread.join()

        total = writers_count * inserts * rows
        check(not errors, "every insert of every writer commits", "; ".join(errors[:3]))
        check(all(count % rows == 0 for count in seen), "the replica reads whole inserts throughout", str(seen[:20]))
        check(all(a <= b for a, b in zip(seen, seen[1:])), "the replica's counts never go back")
        for server in writers:
            ok = wait_for(lambda: server.value("SELECT count() FROM s.src") == str(total))
            check(ok, f"{server.name} reads every writer's rows", server.value("SELECT count() FROM s.src"))
        check(wait_for(lambda: replica.value("SELECT count() FROM s.src") == str(total)), "the replica reads every row",
              f"{replica.value('SELECT count() FROM s.src')} rows in {replica.value('SELECT count() FROM system.parts WHERE active AND table = ' + chr(39) + 'src' + chr(39))} parts")
        sums = writers[0].value("SELECT w, sum(c) FROM s.dst GROUP BY w ORDER BY w")
        check(sums == "\n".join(f"{i}\t{inserts * rows}" for i in range(writers_count)), "the view's table counts every row once", sums)
        by_projection = writers[0].value("SELECT w, count() FROM s.src GROUP BY w ORDER BY w",
                                         settings={"optimize_use_projections": 1, "force_optimize_projection": 1})
        check(by_projection == "\n".join(f"{i}\t{inserts * rows}" for i in range(writers_count)), "the projection counts every row once", by_projection)
        numbers = writers[0].query("SELECT min_block_number FROM system.parts WHERE database = 's' AND table = 'src' AND level = 0").split()
        check(len(numbers) == len(set(numbers)), "no two inserted parts share a block number", f"{len(numbers)} parts, {len(set(numbers))} numbers")
        retries = sum(events(server, "InodeDBCommitRetries") for server in writers)
        print(f"    {retries} commits retried with a new block number", flush=True)
        check(disjoint(active_ranges(writers[0], "src")), "no two active parts intersect")
    finally:
        run.stop_all()
    fresh_exclusive_check(run, "src", writers_count * inserts * rows)


def pause_and_run(server: stack.Server, failpoint: str, sql: str, settings: dict[str, object]) -> threading.Thread:
    """Starts `sql` on `server` with `failpoint` paused, and returns once the query waits there."""
    server.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
    thread = threading.Thread(target=lambda: _ignore(lambda: server.query(sql, settings=settings, timeout=120)))
    thread.start()
    server.query(f"SYSTEM WAIT FAILPOINT {failpoint} PAUSE", timeout=60)
    return thread


def pause_merge(server: stack.Server, attempts: int = 20) -> threading.Thread:
    """Runs `OPTIMIZE FINAL` until its merge waits before committing. Background merges may claim the parts first."""
    server.query("SYSTEM ENABLE FAILPOINT inodedb_merge_pause_before_commit")
    for _ in range(attempts):
        thread = threading.Thread(target=lambda: _ignore(lambda: server.query("OPTIMIZE TABLE s.l FINAL", timeout=120)))
        thread.start()
        try:
            server.query("SYSTEM WAIT FAILPOINT inodedb_merge_pause_before_commit PAUSE", timeout=5)
            return thread
        except (stack.QueryError, OSError):
            thread.join()
            time.sleep(1)
    raise RuntimeError(f"{server.name}: no merge reached the failpoint")


def merge_to_one(server: stack.Server, timeout: float = 30) -> bool:
    """Runs `OPTIMIZE FINAL` until the table is one part. Another writer's claims may hold the parts a while."""
    def one_part() -> bool:
        if server.value("SELECT uniqExact(_part) FROM s.l") == "1":
            return True
        _ignore(lambda: server.query("OPTIMIZE TABLE s.l FINAL"))
        return server.value("SELECT uniqExact(_part) FROM s.l") == "1"
    return wait_for(one_part, timeout=timeout, interval=1)


def _ignore(fn: Callable[[], object]) -> None:
    try:
        fn()
    except (stack.QueryError, OSError):
        pass


def scenario_exactly_once() -> None:
    run = Run("exactly_once")
    run.exclusive(VIEW_DDL)
    keys = [run.key, stack.mint_key(run.root, "write"), stack.mint_key(run.root, "write")]
    writers = [run.server(i + 1, key=keys[i]) for i in range(3)]
    batches: list[int] = []

    def batch_sql(n: int) -> str:
        return f"INSERT INTO s.src SELECT {n % 4}, {n} * 1000000 + number, 'x' FROM numbers(1000)"

    def settings(n: int) -> dict[str, object]:
        return {"insert_deduplication_token": f"batch-{n}", "deduplicate_blocks_in_dependent_materialized_views": 1}

    def expect(what: str) -> None:
        live = next(server for server in writers + [run.servers.get(0)] if server and server.process and server.process.poll() is None)
        want = len(batches) * 1000
        ok = wait_for(lambda: live.value("SELECT count() FROM s.src") == str(want) and live.value("SELECT sum(c) FROM s.dst") == str(want))
        check(ok, what, f"src {live.value('SELECT count() FROM s.src')}, dst {live.value('SELECT sum(c) FROM s.dst')}, want {want}")

    try:
        for server in writers:
            server.start()

        # Killed before its commit.
        thread = pause_and_run(writers[0], "inodedb_insert_pause_before_commit", batch_sql(1), settings(1))
        kill(writers[0])
        thread.join()
        batches.append(1)
        insert_retrying([writers[1]], batch_sql(1), "batch-1")
        insert_retrying([writers[1]], batch_sql(1), "batch-1")
        expect("a writer killed before its commit: the retry lands once in the table and its view")

        # Killed after its first commit, before the rest of the insert.
        writers[0].start()
        thread = pause_and_run(writers[0], "inodedb_insert_pause_after_commit", batch_sql(2), settings(2))
        kill(writers[0])
        thread.join()
        batches.append(2)
        insert_retrying([writers[1]], batch_sql(2), "batch-2")
        expect("a writer killed between a table's commit and its view's: the retry fills in the rest once")

        # Killed after the insert returned.
        writers[0].start()
        writers[0].query(batch_sql(3), settings=settings(3))
        kill(writers[0])
        batches.append(3)
        insert_retrying([writers[1]], batch_sql(3), "batch-3")
        expect("a writer killed after its insert returned: the retry adds nothing")

        # Revoked while it waits to commit.
        thread = pause_and_run(writers[2], "inodedb_insert_pause_before_commit", batch_sql(4), settings(4))
        stack.idb("key", "revoke", keys[2])
        writers[2].query("SYSTEM DISABLE FAILPOINT inodedb_insert_pause_before_commit")
        thread.join()
        batches.append(4)
        insert_retrying([writers[1]], batch_sql(4), "batch-4")
        expect("a writer whose key is revoked mid-insert: the retry lands once")

        # Fenced by an exclusive writer while it waits to commit.
        thread = pause_and_run(writers[1], "inodedb_insert_pause_before_commit", batch_sql(5), settings(5))
        exclusive = run.server(0, shared=False)
        exclusive.start()
        writers[1].query("SYSTEM DISABLE FAILPOINT inodedb_insert_pause_before_commit")
        thread.join()
        batches.append(5)
        insert_retrying([exclusive], batch_sql(5), "batch-5")
        insert_retrying([exclusive], batch_sql(5), "batch-5")
        expect("a writer fenced mid-insert by an exclusive writer: the retry on the exclusive writer lands once")
        exclusive.stop()
    finally:
        run.stop_all()
    fresh_exclusive_check(run, "src", len(batches) * 1000)


def scenario_merges(inserting: int = 2, merging: int = 2, parts: int = 1300) -> None:
    run = Run("merges")
    run.exclusive("CREATE DATABASE s; CREATE TABLE s.m (k UInt64) ENGINE = MergeTree ORDER BY k "
                  "SETTINGS parts_to_delay_insert = 100000, parts_to_throw_insert = 100000")
    inserters = [run.server(i + 1) for i in range(inserting)]
    mergers = [run.server(inserting + i + 1) for i in range(merging)]
    try:
        for server in inserters + mergers:
            server.start()
        for server in inserters:
            server.query("SYSTEM STOP MERGES s.m")

        per_writer = parts // inserting

        def insert(index: int) -> None:
            for n in range(per_writer):
                inserters[index].query(f"INSERT INTO s.m SELECT {index} * 1000000000 + {n} * 10 + number FROM numbers(10)")

        threads = [threading.Thread(target=insert, args=(i,)) for i in range(inserting)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        total = inserting * per_writer * 10
        # Let the merging writers catch up, then stop them all.
        time.sleep(15)
        merged = sum(int(server.value("SELECT count() FROM system.parts WHERE database = 's' AND table = 'm' AND level > 0")) for server in mergers)
        taken = sum(events(server, "InodeDBClaimsTaken") for server in mergers)
        check(merged > 0 and taken > 0, "the merging writers merge", f"{merged} merged parts, {taken} claims")
        for server in mergers:
            check(disjoint(active_ranges(server, "m")), f"{server.name}: no two active parts intersect")
    finally:
        run.stop_all()
    fresh_exclusive_check(run, "m", inserting * (parts // inserting) * 10)


def scenario_leases() -> None:
    run = Run("leases", claim_ttl_ms=3000)
    run.exclusive("CREATE DATABASE s; CREATE TABLE s.l (k UInt64) ENGINE = MergeTree ORDER BY k; SYSTEM STOP MERGES s.l; "
                  + "; ".join(f"INSERT INTO s.l SELECT {n} * 100 + number FROM numbers(100)" for n in range(20)))
    a, b = run.server(1), run.server(2)
    try:
        a.start()
        b.start()
        # Two writers race for the same parts; one merges them.
        results: dict[str, str] = {}

        def optimize(server: stack.Server) -> None:
            try:
                server.query("OPTIMIZE TABLE s.l FINAL")
                results[server.name] = "merged"
            except stack.QueryError as error:
                results[server.name] = str(error)[:200]

        threads = [threading.Thread(target=optimize, args=(server,)) for server in (a, b)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        check(wait_for(lambda: a.value("SELECT count(), uniqExact(_part) FROM s.l") == "2000\t1"), "two writers race for the same parts and one merges them",
              f"{results}, {a.value('SELECT count(), uniqExact(_part) FROM s.l')}")
        check(disjoint(active_ranges(a, "l")), "no two active parts intersect after the race")

        for n in range(20, 30):
            a.query(f"INSERT INTO s.l SELECT {n} * 100 + number FROM numbers(100)")
        wait_for(lambda: b.value("SELECT count() FROM s.l") == "3000")
        time.sleep(1.5)

        # A killed holder's claims expire, and another writer takes over.
        thread = pause_merge(a)
        kill(a)
        thread.join()
        check(merge_to_one(b), "a killed holder's claims expire and another writer merges the parts")
        check(b.value("SELECT count() FROM s.l") == "3000", "the takeover merge keeps every row")

        # A holder whose claims another writer took over commits nothing.
        a.start()
        for n in range(30, 40):
            b.query(f"INSERT INTO s.l SELECT {n} * 100 + number FROM numbers(100)")
        wait_for(lambda: a.value("SELECT count() FROM s.l") == "4000")
        time.sleep(1.5)
        thread = pause_merge(a)
        signal_server(a, signal.SIGSTOP)
        time.sleep(run.claim_ttl_ms / 1000 + 1)
        check(merge_to_one(b), "another writer takes over the claims of a stopped holder")
        winner = b.value("SELECT _part FROM s.l LIMIT 1")
        signal_server(a, signal.SIGCONT)
        a.query("SYSTEM DISABLE FAILPOINT inodedb_merge_pause_before_commit")
        thread.join()
        time.sleep(2)
        check(b.value("SELECT count(), uniqExact(_part), any(_part) FROM s.l") == f"4000\t1\t{winner}",
              "the stopped holder's merge commits nothing visible", b.value("SELECT count(), uniqExact(_part), any(_part) FROM s.l"))
        check(events(a, "InodeDBClaimsLost") + events(a, "InodeDBCommitConflicts") > 0, "the stopped holder counts its lost claims")
    finally:
        run.stop_all()
    fresh_exclusive_check(run, "l", 4000)


def scenario_mode_changes(duration: float = 20) -> None:
    run = Run("mode_changes")
    run.exclusive(VIEW_DDL)
    writers = [run.server(i + 1) for i in range(3)]
    for server in writers:
        server.start()
    exclusive = run.server(0, shared=False)
    acknowledged: set[int] = set()
    stop = threading.Event()
    counter = iter(range(1, 1_000_000))
    lock = threading.Lock()

    def insert_loop(index: int) -> None:
        while not stop.is_set():
            with lock:
                n = next(counter)
            live = [server for server in writers + [exclusive] if server.process and server.process.poll() is None]
            live.sort(key=lambda server: server is not writers[index])
            try:
                insert_retrying(live, f"INSERT INTO s.src SELECT {n % 4}, {n} * 1000000 + number, 'x' FROM numbers(100)", f"m-{n}", timeout=60)
                with lock:
                    acknowledged.add(n)
            except (stack.QueryError, OSError):
                pass

    threads = [threading.Thread(target=insert_loop, args=(i,)) for i in range(3)]
    try:
        for thread in threads:
            thread.start()
        time.sleep(duration / 4)
        exclusive.start()
        time.sleep(duration / 4)
        exclusive.stop()
        for server in writers:
            server.stop()
            server.start()
        time.sleep(duration / 2)
        stop.set()
        for thread in threads:
            thread.join()
        want = len(acknowledged) * 100
        live = writers[0]
        ok = wait_for(lambda: live.value("SELECT count() FROM s.src") == str(want))
        check(ok, "every acknowledged insert lands once across the mode changes", f"{live.value('SELECT count() FROM s.src')} rows, want {want}")
        numbers = live.query("SELECT min_block_number FROM system.parts WHERE database = 's' AND table = 'src' AND level = 0").split()
        check(len(numbers) == len(set(numbers)), "block numbers stay unique across the mode changes")
    finally:
        stop.set()
        run.stop_all()
    fresh_exclusive_check(run, "src", len(acknowledged) * 100)


ENGINES = {
    "plain": ("MergeTree ORDER BY k", "k, v", "SELECT k, v FROM s.{t} ORDER BY k, v"),
    "replacing": ("ReplacingMergeTree(ver) ORDER BY k", "k, v, ver", "SELECT k, v, ver FROM s.{t} FINAL ORDER BY k"),
    "summing": ("SummingMergeTree ORDER BY k", "k, v", "SELECT k, sum(v) FROM s.{t} GROUP BY k ORDER BY k"),
    "aggregating": ("AggregatingMergeTree ORDER BY k", "k, v", "SELECT k, sumMerge(v) FROM s.{t} GROUP BY k ORDER BY k"),
    "collapsing": ("CollapsingMergeTree(sign) ORDER BY k", "k, v, sign", "SELECT k, v, sign FROM s.{t} FINAL ORDER BY k, v"),
    "versioned": ("VersionedCollapsingMergeTree(sign, ver) ORDER BY k", "k, v, sign, ver", "SELECT k, v, sign, ver FROM s.{t} FINAL ORDER BY k, v, ver"),
}
COLUMNS = {
    "plain": "k UInt64, v UInt64",
    "replacing": "k UInt64, v UInt64, ver UInt64",
    "summing": "k UInt64, v UInt64",
    "aggregating": "k UInt64, v AggregateFunction(sum, UInt64)",
    "collapsing": "k UInt64, v UInt64, sign Int8",
    "versioned": "k UInt64, v UInt64, sign Int8, ver UInt64",
}


def workload(engine: str, batch: int) -> str:
    rows = f"numbers({batch * 50}, 200)"
    key = "number % 300"
    return {
        "plain": f"INSERT INTO s.{engine} SELECT {key}, number FROM {rows}",
        "replacing": f"INSERT INTO s.{engine} SELECT {key}, number, number FROM {rows}",
        "summing": f"INSERT INTO s.{engine} SELECT {key}, number FROM {rows}",
        "aggregating": f"INSERT INTO s.{engine} SELECT {key}, sumState(number) FROM {rows} GROUP BY {key}",
        "collapsing": f"INSERT INTO s.{engine} SELECT {key}, number % 7, if(number % 3 = 0, -1, 1) FROM {rows}",
        "versioned": f"INSERT INTO s.{engine} SELECT {key}, number % 7, if(number % 3 = 0, -1, 1), number % 5 FROM {rows}",
    }[engine]


def scenario_differential(batches: int = 30) -> None:
    ddl = "CREATE DATABASE s; " + "; ".join(f"CREATE TABLE s.{name} ({COLUMNS[name]}) ENGINE = {spec[0]}" for name, spec in ENGINES.items())
    answers: dict[str, dict[str, str]] = {}
    for mode in ("exclusive", "shared"):
        run = Run(f"differential-{mode}")
        run.exclusive(ddl)
        if mode == "exclusive":
            writers = [run.server(0, shared=False)]
        else:
            writers = [run.server(i + 1) for i in range(3)]
        try:
            for server in writers:
                server.start()
            for batch in range(batches):
                for engine in ENGINES:
                    writers[batch % len(writers)].query(workload(engine, batch))
            reader = writers[0]
            for engine in ENGINES:
                wait_for(lambda engine=engine: reader.value(f"SELECT count() FROM s.{engine}") ==
                         writers[-1].value(f"SELECT count() FROM s.{engine}"))
            time.sleep(2)
            for engine in ENGINES:
                reader.query(f"OPTIMIZE TABLE s.{engine} FINAL")
            answers[mode] = {engine: reader.query(query.format(t=engine)) for engine, (_, _, query) in ENGINES.items()}
        finally:
            run.stop_all()
    for engine in ENGINES:
        check(answers["exclusive"][engine] == answers["shared"][engine], f"{engine}: shared writers answer as one exclusive writer does",
              f"{len(answers['exclusive'][engine])} against {len(answers['shared'][engine])} bytes")


def scenario_kill_loop(rounds: int) -> None:
    run = Run("kill_loop", claim_ttl_ms=3000)
    run.exclusive("CREATE DATABASE s; CREATE TABLE s.k (n UInt64, i UInt64) ENGINE = MergeTree ORDER BY (n, i) "
                  "SETTINGS non_replicated_deduplication_window = 100000")
    writers = [run.server(i + 1) for i in range(3)]
    for server in writers:
        server.start()
    acknowledged: set[int] = set()
    stop = threading.Event()
    lock = threading.Lock()
    counter = iter(range(1, 10_000_000))
    rng = random.Random(int(run.id, 16))

    def insert_loop() -> None:
        while not stop.is_set():
            with lock:
                n = next(counter)
            try:
                live = [server for server in writers if server.process and server.process.poll() is None]
                rng.shuffle(live)
                insert_retrying(live or writers, f"INSERT INTO s.k SELECT {n}, number FROM numbers(50)", f"k-{n}", timeout=90)
                with lock:
                    acknowledged.add(n)
            except (stack.QueryError, OSError, IndexError):
                pass

    threads = [threading.Thread(target=insert_loop) for _ in range(4)]
    try:
        for thread in threads:
            thread.start()
        for round_ in range(rounds):
            time.sleep(rng.uniform(0.5, 3))
            victim = rng.choice(writers)
            kill(victim)
            time.sleep(rng.uniform(0, 1))
            victim.start()
            if (round_ + 1) % 10 == 0:
                print(f"    round {round_ + 1}: {len(acknowledged)} inserts acknowledged", flush=True)
        stop.set()
        for thread in threads:
            thread.join()
    finally:
        stop.set()
        run.stop_all()
    print(f"    {rounds} kills, {len(acknowledged)} inserts acknowledged", flush=True)
    fresh_exclusive_check(run, "k", len(acknowledged) * 50)


SCENARIOS = ["modes", "refused", "definition", "concurrent", "exactly_once", "merges", "leases", "mode_changes", "differential", "kill_loop"]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("scenarios", nargs="*", metavar="scenario", help=", ".join(SCENARIOS))
    parser.add_argument("--kill-rounds", type=int, default=30)
    args = parser.parse_args()
    for name in args.scenarios or SCENARIOS:
        print(f"== {name}", flush=True)
        try:
            if name == "kill_loop":
                scenario_kill_loop(args.kill_rounds)
            else:
                globals()[f"scenario_{name}"]()
        except Exception as error:  # noqa: BLE001
            check(False, f"{name} ran to the end", f"{type(error).__name__}: {str(error)[:500]}")
    print(f"{failures} failed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
