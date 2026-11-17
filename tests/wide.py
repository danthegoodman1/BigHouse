#!/usr/bin/env python3
"""Parts of more files than one InodeDB batch holds (PLAN.md, Phase 8).

A wide part keeps ten files or more for each path of a `JSON` column, and a column holds 1,024 paths by default, so
one part passes the 10,000 ops and 8 MiB of an InodeDB batch. The disk commits such a part's files in stages under the
part's temporary directory, and the part's last commit publishes them with its rename.

- `exclusive`: on an exclusive writer, inserts of a `JSON` column of 1,024 paths, of paths with three types each, and
  of three such columns commit, and their rows read back. A part that fits one commit takes one. A duplicate of a
  staged insert leaves nothing. Parts with a projection of the column merge. Compact parts merge into a wide part, a
  mutation rewrites it, and dropping the tables frees every blob.
- `shared`: the same inserts and the merge through shared writers. A second writer and a replica see nothing of a
  part between its stages, and all of it after its last commit.
- `kills`: a writer killed after each stage of an insert and of a merge, and before an insert's last commit, restarts
  with the table as it was and no temporary directory, and the blobs its stages committed go. A killed shared writer
  leaves its directory to the cleanup of temporary directories.
- `cleanup`: another writer's cleanup removes a directory between two stages. The insert fails and publishes nothing,
  and a retry commits.
- `fenced`: an exclusive writer starts between two stages of another's insert. The fenced writer commits nothing more.

    python3 tests/wide.py [scenario ...]
"""

from __future__ import annotations

import argparse
import os
import signal
import sys
import threading
import time
import uuid
from collections.abc import Callable
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bighouse import catalog, fleet, stack  # noqa: E402

failures = 0
PATHS = 1024
STAGE = "inodedb_pause_after_stage"
# Background cleanup each second, so a test waits seconds for what a server does in minutes.
FAST_CLEANUP = ("<old_parts_lifetime>1</old_parts_lifetime><merge_tree_clear_old_parts_interval_seconds>1</merge_tree_clear_old_parts_interval_seconds>"
                "<merge_tree_clear_old_temporary_directories_interval_seconds>1</merge_tree_clear_old_temporary_directories_interval_seconds>"
                "<cleanup_delay_period>1</cleanup_delay_period><cleanup_delay_period_random_add>0</cleanup_delay_period_random_add>"
                "<max_cleanup_delay_period>1</max_cleanup_delay_period>")
# Merges run only when a test asks for one.
NO_BACKGROUND_MERGES = "max_bytes_to_merge_at_max_space_in_pool = 1, max_bytes_to_merge_at_min_space_in_pool = 1"
WIDE = f"min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, {NO_BACKGROUND_MERGES}"
# Parts under 1,000 rows are compact, so inserts of 400 rows merge into a wide part.
WIDE_FROM_1000_ROWS = f"min_bytes_for_wide_part = 0, min_rows_for_wide_part = 1000, {NO_BACKGROUND_MERGES}"
# A reader's lease holds freed blobs this long after it reads newer versions; the default is ten minutes.
LEASE_LAG = "<inodedb_lease_lag_ms>1000</inodedb_lease_lag_ms>"
# A number below 2^63 for row `k` and path `i`, which no codec shrinks.
HASH = "bitShiftRight(cityHash64({k}, i), 1)"


def check(ok: bool, what: str, detail: str = "") -> bool:
    global failures
    failures += not ok
    print(f"{'PASS' if ok else 'FAIL'} {what}" + (f": {detail}" if detail and not ok else ""), flush=True)
    return ok


def eventually(fn: Callable[[], str], want: str, timeout: float = 60) -> tuple[bool, str]:
    deadline = time.monotonic() + timeout
    got = ""
    while True:
        try:
            got = fn()
        except (stack.QueryError, OSError) as error:
            got = str(error)[:200]
        if got == want or time.monotonic() > deadline:
            return got == want, got
        time.sleep(0.2)


def json_text(paths: int, kinds: int = 1, hash_of: str | None = None) -> str:
    """An expression for a JSON object of `paths` paths. With one kind, path `p<i>` holds `number * i`. With three, a
    row's paths hold that number, a string or an array, by `number % 3`. With `hash_of`, an expression for the row's
    key, the numbers are hashes of the key and the path, so a few thousand rows give each path a blob."""
    value = HASH.format(k=hash_of) if hash_of else "number * i"
    if kinds == 1:
        return (f"toJSONString(mapFromArrays(arrayMap(i -> concat('p', toString(i)), range({paths})), "
                f"arrayMap(i -> {value}, range({paths}))))::JSON")
    one = (f"concat('\"p', toString(i), '\":', multiIf(number % 3 = 0, toString({value}), "
           f"number % 3 = 1, concat('\"s', toString(i), '\"'), '[1, 2]'))")
    return f"concat('{{', arrayStringConcat(arrayMap(i -> {one}, range({paths})), ','), '}}')::JSON"


class Run:
    """An InodeDB root with its servers: index 0 is an exclusive writer, the rest share the disk unless asked otherwise."""

    def __init__(self, name: str):
        self.id = uuid.uuid4().hex[:8]
        self.root = f"wide/{name}/{self.id}"
        self.key = stack.mint_key(self.root, "write")
        self.servers: dict[int, stack.Server] = {}

    def server(self, index: int, *, shared: bool = False, merge_tree: str = "") -> stack.Server:
        mode = "<inodedb_writer_mode>shared</inodedb_writer_mode>" if shared else ""
        server = stack.Server(
            name=f"wide-{self.id}-{index}", tcp_port=29000 + index, http_port=29100 + index, interserver_port=29200 + index,
            disks=[stack.inodedb_disk_xml(fleet.DISK, s3_prefix=self.root, key=self.key, extra=mode + LEASE_LAG)],
            policies=[stack.policy_xml(fleet.DISK, fleet.DISK)],
            extra_config=f"<merge_tree><storage_policy>{fleet.DISK}</storage_policy>{FAST_CLEANUP}{merge_tree}</merge_tree>"
                         + fleet.database_disk_xml() + "<database_atomic_delay_before_drop_table_sec>0</database_atomic_delay_before_drop_table_sec>",
        )
        self.servers[index] = server
        return server

    def replica(self) -> stack.Server:
        read_key = stack.mint_key(self.root, "read")
        server = stack.Server(
            name=f"wide-{self.id}-replica", tcp_port=29099, http_port=29199, interserver_port=29299,
            disks=[stack.inodedb_disk_xml(fleet.DISK, s3_prefix=self.root, key=read_key, read_only=True, extra="<readonly>1</readonly>" + LEASE_LAG)],
            policies=[stack.policy_xml(fleet.DISK, fleet.DISK)],
            extra_config=f"<merge_tree><storage_policy>{fleet.DISK}</storage_policy><refresh_parts_interval>1</refresh_parts_interval></merge_tree>",
        )
        self.servers[99] = server
        server.start()
        catalog.attach(server, read_key)
        return server

    def exclusive(self, sql: str) -> None:
        """Runs DDL on a brief exclusive writer, which fences every shared writer running."""
        server = self.server(0)
        server.start()
        try:
            for statement in filter(None, (part.strip() for part in sql.split(";"))):
                server.query(statement)
        finally:
            server.stop()

    def stop_all(self) -> None:
        for server in self.servers.values():
            server.stop()
            server.wipe()

    def table_dir(self, server: stack.Server, table: str) -> str:
        table_uuid = server.value(f"SELECT uuid FROM system.tables WHERE database = 'w' AND name = '{table}'")
        return f"{self.root}/tree/store/{table_uuid[:3]}/{table_uuid}"

    def blobs(self) -> set[str]:
        return set(stack.s3_objects(self.root))


def names(directory: str) -> list[str]:
    """The names in an InodeDB directory."""
    return [name.rstrip("/") for name in stack.idb("ls", directory).splitlines()]


def temporary(directory: str) -> list[str]:
    return [name for name in names(directory) if name.startswith(("tmp", "delete_tmp"))]


def kill(server: stack.Server) -> None:
    """SIGKILL to the watchdog and the server alike, so neither finishes anything."""
    os.killpg(server.process.pid, signal.SIGKILL)
    server.process.wait()
    server.process = None
    server.pidfile.unlink(missing_ok=True)


def events(server: stack.Server, name: str) -> int:
    return int(server.value(f"SELECT value FROM system.events WHERE event = '{name}'") or 0)


def commits_of(server: stack.Server, sql: str, settings: dict[str, object] | None = None) -> tuple[int, int]:
    """Runs `sql` and returns how many InodeDB commits the server made meanwhile, and how many of them were stages."""
    before = events(server, "InodeDBCommit"), events(server, "InodeDBCommitStages")
    server.query(sql, settings=settings)
    return events(server, "InodeDBCommit") - before[0], events(server, "InodeDBCommitStages") - before[1]


class Background:
    """A query on its own thread that keeps its error."""

    def __init__(self, server: stack.Server, sql: str, settings: dict[str, object] | None = None):
        self.error: str | None = None
        self.thread = threading.Thread(target=self._run, args=(server, sql, settings or {}))
        self.thread.start()

    def _run(self, server: stack.Server, sql: str, settings: dict[str, object]) -> None:
        try:
            server.query(sql, settings=settings, timeout=300)
        except (stack.QueryError, OSError) as error:
            self.error = str(error)[:600]

    def join(self) -> str | None:
        self.thread.join()
        return self.error


def paused_after_stage(server: stack.Server, sql: str, stage: int = 1) -> Background:
    """Starts `sql` and returns once it waits after committing its stage number `stage`."""
    server.query(f"SYSTEM ENABLE FAILPOINT {STAGE}")
    query = Background(server, sql)
    for passed in range(stage):
        if passed:
            server.query(f"SYSTEM NOTIFY FAILPOINT {STAGE}")
        server.query(f"SYSTEM WAIT FAILPOINT {STAGE} PAUSE", timeout=120)
    return query


def staged_blobs(server: stack.Server, directory: str) -> set[str]:
    """The blobs that the files of a temporary directory name: what its stages committed."""
    rows = server.query(f"SELECT remote_path FROM system.remote_data_paths WHERE disk_name = '{fleet.DISK}' "
                        f"AND position(local_path, '/{directory}/') > 0 FORMAT TSV")
    return {Path(row).name for row in rows.splitlines()}


def blob_names(run: Run) -> set[str]:
    return {Path(key).name for key in run.blobs()}


def scenario_exclusive() -> None:
    run = Run("exclusive")
    server = run.server(0)
    server.start()
    try:
        server.query("CREATE DATABASE w")
        baseline = run.blobs()

        # One column, one type a path.
        server.query(f"CREATE TABLE w.j (k UInt64, data JSON) ENGINE = MergeTree ORDER BY k SETTINGS {WIDE}, non_replicated_deduplication_window = 10")
        directory = run.table_dir(server, "j")
        check(commits_of(server, f"INSERT INTO w.j SELECT number, {json_text(4)} FROM numbers(20)") == (1, 0), "a small part takes one commit")

        commits = commits_of(server, f"INSERT INTO w.j SELECT 100 + number, {json_text(380)} FROM numbers(20)")
        files = len(names(f"{directory}/all_2_2_0"))
        check(commits == (1, 0) and 3500 < files <= 4090, "a part of 380 paths, which fits one commit, takes one", f"{commits} commits for {files} files")

        token = {"insert_deduplication_token": "wide"}
        commits, stages = commits_of(server, f"INSERT INTO w.j SELECT 1000 + number, {json_text(PATHS)} FROM numbers(20)", token)
        files = len(names(f"{directory}/all_3_3_0"))
        check(files > 10000 and 2 <= stages <= 4 and commits == stages + 1, "a part of 1,024 paths commits in stages", f"{commits} commits, {stages} of them stages, for {files} files")
        print(f"    {files} files in {commits} commits", flush=True)
        got = server.value("SELECT count(), sum(data.p1023::UInt64) FROM w.j WHERE k >= 1000 FORMAT TSV")
        check(got == f"20\t{1023 * 190}", "its rows read back", got)
        check(server.value("SELECT DISTINCT part_type FROM system.parts WHERE database = 'w' AND table = 'j' AND active") == "Wide", "the part is wide")
        check(not temporary(directory), "no temporary directory is left", str(temporary(directory)))

        # A duplicate stages its files before it learns its token is taken, and removes them.
        server.query(f"INSERT INTO w.j SELECT 1000 + number, {json_text(PATHS)} FROM numbers(20)", settings=token)
        check(server.value("SELECT count() FROM w.j WHERE k >= 1000") == "20", "a duplicate of the staged insert adds no rows")
        ok, got = eventually(lambda: str(temporary(directory)), "[]", timeout=20)
        check(ok, "the duplicate leaves no temporary directory", got)

        # Three types a path, then three columns.
        server.query(f"CREATE TABLE w.types (k UInt64, data JSON) ENGINE = MergeTree ORDER BY k SETTINGS {WIDE}")
        commits, _ = commits_of(server, f"INSERT INTO w.types SELECT number, {json_text(PATHS, kinds=3)} FROM numbers(21)")
        files = len(names(f"{run.table_dir(server, 'types')}/all_1_1_0"))
        got = server.value("SELECT count(), sum(data.p1023.:Int64), countIf(data.p7.:String = 's7'), countIf(length(data.p9.:`Array(Nullable(Int64))`) = 2) FROM w.types FORMAT TSV")
        check(got == f"21\t{1023 * 63}\t7\t7", "a part of 1,024 paths of three types each commits and reads back", got)
        print(f"    {files} files in {commits} commits", flush=True)

        server.query(f"CREATE TABLE w.columns (k UInt64, a JSON, b JSON, c JSON) ENGINE = MergeTree ORDER BY k SETTINGS {WIDE}")
        commits, _ = commits_of(server, f"INSERT INTO w.columns SELECT number, {json_text(PATHS)}, {json_text(PATHS, kinds=3)}, {json_text(PATHS)} FROM numbers(21)")
        files = len(names(f"{run.table_dir(server, 'columns')}/all_1_1_0"))
        got = server.value("SELECT count(), sum(a.p1023::UInt64), sum(b.p1023.:Int64), sum(c.p5::UInt64) FROM w.columns FORMAT TSV")
        check(got == f"21\t{1023 * 210}\t{1023 * 63}\t{5 * 210}", "a part of three such columns commits and reads back", got)
        print(f"    {files} files in {commits} commits", flush=True)

        # A projection of the column doubles a part's files, and a merge commits the projection in a transaction of its own.
        server.query(f"CREATE TABLE w.projected (k UInt64, data JSON, PROJECTION p (SELECT k, data ORDER BY k)) ENGINE = MergeTree ORDER BY tuple() SETTINGS {WIDE}")
        for i in range(2):
            server.query(f"INSERT INTO w.projected SELECT {i} * 100 + number, {json_text(PATHS)} FROM numbers(20)")
        files = len(names(f"{run.table_dir(server, 'projected')}/all_1_1_0/p.proj"))
        _, stages = commits_of(server, "OPTIMIZE TABLE w.projected FINAL")
        got = server.value("SELECT count(), sum(data.p1023::UInt64), uniqExact(_part) FROM w.projected FORMAT TSV")
        check(files > 10000 and stages >= 4 and got == f"40\t{1023 * 380}\t1", "parts with a projection of 1,024 paths commit, and merge in stages", f"{files} files in the projection, {stages} stages, {got}")
        check(server.value("CHECK TABLE w.projected SETTINGS check_query_single_value_result = 1") == "1", "CHECK TABLE passes on the merged part")

        # Compact parts merge into a wide part whose paths keep blobs; a mutation hardlinks them into its part.
        server.query(f"CREATE TABLE w.merged (k UInt64, v UInt64, data JSON) ENGINE = MergeTree ORDER BY k SETTINGS {WIDE_FROM_1000_ROWS}")
        for i in range(6):
            server.query(f"INSERT INTO w.merged SELECT {i} * 400 + number AS k, 1, {json_text(PATHS, hash_of='k')} FROM numbers(400)")
        check(server.value("SELECT DISTINCT part_type FROM system.parts WHERE database = 'w' AND table = 'merged' AND active") == "Compact", "six inserts leave compact parts")
        _, stages = commits_of(server, "OPTIMIZE TABLE w.merged FINAL")
        got = server.value("SELECT part_type, count() FROM system.parts WHERE database = 'w' AND table = 'merged' AND active GROUP BY part_type FORMAT TSV")
        check(got == "Wide\t1" and stages >= 2, "they merge into one wide part, committed in stages", f"{got}, {stages} stages")
        want = server.value(f"SELECT sum({HASH.format(k='number').replace(', i)', ', toUInt16(1023))')}) FROM numbers(2400)")
        got = server.value("SELECT count(), sum(v), sum(data.p1023::UInt64) FROM w.merged FORMAT TSV")
        check(got == f"2400\t2400\t{want}", "the merged part reads back", got)
        check(len(run.blobs() - baseline) > PATHS, "the wide part keeps a blob for each path", str(len(run.blobs() - baseline)))

        _, stages = commits_of(server, "ALTER TABLE w.merged UPDATE v = 2 WHERE 1", {"mutations_sync": 2})
        got = server.value("SELECT count(), sum(v), sum(data.p1023::UInt64) FROM w.merged FORMAT TSV")
        check(got == f"2400\t4800\t{want}" and stages >= 2, "a mutation of the wide part commits in stages and reads back", f"{got}, {stages} stages")
        check(server.value("CHECK TABLE w.merged SETTINGS check_query_single_value_result = 1") == "1", "CHECK TABLE passes")

        for table in ("j", "types", "columns", "projected", "merged"):
            server.query(f"DROP TABLE w.{table} SYNC")
        ok, got = eventually(lambda: str(len(run.blobs() - baseline)), "0", timeout=120)
        check(ok, "dropping the tables frees every blob", f"{got} left")
        log = server.log.read_text().splitlines()
        fallbacks = [line for line in log if "Cannot quickly remove directory" in line]
        check(not fallbacks, "no part's removal falls back from a batch InodeDB refused", "; ".join(line[:300] for line in fallbacks[:3]))
        refused = [line for line in log if "LIMIT_EXCEEDED" in line or "too-many-ops" in line or "too-large" in line]
        check(not refused, "InodeDB refused no batch", "; ".join(line[:300] for line in refused[:3]))
    finally:
        run.stop_all()


def scenario_shared() -> None:
    run = Run("shared")
    run.exclusive(f"""CREATE DATABASE w;
        CREATE TABLE w.j (k UInt64, data JSON) ENGINE = MergeTree ORDER BY k SETTINGS {WIDE};
        CREATE TABLE w.columns (k UInt64, a JSON, b JSON, c JSON) ENGINE = MergeTree ORDER BY k SETTINGS {WIDE};
        CREATE TABLE w.merged (k UInt64, v UInt64, data JSON) ENGINE = MergeTree ORDER BY k SETTINGS {WIDE_FROM_1000_ROWS}""")
    a, b = run.server(1, shared=True), run.server(2, shared=True)
    try:
        a.start()
        b.start()
        replica = run.replica()
        directory = run.table_dir(a, "j")
        count = "SELECT count() FROM w.j"

        check(commits_of(a, f"INSERT INTO w.j SELECT number, {json_text(4)} FROM numbers(20)") == (1, 0), "a small part takes one commit on a shared writer")
        ok, got = eventually(lambda: replica.value(count), "20")
        check(ok, "the replica sees it", got)

        # Between two stages of a part, no other server sees any of it.
        insert = paused_after_stage(a, f"INSERT INTO w.j SELECT 1000 + number, {json_text(PATHS)} FROM numbers(20)")
        staged = temporary(directory)
        check(len(staged) == 1 and len(names(f"{directory}/{staged[0]}")) > 3000, "a stage commits files under the part's temporary directory", str(staged))
        time.sleep(3)
        check(b.value(count) == "20" and replica.value(count) == "20", "between stages, another writer and a replica see none of the part")
        a.query(f"SYSTEM DISABLE FAILPOINT {STAGE}")
        check(insert.join() is None, "the insert commits", str(insert.error))
        for server, who in ((a, "its writer"), (b, "another writer"), (replica, "a replica")):
            ok, got = eventually(lambda s=server: s.value("SELECT count(), sum(data.p1023::UInt64) FROM w.j WHERE k >= 1000 FORMAT TSV"), f"20\t{1023 * 190}")
            check(ok, f"{who} reads the whole part after its last commit", got)
        check(not temporary(directory), "no temporary directory is left", str(temporary(directory)))

        commits, _ = commits_of(b, f"INSERT INTO w.columns SELECT number, {json_text(PATHS)}, {json_text(PATHS, kinds=3)}, {json_text(PATHS)} FROM numbers(21)")
        want = f"21\t{1023 * 210}\t{1023 * 63}\t{5 * 210}"
        for server, who in ((a, "another writer"), (replica, "a replica")):
            ok, got = eventually(lambda s=server: s.value("SELECT count(), sum(a.p1023::UInt64), sum(b.p1023.:Int64), sum(c.p5::UInt64) FROM w.columns FORMAT TSV"), want)
            check(ok, f"{who} reads a part of three columns of 1,024 paths", got)
        print(f"    {commits} commits", flush=True)

        # Both writers insert compact parts, and one merges them into a wide part.
        for i in range(6):
            (a, b)[i % 2].query(f"INSERT INTO w.merged SELECT {i} * 400 + number AS k, 1, {json_text(PATHS, hash_of='k')} FROM numbers(400)")
        ok, got = eventually(lambda: a.value("SELECT count() FROM system.parts WHERE database = 'w' AND table = 'merged' AND active"), "6")
        check(ok, "a writer sees the six compact parts of both", got)
        _, stages = commits_of(a, "OPTIMIZE TABLE w.merged FINAL")
        got = a.value("SELECT part_type, count() FROM system.parts WHERE database = 'w' AND table = 'merged' AND active GROUP BY part_type FORMAT TSV")
        check(got == "Wide\t1" and stages >= 2, "a shared writer merges them into one wide part, committed in stages", f"{got}, {stages} stages")
        hashes = a.value(f"SELECT sum({HASH.format(k='number').replace(', i)', ', toUInt16(1023))')}) FROM numbers(2400)")
        for server, who in ((b, "another writer"), (replica, "a replica")):
            ok, got = eventually(lambda s=server: s.value("SELECT count(), sum(data.p1023::UInt64), uniqExact(_part) FROM w.merged FORMAT TSV"), f"2400\t{hashes}\t1")
            check(ok, f"{who} reads the merged part", got)
        check(a.value("CHECK TABLE w.merged SETTINGS check_query_single_value_result = 1") == "1", "CHECK TABLE passes")
    finally:
        run.stop_all()


def scenario_kills() -> None:
    run = Run("kills")
    server = run.server(0)
    server.start()
    try:
        server.query("CREATE DATABASE w")
        server.query(f"CREATE TABLE w.j (k UInt64, data JSON) ENGINE = MergeTree ORDER BY k SETTINGS {WIDE}")
        server.query(f"CREATE TABLE w.probe (k UInt64, data JSON) ENGINE = MergeTree ORDER BY k SETTINGS {WIDE}")
        server.query(f"INSERT INTO w.j SELECT number, {json_text(4)} FROM numbers(20)")
        replica = run.replica()
        directory = run.table_dir(server, "j")
        # Hashes for 3,000 rows give each path a blob, so the stages commit blobs.
        values = f"SELECT 1000 + number AS k, {json_text(PATHS, hash_of='k')} FROM numbers(3000)"
        insert = f"INSERT INTO w.j {values}"

        _, stages = commits_of(server, f"INSERT INTO w.probe {values}")
        check(stages >= 2, "an insert of 1,024 paths with a blob each commits in stages", str(stages))
        server.query("DROP TABLE w.probe SYNC")

        before = blob_names(run)

        def after_kill(what: str, staged: set[str]) -> None:
            server.start()
            check(server.value("SELECT count() FROM w.j") == "20" and replica.value("SELECT count() FROM w.j") == "20",
                  f"{what}: the table reads as before, on the writer and on a replica")
            check(server.value("CHECK TABLE w.j SETTINGS check_query_single_value_result = 1") == "1", f"{what}: CHECK TABLE passes")
            ok, got = eventually(lambda: str(temporary(directory)), "[]", timeout=30)
            check(ok, f"{what}: the restarted writer leaves no temporary directory", got)
            ok, got = eventually(lambda: str(len(staged & blob_names(run))), "0", timeout=120)
            check(ok, f"{what}: the {len(staged)} blobs its stages committed go", f"{got} left")
            # Blobs the insert uploaded for files no commit named stay until a sweep of the bucket (PLAN.md, 4C).
            print(f"    {len(blob_names(run) - before)} blobs that no commit named stay", flush=True)

        for stage in range(1, stages + 1):
            query = paused_after_stage(server, insert, stage)
            staged = staged_blobs(server, temporary(directory)[0])
            check(len(staged) > 0 and replica.value("SELECT count() FROM w.j") == "20", f"an insert waits after stage {stage} with blobs committed, unseen by a replica", str(len(staged)))
            kill(server)
            query.join()
            after_kill(f"killed after stage {stage} of an insert", staged)

        server.query("SYSTEM ENABLE FAILPOINT inodedb_insert_pause_before_commit")
        query = Background(server, insert)
        server.query("SYSTEM WAIT FAILPOINT inodedb_insert_pause_before_commit PAUSE", timeout=120)
        staged = staged_blobs(server, temporary(directory)[0])
        kill(server)
        query.join()
        after_kill("killed before an insert's last commit", staged)

        # A merge of compact parts into a wide one.
        server.query(f"CREATE TABLE w.merged (k UInt64, data JSON) ENGINE = MergeTree ORDER BY k SETTINGS {WIDE_FROM_1000_ROWS}")
        for i in range(6):
            server.query(f"INSERT INTO w.merged SELECT {i} * 400 + number AS k, {json_text(PATHS, hash_of='k')} FROM numbers(400)")
        merged_dir = run.table_dir(server, "merged")
        parts = "SELECT count(), sum(rows) FROM system.parts WHERE database = 'w' AND table = 'merged' AND active FORMAT TSV"
        for stage in (1, 2):
            query = paused_after_stage(server, "OPTIMIZE TABLE w.merged FINAL", stage)
            staged = staged_blobs(server, temporary(merged_dir)[0])
            check(len(staged) > 0, f"a merge waits after stage {stage} with blobs committed", str(len(staged)))
            kill(server)
            query.join()
            server.start()
            what = f"killed after stage {stage} of a merge"
            check(server.value(parts) == "6\t2400" and server.value("SELECT count() FROM w.merged") == "2400", f"{what}: the table keeps its six parts")
            ok, got = eventually(lambda: str(temporary(merged_dir)), "[]", timeout=30)
            check(ok, f"{what}: the restarted writer leaves no temporary directory", got)
            ok, got = eventually(lambda: str(len(staged & blob_names(run))), "0", timeout=120)
            check(ok, f"{what}: the {len(staged)} blobs its stages committed go", f"{got} left")
        server.query("OPTIMIZE TABLE w.merged FINAL")
        check(server.value(parts) == "1\t2400", "the merge then commits")

        # A shared writer skips the startup cleanup, so the cleanup of old temporary directories removes its leftover.
        server.stop()
        lifetime = "<temporary_directories_lifetime>5</temporary_directories_lifetime>"
        shared = run.server(1, shared=True, merge_tree=lifetime)
        shared.start()
        query = paused_after_stage(shared, insert)
        left = temporary(directory)
        staged = staged_blobs(shared, left[0])
        kill(shared)
        query.join()
        shared.start()
        ok, got = eventually(lambda: str(temporary(directory)), "[]", timeout=60)
        check(ok, "a killed shared writer's temporary directory goes with the cleanup of old ones", got)
        ok, got = eventually(lambda: str(len(staged & blob_names(run))), "0", timeout=120)
        check(ok, f"the {len(staged)} blobs its stage committed go", f"{got} left")
        check(shared.value("SELECT count() FROM w.j") == "20" and replica.value("SELECT count() FROM w.j") == "20", "the table reads as before")
    finally:
        run.stop_all()


def scenario_cleanup() -> None:
    run = Run("cleanup")
    run.exclusive(f"CREATE DATABASE w; CREATE TABLE w.j (k UInt64, data JSON) ENGINE = MergeTree ORDER BY k SETTINGS {WIDE}")
    # Writer `b` removes temporary directories two seconds old. Writer `a` knows its own and keeps them.
    a = run.server(1, shared=True)
    b = run.server(2, shared=True, merge_tree="<temporary_directories_lifetime>2</temporary_directories_lifetime>")
    try:
        a.start()
        b.start()
        directory = run.table_dir(a, "j")
        insert = f"INSERT INTO w.j SELECT number, {json_text(PATHS)} FROM numbers(20)"

        query = paused_after_stage(a, insert)
        check(len(temporary(directory)) == 1, "an insert waits after its first stage")
        ok, got = eventually(lambda: str(temporary(directory)), "[]", timeout=60)
        check(ok, "another writer's cleanup removes the old temporary directory", got)
        a.query(f"SYSTEM DISABLE FAILPOINT {STAGE}")
        error = query.join() or ""
        check("ABORTED" in error and "committed in stages" in error, "the insert fails, naming the directory that changed", error[:300])
        check(a.value("SELECT count() FROM w.j") == "0" and b.value("SELECT count() FROM w.j") == "0", "it published nothing")
        check([n for n in names(directory) if n.startswith("all_")] == [], "the table holds no part", str(names(directory)))

        a.query(insert)
        ok, got = eventually(lambda: b.value("SELECT count(), sum(data.p1023::UInt64) FROM w.j FORMAT TSV"), f"20\t{1023 * 190}")
        check(ok, "a retry commits, and the other writer reads it", got)
    finally:
        run.stop_all()


def scenario_fenced() -> None:
    run = Run("fenced")
    old, new = run.server(0), run.server(3)
    old.start()
    try:
        old.query("CREATE DATABASE w")
        old.query(f"CREATE TABLE w.j (k UInt64, data JSON) ENGINE = MergeTree ORDER BY k SETTINGS {WIDE}")
        old.query(f"INSERT INTO w.j SELECT number, {json_text(4)} FROM numbers(20)")
        directory = run.table_dir(old, "j")

        query = paused_after_stage(old, f"INSERT INTO w.j SELECT 1000 + number, {json_text(PATHS)} FROM numbers(20)")
        check(len(temporary(directory)) == 1, "an insert waits after its first stage")
        new.start()
        ok, got = eventually(lambda: str(temporary(directory)), "[]", timeout=30)
        check(ok, "a new exclusive writer removes the temporary directory as it loads the table", got)
        old.query(f"SYSTEM DISABLE FAILPOINT {STAGE}")
        error = query.join() or ""
        check("lost ownership" in error, "the fenced writer's next commit fails", error[:300])
        time.sleep(2)
        check(not temporary(directory), "the fenced writer wrote nothing more", str(temporary(directory)))
        check(new.value("SELECT count() FROM w.j") == "20", "the table reads as before on the new writer")
        check(new.value("CHECK TABLE w.j SETTINGS check_query_single_value_result = 1") == "1", "CHECK TABLE passes")
        new.query(f"INSERT INTO w.j SELECT 1000 + number, {json_text(PATHS)} FROM numbers(20)")
        check(new.value("SELECT count(), sum(data.p1023::UInt64) FROM w.j WHERE k >= 1000 FORMAT TSV") == f"20\t{1023 * 190}", "the new writer commits the part")
    finally:
        run.stop_all()


SCENARIOS = {
    "exclusive": scenario_exclusive,
    "shared": scenario_shared,
    "kills": scenario_kills,
    "cleanup": scenario_cleanup,
    "fenced": scenario_fenced,
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("scenarios", nargs="*", choices=[*SCENARIOS, []], help="the scenarios to run; all by default")
    args = parser.parse_args()
    for name in args.scenarios or SCENARIOS:
        print(f"== {name}", flush=True)
        SCENARIOS[name]()
    print(f"{failures} failed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
