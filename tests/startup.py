#!/usr/bin/env python3
"""What attaching an `inodedb` disk starts (PLAN.md, Phase 9).

A process that attaches the disk for one small insert, such as a worker forked for each operation, pays for whatever
the disk builds and starts. The disk builds its S3 client with its first blob, and `inodedb_grpc_threads` sets how
many threads gRPC keeps while idle.

- `s3_client`: a server whose only disk is `inodedb` builds no S3 client as it starts, creates a table and inserts
  rows that stay in InodeDB, or as it reloads a config that changes the client's settings. It builds one for its
  first blob. After a restart it builds none until a query reads a blob, and a server that built none stops cleanly.
- `grpc_threads`: a server with `inodedb_grpc_threads` at 4 keeps 4 threads in gRPC's pool, where a server that
  leaves the setting out keeps one for each core, between 4 and 16. Both insert and read the same rows. A server
  asked for 2 refuses its disk.

    python3 tests/startup.py [scenario ...]
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bighouse import fleet, stack  # noqa: E402

failures = 0
# gRPC's pool threads, by the function each runs.
POOL_THREADS = ("SELECT count() FROM system.stack_trace "
                "WHERE arrayExists(frame -> demangle(addressToSymbol(frame)) LIKE '%WorkStealingThreadPool::ThreadState::ThreadBody%', trace)")


def check(ok: bool, what: str, detail: str = "") -> bool:
    global failures
    failures += not ok
    print(f"{'PASS' if ok else 'FAIL'} {what}" + (f": {detail}" if detail and not ok else ""), flush=True)
    return ok


class Run:
    """An InodeDB root and servers whose only disk is the `inodedb` disk over it."""

    def __init__(self, name: str):
        self.id = uuid.uuid4().hex[:8]
        self.root = f"startup/{name}/{self.id}"
        self.key = stack.mint_key(self.root, "write")
        self.servers: list[stack.Server] = []

    def server(self, index: int, settings: str = "") -> stack.Server:
        server = stack.Server(
            name=f"startup-{self.id}-{index}", tcp_port=29400 + index, http_port=29500 + index, interserver_port=29600 + index,
            disks=[stack.inodedb_disk_xml(fleet.DISK, s3_prefix=self.root, key=self.key, extra=settings)],
            policies=[stack.policy_xml(fleet.DISK, fleet.DISK)],
            extra_config=f"<merge_tree><storage_policy>{fleet.DISK}</storage_policy></merge_tree>" + fleet.database_disk_xml(),
        )
        self.servers.append(server)
        return server

    def stop_all(self) -> None:
        for server in self.servers:
            server.stop()
            server.wipe()


def pool_threads(server: stack.Server, want: int) -> str:
    """How many threads gRPC's pool holds. On a busy machine a thread can miss its turn to report its stack, and the
    pool can hold a thread more for a moment, so this asks until it counts `want`, for a minute."""
    deadline = time.monotonic() + 60
    while True:
        got = server.value(POOL_THREADS, settings={"allow_introspection_functions": 1})
        if got == str(want) or time.monotonic() > deadline:
            return got
        time.sleep(0.2)


def s3_clients(server: stack.Server) -> int:
    return int(server.value("SELECT value FROM system.events WHERE event = 'S3Clients'") or 0)


def scenario_s3_client() -> None:
    run = Run("s3client")
    server = run.server(0)
    server.start()
    try:
        check(s3_clients(server) == 0, "a server whose only disk is `inodedb` starts without an S3 client", f"{s3_clients(server)} clients")

        server.query("CREATE DATABASE s")
        server.query("CREATE TABLE s.small (k UInt64, v String) ENGINE = MergeTree ORDER BY k")
        server.query("INSERT INTO s.small SELECT number, toString(number) FROM numbers(1000)")
        got = server.value("SELECT count(), sum(k) FROM s.small")
        check(got == "1000\t499500" and s3_clients(server) == 0 and not stack.s3_objects(run.root),
              "an insert that stays in InodeDB commits and reads back without an S3 client",
              f"{got}, {s3_clients(server)} clients, {len(stack.s3_objects(run.root))} blobs")

        # A reload with another timeout for the disk's S3 requests calls for another client.
        server.disks = [stack.inodedb_disk_xml(fleet.DISK, s3_prefix=run.root, key=run.key, extra="<request_timeout_ms>31000</request_timeout_ms>")]
        server.write_config()
        server.query("SYSTEM RELOAD CONFIG")
        check(s3_clients(server) == 0, "a reload that changes the client's settings builds no client for a disk that has none", f"{s3_clients(server)} clients")

        server.query("CREATE TABLE s.big (k UInt64, v String) ENGINE = MergeTree ORDER BY k")
        server.query("INSERT INTO s.big SELECT number, repeat(toString(cityHash64(number)), 20000) FROM numbers(20)")
        blobs = len(stack.s3_objects(run.root))
        check(blobs > 0 and s3_clients(server) >= 1, "the first blob builds the client", f"{blobs} blobs, {s3_clients(server)} clients")
        size = server.value("SELECT sum(length(v)) FROM s.big")

        server.stop()
        server.start()
        got = server.value("SELECT count(), sum(k) FROM s.small")
        check(got == "1000\t499500" and s3_clients(server) == 0, "a restarted server reads the small table without an S3 client",
              f"{got}, {s3_clients(server)} clients")
        got = server.value("SELECT sum(length(v)) FROM s.big")
        check(got == size and s3_clients(server) >= 1, "a read of a blob builds the client", f"{got} against {size}, {s3_clients(server)} clients")

        server.stop()
        server.start()
        check(s3_clients(server) == 0, "the restarted server holds no S3 client", f"{s3_clients(server)} clients")
        server.stop()
        errors = [line for line in server.log.read_text(errors="replace").splitlines() if "<Error>" in line or "<Fatal>" in line]
        check(not errors, "a server that built no S3 client stops without an error", "; ".join(errors[-3:])[:600])
    finally:
        run.stop_all()


def scenario_grpc_threads() -> None:
    run = Run("grpcthreads")
    cores = os.cpu_count() or 1
    own = min(max(cores, 4), 16)
    counts: dict[str, str] = {}
    try:
        for index, (name, settings) in enumerate({"own": "", "four": "<inodedb_grpc_threads>4</inodedb_grpc_threads>"}.items()):
            server = run.server(index, settings)
            server.start()
            if index == 0:
                server.query("CREATE DATABASE g")
                server.query("CREATE TABLE g.t (k UInt64, v String) ENGINE = MergeTree ORDER BY k")
            server.query(f"INSERT INTO g.t SELECT number + {index} * 1000, toString(number) FROM numbers(1000)")
            counts[name] = pool_threads(server, own if index == 0 else 4)
            rows = server.value("SELECT count(), sum(k) FROM g.t")
            want = "1000\t499500" if index == 0 else "2000\t1999000"
            check(rows == want, f"a server with {'gRPC`s own thread count' if index == 0 else '`inodedb_grpc_threads` at 4'} inserts and reads", rows)
            server.stop()
        check(counts["own"] == str(own), f"gRPC's pool keeps {own} threads on {cores} cores when the disk leaves it alone", counts["own"])
        check(counts["four"] == "4", "with `inodedb_grpc_threads` at 4, gRPC's pool keeps 4 threads", counts["four"])

        refused = run.server(2, "<inodedb_grpc_threads>2</inodedb_grpc_threads>")
        try:
            refused.start(timeout=30)
            check(False, "a server asked for 2 gRPC threads refuses its disk", "it started")
        except RuntimeError as error:
            log = refused.log.read_text(errors="replace") if refused.log.exists() else ""
            stdout = refused.dir / "log/stdout.log"
            log += stdout.read_text(errors="replace") if stdout.exists() else ""
            check("`inodedb_grpc_threads` must be 0" in log, "a server asked for 2 gRPC threads refuses its disk", f"{error}; the log lacks the reason")
    finally:
        run.stop_all()


SCENARIOS = {
    "s3_client": scenario_s3_client,
    "grpc_threads": scenario_grpc_threads,
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
