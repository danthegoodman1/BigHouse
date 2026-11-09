#!/usr/bin/env python3
"""The demo's end-to-end check, through the query API's HTTP interface.

1. Fleets of 1, 2 and 4 servers answer a query suite over `demo.events` exactly as the writer does.
2. Meanwhile the writer keeps inserting 1,000-row batches into `demo.ingest` and merging them. A fleet must see
   whole batches only: n batches give n * 1000 rows whose `batch` values sum to 1000 * (0 + ... + n-1).
3. Every server of a fleet reads a share of the query.
4. Two queries at once take separate slots and both answer, and a query that needs servers still booting waits
   for them.
5. A fleet answers while the writer is down.
6. Each fleet server serves one query: the pool stops it and boots a blank server in its slot, and leaves no server
   behind when it closes.

    demo/e2e.py [rows]        rows demo.events needs, 20 million by default: enough parts that every server of a
                              fleet of 4 gets a share
"""

from __future__ import annotations

import json
import socket
import subprocess
import sys
import threading
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from http.server import ThreadingHTTPServer

from common import CONFIG, alive, down, up

from bighouse import api, fleet, stack

API_PORT = 18080
SUITE = [
    "SELECT count(), sum(duration_ms) FROM demo.events",
    "SELECT kind, count(), avg(duration_ms) FROM demo.events GROUP BY kind ORDER BY kind",
    "SELECT uniqExact(user_id) FROM demo.events",
    "SELECT url, count() AS c FROM demo.events GROUP BY url ORDER BY c DESC, url LIMIT 5",
    "SELECT toStartOfHour(time) AS h, max(duration_ms) FROM demo.events WHERE kind = 'purchase' GROUP BY h ORDER BY h LIMIT 3",
]
INGEST_CHECK = "SELECT count(), uniqExact(batch), sum(batch) FROM demo.ingest FORMAT TSV"
# Enough work per mark that every server of a fleet asks for a share before the marks run out.
SPREAD = "SELECT count(), sum(duration_ms) FROM demo.events WHERE match(url, '7[0-9]{3}$')"

failures = 0


def check(ok: bool, what: str, detail: str = "") -> None:
    global failures
    failures += not ok
    print(f"{'PASS' if ok else 'FAIL'} {what}" + (f": {detail}" if detail and not ok else ""))


def ask(sql: str, replicas: int) -> tuple[str, dict]:
    """The answer and the timings, through the API's HTTP interface."""
    request = urllib.request.Request(f"http://127.0.0.1:{API_PORT}/query?replicas={replicas}", data=sql.encode(), method="POST")
    with urllib.request.urlopen(request, timeout=600) as response:
        return response.read().decode(), json.loads(response.headers["X-BigHouse-Timings"])


def ingest(writer: stack.Server, stop: threading.Event) -> None:
    batch = 0
    while not stop.is_set():
        writer.query(f"INSERT INTO demo.ingest SELECT {batch}, number FROM numbers(1000)")
        batch += 1
        if batch % 20 == 0:
            writer.query("OPTIMIZE TABLE demo.ingest")


def whole_batches(answer: str) -> bool:
    count, distinct, total = (int(x) for x in answer.split())
    return count == 1000 * distinct and total == 1000 * distinct * (distinct - 1) // 2


def port_open(port: int) -> bool:
    with socket.socket() as sock:
        return sock.connect_ex(("127.0.0.1", port)) == 0


def blank(server: stack.Server) -> bool:
    """Whether `server` holds no tenant: no `bighouse` disk, no config but its boot config, and no databases."""
    disks = server.value(f"SELECT count() FROM system.disks WHERE name = '{fleet.DISK}'")
    databases = server.value("SELECT count() FROM system.databases WHERE name NOT IN "
                             "('system', 'default', 'INFORMATION_SCHEMA', 'information_schema')")
    tables = server.value("SELECT count() FROM system.tables WHERE database = 'default'")
    return (disks, databases, tables) == ("0", "0", "0") and not (server.dir / "config.d").exists()


def run_checks(writer: stack.Server, query_api: api.QueryApi) -> None:
    stop = threading.Event()
    ingester = threading.Thread(target=ingest, args=(writer, stop))
    ingester.start()
    try:
        expected = [writer.query(sql) for sql in SUITE]
        for replicas in (1, 2, 4):
            for sql, want in zip(SUITE, expected):
                got, timings = ask(sql, replicas)
                check(got == want, f"{replicas} replicas: {sql[:60]}", f"got {got!r}, want {want!r}")
            print(f"    last timings at {replicas} replicas: {timings}")

            before = int(writer.value("SELECT count() FROM demo.ingest"))
            got, _ = ask(INGEST_CHECK, replicas)
            after = int(writer.value("SELECT count() FROM demo.ingest"))
            count = int(got.split()[0])
            check(whole_batches(got) and before <= count <= after, f"{replicas} replicas see whole batches mid-ingest",
                  f"got {got.strip()}, writer had {before} then {after}")

        # Every server reads a share. The API recycles its fleets, so summon one here.
        with query_api.summon(4) as summoned:
            got = summoned.query(SPREAD)
            shares = summoned.marks_read()
            served = [server.process.pid for server in summoned.servers]
        check(got == writer.query(SPREAD), "a fleet of 4 answers directly")
        check(all(n > 0 for n in shares), "every server of a fleet of 4 reads a share", f"marks read per server: {shares}")
        print(f"    marks read per server: {shares}")
        query_api.pool.settle()
        check(not any(alive(pid) for pid in served), "a fleet's servers stop once its query ends")
        idle = query_api.pool.idle()
        check(len(idle) == 8 and all(blank(server) for server in idle) and not {s.process.pid for s in idle} & set(served),
              "the pool holds a fresh blank server in every slot")

        with ThreadPoolExecutor(2) as pool:
            answers = list(pool.map(lambda _: ask(SUITE[0], 2)[0], range(2)))
        check(answers == [expected[0]] * 2, "two queries at once both answer")

        # Each query takes every slot, so the second waits for the first's servers to be replaced. A replacement
        # takes a stop and a boot, over 100 ms; a query that finds its servers idle waits microseconds.
        (first, _), (second, timings) = ask(SUITE[0], 8), ask(SUITE[0], 8)
        check([first, second] == [expected[0]] * 2 and timings["wait_ms"] >= 10,
              "a query waits for servers still booting", f"answers {first!r} and {second!r}, timings {timings}")
        print(f"    timings of a query that waited: {timings}")

        stop.set()
        ingester.join()
        down()
        try:
            got, _ = ask(SUITE[0], 2)
            check(got == expected[0], "a fleet answers while the writer is down", f"got {got!r}")
        finally:
            up()
    finally:
        stop.set()
        ingester.join()


def main() -> int:
    writer = up()
    rows = int(sys.argv[1]) if len(sys.argv) > 1 else 20_000_000
    if writer.value("EXISTS TABLE demo.events") != "1" or int(writer.value("SELECT count() FROM demo.events")) != rows:
        subprocess.run([sys.executable, str(stack.ROOT / "demo/load"), str(rows)], check=True)
    writer.query("DROP TABLE IF EXISTS demo.ingest SYNC")
    writer.query("CREATE TABLE demo.ingest (batch UInt64, n UInt64) ENGINE = MergeTree ORDER BY (batch, n)")

    with api.QueryApi(json.loads(CONFIG.read_text()), slots=8) as query_api:
        http = ThreadingHTTPServer(("127.0.0.1", API_PORT), api.make_handler(query_api))
        threading.Thread(target=http.serve_forever, daemon=True).start()
        try:
            run_checks(writer, query_api)
        finally:
            http.shutdown()

    leftover = [port for slot in range(8) for port in (fleet.Slot(slot).tcp_port, fleet.Slot(slot).http_port) if port_open(port)]
    check(not leftover, "no fleet server outlives the pool", f"ports still open: {leftover}")
    check(not list((stack.DEV / "servers").glob("fleet-*")), "every fleet directory is gone")
    print(f"{failures} failed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
