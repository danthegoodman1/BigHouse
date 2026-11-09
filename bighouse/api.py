"""The BigHouse query API: each query takes a fleet of warm servers, assigns them to the disk, and runs on them.

    POST /query?replicas=N    the body is the SQL; the answer is ClickHouse's output

The response's `X-BigHouse-Timings` header holds, in milliseconds, how long the query waited for servers still
booting, assigned them, attached the databases and ran, and the total; `X-BigHouse-Replicas` holds the fleet size.
`X-BigHouse-Left-Out` maps each of the writer's databases the fleet could not serve to the reason; a failed query's
error names them too. A host runs as many fleet servers at once as it has slots; a query that finds too few slots
free gets 503.

    python3 -m bighouse.api [--port 8080] [--slots 16]

It reads the disk's location in InodeDB and S3 from `.dev/demo/config.json`, which `demo/up` writes. Fleets read the
writer's databases from InodeDB, so queries need nothing from the writer and keep working while it is down.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import signal
import sys
import time
import traceback
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from bighouse import fleet, stack

CONFIG = stack.DEV / "demo/config.json"


class Busy(RuntimeError):
    pass


class QueryApi:
    """Runs each query on a fleet from a pool of blank servers. Entering it boots the pool; leaving it stops it."""

    def __init__(self, config: dict, slots: int):
        self.config = config
        self.pool = fleet.Pool(slots)

    def __enter__(self) -> QueryApi:
        try:
            self.pool.start()
        except BaseException:
            self.pool.close()
            raise
        return self

    def __exit__(self, *exc) -> None:
        self.pool.close()

    @contextlib.contextmanager
    def summon(self, replicas: int) -> Iterator[fleet.Fleet]:
        """A fleet of `replicas` servers assigned to the disk. The pool replaces them once the block ends."""
        started = time.monotonic()
        servers = self.pool.take(replicas)
        if servers is None:
            raise Busy(f"fewer than {replicas} free slots")
        waited = (time.monotonic() - started) * 1000
        key = ""
        try:
            # A read key rooted at the disk: it can't write, and it lapses on its own if this process dies.
            key = stack.mint_key(self.config["inodedb_root"], "read", ttl="15m")
            summoned = fleet.Fleet(servers, s3_prefix=self.config["s3_prefix"], key=key)
            summoned.timings["wait_ms"] = waited
            summoned.assign()
            yield summoned
        finally:
            # A server releases its InodeDB lease as it stops, which takes the key, so the key goes after.
            self.pool.recycle(servers, then=(lambda: stack.idb("key", "revoke", key)) if key else None)

    def run(self, sql: str, replicas: int) -> tuple[str, dict[str, float], dict[str, str]]:
        """The query's answer, the fleet's timings, and why the fleet left out each database it could not serve."""
        started = time.monotonic()
        with self.summon(replicas) as summoned:
            try:
                result = summoned.query(sql)
            except stack.QueryError as error:
                if not summoned.left_out:
                    raise
                left_out = "; ".join(f"{name}: {why}" for name, why in summoned.left_out.items())
                raise stack.QueryError(f"{error}\nThe fleet left out these databases: {left_out}") from None
        summoned.timings["total_ms"] = (time.monotonic() - started) * 1000
        return result, summoned.timings, summoned.left_out


def make_handler(api: QueryApi):
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802
            url = urlparse(self.path)
            if url.path != "/query":
                return self._reply(404, b"POST /query?replicas=N with the SQL as the body\n")
            try:
                replicas = int(parse_qs(url.query).get("replicas", ["1"])[0])
                if replicas < 1:
                    raise ValueError
            except ValueError:
                return self._reply(400, b"replicas must be a positive integer\n")
            sql = self.rfile.read(int(self.headers.get("Content-Length", 0))).decode()
            try:
                result, timings, left_out = api.run(sql, replicas)
            except Busy as error:
                return self._reply(503, f"{error}\n".encode())
            except Exception as error:  # noqa: BLE001
                traceback.print_exc()
                return self._reply(500, f"{error}\n".encode())
            headers = {
                "X-BigHouse-Replicas": str(replicas),
                "X-BigHouse-Timings": json.dumps({k: round(v, 1) for k, v in timings.items()}),
            }
            if left_out:
                headers["X-BigHouse-Left-Out"] = json.dumps(left_out)
            self._reply(200, result.encode(), headers)

        def _reply(self, status: int, body: bytes, headers: dict[str, str] | None = None) -> None:
            self.send_response(status)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            for name, value in (headers or {}).items():
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(body)

    return Handler


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--slots", type=int, default=16)
    args = parser.parse_args()

    if not CONFIG.exists():
        print(f"{CONFIG} is missing; run demo/up first", file=sys.stderr)
        return 1
    # SIGTERM unwinds like Ctrl-C, so the pool stops its servers.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    with QueryApi(json.loads(CONFIG.read_text()), args.slots) as api:
        server = ThreadingHTTPServer(("127.0.0.1", args.port), make_handler(api))
        print(f"BigHouse query API on http://127.0.0.1:{args.port}/query with {args.slots} warm slots", flush=True)
        with contextlib.suppress(KeyboardInterrupt):
            server.serve_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
