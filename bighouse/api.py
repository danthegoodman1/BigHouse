"""The BigHouse query API: each query summons a fleet, runs on it, and dismisses it.

    POST /query?replicas=N    the body is the SQL; the answer is ClickHouse's output

The response's `X-BigHouse-Timings` header holds the fleet's start, attach, query and stop times in milliseconds,
and `X-BigHouse-Replicas` the fleet size. A host runs as many fleet servers at once as it has slots; a query that
finds too few free gets 503.

    python3 -m bighouse.api [--port 8080] [--slots 16]

It reads the writer's address and the disk's location from `.dev/demo/config.json`, which `demo/up` writes.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from bighouse import fleet, stack

CONFIG = stack.DEV / "demo/config.json"


class Busy(RuntimeError):
    pass


class QueryApi:
    def __init__(self, config: dict, slots: int):
        self.config = config
        self.writer = stack.Server(name="writer", tcp_port=config["writer_tcp_port"], http_port=config["writer_http_port"], interserver_port=0)
        self.pool = fleet.SlotPool(slots)

    def run(self, sql: str, replicas: int) -> tuple[str, dict[str, float]]:
        slots = self.pool.acquire(replicas)
        if slots is None:
            raise Busy(f"fewer than {replicas} free slots")
        started = time.monotonic()
        # A read key rooted at the disk: it can't write, and it lapses on its own if this process dies.
        key = stack.mint_key(self.config["inodedb_root"], "read", ttl="15m")
        summoned = fleet.Fleet(slots, s3_prefix=self.config["s3_prefix"], key=key)
        try:
            summoned.start(fleet.table_ddls(self.writer))
            result = summoned.query(sql)
        finally:
            summoned.stop()
            self.pool.release(slots)
            stack.idb("key", "revoke", key)
        summoned.timings["total_ms"] = (time.monotonic() - started) * 1000
        return result, summoned.timings


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
                result, timings = api.run(sql, replicas)
            except Busy as error:
                return self._reply(503, f"{error}\n".encode())
            except Exception as error:  # noqa: BLE001
                traceback.print_exc()
                return self._reply(500, f"{error}\n".encode())
            self._reply(200, result.encode(), {
                "X-BigHouse-Replicas": str(replicas),
                "X-BigHouse-Timings": json.dumps({k: round(v, 1) for k, v in timings.items()}),
            })

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
    api = QueryApi(json.loads(CONFIG.read_text()), args.slots)
    server = ThreadingHTTPServer(("127.0.0.1", args.port), make_handler(api))
    print(f"BigHouse query API on http://127.0.0.1:{args.port}/query with {args.slots} slots")
    server.serve_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
