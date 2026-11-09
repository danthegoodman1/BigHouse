"""Fleets: ClickHouse servers summoned for one query, reading the writer's tables as parallel replicas.

Every fleet server runs in a slot, a fixed set of ports known ahead, and gets a read-only InodeDB disk with the
writer's disk and policy names. It attaches the writer's databases from their definitions in InodeDB
(`bighouse.catalog`), so each table resolves to the writer's directory there. One server coordinates the query and
the rest read their share of its marks.
"""

from __future__ import annotations

import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from bighouse import catalog, stack

DISK = catalog.DISK
CLUSTER = "bighouse"


@dataclass(frozen=True)
class Slot:
    index: int

    @property
    def tcp_port(self) -> int:
        return 20000 + self.index

    @property
    def http_port(self) -> int:
        return 21000 + self.index

    @property
    def interserver_port(self) -> int:
        return 22000 + self.index


class SlotPool:
    """The slots a host offers. A slot runs at most one fleet server at a time."""

    def __init__(self, size: int):
        self._free = set(range(size))
        self._lock = threading.Lock()

    def acquire(self, count: int) -> list[Slot] | None:
        with self._lock:
            if len(self._free) < count:
                return None
            taken = sorted(self._free)[:count]
            self._free.difference_update(taken)
            return [Slot(i) for i in taken]

    def release(self, slots: list[Slot]) -> None:
        with self._lock:
            self._free.update(slot.index for slot in slots)


def writer_disk_xml(*, s3_prefix: str, key: str) -> str:
    return stack.inodedb_disk_xml(DISK, s3_prefix=s3_prefix, key=key)


def replica_disk_xml(*, s3_prefix: str, key: str) -> str:
    # `readonly` makes the S3 storage read-only too, so MergeTree treats the tables as static: no merges,
    # no mutations, no cleanup.
    return stack.inodedb_disk_xml(DISK, s3_prefix=s3_prefix, key=key, read_only=True, extra="<readonly>1</readonly>")


def default_policy_xml() -> str:
    return f"<merge_tree><storage_policy>{DISK}</storage_policy></merge_tree>"


def database_disk_xml() -> str:
    """Keeps a writer's database definitions on the disk, where fleets find them."""
    return f"<database_disk><disk>{DISK}</disk></database_disk>"


def writer_config_xml() -> str:
    return default_policy_xml() + database_disk_xml()


class Fleet:
    def __init__(self, slots: list[Slot], *, s3_prefix: str, key: str):
        self.id = uuid.uuid4().hex[:8]
        self.key = key
        replicas = "".join(f"<replica><host>127.0.0.1</host><port>{slot.tcp_port}</port></replica>" for slot in slots)
        cluster = f"<remote_servers><{CLUSTER}><shard>{replicas}</shard></{CLUSTER}></remote_servers>"
        self.servers = [
            stack.Server(
                name=f"fleet-{self.id}-{slot.index}",
                tcp_port=slot.tcp_port,
                http_port=slot.http_port,
                interserver_port=slot.interserver_port,
                disks=[replica_disk_xml(s3_prefix=s3_prefix, key=key)],
                policies=[stack.policy_xml(DISK, DISK)],
                extra_config=cluster + default_policy_xml(),
            )
            for slot in slots
        ]
        self.timings: dict[str, float] = {}
        # Each of the writer's databases the fleet left out, with the reason.
        self.left_out: dict[str, str] = {}

    def _each(self, fn, servers: list[stack.Server] | None = None) -> None:
        servers = self.servers if servers is None else servers
        if not servers:
            return
        with ThreadPoolExecutor(len(servers)) as pool:
            for future in [pool.submit(fn, server) for server in servers]:
                future.result()

    def start(self) -> None:
        started = time.monotonic()
        self._each(lambda server: server.start())
        self.timings["start_ms"] = (time.monotonic() - started) * 1000

        # The coordinator settles which databases the fleet serves; the others attach the same ones.
        started = time.monotonic()
        databases, self.left_out = catalog.attach(self.servers[0], self.key)

        def attach(server: stack.Server) -> None:
            for database in databases:
                database.attach(server)

        self._each(attach, self.servers[1:])
        self.timings["attach_ms"] = (time.monotonic() - started) * 1000

    def query(self, sql: str, settings: dict[str, object] | None = None) -> str:
        parallel = {
            "enable_parallel_replicas": 1,
            "max_parallel_replicas": len(self.servers),
            "cluster_for_parallel_replicas": CLUSTER,
            "parallel_replicas_for_non_replicated_merge_tree": 1,
            "parallel_replicas_min_number_of_rows_per_replica": 0,
        }
        started = time.monotonic()
        result = self.servers[0].query(sql, settings={**parallel, **(settings or {})})
        self.timings["query_ms"] = (time.monotonic() - started) * 1000
        return result

    def marks_read(self) -> list[int]:
        """Marks each server read as a parallel replica since it started: its share of the fleet's queries."""
        return [
            int(server.value("SELECT sum(value) FROM system.events WHERE event = 'ParallelReplicasReadMarks'") or 0)
            for server in self.servers
        ]

    def stop(self) -> None:
        started = time.monotonic()

        # Replicas hold nothing to flush: they wrote nothing.
        def stop(server: stack.Server) -> None:
            server.kill()
            server.wipe()

        self._each(stop)
        self.timings["stop_ms"] = (time.monotonic() - started) * 1000
