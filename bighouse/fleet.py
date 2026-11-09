"""Fleets: ClickHouse servers that serve one query as parallel replicas of the writer's tables.

A host runs a pool of slots, each a fixed set of ports with one server booted ahead of demand. A slot's server
boots blank: it has no `bighouse` disk, no cluster and no tenant credentials. A query takes servers from the pool
and assigns them with one `SYSTEM RELOAD CONFIG` each, which adds a read-only InodeDB disk under the query's read
key, the writer's disk and policy names, and a cluster naming exactly the fleet's servers. The servers then attach
the writer's databases from their definitions in InodeDB (`bighouse.catalog`). One server coordinates the query
and the rest read their share of its marks. Once the query ends, the pool stops each server, which releases its
InodeDB lease, and boots a blank one in its slot. A reload can add a disk but cannot remove one or change its key,
so each server serves one query.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import ClassVar

from bighouse import catalog, stack

DISK = catalog.DISK
CLUSTER = "bighouse"


def each(fn: Callable[[object], object], items: list) -> None:
    """Runs `fn` on every item at once, and raises the first failure once all have finished."""
    if not items:
        return
    with ThreadPoolExecutor(len(items)) as pool:
        for future in [pool.submit(fn, item) for item in items]:
            future.result()


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


@dataclass(frozen=True)
class Slot:
    index: int

    TCP_PORTS: ClassVar[int] = 20000
    HTTP_PORTS: ClassVar[int] = 21000
    INTERSERVER_PORTS: ClassVar[int] = 22000

    @property
    def tcp_port(self) -> int:
        return self.TCP_PORTS + self.index

    @property
    def http_port(self) -> int:
        return self.HTTP_PORTS + self.index

    @property
    def interserver_port(self) -> int:
        return self.INTERSERVER_PORTS + self.index

    @classmethod
    def of(cls, server: stack.Server) -> Slot:
        return cls(server.tcp_port - cls.TCP_PORTS)

    def blank_server(self) -> stack.Server:
        # MergeTree reads its default settings only at boot, so the boot config names the default policy. The
        # server looks the name up when a table attaches, by which time assignment has added the policy.
        return stack.Server(
            name=f"fleet-{self.index}",
            tcp_port=self.tcp_port,
            http_port=self.http_port,
            interserver_port=self.interserver_port,
            extra_config=default_policy_xml(),
        )


def _dismiss(server: stack.Server) -> None:
    # SIGTERM: the disk shuts down and releases the server's InodeDB lease, so the writer's deletes go on at once.
    server.stop()
    server.wipe()


class Pool:
    """The host's slots, each running one fleet server.

    A slot's server boots blank, serves one fleet, and is replaced: `recycle` stops it and boots a blank one in its
    slot, in the background. A slot whose server fails to boot stays empty, and a `take` that would wait for it
    raises the failure.
    """

    def __init__(self, size: int):
        self.size = size
        self._changed = threading.Condition()
        self._idle: list[stack.Server] = []
        # Slots no caller holds or waits for. Each one's server is idle, booting or stopping.
        self._unclaimed = size
        self._recycling = 0
        self._failure: str | None = None
        self._closed = False

    def start(self) -> None:
        """Boots a blank server in every slot."""
        each(self._boot, [Slot(i) for i in range(self.size)])
        if self._failure:
            raise RuntimeError(self._failure)

    def take(self, count: int) -> list[stack.Server] | None:
        """`count` blank servers, once any it waits for have booted. None when fewer slots are unclaimed."""
        with self._changed:
            if self._closed:
                raise RuntimeError("the pool is closed")
            if count > self._unclaimed:
                return None
            self._unclaimed -= count
            while len(self._idle) < count and self._failure is None and not self._closed:
                self._changed.wait()
            if len(self._idle) < count:
                self._unclaimed += count
                raise RuntimeError(self._failure or "the pool is closed")
            taken, self._idle = self._idle[:count], self._idle[count:]
            return taken

    def recycle(self, servers: list[stack.Server], then: Callable[[], object] | None = None) -> None:
        """Stops `servers` and boots blank ones in their slots, in the background. `then` runs once all have stopped."""
        with self._changed:
            self._unclaimed += len(servers)
            self._recycling += 1
        threading.Thread(target=self._recycle, args=(servers, then), name="recycle").start()

    def idle(self) -> list[stack.Server]:
        with self._changed:
            return list(self._idle)

    def settle(self) -> None:
        """Waits until every recycle has finished."""
        with self._changed:
            while self._recycling:
                self._changed.wait()

    def close(self) -> None:
        """Stops every server once the recycles finish. A server a caller still holds stops when it is recycled."""
        with self._changed:
            self._closed = True
            self._changed.notify_all()
            while self._recycling:
                self._changed.wait()
            idle, self._idle = self._idle, []
        each(_dismiss, idle)

    def _recycle(self, servers: list[stack.Server], then: Callable[[], object] | None) -> None:
        try:
            each(_dismiss, servers)
            if then:
                then()
        finally:
            with self._changed:
                closed = self._closed
            if not closed:
                each(self._boot, [Slot.of(server) for server in servers])
            with self._changed:
                self._recycling -= 1
                self._changed.notify_all()

    def _boot(self, slot: Slot) -> None:
        server = slot.blank_server()
        # A pool that was killed leaves its servers' directories, with a tenant's config and databases.
        server.wipe()
        try:
            server.start()
        except Exception as error:  # noqa: BLE001: the failure goes to every `take` that waits on this slot
            with self._changed:
                self._failure = f"slot {slot.index} failed to boot: {error}"
                self._changed.notify_all()
            return
        with self._changed:
            self._idle.append(server)
            self._changed.notify_all()


class Fleet:
    """Blank servers from a pool, assigned to read the disk under `key`."""

    def __init__(self, servers: list[stack.Server], *, s3_prefix: str, key: str):
        self.servers = servers
        self.s3_prefix = s3_prefix
        self.key = key
        self.timings: dict[str, float] = {}
        # Each of the writer's databases the fleet left out, with the reason.
        self.left_out: dict[str, str] = {}

    def tenant_config_xml(self) -> str:
        replicas = "".join(f"<replica><host>127.0.0.1</host><port>{server.tcp_port}</port></replica>" for server in self.servers)
        return f"""<clickhouse>
    <storage_configuration>
        <disks>{replica_disk_xml(s3_prefix=self.s3_prefix, key=self.key)}
        </disks>
        <policies>{stack.policy_xml(DISK, DISK)}</policies>
    </storage_configuration>
    <remote_servers><{CLUSTER}><shard>{replicas}</shard></{CLUSTER}></remote_servers>
</clickhouse>
"""

    def assign(self) -> None:
        """Gives every server the disk, its policy and the cluster, then attaches the writer's databases."""
        started = time.monotonic()
        config = self.tenant_config_xml()

        def configure(server: stack.Server) -> None:
            # The server also rereads `config.d` every 2 s on its own, so the file appears there whole.
            staged = server.dir / "tenant.xml"
            staged.write_text(config)
            (server.dir / "config.d").mkdir(exist_ok=True)
            staged.replace(server.dir / "config.d/tenant.xml")
            server.query("SYSTEM RELOAD CONFIG")

        each(configure, self.servers)
        self.timings["assign_ms"] = (time.monotonic() - started) * 1000

        # The coordinator settles which databases the fleet serves; the others attach the same ones.
        started = time.monotonic()
        databases, self.left_out = catalog.attach(self.servers[0], self.key)

        def attach(server: stack.Server) -> None:
            for database in databases:
                database.attach(server)

        each(attach, self.servers[1:])
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
