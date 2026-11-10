"""The processes of a local BigHouse stack.

`scripts/dev-up` starts s3proxy and InodeDB. This module mints InodeDB keys and runs ClickHouse servers whose
disks keep blobs in s3proxy and metadata in InodeDB. It uses only the standard library.
"""

from __future__ import annotations

import os
import select
import shutil
import signal
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from xml.sax.saxutils import escape

ROOT = Path(__file__).resolve().parent.parent
DEV = Path(os.environ.get("BIGHOUSE_DEV_DIR", ROOT / ".dev"))
CLICKHOUSE = Path(os.environ.get("BIGHOUSE_CLICKHOUSE", ROOT / "build/programs/clickhouse"))
IDB = Path(os.environ.get("BIGHOUSE_IDB", ROOT / "inodedb/target/release/idb"))
INODEDB = Path(os.environ.get("BIGHOUSE_INODEDB", ROOT / "inodedb/target/release/inodedb"))

S3_ENDPOINT = "http://127.0.0.1:11111"
S3_BUCKET = "test"
S3_ACCESS_KEY = "clickhouse"
S3_SECRET_KEY = "clickhouse"
S3_DIR = DEV / "s3" / S3_BUCKET
INODEDB_ENDPOINT = os.environ.get("BIGHOUSE_INODEDB_ENDPOINT", "127.0.0.1:7070")
INODEDB_ADMIN_KEY = Path(os.environ.get("BIGHOUSE_INODEDB_ADMIN_KEY", DEV / "inodedb/admin.key"))


def admin_key() -> str:
    return INODEDB_ADMIN_KEY.read_text().strip()


def idb_bytes(*args: str, key: str | None = None) -> bytes:
    """What `idb` prints, under `key` or the admin key."""
    result = subprocess.run(
        [str(IDB), "--endpoint", f"http://{INODEDB_ENDPOINT}", "--key", key or admin_key(), *args],
        check=True,
        capture_output=True,
    )
    return result.stdout


def idb(*args: str, key: str | None = None) -> str:
    return idb_bytes(*args, key=key).decode()


def mint_key(root: str, grant: str, ttl: str | None = None) -> str:
    """A key rooted at InodeDB directory `root`, which this creates. `grant` is read, write or admin."""
    idb("mkdir", "-p", root)
    args = ["key", "mint", "--root", root, "--grant", grant]
    if ttl:
        args += ["--ttl", ttl]
    return idb(*args).strip()


def s3_objects(prefix: str) -> list[str]:
    """The keys s3proxy holds under `prefix`, read from its filesystem backend."""
    base = S3_DIR / prefix
    if not base.exists():
        return []
    return sorted(str(path.relative_to(S3_DIR)) for path in base.rglob("*") if path.is_file())


def inodedb_disk_xml(name: str, *, s3_prefix: str, key: str, read_only: bool = False, extra: str = "") -> str:
    """A disk whose blobs live in s3proxy under `s3_prefix` and whose metadata lives in InodeDB under `key`."""
    return f"""
        <{name}>
            <type>object_storage</type>
            <object_storage_type>s3</object_storage_type>
            <metadata_type>inodedb</metadata_type>
            <endpoint>{S3_ENDPOINT}/{S3_BUCKET}/{s3_prefix}/</endpoint>
            <access_key_id>{S3_ACCESS_KEY}</access_key_id>
            <secret_access_key>{S3_SECRET_KEY}</secret_access_key>
            <inodedb_endpoint>{INODEDB_ENDPOINT}</inodedb_endpoint>
            <inodedb_key>{escape(key)}</inodedb_key>
            <inodedb_read_only>{1 if read_only else 0}</inodedb_read_only>
            {extra}
        </{name}>"""


def local_metadata_disk_xml(name: str, *, s3_prefix: str) -> str:
    """A disk whose blobs live in s3proxy under `s3_prefix` and whose metadata lives on the server's local disk."""
    return f"""
        <{name}>
            <type>object_storage</type>
            <object_storage_type>s3</object_storage_type>
            <metadata_type>local</metadata_type>
            <endpoint>{S3_ENDPOINT}/{S3_BUCKET}/{s3_prefix}/</endpoint>
            <access_key_id>{S3_ACCESS_KEY}</access_key_id>
            <secret_access_key>{S3_SECRET_KEY}</secret_access_key>
        </{name}>"""


def policy_xml(name: str, disk: str) -> str:
    return f"<{name}><volumes><main><disk>{disk}</disk></main></volumes></{name}>"


class QueryError(RuntimeError):
    pass


@dataclass
class Server:
    """A ClickHouse server with a directory of its own under .dev/servers/<name>."""

    name: str
    tcp_port: int
    http_port: int
    interserver_port: int
    disks: list[str] = field(default_factory=list)
    policies: list[str] = field(default_factory=list)
    extra_config: str = ""
    process: subprocess.Popen | None = None

    @property
    def dir(self) -> Path:
        return DEV / "servers" / self.name

    @property
    def log(self) -> Path:
        return self.dir / "log/clickhouse-server.log"

    @property
    def pidfile(self) -> Path:
        return self.dir / "pid"

    def write_config(self) -> None:
        for sub in ("data", "tmp", "user_files", "log", "format_schemas"):
            (self.dir / sub).mkdir(parents=True, exist_ok=True)
        (self.dir / "users.xml").write_text(
            """<clickhouse>
    <profiles><default/></profiles>
    <users><default><password/><networks><ip>::/0</ip></networks><profile>default</profile><quota>default</quota><access_management>1</access_management></default></users>
    <quotas><default/></quotas>
</clickhouse>
"""
        )
        (self.dir / "config.xml").write_text(
            f"""<clickhouse>
    <logger>
        <level>information</level>
        <log>{self.dir}/log/clickhouse-server.log</log>
        <errorlog>{self.dir}/log/clickhouse-server.err.log</errorlog>
        <size>200M</size>
        <count>3</count>
    </logger>
    <listen_host>127.0.0.1</listen_host>
    <tcp_port>{self.tcp_port}</tcp_port>
    <http_port>{self.http_port}</http_port>
    <interserver_http_port>{self.interserver_port}</interserver_http_port>
    <interserver_http_host>127.0.0.1</interserver_http_host>
    <path>{self.dir}/data/</path>
    <tmp_path>{self.dir}/tmp/</tmp_path>
    <user_files_path>{self.dir}/user_files/</user_files_path>
    <format_schema_path>{self.dir}/format_schemas/</format_schema_path>
    <users_config>users.xml</users_config>
    <mark_cache_size>1073741824</mark_cache_size>
    <storage_configuration>
        <disks>{''.join(self.disks)}
        </disks>
        <policies>{''.join(self.policies)}
        </policies>
    </storage_configuration>
    {self.extra_config}
</clickhouse>
"""
        )

    def start(self, timeout: float = 60) -> None:
        self.write_config()
        out = open(self.dir / "log/stdout.log", "ab")
        self.process = subprocess.Popen(
            [str(CLICKHOUSE), "server", "--config-file", str(self.dir / "config.xml")],
            cwd=self.dir,
            stdout=out,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        self.pidfile.write_text(f"{self.process.pid}\n")
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f"{self.name} exited with {self.process.returncode}; see {self.dir}/log")
            try:
                if self.query("SELECT 1").strip() == "1":
                    return
            except (OSError, QueryError):
                pass
            time.sleep(0.05)
        self.kill()
        raise RuntimeError(f"{self.name} did not answer within {timeout} s; see {self.dir}/log")

    def stop(self, timeout: float = 60) -> None:
        if self.process and self.process.poll() is None:
            self.process.send_signal(signal.SIGTERM)
            try:
                self.process.wait(timeout)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        self.process = None
        self.pidfile.unlink(missing_ok=True)

    def kill(self) -> None:
        """SIGKILL: the server gets no chance to finish anything."""
        if self.process and self.process.poll() is None:
            self.process.kill()
            self.process.wait()
        self.process = None
        self.pidfile.unlink(missing_ok=True)

    def stop_leftover(self, timeout: float = 60) -> None:
        """Stops, as `stop` does, a server that a process now gone started in this directory and left running."""
        try:
            pid = int(self.pidfile.read_text())
            pidfd = os.pidfd_open(pid)
        except (FileNotFoundError, ProcessLookupError):
            return
        try:
            # The pid may name another process by now. The pidfd keeps naming the one checked here.
            if self.runs_as(pid):
                signal.pidfd_send_signal(pidfd, signal.SIGTERM)
                if not select.select([pidfd], [], [], timeout)[0]:
                    signal.pidfd_send_signal(pidfd, signal.SIGKILL)
                    select.select([pidfd], [], [])
        except ProcessLookupError:
            pass  # it exited on its own
        finally:
            os.close(pidfd)
        self.pidfile.unlink(missing_ok=True)

    def runs_as(self, pid: int) -> bool:
        """Whether process `pid` is a server running this directory's config."""
        try:
            return str(self.dir / "config.xml") in Path(f"/proc/{pid}/cmdline").read_text().split("\0")
        except (FileNotFoundError, ProcessLookupError):
            return False

    def wipe(self) -> None:
        shutil.rmtree(self.dir, ignore_errors=True)

    def query(self, sql: str, settings: dict[str, object] | None = None, timeout: float = 600) -> str:
        params = {"query": sql}
        for key, value in (settings or {}).items():
            params[key] = str(value)
        url = f"http://127.0.0.1:{self.http_port}/?" + urllib.parse.urlencode(params)
        request = urllib.request.Request(url, data=b"", method="POST")
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return response.read().decode()
        except urllib.error.HTTPError as error:
            raise QueryError(f"{self.name}: {error.read().decode().strip()}") from None

    def value(self, sql: str, **kwargs) -> str:
        return self.query(sql, **kwargs).strip()
