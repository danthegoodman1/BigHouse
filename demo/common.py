"""The demo's writer: a persistent ClickHouse server whose `bighouse` disk keeps its metadata in InodeDB.

The disk holds the writer's database definitions too, so fleets find every table in InodeDB.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from bighouse import fleet, stack  # noqa: E402

STATE = stack.DEV / "demo"
CONFIG = STATE / "config.json"
INODEDB_ROOT = "bighouse/demo"
S3_PREFIX = "bighouse/demo"


def writer() -> stack.Server:
    key_file = STATE / "writer.key"
    if not key_file.exists():
        STATE.mkdir(parents=True, exist_ok=True)
        key_file.write_text(stack.mint_key(INODEDB_ROOT, "write") + "\n")
    return stack.Server(
        name="demo-writer",
        tcp_port=19100,
        http_port=18200,
        interserver_port=19109,
        disks=[fleet.writer_disk_xml(s3_prefix=S3_PREFIX, key=key_file.read_text().strip())],
        policies=[stack.policy_xml(fleet.DISK, fleet.DISK)],
        extra_config=fleet.writer_config_xml(),
    )


def writer_pid() -> int | None:
    pid_file = STATE / "writer.pid"
    if not pid_file.exists():
        return None
    pid = int(pid_file.read_text())
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return None
    return pid


def up() -> stack.Server:
    subprocess.run([str(ROOT / "scripts/dev-up")], check=True)
    server = writer()
    if writer_pid() is None:
        server.start()
        (STATE / "writer.pid").write_text(str(server.process.pid))
    CONFIG.write_text(json.dumps({
        "inodedb_root": INODEDB_ROOT,
        "s3_prefix": S3_PREFIX,
    }, indent=2) + "\n")
    return server


def alive(pid: int) -> bool:
    try:
        # The writer may be this process's own child, which stays a zombie until reaped.
        if os.waitpid(pid, os.WNOHANG)[0] == pid:
            return False
    except ChildProcessError:
        pass
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def down(timeout: float = 60) -> None:
    """Stops the writer and waits for it to exit."""
    if pid := writer_pid():
        os.kill(pid, signal.SIGTERM)
        deadline = time.monotonic() + timeout
        while alive(pid):
            if time.monotonic() > deadline:
                raise RuntimeError(f"the writer (pid {pid}) is still running {timeout} s after SIGTERM")
            time.sleep(0.05)
    (STATE / "writer.pid").unlink(missing_ok=True)
