#!/usr/bin/env python3
"""Runs ClickHouse's stateless tests twice, with every MergeTree table on an S3 disk: once with `local` metadata
and once with `inodedb` metadata. Prints the tests that pass on `local` and fail on `inodedb`.

Both runs copy one install of ClickHouse's test configs (`tests/config/install.sh --s3-storage --no-azure`) into
.dev/parity/<variant> and rewrite their absolute paths into that directory. The `inodedb` run gives the `s3` disk, which the `cached_s3`
cache disk wraps and the default MergeTree policy uses, `inodedb` metadata and blob keys under `parity-inodedb/`. Tests that fail on both,
for reasons of this machine, cancel out.

    python3 tests/parity.py [--jobs N] [--variants local,inodedb] [test-name-regex ...]

Needs `scripts/dev-up` (s3proxy serves the `localhost:11111/test` endpoint the test configs use).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.request
import uuid
import xml.etree.ElementTree as ET
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bighouse import stack  # noqa: E402

CLICKHOUSE_SRC = stack.ROOT / "ClickHouse"
RESULT = re.compile(r"^(?:\[\s*\d+\s*/\s*\d+\]\s+)?(\S+?):\s.*\[ (OK|FAIL|SKIPPED|UNKNOWN|BROKEN|SERVER_DIED) \]")


def tools(base: Path) -> tuple[Path, dict[str, str]]:
    """A bin directory naming every ClickHouse tool, and an environment that finds it first."""
    bin_dir = base / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    for tool in ("clickhouse", "clickhouse-client", "clickhouse-local", "clickhouse-server", "clickhouse-compressor",
                 "clickhouse-format", "clickhouse-disks", "clickhouse-obfuscator", "ch"):
        link = bin_dir / tool
        if not link.exists():
            link.symlink_to(stack.CLICKHOUSE)
    return bin_dir, {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}"}


def configure() -> Path:
    """Installs the test configs once, with every symlink replaced by its target. install.sh randomizes some
    settings, so both variants copy this one install and differ only in the `s3` disk's metadata."""
    base = stack.DEV / "parity/template"
    shutil.rmtree(base, ignore_errors=True)
    _, env = tools(base)
    server_etc, client_etc = base / "etc/clickhouse-server", base / "etc/clickhouse-client"
    server_etc.mkdir(parents=True)
    client_etc.mkdir(parents=True)
    for name in ("config.xml", "users.xml"):
        shutil.copy(CLICKHOUSE_SRC / "programs/server" / name, server_etc / name)
    with open(base / "install.log", "w") as log:
        subprocess.run([str(CLICKHOUSE_SRC / "tests/config/install.sh"), str(server_etc), str(client_etc), "--s3-storage", "--no-azure"],
                       cwd=CLICKHOUSE_SRC, check=True, stdout=log, stderr=subprocess.STDOUT, env=env)
    for path in sorted(base.rglob("*")):
        if path.is_symlink() and path.parent.name != "bin":
            target = path.resolve()
            path.unlink()
            if target.is_dir():
                shutil.copytree(target, path)
            else:
                shutil.copy(target, path)
    return base / "etc"


def install(variant: str, etc: Path) -> Path:
    base = stack.DEV / "parity" / variant
    shutil.rmtree(base, ignore_errors=True)
    tools(base)
    shutil.copytree(etc, base / "etc")
    server_etc = base / "etc/clickhouse-server"
    replacements = {
        str(etc): str(base / "etc"),
        "/var/lib/clickhouse": str(base / "data"),
        "/var/log/clickhouse-server": str(base / "log"),
        "/etc/clickhouse-server": str(server_etc),
        "/etc/clickhouse-client": str(base / "etc/clickhouse-client"),
        ">/test/chroot": f">{base / 'chroot'}",
        # Keeper and interserver ports move clear of other servers on this machine.
        "<port>9181</port>": "<port>29181</port>",
        "<tcp_port>9181</tcp_port>": "<tcp_port>29181</tcp_port>",
        "<port>9234</port>": "<port>29234</port>",
        "<interserver_http_port>9009</interserver_http_port>": "<interserver_http_port>29009</interserver_http_port>",
    }
    for path in base.rglob("*.xml"):
        text = path.read_text()
        for old, new in replacements.items():
            text = text.replace(old, new)
        path.write_text(text)

    if variant == "inodedb":
        # Blob keys move under parity-inodedb/, by whichever key scheme install.sh picked.
        disk = ET.parse(server_etc / "config.d/s3_storage_policy_by_default.xml").getroot().find("storage_configuration/disks/s3")
        if disk.find("key_template") is not None:
            keys = "<key_template>parity-inodedb/[a-z]{3}/[a-z]{29}</key_template>"
        else:
            keys = "<endpoint>http://localhost:11111/test/parity-inodedb/</endpoint>"
        # `local` metadata's removal log; InodeDB's free queue is durable already.
        local_only = "".join(f'<{name} remove="1"/>' for name in ("persistent_removal_log", "metadata_removal_log_compaction_threshold")
                             if disk.find(name) is not None)
        key = stack.mint_key(f"parity/{uuid.uuid4().hex[:8]}", "write")
        (server_etc / "config.d/zz_inodedb_metadata.xml").write_text(f"""<clickhouse>
    <storage_configuration>
        <disks>
            <s3>
                <type>object_storage</type>
                <object_storage_type>s3</object_storage_type>
                <metadata_type>inodedb</metadata_type>
                {keys}{local_only}
                <inodedb_endpoint>{stack.INODEDB_ENDPOINT}</inodedb_endpoint>
                <inodedb_key>{key}</inodedb_key>
            </s3>
        </disks>
    </storage_configuration>
</clickhouse>
""")
    return base


def start(base: Path) -> subprocess.Popen:
    (base / "log").mkdir(exist_ok=True)
    process = subprocess.Popen(
        [str(stack.CLICKHOUSE), "server", "--config-file", str(base / "etc/clickhouse-server/config.xml")],
        cwd=base, stdout=open(base / "log/stdout.log", "ab"), stderr=subprocess.STDOUT, start_new_session=True)
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"the server exited; see {base}/log")
        try:
            with urllib.request.urlopen("http://127.0.0.1:8123/?query=SELECT%201", timeout=2) as response:
                if response.read().strip() == b"1":
                    return process
        except OSError:
            pass
        time.sleep(0.2)
    process.kill()
    process.wait()
    raise RuntimeError(f"the server did not answer; see {base}/log")


def run_tests(base: Path, jobs: int, patterns: list[str]) -> dict[str, str]:
    bin_dir, env = tools(base)
    env["CLICKHOUSE_CONFIG_CLIENT"] = str(base / "etc/clickhouse-client/config.xml")
    command = [
        sys.executable, str(CLICKHOUSE_SRC / "tests/clickhouse-test"),
        "--binary", str(bin_dir / "clickhouse"),
        "--configclient", str(base / "etc/clickhouse-client/config.xml"),
        "--configserver", str(base / "etc/clickhouse-server/config.xml"),
        "--tmp", str(base / "tmp"),
        "--jobs", str(jobs),
        "--no-random-settings", "--no-random-merge-tree-settings", "--no-stateful", "--no-long",
        "--zookeeper", "--shard",
        *patterns,
    ]
    results = {}
    with open(base / "clickhouse-test.log", "w") as log:
        process = subprocess.Popen(command, cwd=CLICKHOUSE_SRC / "tests", stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env)
        for line in process.stdout:
            log.write(line)
            if match := RESULT.match(line):
                results[match.group(1)] = match.group(2)
        process.wait()
    (base / "results.json").write_text(json.dumps(results, indent=1, sort_keys=True))
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--jobs", type=int, default=16)
    parser.add_argument("--variants", default="local,inodedb")
    parser.add_argument("patterns", nargs="*")
    args = parser.parse_args()

    etc = configure()
    for variant in args.variants.split(","):
        base = install(variant, etc)
        server = start(base)
        try:
            results = run_tests(base, args.jobs, args.patterns)
        finally:
            server.terminate()
            server.wait(120)
        counts = {status: list(results.values()).count(status) for status in sorted(set(results.values()))}
        print(f"{variant}: {counts}")

    results = {v: json.loads((stack.DEV / "parity" / v / "results.json").read_text()) for v in ("local", "inodedb")}
    regressions = sorted(t for t, s in results["local"].items() if s == "OK" and results["inodedb"].get(t) not in ("OK", "SKIPPED"))
    improvements = sorted(t for t, s in results["inodedb"].items() if s == "OK" and results["local"].get(t) not in ("OK", "SKIPPED"))
    print(f"{len(regressions)} tests pass with local metadata and fail with inodedb metadata")
    for test in regressions:
        print(f"    {test}: {results['inodedb'].get(test, 'missing')}")
    print(f"{len(improvements)} tests pass with inodedb metadata and fail with local metadata")
    (stack.DEV / "parity/diff.json").write_text(json.dumps({"regressions": regressions, "improvements": improvements}, indent=1))
    return 1 if regressions else 0


if __name__ == "__main__":
    sys.exit(main())
