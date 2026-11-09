#!/usr/bin/env python3
"""Runs ClickHouse's stateless tests twice, with every MergeTree table on an S3 disk: once with `local` metadata
and once with `inodedb` metadata. Prints the tests that pass on `local` and fail on `inodedb`.

Both runs copy one install of ClickHouse's test configs (`tests/config/install.sh --s3-storage --no-azure`) into
.dev/parity/<variant> and rewrite their absolute paths into that directory. The `inodedb` run gives the `s3` disk, which the `cached_s3`
cache disk wraps and the default MergeTree policy uses, `inodedb` metadata and blob keys under `parity-inodedb/`. Tests that fail on both,
for reasons of this machine, cancel out. A run of one variant compares against the other's last results.

Each test that fails on `inodedb`, and passes or never ran on `local`, runs again with fewer jobs: on `local` first, then
on `inodedb` up to three times if it passed on `local`. A test that passes one of those `inodedb` rechecks is flaky.
Each test that fails all three runs once more on `inodedb`, rewritten to observe InodeDB where it observes a part's
metadata files on the local filesystem (`rewrite_for_inodedb`). A test that passes rewritten observes local files; one
that fails is a regression, and the script exits 1. `--variants ''` compares and rechecks the last results only.

    python3 tests/parity.py [--jobs N] [--timeout S] [--variants local,inodedb] [--remote-database-disk] [test-name-regex ...]

Needs `scripts/dev-up` (s3proxy serves the `localhost:11111/test` endpoint the test configs use) and Docker, in which
it starts Azurite.
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
import urllib.error
import urllib.request
import uuid
import xml.etree.ElementTree as ET
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bighouse import stack  # noqa: E402

CLICKHOUSE_SRC = stack.ROOT / "ClickHouse"
# Both runs use one copy of the binary, so a rebuild during a run changes neither.
BINARY = stack.DEV / "parity/clickhouse"
# A regression fails this many `inodedb` rechecks in a row.
RECHECKS = 3
RESULT = re.compile(r"^(?:\[\s*\d+\s*/\s*\d+\]\s+)?(\S+?):\s.*\[ (OK|FAIL|SKIPPED|UNKNOWN|BROKEN|SERVER_DIED) \]")


def tools(base: Path) -> tuple[Path, dict[str, str]]:
    """A bin directory naming every ClickHouse tool, and an environment that finds it first."""
    bin_dir = base / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    for tool in ("clickhouse", "clickhouse-client", "clickhouse-local", "clickhouse-server", "clickhouse-compressor",
                 "clickhouse-format", "clickhouse-disks", "clickhouse-obfuscator", "ch"):
        link = bin_dir / tool
        if not link.exists():
            link.symlink_to(BINARY)
    return bin_dir, {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}"}


def start_azurite() -> None:
    """Azurite on localhost:10000 for the tests that create Azure disks. Without it, their access checks retry while
    holding the server's storage-policy lock, which stalls every other test."""
    if subprocess.run(["docker", "inspect", "bighouse-azurite"], capture_output=True).returncode != 0:
        subprocess.run(["docker", "run", "-d", "--rm", "--name", "bighouse-azurite", "-p", "127.0.0.1:10000:10000",
                        "mcr.microsoft.com/azure-storage/azurite:3.34.0", "azurite-blob", "--blobHost", "0.0.0.0",
                        "--blobPort", "10000", "--inMemoryPersistence", "--skipApiVersionCheck", "--loose", "--silent"],
                       check=True, capture_output=True)
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            urllib.request.urlopen("http://127.0.0.1:10000/", timeout=2)
            return
        except urllib.error.HTTPError:
            return
        except OSError:
            time.sleep(0.5)
    raise RuntimeError("Azurite did not answer on localhost:10000")


def configure(remote_database_disk: bool) -> Path:
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
        options = ["--s3-storage", "--no-azure"] + (["--remote-database-disk"] if remote_database_disk else [])
        subprocess.run([str(CLICKHOUSE_SRC / "tests/config/install.sh"), str(server_etc), str(client_etc), *options],
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


def install(variant: str, etc: Path, name: str) -> Path:
    """Installs `variant` into .dev/parity/<name>."""
    base = stack.DEV / "parity" / name
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
        run = uuid.uuid4().hex[:8]
        key = stack.mint_key(f"parity/{run}/s3", "write")
        # With --remote-database-disk, the database disk moves from `plain_rewritable` to InodeDB too.
        database_disk = ""
        if (server_etc / "config.d/remote_database_disk.xml").exists():
            database_disk = f"""
            <disk_db_remote>
                <metadata_type>inodedb</metadata_type>
                <endpoint>http://localhost:11111/test/parity-inodedb-databases/</endpoint>
                <inodedb_endpoint>{stack.INODEDB_ENDPOINT}</inodedb_endpoint>
                <inodedb_key>{stack.mint_key(f"parity/{run}/databases", "write")}</inodedb_key>
            </disk_db_remote>"""
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
            </s3>{database_disk}
        </disks>
    </storage_configuration>
</clickhouse>
""")
    return base


def start(base: Path) -> subprocess.Popen:
    (base / "log").mkdir(exist_ok=True)
    process = subprocess.Popen(
        [str(BINARY), "server", "--config-file", str(base / "etc/clickhouse-server/config.xml")],
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


def run_tests(base: Path, jobs: int, timeout: int, patterns: list[str], queries: Path | None = None) -> dict[str, str]:
    bin_dir, env = tools(base)
    env["CLICKHOUSE_CONFIG_CLIENT"] = str(base / "etc/clickhouse-client/config.xml")
    command = [
        sys.executable, str(CLICKHOUSE_SRC / "tests/clickhouse-test"),
        "--binary", str(bin_dir / "clickhouse"),
        "--configclient", str(base / "etc/clickhouse-client/config.xml"),
        "--configserver", str(base / "etc/clickhouse-server/config.xml"),
        "--tmp", str(base / "tmp"),
        "--jobs", str(jobs),
        "--timeout", str(timeout),
        # A recheck runs mostly failing tests, which would trip the default limit of 20 failures in a row.
        "--max-failures-chain", "1000000",
        "--no-random-settings", "--no-random-merge-tree-settings", "--no-stateful", "--no-long",
        "--zookeeper", "--shard",
        *(["--queries", str(queries)] if queries else []),
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


def exactly(tests: list[str]) -> str:
    """A clickhouse-test selector for exactly these tests; it matches file names."""
    return "^(?:" + "|".join(re.escape(t) for t in tests) + r")\."


def rewrite_for_inodedb(text: str, mount: Path) -> str:
    """Points a test's observations of local metadata files at InodeDB: paths from `system.parts`, `system.tables` and
    `FREEZE` lead into `mount`, an `idb mount` of the disk's InodeDB root, which keeps the disk's files under `tree/`,
    and `FileOpen`, which counts local file opens, becomes `InodeDBCommit`."""
    prefix = f"inodedb://{stack.INODEDB_ENDPOINT}/"
    text = re.sub(r"SELECT (path|data_paths\[1\]|part_backup_path) FROM", rf"SELECT replaceOne(\1, '{prefix}', '{mount}/tree/') FROM", text)
    return text.replace("ProfileEvents['FileOpen']", "ProfileEvents['InodeDBCommit']")


def run_rewritten(etc: Path, tests: list[str], jobs: int, timeout: int) -> dict[str, str]:
    """Runs each of `tests` that `rewrite_for_inodedb` changes, rewritten, on `inodedb` with the disk's tree mounted."""
    base = install("inodedb", etc, "inodedb-rewritten")
    mount, queries = base / "mnt", base / "queries"
    mount.mkdir()
    (queries / "0_stateless").mkdir(parents=True)
    (queries / "shell_config.sh").symlink_to(CLICKHOUSE_SRC / "tests/queries/shell_config.sh")
    rewritten = []
    for test in tests:
        changed = False
        for source in (CLICKHOUSE_SRC / "tests/queries/0_stateless").glob(f"{test}.*"):
            text = source.read_text()
            new = text if source.suffix == ".reference" else rewrite_for_inodedb(text, mount)
            changed |= new != text
            (queries / "0_stateless" / source.name).write_text(new)
        if changed:
            rewritten.append(test)
    if not rewritten:
        return {}

    disk = ET.parse(base / "etc/clickhouse-server/config.d/zz_inodedb_metadata.xml").getroot().find("storage_configuration/disks/s3")
    mounter = subprocess.Popen([str(stack.IDB), "--endpoint", f"http://{stack.INODEDB_ENDPOINT}", "--key", disk.findtext("inodedb_key"),
                                "mount", str(mount)])
    try:
        deadline = time.monotonic() + 30
        while not mount.is_mount():
            if mounter.poll() is not None or time.monotonic() > deadline:
                raise RuntimeError("idb mount failed; scripts/inodedb-build builds idb with it")
            time.sleep(0.1)
        server = start(base)
        try:
            results = run_tests(base, jobs, timeout, [exactly(rewritten)], queries)
        finally:
            server.terminate()
            server.wait(120)
    finally:
        subprocess.run(["fusermount3", "-u", str(mount)], check=True)
        mounter.wait(30)
    print(f"inodedb-rewritten: {results}", flush=True)
    return results


def run(variant: str, etc: Path, name: str, jobs: int, timeout: int, patterns: list[str]) -> dict[str, str]:
    base = install(variant, etc, name)
    server = start(base)
    try:
        results = run_tests(base, jobs, timeout, patterns)
    finally:
        server.terminate()
        server.wait(120)
    counts = {status: list(results.values()).count(status) for status in sorted(set(results.values()))}
    print(f"{name}: {counts}", flush=True)
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--jobs", type=int, default=16)
    # The slowest passing test takes about a minute; tests that wait on absent services hang until the timeout.
    parser.add_argument("--timeout", type=int, default=180, help="seconds per test")
    parser.add_argument("--variants", default="local,inodedb")
    parser.add_argument("--remote-database-disk", action="store_true",
                        help="keep database metadata on an S3 disk: plain_rewritable for local, inodedb for inodedb")
    parser.add_argument("patterns", nargs="*")
    args = parser.parse_args()

    start_azurite()
    BINARY.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(stack.CLICKHOUSE, BINARY)
    etc = configure(args.remote_database_disk)
    for variant in filter(None, args.variants.split(",")):
        run(variant, etc, variant, args.jobs, args.timeout, args.patterns)

    passed = ("OK", "SKIPPED")
    results = {v: json.loads((stack.DEV / "parity" / v / "results.json").read_text()) for v in ("local", "inodedb")}
    suspects = sorted(t for t, s in results["inodedb"].items() if s not in passed and results["local"].get(t, "OK") == "OK")
    improvements = sorted(t for t, s in results["inodedb"].items() if s == "OK" and results["local"].get(t, "OK") not in passed)
    jobs = min(args.jobs, 4)
    regressions, flaky, local_files = [], [], []
    if suspects:
        print(f"rechecking {len(suspects)} tests that fail with inodedb metadata", flush=True)
        local = run("local", etc, "local-recheck", jobs, args.timeout, [exactly(suspects)])
        regressions = [t for t in suspects if local.get(t) == "OK"]
        for _ in range(RECHECKS):
            if not regressions:
                break
            recheck = run("inodedb", etc, "inodedb-recheck", jobs, args.timeout, [exactly(regressions)])
            flaky += [t for t in regressions if recheck.get(t) in passed]
            regressions = [t for t in regressions if recheck.get(t) not in passed]
    if regressions:
        rewritten = run_rewritten(etc, regressions, jobs, args.timeout)
        local_files = [t for t in regressions if rewritten.get(t) == "OK"]
        regressions = [t for t in regressions if t not in local_files]
    print(f"{len(regressions)} tests pass with local metadata and fail with inodedb metadata")
    for test in regressions:
        print(f"    {test}")
    print(f"{len(local_files)} tests observe local metadata files and pass once they observe InodeDB")
    for test in local_files:
        print(f"    {test}")
    print(f"{len(flaky)} tests failed with inodedb metadata and passed on a recheck")
    for test in flaky:
        print(f"    {test}")
    print(f"{len(improvements)} tests pass with inodedb metadata and fail with local metadata")
    diff = {"regressions": regressions, "local_files": local_files, "flaky": flaky, "improvements": improvements}
    (stack.DEV / "parity/diff.json").write_text(json.dumps(diff, indent=1))
    return 1 if regressions else 0


if __name__ == "__main__":
    sys.exit(main())
