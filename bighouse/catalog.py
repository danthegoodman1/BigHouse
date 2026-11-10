"""The writer's catalog, read from InodeDB.

A writer keeps its database definitions on the shared disk (`database_disk`), so InodeDB holds them next to the
tables' parts: `metadata/<name>.sql` gives each database's UUID, and `store/<uuid[:3]>/<uuid>/` holds its tables'
definitions. A server attaches a database from there with one statement and loads every table in it, so fleets
read the writer's last committed schema without asking the writer, even while it is down.

A server attaches only what it can serve as the writer would: tables whose data lies on the shared disk, views,
and materialized views that refresh only on insert. A database holding anything else stays detached, and the
caller learns why.
"""

from __future__ import annotations

import re
import urllib.parse
from dataclasses import dataclass

from bighouse import stack

DISK = "bighouse"
# The server creates these itself, with contents of its own.
SERVER_DATABASES = {"system", "INFORMATION_SCHEMA", "information_schema"}
# What `CREATE DATABASE name [COMMENT '...']` on the writer stores.
DATABASE_DEFINITION = re.compile(r"ATTACH DATABASE _ UUID '([0-9a-f-]{36})'\nENGINE = Atomic(?:\nCOMMENT '.*')?\n?", re.S)
# `writeEscapedString`'s escapes; every other escaped character stands for itself.
ESCAPES = {"b": "\b", "f": "\f", "n": "\n", "r": "\r", "t": "\t", "0": "\0"}
# Each table that keeps a database off this server, with the reason.
UNSERVABLE = f"""
WITH (SELECT path FROM system.disks WHERE name = '{DISK}') AS disk_path
SELECT database, name, multiIf(
    engine LIKE '%MergeTree', 'keeps data off the `{DISK}` disk',
    engine = 'MaterializedView', 'refreshes on a schedule',
    'has engine ' || engine)
FROM system.tables
WHERE database IN {{databases:Array(String)}}
  AND NOT (
    engine LIKE '%MergeTree' AND arrayAll(p -> startsWith(p, disk_path), data_paths)
    OR engine = 'View'
    OR engine = 'MaterializedView' AND (database, name) NOT IN (SELECT database, view FROM system.view_refreshes))
ORDER BY database, name
FORMAT TSV
"""


def read_file(key: str, path: str) -> str:
    """The text of a small file on the disk: the inline bytes of its InodeDB record at `tree/<path>`.

    A value is an 11-byte header (format version, kind, flags, change time) and a body. A record's body is a
    `DiskObjectStorageMetadata` in its text form, which ends with the file's inline bytes, escaped.
    """
    value = stack.idb_bytes("cat", f"tree/{path}", key=key)
    if len(value) < 11 or value[0] != 1:
        raise ValueError(f"{path}: an InodeDB value of an unknown format")
    kind, body = chr(value[1]), value[11:].decode()
    if kind == "S":
        return body
    if kind != "R":
        raise ValueError(f"{path}: an InodeDB value of kind {kind!r}, not a file")
    _version, counts, rest = body.split("\n", 2)
    if counts.split("\t")[0] != "0":
        raise ValueError(f"{path}: keeps its bytes in blobs")
    _ref_count, _read_only, inline = rest.split("\n", 2)
    return re.sub(r"\\(.)", lambda m: ESCAPES.get(m.group(1), m.group(1)), inline.removesuffix("\n"), flags=re.S)


def quote(name: str, mark: str = "`") -> str:
    """`name` as an identifier, or with `mark = "'"` as a string literal."""
    return mark + name.replace("\\", "\\\\").replace(mark, "\\" + mark) + mark


@dataclass(frozen=True)
class Database:
    name: str
    uuid: str

    def attach(self, server: stack.Server) -> None:
        # A server starts with an empty `default` of its own; the writer's takes its place.
        if self.name == "default":
            server.query("DROP DATABASE default SYNC", settings={"database": "system"})
        server.query(f"ATTACH DATABASE {quote(self.name)} UUID '{self.uuid}' ENGINE = Atomic SETTINGS disk = '{DISK}'",
                     settings={"database": "system"})

    def detach(self, server: stack.Server) -> None:
        # Dropping a database attached from the read-only disk removes only the server's own record of it.
        server.query(f"DROP DATABASE {quote(self.name)} SYNC", settings={"database": "system"})
        if self.name == "default":
            server.query("CREATE DATABASE default", settings={"database": "system"})


def find(key: str) -> tuple[list[Database], dict[str, str]]:
    """The writer's databases a server can attach, and why each other one is left out."""
    found: list[Database] = []
    left_out: dict[str, str] = {}
    for entry in stack.idb("ls", "tree/metadata", key=key).splitlines():
        if not entry.endswith(".sql"):
            continue
        name = urllib.parse.unquote(entry.removesuffix(".sql"))
        if name in SERVER_DATABASES:
            continue
        definition = read_file(key, f"metadata/{entry}")
        if match := DATABASE_DEFINITION.fullmatch(definition):
            found.append(Database(name, match.group(1)))
        else:
            left_out[name] = f"its definition is not a plain Atomic database: {definition.strip()!r}"
    return found, left_out


def attach(server: stack.Server, key: str) -> tuple[list[Database], dict[str, str]]:
    """Attaches on `server` every database it can serve. Returns those, and why each other one is left out."""
    found, left_out = find(key)
    attached = []
    for database in found:
        try:
            database.attach(server)
            attached.append(database)
        except stack.QueryError as error:
            left_out[database.name] = f"it fails to attach: {str(error)[:300]}"
    rows = server.query(UNSERVABLE, settings={"param_databases": "[" + ",".join(quote(d.name, "'") for d in attached) + "]"})
    unservable: dict[str, str] = {}
    for row in rows.splitlines():
        database, table, why = row.split("\t")
        unservable.setdefault(database, f"{database}.{table} {why}")
    for database in [d for d in attached if d.name in unservable]:
        database.detach(server)
        left_out[database.name] = unservable[database.name]
    return [d for d in attached if d.name not in unservable], left_out
