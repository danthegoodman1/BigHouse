# BigHouse

BigHouse runs ClickHouse like BigQuery. One persistent writer ingests into `MergeTree` tables stored on S3, and each query summons a fleet of short-lived ClickHouse servers that read the same tables as parallel replicas, answer, and exit.

Two pieces make that work:

- **The `inodedb` metadata type.** An object-storage disk keeps its file tree in [InodeDB](inodedb), a transactional tree of inodes, and its blobs in S3. Directories, renames and hardlinks are InodeDB's own, each metadata transaction commits as one fenced batch, and blobs leave S3 through InodeDB's free queue only once no reader lease can reach them. It matches an S3 disk with `local` metadata feature for feature, with one substitution: in place of zero-copy replication, which ships local metadata files between replicas, any number of servers read the disk while its writer changes it. It lives in the `ClickHouse/` submodule, a fork of ClickHouse, under `src/Disks/DiskObjectStorage/MetadataStorages/InodeDB/`.
- **The query API.** A small Python service keeps a blank ClickHouse server booted in each of a set of known slots. For each query it assigns a fleet of them to the disk with one config reload each, attaches the writer's databases from their definitions in InodeDB, runs the query with parallel replicas, and replaces the fleet's servers with blank ones. Queries need nothing from the writer, so they keep working while it is down.

[Custom Metadata for Object-Storage Disks](Custom%20Metadata%20for%20Object-Storage%20Disks.md) explains the design, [PLAN.md](PLAN.md) tracks the work, [demo/README.md](demo/README.md) walks through the demo, and [PRODUCTION.md](PRODUCTION.md) describes a production deployment.

## A disk

```xml
<storage_configuration>
    <disks>
        <bighouse>
            <type>object_storage</type>
            <object_storage_type>s3</object_storage_type>
            <metadata_type>inodedb</metadata_type>
            <endpoint>http://127.0.0.1:11111/test/bighouse/</endpoint>
            <access_key_id>clickhouse</access_key_id>
            <secret_access_key>clickhouse</secret_access_key>
            <inodedb_endpoint>127.0.0.1:7070</inodedb_endpoint>
            <inodedb_key_file>/etc/clickhouse-server/inodedb.key</inodedb_key_file>
        </bighouse>
    </disks>
</storage_configuration>
```

The disk lives under the InodeDB key's root. A write key makes a writer, and the writer that starts last owns the disk. A read key, or `<inodedb_read_only>1</inodedb_read_only>`, makes a replica.

A writer that also keeps its database definitions on the disk, with `<database_disk><disk>bighouse</disk></database_disk>`, lets a replica attach a whole database from InodeDB: `ATTACH DATABASE name UUID '...' ENGINE = Atomic SETTINGS disk = 'bighouse'` loads every table in it. The UUID is in `metadata/<name>.sql` on the disk.

| Setting | Default | Meaning |
| --- | --- | --- |
| `inodedb_endpoint` | required | InodeDB's address. |
| `inodedb_key`, `inodedb_key_file` | required | The InodeDB key, inline or in a file. |
| `inodedb_read_only` | `0` | A replica: it writes nothing and deletes no blob. The S3 setting `readonly` implies it. |
| `inodedb_inline_file_max_bytes` | `16384` | Files up to this size keep their bytes in InodeDB instead of a blob. |
| `inodedb_spill_threshold` | `32768` | A record larger than this moves to a blob of its own. |
| `inodedb_lease_lag_ms` | `600000` | How long a replica's lease holds blobs it may still read. |
| `inodedb_cache_bytes` | `268435456` | The metadata cache's budget. |
| `inodedb_attempt_timeout_ms`, `inodedb_retry_timeout_ms` | `10000`, `60000` | Deadlines for one InodeDB call and for its retries. |

## Build and run

```
git submodule update --init
scripts/clickhouse-build          # build/programs/clickhouse; the first build takes hours
scripts/inodedb-build             # inodedb/target/release/{inodedb,idb}
scripts/dev-up                    # s3proxy on 127.0.0.1:11111 and InodeDB on 127.0.0.1:7070
```

`scripts/clickhouse-build` needs clang 21, CMake, Ninja and ccache; `scripts/inodedb-build` needs Rust; `scripts/dev-up` needs Docker; `tests/parity.py` needs FUSE (`fusermount3`). Then follow [demo/README.md](demo/README.md).

## Tests

Each suite runs against the stack `scripts/dev-up` starts and the server in `build/`.

| Command | Checks |
| --- | --- |
| `build/src/unit_tests_dbms --gtest_filter='Metadata/*:InodeDBClientTest.*'` with `INODEDB_TEST_ENDPOINT` and `INODEDB_TEST_KEY` set | Every metadata storage case against `local` and `inodedb` metadata, and the InodeDB client's standby redirect and retried commit (`scripts/clickhouse-build unit_tests_dbms` builds it). |
| `python3 tests/features.py` | Each feature on both metadata types, with equal answers, then restart and blob cleanup. |
| `python3 tests/crash.py [rounds]` | `SIGKILL` under a mixed workload: tables stay consistent and no record names a deleted blob. |
| `python3 tests/claims.py` | A writer killed between claiming freed files and deleting their blobs leaks none: the restarted writer deletes them once the claims expire. Pauses s3proxy, so run it alone. |
| `python3 tests/fence.py` | A second writer takes the disk, and the first can no longer write. |
| `python3 tests/replicas.py` | A replica sees the writer's parts and drops, its queries outlive the writer's cleanup, and its lease ends when it stops. |
| `python3 tests/discovery.py` | Fleets attach the writer's databases from InodeDB, leave out and name those they cannot serve, see each schema change, and answer while the writer is down. |
| `python3 tests/parity.py` | ClickHouse's stateless tests on S3 with `local` and with `inodedb` metadata. Rechecks each `inodedb` failure, reruns tests that read local metadata files against an `idb mount` of InodeDB's tree, and exits 1 on any test that passes only on `local`. |
| `python3 tests/bench.py [--tmpfs]` | Insert rate and cold start against `local` metadata, with InodeDB's store on the drive or in `/dev/shm` ([BENCHMARKS.md](BENCHMARKS.md)). |
| `python3 demo/e2e.py` | The query API answers as the writer does at fleet sizes 1, 2, 4 and 8, and while the writer is down, and replaces each fleet's servers with blank ones. |

## License

`ClickHouse/LICENSE-BIGHOUSE` reserves all rights to BigHouse's code in the ClickHouse fork and grants no license for it. The rest of ClickHouse stays under its Apache License 2.0.
