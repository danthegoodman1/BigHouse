# BigHouse Development Plan

## Overarching Goal

BigHouse runs ClickHouse like BigQuery. One persistent writer ingests into `MergeTree` tables stored on S3, and each query summons a fleet of short-lived ClickHouse servers that read the same tables as parallel replicas, answer, and exit. The foundation is a new object-storage `metadata_type`, `inodedb`, that keeps the disk's file tree in [InodeDB](inodedb) and the blobs in S3, as [Custom Metadata for Object-Storage Disks](Custom%20Metadata%20for%20Object-Storage%20Disks.md) describes.

The `inodedb` metadata type matches an S3 disk with `local` metadata feature for feature. Every `MergeTree` workload that runs on `local` metadata runs on `inodedb` unchanged: hardlinks, mutations, lightweight deletes and updates, `FREEZE`, partition moves and copies, TTL moves, backups, appends, truncation, the `cache` and `encrypted` wrappers, and `ReplicatedMergeTree`. InodeDB has a real rename and real hardlinks, so the disk implements each operation directly, where `plain_rewritable` copies objects or refuses.

Each table has one writer. The demo runs on one machine with s3proxy standing in for S3.

## Layout

| Path | Contents |
| --- | --- |
| `ClickHouse/` | Submodule: branch `bighouse` of `danthegoodman1/ClickHouse`, cut from ClickHouse `v26.10.2.103-stable`, shallow. `ClickHouse/LICENSE-BIGHOUSE` reserves all rights to BigHouse's code there. |
| `ClickHouse/src/Disks/DiskObjectStorage/MetadataStorages/InodeDB/` | The `inodedb` metadata type: gRPC client, value format, cache, storage and transaction. |
| `inodedb/` | Submodule: InodeDB at the revision the client's `proto/inodedb.proto` copies. |
| `scripts/` | `clickhouse-build`, `inodedb-build`, `dev-up`, `dev-down`. |
| `bighouse/` | `stack.py` (keys, disk configs, servers), `catalog.py` (the writer's databases in InodeDB), `fleet.py` (slots, the pool of blank servers, and fleets), `api.py` (the query API). |
| `tests/` | `features.py`, `crash.py`, `claims.py`, `fence.py`, `replicas.py`, `discovery.py`, `parity.py`, `bench.py`. |
| `demo/` | `up`, `down`, `load`, `e2e.py`. |

## Implementation Principles

- **Parity with `local` is the bar.** Implement every method `MetadataStorageFromDisk` and its transaction implement (`ClickHouse/src/Disks/DiskObjectStorage/MetadataStorages/Local/MetadataStorageFromDisk.h`), with the same capability flags: `supportsEmptyFilesWithoutBlobs`, `areBlobPathsRandom`, `supportWritingWithAppend`, `supportsChmod` and `supportsStat`. A differential run against `local` decides parity, and a test that fails only on `inodedb` is a bug.
- **Keep ClickHouse changes contained.** New code lives in `ClickHouse/src/Disks/DiskObjectStorage/MetadataStorages/InodeDB/`. The ClickHouse files changes table below names every change outside that directory and its reason, so rebasing onto a new ClickHouse release stays small.
- **BigHouse's code stays unlicensed.** `ClickHouse/LICENSE-BIGHOUSE` grants no license for BigHouse's directory in the fork or for its changes to ClickHouse files, and Apache 2.0 covers ClickHouse's own code. A new BigHouse directory gets added to that file's list, and each modified ClickHouse file gets a short `BigHouse:` notice at the change, as Apache 2.0 §4(b) requires.
- **InodeDB owns tree semantics.** The disk maps ClickHouse files onto inodes and values. When InodeDB lacks a capability, build it in `inodedb/` under that project's plan and tests.
- **One fenced batch per transaction.** Each metadata transaction commits as one `all_or_nothing` InodeDB batch carrying the writer's owner fence.
- **Blobs outlive every reader.** A blob leaves S3 only after the commit that frees it, and only once no reader lease can reach it.
- **Everything runs locally.** s3proxy serves S3, an InodeDB server holds metadata, and ClickHouse comes from the submodule. Tests, CI and the demo use the same stack.

## ClickHouse Files Changed

| File | Change | Reason |
| --- | --- | --- |
| `src/Disks/DiskType.h`, `src/Disks/DiskType.cpp` | `MetadataStorageType::InodeDB` and its `"inodedb"` name. | Registration. |
| `src/Disks/DiskObjectStorage/MetadataStorages/MetadataStorageFactory.cpp` | Calls `registerMetadataStorageFromInodeDB`. | Registration. |
| `src/CMakeLists.txt` | Adds the `InodeDB` sources and the generated gRPC library. | Build. |
| `src/Storages/MergeTree/MergeTreeData.cpp` | `refreshDataPartsOnce` drops active parts that vanished from an `inodedb` disk's listing; partition `ALTER`s accept `InodeDB`. | Replicas see drops, merges and truncation. |
| `src/Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h` | `getInlineFileMaxBytes`, default 0. | A metadata storage sets the inline threshold. |
| `src/Disks/DiskObjectStorage/MetadataStorages/Cache/MetadataStorageFromCacheObjectStorage.{h,cpp}` | Delegates `getInlineFileMaxBytes`. | Inline files under the `cache` wrapper. |
| `src/Disks/DiskObjectStorage/DiskObjectStorageTransaction.cpp` | `writeFile` inlines up to the larger of the write settings' and the metadata storage's threshold. | Inline files. |
| `src/Disks/DiskObjectStorage/DiskObjectStorage.h` | `supportZeroCopyReplication` returns false for `InodeDB`. | Zero-copy writes received metadata to local files. |
| `src/Storages/MergeTree/DataPartStorageOnDiskBase.cpp` | `rename` sets the directory's modification time and moves it in one disk transaction when the part has none open. | One InodeDB commit per rename, not two. |
| `src/Backups/BackupEntryWithChecksumCalculation.cpp` | A non-empty file without blobs takes its checksum from its bytes. | Backups of inline files. |
| `src/Common/ProfileEvents.cpp` | `InodeDB*` events. | Call counts in `system.events`. |
| `src/Disks/tests/gtest_metadata_local_disk.cpp` | Runs every case against `local` and `inodedb`, plus `inodedb`-only inline, spill and replica cases. | Unit parity. |
| `src/Disks/tests/gtest_inodedb_client.cpp` | The client's standby redirect and retried commit, through a fault-injecting proxy. | Client behavior on failover and lost answers. |

## Testing Strategy

- **Unit.** `ClickHouse/src/Disks/tests/gtest_metadata_local_disk.cpp` runs every case against both metadata types, and `gtest_inodedb_client.cpp` tests the client's redirect and retry; the InodeDB cases need `INODEDB_TEST_ENDPOINT` and `INODEDB_TEST_KEY`.
- **Parity.** `tests/parity.py` runs ClickHouse's stateless tests twice over s3proxy with every MergeTree table on an S3 disk, once with `local` metadata and once with `inodedb`, from one install of the test configs (`install.sh --s3-storage`). It runs the tests tagged `no-object-storage` too, since an S3 disk with `local` metadata passes them. Each `inodedb` failure reruns on `local`, then up to three times on `inodedb`, to separate regressions from flaky tests; a test that fails all three reruns once more, rewritten to observe InodeDB's tree through `idb mount` where it observes local metadata files. `inodedb` must pass every test `local` passes, and the script exits 1 otherwise.
- **Features.** `tests/features.py` runs each feature scenario on both metadata types in one server and requires equal answers, then checks restart and blob cleanup.
- **Crash.** `tests/crash.py` kills the writer with `SIGKILL` under a mixed workload, restarts it, and checks `CHECK TABLE`, acknowledged batches, partial batches and blobs that records name.
- **Replicas.** `tests/replicas.py`: a writer and a replica; new parts appear, drops vanish, and queries outlive the writer's cleanup.
- **End to end.** `demo/e2e.py`: the query API returns the writer's answers at fleet sizes 1, 2, 4 and 8, and replaces each fleet's servers with blank ones.
- **Benchmarks.** `tests/bench.py` against `local` metadata, with InodeDB's store on the drive and on tmpfs (`--tmpfs`), recorded in `BENCHMARKS.md`.
- **CI.** GitHub Actions runs s3proxy and InodeDB, builds ClickHouse with a restored ccache, and runs the suites. The full stateless parity run runs on demand.

## Phase 0: Foundations

Goal:
A clean branch that builds ClickHouse from the submodule, runs InodeDB and s3proxy locally, and reproduces the baseline: `MergeTree` on an S3 disk with `local` metadata over s3proxy.

Scope:
- The `rebuild` branch, holding the design, this plan and the ClickHouse submodule.
- A ClickHouse fork, `danthegoodman1/ClickHouse`, with a `bighouse` branch cut from the pinned tag; the submodule points there.
- InodeDB pinned as an `inodedb/` submodule.
- `scripts/clickhouse-build`, `scripts/inodedb-build`, `scripts/dev-up`, `scripts/dev-down`.
- The baseline on `local` metadata.
- A CI workflow.

Completion gate:
`scripts/clickhouse-build` produces `build/programs/clickhouse` from the pinned submodule, and with `scripts/dev-up` running, a `MergeTree` table on an S3 disk with `local` metadata over s3proxy takes inserts, merges and selects, locally and in CI.

Testing plan:
- The `s3_local` half of `tests/features.py` is the baseline.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 0A: `rebuild` branch with the old project removed | `fd4451f` on `origin/rebuild`. |
| Complete | Work | 0B: ClickHouse submodule from `v26.10.2.103-stable` | `.gitmodules` (`shallow = true`, `branch = bighouse`); the tag is `c6428ebcc39f`. |
| Complete | Doc | Design doc line references | `Custom Metadata for Object-Storage Disks.md` cites lines of the pinned tag. |
| Complete | Work | 0C: ClickHouse fork with a `bighouse` branch | `danthegoodman1/ClickHouse`, branch `bighouse`; `632ffeccc` adds `LICENSE-BIGHOUSE`, `38edf7c26` the metadata type. |
| Complete | Work | 0D: InodeDB pinned as a submodule | `inodedb/` at `dc97a2c` of `github.com/danthegoodman1/inodedb`. |
| Complete | Work | 0E: `scripts/clickhouse-build`, `scripts/inodedb-build` | Builds `build/programs/clickhouse` (RelWithDebInfo, clang, Ninja, ccache; log in `build/build.log`) and `inodedb/target/release/{inodedb,idb}`. |
| Complete | Work | 0F: `scripts/dev-up`, `scripts/dev-down` | s3proxy `4.1.1` on `127.0.0.1:11111` (bucket `test`, filesystem backend under `.dev/s3`) and InodeDB on `127.0.0.1:7070` (SQLite under `.dev/inodedb`). |
| Complete | Test | Baseline on `local` metadata | Every `tests/features.py` scenario runs on `s3_local` and matches `s3_inodedb`. |
| In Progress | Work | 0G: CI workflow | `.github/workflows/ci.yml` builds InodeDB and ClickHouse with ccache, starts the stack, and runs the gtests, features, fence, replicas, 20 crash rounds and the demo e2e on pull requests; parity runs on manual dispatch. Missing: a green run. |
| Blocked | Risk | ClickHouse build time in CI | A cold build takes hours on a hosted runner. Blocked by: a choice between a larger or self-hosted runner and a hosted runner with a long first build. |
| Incomplete | Gate | Baseline runs locally and in CI | Local runs pass; missing: a CI run link. |

## Phase 1: InodeDB Client in ClickHouse

Goal:
ClickHouse code calls InodeDB through a C++ API: batches with conditions, stat, read, list, leases, keys, and claiming and acknowledging freed files.

Scope:
- 1A: Choose the client.
- 1D: `InodeDB::Client`, the C++ client the metadata type uses: request-id retries, writer redirects, deadlines, and InodeDB errors mapped to ClickHouse error codes.

Completion gate:
The metadata gtests pass against the server `scripts/dev-up` starts.

Testing plan:
- The metadata gtests exercise every call the metadata type makes; the crash and replica suites exercise retries and leases.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Decision | 1A: C++ gRPC client | ClickHouse's Rust workspace vendors no tokio, tonic or prost, so a bridge over InodeDB's Rust client meant vendoring an async stack. The client uses `contrib/grpc` and `PROTOBUF_GENERATE_GRPC_CPP` (`InodeDB/proto/CMakeLists.txt`), with no Rust. |
| Complete | Work | 1D: `InodeDB::Client` | `InodeDB/InodeDBClient.{h,cpp}`: `Commit` with a random 16-byte request id, `Stat`, `Read`, paged `List` that resumes after its last entry, `ClaimFreed`, `AckFreed`, `MintKey`, and `Lease` streams. Retries `UNAVAILABLE` and `DEADLINE_EXCEEDED` with backoff, follows `not-writer` redirects, and counts every call in `InodeDB*` ProfileEvents. |
| Complete | Test | Writer redirect and a retried batch applied once | `src/Disks/tests/gtest_inodedb_client.cpp` puts an in-process gRPC proxy before InodeDB. `StandbyRedirectsToTheWriter`: the proxy answers as a standby, and the client commits through the writer it names and sends later calls there directly. `RetriedCommitAppliesOnce`: the proxy forwards a mkdir and drops its answer; the retry carries the same request id and gets `applied`, while a new commit of the same mkdir fails with `EXISTS`. Both pass. |
| Complete | Gate | Metadata gtests pass against InodeDB | `unit_tests_dbms --gtest_filter='Metadata/MetadataDiskTest.*'`: 79 passed, 3 skipped: `TestRecursiveCyclicRemove/inodedb`, which corrupts `local`'s files on disk, and the `local` runs of the two `inodedb`-only cases. |

## Phase 2: The `inodedb` Metadata Type at Parity

Goal:
An object-storage disk with `metadata_type` `inodedb` passes every test that an S3 disk with `local` metadata passes.

Scope:
- 2A: Registration and config: `inodedb_endpoint`, `inodedb_key` or `inodedb_key_file`, `inodedb_read_only`, `inodedb_attempt_timeout_ms`, `inodedb_retry_timeout_ms`, `inodedb_spill_threshold`, `inodedb_inline_file_max_bytes`, `inodedb_lease_lag_ms`, `inodedb_cache_bytes`. The key's root holds the disk.
- 2B: The data model. A directory is an InodeDB directory. A file is an InodeDB file whose value is an `InodeDB::Value`: a format version, a kind, a `retained` flag, a change time, and a body. The reference count comes from the inode's `nlink`, and modification time and mode from its attributes.
- 2C: Every method `MetadataStorageFromDiskTransaction` implements, each transaction one batch.
- 2D: Transactions that read state replay a forward simulation of their operations, so later operations see earlier ones.
- 2E: Every method `MetadataStorageFromDisk` implements.
- 2F: Blob removal on InodeDB's free queue.
- 2G: Feature checks on both metadata types.

Completion gate:
The parity gtests pass, the stateless parity run shows no failure unique to `inodedb` beyond ledger-listed exceptions, and every feature check passes on both metadata types.

Testing plan:
- `gtest_metadata_local_disk.cpp` over both types.
- `tests/parity.py`.
- `tests/features.py`.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 2A: Registration and config | `InodeDB/registerMetadataStorageFromInodeDB.cpp`; the hooks in the ClickHouse files changes table. A disk is `<metadata_type>inodedb</metadata_type>` on an `object_storage` disk (`bighouse/stack.py`, `inodedb_disk_xml`). |
| Complete | Decision | 2B: Value format | `InodeDB/InodeDBValue.{h,cpp}`: kinds `Record` (a serialized `DiskObjectStorageMetadata`), `String` (bytes from `writeStringToFile`), `Spilled` (the key of a blob holding the record) and `Tombstone` (blobs to delete). |
| Complete | Work | 2C: Transaction methods | `MetadataStorageFromInodeDBTransaction` in `InodeDB/MetadataStorageFromInodeDB.cpp`. Transactions that read no state commit blind under a shared lock; a blind create that finds its file retries in read mode. |
| Complete | Work | 2D: Forward simulation | `InodeDBBatchBuilder` tracks files through renames, hardlinks and recursive removals within a batch; gtests `TestFoldedRemoveRecursiveRollback`, `TestComplexUnlink`, `TestComplexRemoveRecursive`. |
| Complete | Work | 2E: Storage methods | `MetadataStorageFromInodeDB`; `local`'s conventions for listings, `getHardlinkCount` and missing directories, checked by the gtests. |
| Complete | Work | 2F: Blob removal through `ClaimFreed` and `AckFreed` | A file whose last link goes frees its record's blobs unless `retained`; blobs a transaction drops from a live file go in a tombstone created and unlinked in the same batch; a spilled record's blob goes last. `verifyBlobsToRemove` gtests and the features suite's blob cleanup check. |
| Complete | Decision | `getLastChanged` | The value carries a change time, set whenever the disk writes the value; `getLastChanged` returns it for files and `mtime` for directories. |
| Complete | Decision | Records over 64 KiB | A record past `inodedb_spill_threshold` (32 KiB) moves to a blob. Rewriting the record frees the previous spill blob through a tombstone, and removing the file deletes the spill blob after the blobs it names. Gtest `TestSpilledRecord`; the `log_engines` scenario appends past the threshold. |
| Complete | Test | Parity gtests | 79 passed, 3 skipped (Phase 1 gate). |
| Complete | Test | Stateless parity run | `tests/parity.py` exits 0 on the build with the single-commit rename and inline files on. Each pass ran 17,275 tests: `inodedb` 15,223 OK, 1,096 failed, 956 skipped; `local` 15,221 OK, 1,098 failed, 956 skipped. 1,091 tests fail on both. The five that fail only on `inodedb` read local metadata files (rows below); the seven that fail only on `local` pass on `inodedb`. `.dev/parity/diff.json`: no regressions. |
| Complete | Test | 2G feature checks | `tests/features.py`, with inline files on, passes all 13 scenarios with equal answers on both types: `insert_merge`, `mutations`, `partitions` (`ATTACH PARTITION FROM`, `REPLACE PARTITION`, `MOVE PARTITION TO TABLE`, detached parts), `freeze`, `ttl_projection_index`, `log_engines`, `tables`, `backup`, `cache_and_encryption`, `lightweight_update`, `backup_to_s3`, `replicated` (two `ReplicatedMergeTree` replicas over an embedded Keeper) and `move_between_disks`; then restart and blob cleanup. `database_disk` runs in `tests/parity.py --remote-database-disk`. |
| Complete | Test | `database_disk` on the `inodedb` disk | `tests/parity.py --remote-database-disk` exits 0 (`build/test_parity_dbdisk.log`). With database metadata on an `inodedb` disk, all 17,275 tests ran: 15,224 OK, 1,095 failed, 956 skipped. The `local` pass, with `plain_rewritable` database metadata, ended at 12,376 tests when `clickhouse-test` received `SIGTERM`; the recheck ran all 377 `inodedb` failures on it, and 372 failed there too. The other five are the tests that read local metadata files. |
| Complete | Decision | Zero-copy replication | Zero-copy ships each part's local metadata files to the other replica, and the receiver writes them to its local filesystem (`Fetcher::fetchSelectedPart`), so upstream turns it off for `keeper` and `memory` metadata. `DiskObjectStorage::supportZeroCopyReplication` turns it off for `inodedb` too, and replicated tables fetch whole parts; BigHouse's replicas share the writer's tree instead. With it on, the sender failed with `Remote metadata 'checksums.txt' is not exists` and the fetch retried forever; now `tests/features.py replicated` passes with `allow_remote_fs_zero_copy_replication = 1` on both types, and both disks remove every blob afterward. |
| Complete | Decision | Tests that read local metadata files | `02378_part_log_profile_events` sums `FileOpen`, which counts local file opens; `03100_lwu_59_freeze_detach_keep_invalidated_columns`, `04429_drop_index_sibling_suffix_collision`, `04538_packed_clone_hardlinks_data_packed` and `04758_part_minmax_block_columns_files_prefix` run `find`, `test -e`, `test -f` or `stat` on paths from `system.parts`, `system.tables` and `FREEZE`. The last four carry upstream's `no-object-storage` tag. `parity.py` (`rewrite_for_inodedb`) points those paths into an `idb mount` of the disk's tree and swaps `FileOpen` for `InodeDBCommit`; rewritten, all five pass against the original references: the clone shares `data.packed`'s inode, frozen and detached copies keep `invalidated_system_columns.txt`, the minmax files form a prefix, `DROP INDEX` keeps the survivor's files, and each mutation commits. |
| Complete | Decision | `02725_memory-for-merges` is flaky on both types | It requires the summed peak memory of a table's merges to stay under 200 MiB. Six runs per disk of its workload ran two merges (one a single-part rewrite) three times of six on `local` and on `inodedb`, and two merges always pass the limit (204 to 226 MiB); one merge peaked at 116 to 135 MiB on both. The latest parity run failed it on both. |
| Complete | Gate | No failure unique to `inodedb` | `tests/parity.py` reports 0 regressions and 5 tests that read local metadata files (`.dev/parity/diff.json`). |

## Phase 3: Durability, Fencing and Crash Safety

Goal:
The writer survives crashes, retries and takeovers with consistent metadata, losing no committed part and leaking no committed blob.

Scope:
- 3A: The owner fence. A starting writer writes `owner`, and every batch carries a `when` on that file's version; a fenced writer stops writing and reports it.
- 3B: Exactly-once commits through InodeDB's request ids.
- 3C: Crash tests.
- 3D: Freed-file claims that a crashed consumer leaves behind return after InodeDB's claim TTL and get deleted.

Completion gate:
A hundred kill-and-restart rounds leave every table consistent, a fenced writer applies nothing, and a retried batch applies once.

Testing plan:
- `tests/crash.py`.
- `tests/fence.py`: the second writer takes ownership, and the first's next commit fails.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 3A: Owner fence | `startup` commits `owner` and keeps its version; every batch carries `when VersionIs(owner, version)`, and a skipped `when` throws `TABLE_IS_READ_ONLY`. `tests/fence.py`: once a second writer starts, the first's insert fails and applies nothing, and the second reads what the first committed and inserts. |
| Complete | Work | 3B: Exactly-once commits | Every `Commit` carries one request id across its retries (`InodeDBClient.cpp`), which InodeDB deduplicates. |
| Complete | Test | 3C: Crash loops | `tests/crash.py 100`, with inline files on: 100 of 100 rounds of inserts, `OPTIMIZE`, mutations and truncation passed, with 3,211 batches acknowledged by the last round, every restart under 1.6 s, and every blob a record names present. |
| Complete | Test | 3D: Abandoned claims | `tests/claims.py`: pauses s3proxy so the writer's deletes hang after it claims a dropped table's freed files, kills the writer, and resumes s3proxy. The killed writer deleted none of the claimed blobs, and the restarted writer deleted all of them 27 s after the kill. |
| Complete | Gate | Consistent after every crash round | 3C and 3D. |

## Phase 4: Commit Path Performance

Goal:
A part commits in few InodeDB round trips, reads come from a cache, and a cold part loads without object-storage reads.

Scope:
- 4A: Cached reads.
- 4B: One commit per part.
- 4C: Orphan sweep.
- 4D: Inline small files.
- 4E: Benchmarks against `local` metadata.

Completion gate:
`BENCHMARKS.md` reports insert rate, InodeDB commits per insert, and a 10,000-part cold start for both metadata types, with the parity run still clean.

Testing plan:
- `tests/bench.py`.
- The gtests, features, crash and parity suites rerun with inline files on.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 4A: Cached reads | `InodeDB/InodeDBMetadataCache.{h,cpp}`: reading a file lists its directory with values, so one `List` serves a part's files. A commit invalidates the paths and subtrees it touched and the other names of inodes it changed; a read keeps its answer only if no commit landed since it began. Replicas forget listings on refresh. Link counts read uncached. |
| Complete | Work | 4B: One commit per part | `MergeTreeDataWriter` writes a part inside one disk transaction, which commits as one batch. The rename to the part's name commits once more, together with the directory's modification time (`DataPartStorageOnDiskBase::rename`, which had committed them separately). `tests/bench.py`: 2.0 InodeDB commits per insert. `MergeTreeSink::commitPart` renames under the table's parts lock, so one table's renames commit in sequence (`BENCHMARKS.md`). |
| Blocked | Work | 4C: Orphan sweep | Blobs uploaded before a crash and never committed stay in S3, as with `local` metadata, which has no sweep either. A safe sweep must know every blob a committed state can still reach, including free-queue entries a lease holds. Blocked by: an InodeDB call that lists free-queue entries without claiming them. |
| Complete | Work | 4D: Inline small files | Files up to `inodedb_inline_file_max_bytes` (16 KiB) keep their bytes in the record. `writeInlineDataToFile` rewrites the file as `MetadataStorageFromMemory` does; an append moves inline bytes to a blob first; truncation cuts inline bytes; backups checksum inline files by their bytes. Gtest `TestInlineRewriteAppendTruncate`; `tests/features.py`, `tests/crash.py 100` and the parity run pass with inline files on. `tests/bench.py`: a 1,000-row part uploads no blob, and 10,000 parts load in 1.2 s against `local`'s 2.3 s. |
| Complete | Test | 4E: Benchmarks | `BENCHMARKS.md`. With InodeDB's store on this machine's NVMe drive (2 ms `fdatasync`), `inodedb` takes 83 inserts/s on one thread against `local`'s 312 and 87 s for 10,000 parts against 5.5 s; with the store on tmpfs, 318 against 316 and 4.8 s against 4.0 s. Cold start: 1.2 s against 2.3 s. |
| Complete | Gate | Benchmarks recorded, parity intact | `BENCHMARKS.md`; the parity run on the same build, with inline files on, reports no regressions (Phase 2). |

## Phase 5: Read Replicas

Goal:
Any number of read-only servers attach the writer's tables, see its committed state including drops, and finish their queries while the writer cleans up.

Scope:
- 5A: Read-only mode.
- 5B: One snapshot per fleet.
- 5C: Dropped parts on refresh.
- 5D: Reader leases.
- 5E: Table discovery from InodeDB.
- 5F: Scoped keys.
- 5G: Schema changes reach long-lived replicas.
- 5H: Warm fleet servers.

Completion gate:
A replica sees a new part and a dropped partition within a few refreshes, a long query on a replica returns the right answer while the writer merges and deletes its inputs, and an `ALTER` on the writer reaches the replica.

Testing plan:
- `tests/replicas.py`, `tests/discovery.py`.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 5A: Read-only mode | `inodedb_read_only`, or the object storage's `readonly`, makes `isReadOnly` true; `refresh` forgets cached listings. `fleet.replica_disk_xml` sets both, so `MergeTree` treats the tables as static. |
| Complete | Decision | 5B: One snapshot per fleet | Parallel replicas read the parts the coordinator's first announcing replica lists (`DefaultCoordinator::initializeReadingState`), so a fleet reads one state without pinning an InodeDB version. `demo/e2e.py` checks whole batches mid-ingest at 1, 2 and 4 servers. |
| Complete | Work | 5C: Dropped parts on refresh | `MergeTreeData::refreshDataPartsOnce` (ClickHouse files changes table). `tests/replicas.py`: new part, dropped partition and truncation each reach the replica. |
| Complete | Work | 5D: Reader leases | `InodeDBReaderLease` beats every 10 s, releases versions it read more than `inodedb_lease_lag_ms` ago, and releases the lease when the disk shuts down, cancelling its stream only after an attempt's deadline. `tests/replicas.py`: a slow replica query reads parts the writer merged away and deleted; no blob goes while the lease holds it, and the writer deletes them 0.2 s after the replica stops, within the test's 20 s bound and well inside the 30 s lease TTL. |
| Complete | Work | 5E: Table discovery from InodeDB | Writers keep database definitions on the disk (`fleet.writer_config_xml`: `database_disk`). `bighouse/catalog.py` reads each database's UUID from `metadata/<name>.sql` in InodeDB under the fleet's read key, and the coordinator attaches each with `ATTACH DATABASE ... SETTINGS disk = 'bighouse'`, then detaches any database holding a table off the disk, a refreshable materialized view or another engine; the other servers attach the rest. A read-only `inodedb` disk commits a transaction that only creates existing directories, as attaching runs `createDirectories` (gtest `TestReplicaCreatesExistingDirectories`). `tests/discovery.py`: 21 of 21 checks, covering tables, views, materialized views and `default`, four kinds of left-out database with reasons, a left-out `default`, `ALTER ADD/DROP COLUMN`, table and database `CREATE`, `DROP` and `RENAME`, and a query with the writer down. `demo/e2e.py`: all checks, attach 22 to 45 ms, and a query with the writer down. |
| Incomplete | Work | 5G: Schema changes reach long-lived replicas | A long-lived replica keeps the schema it attached with; each new fleet reads the current one (5E). Missing: reattaching on InodeDB `Watch` events under `metadata/` and `store/`, and an `ALTER` test against a running replica. |
| Complete | Work | 5H: Warm fleet servers | `fleet.Pool` boots a blank server in every slot: its boot config holds no `bighouse` disk, no cluster and no key, and names `bighouse` as MergeTree's default policy, because MergeTree reads its defaults only at boot and resolves the name only when a table attaches. `Fleet.assign` writes `config.d/tenant.xml` (the read-only disk under the query's key, its policy, and the cluster of the fleet's servers) and runs `SYSTEM RELOAD CONFIG`, which starts the disk and its lease, then attaches the databases. Once the query ends, the pool stops each server with SIGTERM, revokes the key, and boots a blank server in the slot, in the background. Each server serves one query: a reload cannot remove a disk or change its key, and a reattached server would read stale listings and definitions, since `IMetadataStorage::dropCache` is a no-op for `inodedb`. `demo/e2e.py`: a fleet's servers stop once its query ends, the pool then holds a fresh blank server in every slot, and a query that needs servers still booting waits for them. With the pool settled between queries (10 runs, p50), a query takes 95, 112, 126 and 139 ms at 1, 2, 4 and 8 servers (assign 13 to 17 ms, attach 22 to 47 ms, query 48 to 59 ms), against 300 to 420 ms with servers started per query; 4 servers are replaced 437 ms after the answer, so back to back on 8 slots, fleets of 4 average 306 ms. |
| Complete | Risk | Killed fleet servers hold blob deletion | InodeDB's release horizon spans every lease, so a killed fleet server, whose lease lasts until its 30 s TTL, holds back every deletion. The pool stops servers with SIGTERM, which releases their leases: `tests/replicas.py` run right after `tests/discovery.py` saw the merged-away blobs go 0.2 s after its replica stopped, against 13.2 s when fleets were killed. |
| Complete | Work | 5F: Scoped read keys | `bighouse/api.py` mints a read key rooted at the writer's directory with a 15-minute TTL per query, stops the query at 14 minutes (`max_execution_time`), and revokes the key once the servers stop. `tests/fence.py`: a read key cannot write. |
| In Progress | Gate | Replicas track the writer and keep their queries | `tests/replicas.py`: 10 of 10 checks pass, attaching the writer's database from InodeDB. `tests/discovery.py`: each new fleet sees the writer's `ALTER`. Missing: 5G, for a running replica. |

## Phase 6: The BigHouse Query API

Goal:
`demo/` runs on one machine: s3proxy, InodeDB, a writer ClickHouse, and a query API that assigns a fleet of warm ClickHouse servers to each query, runs it with parallel replicas, returns the result, and replaces the fleet's servers with blank ones.

Scope:
- 6A: `demo/up`, `demo/down` and `demo/load`.
- 6B: Slots: fixed ports on `127.0.0.1` for native, HTTP and interserver traffic. Each fleet's configs name exactly its slots as the cluster `bighouse`.
- 6C: The query API, a stdlib-only Python HTTP service.
- 6D: `demo/README.md` and `PRODUCTION.md`.

Completion gate:
`demo/e2e.py` runs a query suite at fleet sizes 1, 2 and 4, each answer matching the writer's, while the writer keeps inserting; after each query its servers stop and every slot holds a blank server again.

Testing plan:
- `demo/e2e.py` locally and in CI.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 6A: `demo/up`, `demo/down`, dataset | `demo/up` starts s3proxy, InodeDB and the writer and writes `.dev/demo/config.json`; `demo/load` fills `demo.events` (20 million rows by default). |
| Complete | Work | 6B: Slots and fleet configs | `bighouse/fleet.py`: slot `i` uses ports 20000+i, 21000+i and 22000+i and runs one server at a time; `Fleet.tenant_config_xml` names exactly the fleet's servers as the cluster `bighouse`. |
| Complete | Work | 6C: Query API | `bighouse/api.py`: `POST /query?replicas=N` takes blank servers from the pool (503 when too few slots are free, a wait when their servers are still booting), mints a read key, assigns the servers, attaches the writer's tables, runs the query with `enable_parallel_replicas`, returns the answer, and has the pool replace the servers. `X-BigHouse-Timings` reports the wait, assignment, attach, query and total. |
| Complete | Decision | Query API language | Python with the standard library only. |
| Complete | Doc | 6D: Walkthrough and production guide | `demo/README.md`: starting the writer, loading data, the query API, a query and its headers, what happens per query, `demo/e2e.py`, and stopping. `PRODUCTION.md`: each part's production counterpart, keys and their TTLs, a query's steps as a durable workflow, fleet hosts and S3. |
| Complete | Test | Concurrent queries | `demo/e2e.py`: two queries at once take separate slots and both answer. |
| In Progress | Gate | `demo/e2e.py` matches the writer at 1, 2 and 4 servers | Locally 27 of 27 checks pass, at 1, 2, 4 and 8 servers on the warm pool (5H), and each server of a fleet of 4 reads a share of the marks (727, 566, 576 and 576). Missing: a CI run. |
