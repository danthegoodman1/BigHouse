# BigHouse Development Plan

## Overarching Goal

BigHouse runs ClickHouse like BigQuery. One persistent writer ingests into `MergeTree` tables stored on S3, and each query summons a fleet of short-lived ClickHouse servers that read the same tables as parallel replicas, answer, and exit. The foundation is a new object-storage `metadata_type`, `inodedb`, that keeps the disk's file tree in [InodeDB](inodedb) and the blobs in S3.

The `inodedb` metadata type matches an S3 disk with `local` metadata feature for feature. Every `MergeTree` workload that runs on `local` metadata runs on `inodedb` unchanged: hardlinks, mutations, lightweight deletes and updates, `FREEZE`, partition moves and copies, TTL moves, backups, appends, truncation, the `cache` and `encrypted` wrappers, and `ReplicatedMergeTree`. InodeDB has a real rename and real hardlinks, so the disk implements each operation directly, where `plain_rewritable` copies objects or refuses.

A disk has one exclusive writer, or several shared writers that insert and merge at once and coordinate through files in InodeDB that each commit checks in the same batch as its part. An exclusive writer runs DDL, mutations and partition commands, and fences every shared writer. The demo runs on one machine with s3proxy standing in for S3.

## Layout

| Path | Contents |
| --- | --- |
| `ClickHouse/` | Submodule: branch `bighouse` of `danthegoodman1/ClickHouse`, cut from ClickHouse `v26.10.2.103-stable`, shallow. `ClickHouse/LICENSE-BIGHOUSE` reserves all rights to BigHouse's code there. |
| `ClickHouse/src/Disks/DiskObjectStorage/MetadataStorages/InodeDB/` | The `inodedb` metadata type: gRPC client, value format, cache, storage and transaction. |
| `inodedb/` | Submodule: InodeDB. The client's `proto/inodedb.proto` copies its API as of `dc97a2c`; the pinned `be669f1` adds `ListFreed`, which the disk leaves uncalled. |
| `scripts/` | `clickhouse-build`, `inodedb-build`, `dev-up`, `dev-down`. |
| `bighouse/` | `stack.py` (keys, disk configs, servers), `catalog.py` (the writer's databases in InodeDB), `fleet.py` (slots, the pool of blank servers, and fleets), `api.py` (the query API). |
| `tests/` | `features.py`, `crash.py`, `claims.py`, `fence.py`, `replicas.py`, `discovery.py`, `parity.py`, `bench.py`, `shared.py`. |
| `demo/` | `up`, `down`, `load`, `bench`, `e2e.py`. |
| `docs/images/` | The README's charts: `charts.html` holds their data, and `render.sh` renders them. |

## Implementation Principles

- **Parity with `local` is the bar.** Implement every method `MetadataStorageFromDisk` and its transaction implement (`ClickHouse/src/Disks/DiskObjectStorage/MetadataStorages/Local/MetadataStorageFromDisk.h`), with the same capability flags: `supportsEmptyFilesWithoutBlobs`, `areBlobPathsRandom`, `supportWritingWithAppend`, `supportsChmod` and `supportsStat`. A differential run against `local` decides parity, and a test that fails only on `inodedb` is a bug.
- **Keep ClickHouse changes contained.** New code lives in `ClickHouse/src/Disks/DiskObjectStorage/MetadataStorages/InodeDB/`. The ClickHouse files changes table below names every change outside that directory and its reason, so rebasing onto a new ClickHouse release stays small.
- **BigHouse's code stays unlicensed.** `ClickHouse/LICENSE-BIGHOUSE` grants no license for BigHouse's directory in the fork or for its changes to ClickHouse files, and Apache 2.0 covers ClickHouse's own code. A new BigHouse directory gets added to that file's list, and each modified ClickHouse file gets a short `BigHouse:` notice at the change, as Apache 2.0 §4(b) requires.
- **InodeDB owns tree semantics.** The disk maps ClickHouse files onto inodes and values. When InodeDB lacks a capability, build it in `inodedb/` under that project's plan and tests.
- **One fenced batch per transaction.** Each metadata transaction commits as one `all_or_nothing` InodeDB batch carrying the writer's owner fence.
- **Blobs outlive every reader.** A blob leaves S3 only after the commit that frees it, and only once no reader lease can reach it.
- **Everything runs locally.** s3proxy serves S3, an InodeDB server holds metadata, and ClickHouse comes from the submodule. Tests and the demo use the same stack.

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
| `src/Storages/MergeTree/IDataPartStorage.h`, `src/Storages/MergeTree/DataPartStorageOnDiskFull.{h,cpp}` | `commitsNewPartWithRename`, true for a full part's storage on an `inodedb` disk. | Asks `MergeTreeSink` to commit a part's files with its rename. |
| `src/Storages/MergeTree/MergeTreeSink.cpp` | `finishDelayedChunk` leaves the part's transaction open when its storage asks, so `commitPart` commits the files with the rename, and undoes the transaction of a part that is not added. | One InodeDB commit per insert, not two. |
| `src/IO/ReadBufferFromFileView.{h,cpp}` | `resizeWorkingBuffer` starts the buffer at the file's first byte. | A file read from an inline `data.packed` counts its own bytes, which `CHECK TABLE` compares with `checksums.txt`. |
| `src/Backups/BackupEntryWithChecksumCalculation.cpp` | A non-empty file without blobs takes its checksum from its bytes. | Backups of inline files. |
| `src/Common/ProfileEvents.cpp` | `InodeDB*` events, among them shared writers' `InodeDBCommitConflicts`, `InodeDBCommitRetries`, `InodeDBClaimsTaken` and `InodeDBClaimsLost`. | Call counts in `system.events`. |
| `src/Common/FailPoint.cpp` | `inodedb_*` failpoints: pauses before and after an insert's commit, before its tokens' and before a merge's commit, and between a listing's pages; a stale view of the parts; and switches that remove a shared writer's protections. | `tests/shared.py` stops writers at exact points and shows each protection's test failing without it. |
| `src/Disks/DiskCommitTransactionOptions.h` | `InodeDBCommitOptions` and `InodeDBCommitOutcome` join the commit variants. | MergeTree adds coordination ops and predicates to a part's batch and learns why one failed. |
| `src/Storages/MergeTree/IDataPartStorage.h`, `src/Storages/MergeTree/DataPartStorageOnDiskBase.{h,cpp}` | `commitTransactionWith` commits a part's open transaction with those options; a rename of a part onto its own path does nothing. | A shared writer commits a part before it enters the table, so the later rename is a no-op. |
| `src/Storages/MergeTree/MergeTreeData.{h,cpp}` | Hooks `isSharedWriter`, `assertExclusiveWriter`, `beginSharedRefresh` and `endSharedRefresh`; `refreshDataPartsOnce` runs for shared writers, counts as vanished only parts held before the listing, skips parts the writer's own commits added meanwhile, and starts over after half of `old_parts_lifetime`; `inodedb_shared_skip_refresh` holds a shared writer's view; partition commands, `RENAME` and `DROP` ask `assertExclusiveWriter`. | Shared writers learn other writers' parts; they refuse writes only an exclusive writer makes. |
| `src/Storages/StorageMergeTree.{h,cpp}` | The table's `InodeDBTableCoordinator`: the counter's reconcile at load, tokens in place of `MergeTreeDeduplicationLog`, the definition's version at startup, claims at merge selection, the claims' renewal task, the merged part's commit, refusals, no mutations or startup cleanup in shared mode, a refresh before `OPTIMIZE`, and directory removal only for parts this writer merged away. | Phase 7. |
| `src/Storages/MergeTree/MergeTreeSink.{h,cpp}` | `commitPartOnInodeDB`: tokens in the part's batch in both modes, and for a shared writer the block number counter and the definition's version, retried on a counter conflict; failpoints that reserve the number first or commit the tokens after the part. | Block numbers and deduplication tokens in the part's batch. |
| `src/Storages/MergeTree/MergePlainMergeTreeTask.cpp` | A shared writer's merge takes a temporary directory named with its writer id and commits through `commitSharedMergedPart`. | Merge claims. |
| `src/Storages/MergeTree/MergeTreeCleanupThread.cpp` | A shared writer expires tokens and sweeps claims in place of clearing mutations, empty parts and patch parts. | Cleanup in shared mode. |
| `src/Storages/MergeTree/Compaction/MergePredicates/MergeTreeMergePredicate.cpp` | `canUsePartInMerges` asks `StorageMergeTree::sharedWriterCannotMerge`. | A merge takes only parts the last refresh saw complete and that no other writer claimed. |
| `src/Disks/tests/gtest_metadata_local_disk.cpp` | Runs every case against `local` and `inodedb`, plus `inodedb`-only inline, spill and replica cases. | Unit parity. |
| `src/Disks/tests/gtest_inodedb_client.cpp` | The client's standby redirect and retried commit, through a fault-injecting proxy. | Client behavior on failover and lost answers. |

## Testing Strategy

- **Unit.** `ClickHouse/src/Disks/tests/gtest_metadata_local_disk.cpp` runs every case against both metadata types, and `gtest_inodedb_client.cpp` tests the client's redirect and retry; the InodeDB cases need `INODEDB_TEST_ENDPOINT` and `INODEDB_TEST_KEY`.
- **Parity.** `tests/parity.py` runs ClickHouse's stateless tests twice over s3proxy with every MergeTree table on an S3 disk, once with `local` metadata and once with `inodedb`, from one install of the test configs (`install.sh --s3-storage`). It runs the tests tagged `no-object-storage` too, since an S3 disk with `local` metadata passes them. Each `inodedb` failure reruns on `local`, then up to three times on `inodedb`, to separate regressions from flaky tests; a test that fails all three reruns once more, rewritten to observe InodeDB's tree through `idb mount` where it observes local metadata files. `inodedb` must pass every test `local` passes, and the script exits 1 otherwise.
- **Features.** `tests/features.py` runs each feature scenario on both metadata types in one server and requires equal answers, then checks restart and blob cleanup.
- **Crash.** `tests/crash.py` kills the writer with `SIGKILL` under a mixed workload, restarts it, and checks `CHECK TABLE`, acknowledged batches, partial batches and blobs that records name.
- **Replicas.** `tests/replicas.py`: a writer and a replica; new parts appear, drops vanish, and queries outlive the writer's cleanup.
- **Shared writers.** `tests/shared.py`: one scenario per Phase 7 gate, a kill loop, and one per hazard, which runs once with its protection and once with a failpoint that removes it, where the hazard must appear.
- **End to end.** `demo/e2e.py`: the query API returns the writer's answers at fleet sizes 1, 2, 4 and 8, and replaces each fleet's servers with blank ones.
- **Benchmarks.** `tests/bench.py` against `local` metadata, with InodeDB's store on the drive and on tmpfs (`--tmpfs`), and `demo/bench` for the query API at each fleet size, recorded in `BENCHMARKS.md`.
- **CI.** GitHub Actions will download prebuilt ClickHouse binaries for the pinned fork commit, run s3proxy and InodeDB, and run the suites, with the full stateless parity run on demand. A hosted runner takes hours to build ClickHouse, so CI waits for those binaries.

## Phase 0: Foundations

Goal:
A clean branch that builds ClickHouse from the submodule, runs InodeDB and s3proxy locally, and reproduces the baseline: `MergeTree` on an S3 disk with `local` metadata over s3proxy.

Scope:
- The `rebuild` branch, holding this plan and the ClickHouse submodule.
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
| Complete | Work | 0C: ClickHouse fork with a `bighouse` branch | `danthegoodman1/ClickHouse`, branch `bighouse`; `632ffeccc` adds `LICENSE-BIGHOUSE`, `38edf7c26` the metadata type. |
| Complete | Work | 0D: InodeDB pinned as a submodule | `inodedb/` at `be669f1` of `github.com/danthegoodman1/inodedb`, which deletes expired keys and lists the free queue (`ListFreed`). The C++ client's copy of `inodedb.proto` is from `dc97a2c`; the two differ only by `ListFreed`, which the disk leaves uncalled. The dev store written by `dc97a2c` loads under `be669f1`, and `idb fsck` finds it sound. |
| Complete | Work | 0E: `scripts/clickhouse-build`, `scripts/inodedb-build` | Builds `build/programs/clickhouse` (RelWithDebInfo, clang, Ninja, ccache; log in `build/build.log`) and `inodedb/target/release/{inodedb,idb}`. |
| Complete | Work | 0F: `scripts/dev-up`, `scripts/dev-down` | s3proxy `4.1.1` on `127.0.0.1:11111` (bucket `test`, filesystem backend under `.dev/s3`) and InodeDB on `127.0.0.1:7070` (SQLite under `.dev/inodedb`). |
| Complete | Test | Baseline on `local` metadata | Every `tests/features.py` scenario runs on `s3_local` and matches `s3_inodedb`. |
| Incomplete | Work | 0G: CI workflow | A workflow that built ClickHouse on a hosted runner, removed until CI can download the binaries, passed `Build InodeDB` once it installed `protoc`, and stopped at ClickHouse's configure, which needs NASM. Missing: a workflow that downloads prebuilt ClickHouse binaries, installs `protoc`, starts the stack and runs the gtests, features, fence, replicas, discovery, crash, claims and e2e suites on pull requests, with parity on manual dispatch; and a green run. |
| Blocked | Risk | ClickHouse build time in CI | A cold build takes hours on a hosted runner, and a warm ccache still hashes every file and links two binaries on 4 cores. The binaries depend only on the fork commit: stripped and compressed with zstd, the server is 150 MB, so CI can download them per commit. Blocked by: a choice of where they are built, on a developer's machine when the submodule moves or by a workflow on the fork. |
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
| Complete | Test | Stateless parity run | `tests/parity.py` exits 0 on the build with one commit per insert and inline files on, fork commit 424959278 (`build/test_parity_file_view_fix.log`). Each pass ran 17,275 tests: `inodedb` 15,224 OK, 1,095 failed, 956 skipped; `local` 15,221 OK, 1,098 failed, 956 skipped. 1,090 tests fail on both. The five that fail only on `inodedb` read local metadata files (rows below); the eight that fail only on `local` pass on `inodedb`. `.dev/parity/diff.json`: no regressions. |
| Complete | Test | 2G feature checks | `tests/features.py`, with inline files on, passes all 13 scenarios with equal answers on both types: `insert_merge`, `mutations`, `partitions` (`ATTACH PARTITION FROM`, `REPLACE PARTITION`, `MOVE PARTITION TO TABLE`, detached parts), `freeze`, `ttl_projection_index`, `log_engines`, `tables`, `backup`, `cache_and_encryption`, `lightweight_update`, `backup_to_s3`, `replicated` (two `ReplicatedMergeTree` replicas over an embedded Keeper) and `move_between_disks`; then restart and blob cleanup. `database_disk` runs in `tests/parity.py --remote-database-disk`. |
| Complete | Test | `database_disk` on the `inodedb` disk | `tests/parity.py --remote-database-disk` exits 0 (`build/test_parity_dbdisk.log`). With database metadata on an `inodedb` disk, all 17,275 tests ran: 15,224 OK, 1,095 failed, 956 skipped. The `local` pass, with `plain_rewritable` database metadata, ended at 12,376 tests when `clickhouse-test` received `SIGTERM`; the recheck ran all 377 `inodedb` failures on it, and 372 failed there too. The other five are the tests that read local metadata files. |
| Complete | Decision | Zero-copy replication | Zero-copy ships each part's local metadata files to the other replica, and the receiver writes them to its local filesystem (`Fetcher::fetchSelectedPart`), so upstream turns it off for `keeper` and `memory` metadata. `DiskObjectStorage::supportZeroCopyReplication` turns it off for `inodedb` too, and replicated tables fetch whole parts; BigHouse's replicas share the writer's tree instead. With it on, the sender failed with `Remote metadata 'checksums.txt' is not exists` and the fetch retried forever; now `tests/features.py replicated` passes with `allow_remote_fs_zero_copy_replication = 1` on both types, and both disks remove every blob afterward. |
| Complete | Decision | Tests that read local metadata files | `02378_part_log_profile_events` sums `FileOpen`, which counts local file opens; `03100_lwu_59_freeze_detach_keep_invalidated_columns`, `04429_drop_index_sibling_suffix_collision`, `04538_packed_clone_hardlinks_data_packed` and `04758_part_minmax_block_columns_files_prefix` run `find`, `test -e`, `test -f` or `stat` on paths from `system.parts`, `system.tables` and `FREEZE`. The last four carry upstream's `no-object-storage` tag. `parity.py` (`rewrite_for_inodedb`) points those paths into an `idb mount` of the disk's tree and swaps `FileOpen` for `InodeDBCommit`; rewritten, all five pass against the original references: the clone shares `data.packed`'s inode, frozen and detached copies keep `invalidated_system_columns.txt`, the minmax files form a prefix, `DROP INDEX` keeps the survivor's files, and each mutation commits. |
| Complete | Decision | Files in an inline archive | A `Packed` part whose `data.packed` fits inline reads from one in-memory buffer holding the whole archive, and `ReadBufferFromFileView` counted from the archive's first byte, so `CHECK TABLE` found each file as long as its end offset: "Unexpected size of file count.txt (3 vs 3949)". The view now starts its buffer at the file. `05394_vertical_merge_packed_part` failed this way in the parity run whose `install.sh` drew `ZSTD(3)` as the default codec, which shrank its merged archive to 3,949 bytes; gtest `ReadBufferFromFileView.MemoryArchiveCountsTheFileAlone` counted 300 bytes for a 200-byte file without the fix. With the fix, the test passes under `ZSTD(3)` on `local`, on `inodedb` with inline files on and off, and on both types behind a cache. |
| Complete | Decision | `02725_memory-for-merges` is flaky on both types | It requires the summed peak memory of a table's merges to stay under 200 MiB. Six runs per disk of its workload ran two merges (one a single-part rewrite) three times of six on `local` and on `inodedb`, and two merges always pass the limit (204 to 226 MiB); one merge peaked at 116 to 135 MiB on both. The latest parity run failed it on `local` and passed it on `inodedb`. |
| Complete | Gate | No failure unique to `inodedb` | `tests/parity.py` reports 0 regressions and 5 tests that read local metadata files (`.dev/parity/diff.json`, `build/test_parity_file_view_fix.log`). |

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
| Complete | Work | 4B: One commit per part | `MergeTreeDataWriter` writes a part inside one disk transaction. On an `inodedb` disk `MergeTreeSink` leaves it open (`commitsNewPartWithRename`), the rename and the directory's modification time join it (`DataPartStorageOnDiskBase::rename`), and `MergeTreeData::Transaction::commit` commits all of it as one batch. A part that is not added, because it was deduplicated, covered or failed, undoes its transaction, which removes its blobs. `tests/features.py`: an insert makes one InodeDB commit, and a deduplicated insert leaves no blob; built without the undo, both that check and the final blob check fail. `tests/bench.py`: 1.0 InodeDB commits per insert, against 2.0 with a commit for the files and one for the rename. On the NVMe drive one thread went from 82 to 133 inserts a second, 8 threads from 145 to 193, and 10,000 parts from 87.8 s to 50.3 s. On tmpfs one thread went from 310 to 336, 8 threads from 2,199 to 1,450, and 10,000 parts from 4.6 s to 5.7 s: `MergeTreeSink::commitPart` commits under the table's parts lock, which now holds the part's whole batch, not just the rename. |
| Incomplete | Work | 4C: Orphan sweep | Blobs uploaded before a crash and never committed stay in S3, a part waiting for the table's parts lock among them (4B); `local` metadata leaves those of parts still being written, and has no sweep either. A safe sweep lists the disk's tree through a snapshot at version V and the free queue with `ListFreed` until `freed_through` reaches V, then deletes the blobs under the disk's prefix, uploaded before the writer's fence, that neither names. Missing: the sweep, and a crash test that compares S3's blobs with that set exactly. |
| Complete | Work | 4D: Inline small files | Files up to `inodedb_inline_file_max_bytes` (16 KiB) keep their bytes in the record. `writeInlineDataToFile` rewrites the file as `MetadataStorageFromMemory` does; an append moves inline bytes to a blob first; truncation cuts inline bytes; backups checksum inline files by their bytes. Gtest `TestInlineRewriteAppendTruncate`; `tests/features.py`, `tests/crash.py 100` and the parity run pass with inline files on. `tests/bench.py`: a 1,000-row part uploads no blob, and 10,000 parts load in 1.3 s against `local`'s 2.3 s. |
| Complete | Test | 4E: Benchmarks | `BENCHMARKS.md`, medians of 3 rounds with the disks in a rotating order. With InodeDB's store on this machine's NVMe drive (2 ms `fdatasync`), `inodedb` takes 131 durable inserts/s on one thread against `local`'s 325, which syncs nothing with `fsync_after_insert` and `fsync_part_directory` on or off (325 with them on), and against 35 for MergeTree on the drive with both on; 10,000 parts take 50.4 s against 7.0 s. With the store on tmpfs, `inodedb` matches `local`: 337 and 1,383 inserts/s on 1 and 8 threads against 325 and 1,386, and 5.6 s against 7.4 s for 10,000 parts. Cold start: 1.29 s against 2.30 s. |
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
| Complete | Work | 5H: Warm fleet servers | `fleet.Pool` boots a blank server in every slot: its boot config holds no `bighouse` disk, no cluster and no key, and names `bighouse` as MergeTree's default policy, because MergeTree reads its defaults only at boot and resolves the name only when a table attaches. `Fleet.assign` writes `config.d/tenant.xml` (the read-only disk under the query's key, its policy, and the cluster of the fleet's servers) and runs `SYSTEM RELOAD CONFIG`, which starts the disk and its lease, then attaches the databases. Once the query ends, the pool stops each server with SIGTERM, revokes the key, and boots a blank server in the slot, in the background. Each server serves one query: a reload cannot remove a disk or change its key, and a reattached server would read stale listings and definitions, since `IMetadataStorage::dropCache` is a no-op for `inodedb`. `demo/e2e.py`: a fleet's servers stop once its query ends, the pool then holds a fresh blank server in every slot, and a query that needs servers still booting waits for them. Each server's directory holds its pid, and booting a slot stops a server that a killed API left in it (`Server.stop_leftover`, which checks the process's command line through a pidfd before it signals); `demo/e2e.py` kills an API with SIGKILL and the next pool stops its servers. With the pool settled between queries (10 runs, p50), a query takes 95, 112, 126 and 139 ms at 1, 2, 4 and 8 servers (assign 13 to 17 ms, attach 22 to 47 ms, query 48 to 59 ms), against 300 to 420 ms with servers started per query; 4 servers are replaced 437 ms after the answer, so back to back on 8 slots, fleets of 4 average 306 ms. |
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
- 6D: `README.md`, `demo/README.md` and `PRODUCTION.md`.

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
| Complete | Doc | 6D: README, walkthrough and production guide | `README.md`: what BigHouse is, its features, performance with two charts (`docs/images/`), trade-offs, a comparison with `local` and `plain_rewritable` metadata, a quick start, configuration and development. `demo/README.md`: starting the writer, loading data, the query API, a query and its headers, what happens per query, `demo/e2e.py`, and stopping. `PRODUCTION.md`: each part's production counterpart, keys and their TTLs, a query's steps as a durable workflow, fleet hosts and S3. |
| Complete | Test | Concurrent queries | `demo/e2e.py`: two queries at once take separate slots and both answer. |
| In Progress | Gate | `demo/e2e.py` matches the writer at 1, 2 and 4 servers | Locally 28 of 28 checks pass on InodeDB `be669f1`, at 1, 2, 4 and 8 servers on the warm pool (5H), and each server of a fleet of 4 reads a share of the marks (599, 694, 576 and 576). `demo/bench`: 98, 115, 126 and 138 ms a query at 1, 2, 4 and 8 servers (`BENCHMARKS.md`). Missing: a CI run. |

## Phase 7: Shared Writers and Merge Leases

Goal:
Several ClickHouse writers write one `inodedb` disk at once, some inserting and some merging, and coordinate block numbers, deduplication and merges through files in InodeDB. State one writer keeps in memory moves into InodeDB, and each commit checks it in the same batch as the part. Read replicas and the query API work unchanged: part names, the tree's layout and part transitions stay stock, so a merged part appears first and its sources leave after `old_parts_lifetime`.

Scope:
- 7A: Writer modes. `inodedb_writer_mode` is `exclusive` (the default, today's behavior) or `shared`. `owner` holds the mode: an exclusive writer writes a new owner and fences every other writer; a shared writer joins a shared owner, or replaces an exclusive one under its version. Every commit requires the owner version the writer started with. Each writer takes a random id at startup, and a shared writer holds a reader lease, as replicas do.
- 7B: Commit options. `InodeDBCommitOptions` joins `TransactionCommitOptionsVariant` (`src/Disks/DiskCommitTransactionOptions.h`, after `MetaInKeeperCommitOptions`): ops and predicates that join the transaction's batch. `InodeDBCommitOutcome` joins `TransactionCommitOutcomeVariant` and tells apart committed, fenced, counter conflict, duplicate token, schema changed and claim lost. A failed outcome keeps the transaction's blobs and operations, so the caller can retry.
- 7C: Coordination layout. `coord/` beside `tree/` mirrors table paths: `coord/<table path>/block_number`, `coord/<table path>/tokens/<block id>` and `coord/<table path>/claims/<part name>`. Removing a directory under `tree/` also removes its mirror in the same batch (`RemoveAll` with `IfExists`), so `DROP TABLE` removes a table's files. A replica's listings, part loading and `idb` paths under `tree/` stay as they are.
- 7D: Block numbers. `StorageMergeTree::increment` becomes the table's counter file in shared mode. An insert's commit replaces the counter with the part's number under the counter's version. On a conflict the writer reads the counter again and retries; each attempt's batch moves the part's temporary directory to that attempt's name. The part enters the table's in-memory set only once InodeDB has committed it.
- 7E: Deduplication tokens. One file per block id, created with `Create` in the part's batch, in both modes, replacing `MergeTreeDeduplicationLog` on `inodedb` disks.
- 7F: Schema. A shared writer's inserts and merges require the version of the table's `.sql` file it loaded; a failed check throws a retryable error.
- 7G: Part-set refresh for shared writers.
- 7H: Merge claims.
- 7I: Cleanup rules in shared mode.
- 7J: Writes a shared writer refuses.
- 7K: ProfileEvents for commit conflicts, retries, and claims taken and lost.
- 7L: `tests/shared.py` and the benchmarks.
- 7M: `README.md`, `PRODUCTION.md` and the layout documentation for a control plane.

Out of scope:
- The supervisor that runs writers per tenant.
- Mutations, DDL, partition commands and lightweight updates and deletes in shared mode.
- One atomic commit for a source part and its materialized views' parts.
- The orphan sweep (4C). Shared writers add orphans: a writer killed mid-insert or mid-merge leaves the blobs it uploaded, as an exclusive writer does, and so does a merge whose claim another writer took over.

Completion gate:
Exclusive mode passes every earlier suite and keeps its benchmarks within noise. Shared writers pass the concurrency, exactly-once, merge, lease, mode-change, differential and kill-loop gates below, and `BENCHMARKS.md` records their insert rates and conflict rates.

Testing plan:
- The earlier suites, unchanged, for exclusive mode: `tests/parity.py`, `features.py`, `crash.py`, `fence.py`, `claims.py`, `replicas.py`, `discovery.py`, `demo/e2e.py`, `tests/bench.py`.
- `tests/shared.py`, one scenario per gate and hazard; each hazard's scenario runs once more with its protection turned off through a failpoint or a setting, where the hazard must appear.
- A kill loop in `tests/shared.py --kill-rounds N`.
- `tests/bench.py --shared` for 1, 2, 4 and 8 writers.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Decision | Coordination files live beside `tree/` | The brief allowed a directory inside the table directory, after `deduplication_logs/`, but a replica's attach lists the table directory, and one more entry there is one more name in every listing. Under `coord/`, mirrored by path, nothing under `tree/` changes. `DROP TABLE` reaches the disk as removals of the table's directory (`MergeTreeData::dropAllData`), which remove the mirror in the same batch. |
| Complete | Decision | Shared writers reconcile the counter at startup; exclusive writers leave it | A shared writer that starts a table sets the counter to at least the highest block number it loaded, its mutations' versions included (`StorageMergeTree::loadMutations` raises `increment` the same way), under the counter's version. It joined its epoch, which fences every exclusive writer, before it loaded the parts (`MetadataStorageFromInodeDB::startup` runs before tables load), so it sees every part an exclusive writer committed. Exclusive commits stay as they are, and no benchmark of them changes; the brief's alternative, an exclusive writer writing the counter with each part, buys nothing a shared writer's startup needs. |
| Complete | Decision | A merge's view is complete below the counter it read | A refresh reads the counter, C, then lists the table directory. Every part numbered at or below C committed with the counter at or before C's version, so the listing, which reflects at least its first page's version, holds each such part, or a part that covers it while the covered part's directory lasts `old_parts_lifetime`. A merge takes only parts whose highest block is at most C; a refresh that runs longer than half of `old_parts_lifetime` starts over. InodeDB pages a listing at 1,024 entries, and later pages can be newer (`inodedb/DESIGN.md`, API), which is the hazard below. |
| Complete | Decision | Claims record their result and never move once the result commits | A claim file holds the holder's writer id, an expiry and the result part's name, `held`; the merge's commit rewrites each claim, under its version, to `done`. A writer takes over only a `held` claim past its expiry. A `done` claim stays until its source part's directory is gone, and cleanup removes it after the directory, never before, so a writer whose view is stale finds either the claim or no source directory, and a merge's commit requires every source directory. |
| Complete | Decision | Exclusive writers keep the deduplication window by count | `non_replicated_deduplication_window` counts blocks, and the stateless suite tests the count. An exclusive writer keeps the window in memory, as `MergeTreeDeduplicationLog` does, loads it from the token files' creation versions at startup, and removes the tokens that fall out of the window in the batch that adds new ones. Shared writers expire tokens by age, `inodedb_deduplication_token_ttl_seconds`, and keep at most the window's count, oldest first, in their cleanup. Dropping a part removes its tokens, as `MergeTreeDeduplicationLog::dropPart` does. |
| Complete | Decision | Pending mutations in shared mode | A shared writer loads mutation entries and leaves them: it runs no mutation and removes no entry, and `MergeTreeData`'s merge selector already keeps parts that a pending mutation still has to change from merging with parts it has changed. An exclusive writer finishes them. New parts take numbers above every entry's version, so no mutation applies to them. |
| Complete | Decision | Temporary directories carry the writer id | `tmp_insert_` names come from the part name and `insert_increment`, which restarts at 0 in every process, so two writers name their first temporary parts alike. Inserts take the writer id through `MergeTreeData::getPostfixForTempInsertName`, and merges through the merge's suffix, which `MergeTask` already appends for `OPTIMIZE ... DRY RUN`. |
| Complete | Decision | Checks of a file the batch changes go on the changing op | InodeDB evaluates a batch's `when` predicates at every op, against the state earlier ops left, so a `VersionIs` on a claim the batch itself rewrites fails at each later op. The first shared merges failed this way: "ClaimLost at coord/store/fb4/.../claims/all_1_1_0 ... expected version 3246388, found `held f56abe92 ...` at version 3246388" (`refused`, 2026-11-10). Each op that replaces or removes a coordination file carries its version as its own `cond` (`InodeDBTableCoordinator`'s `atVersion`), and `when` keeps checks of files the batch leaves alone: the owner, the definition and source directories. |
| Complete | Decision | Each attempt's batch renames the part | Renaming the part in its open transaction before each attempt chained the renames, `tmp_insert_... → N1 → N2`, and a retry whose `N1` another writer had committed failed with `NOT_EMPTY`. Each attempt adds one `Rename` from the temporary directory to that attempt's name as an extra op (`addPartRename`). Once the batch commits, the part's storage takes the new path (`setRelativePath`), and `renameTempPartAndAdd` renames the part onto its own path, which `DataPartStorageOnDiskBase::rename` skips. |
| Complete | Decision | `OPTIMIZE` refreshes first on a shared writer | A merge takes parts at or below the counter the last refresh read, so `OPTIMIZE` right after the writer's own insert left that part out and ended with two parts. `StorageMergeTree::optimize` refreshes first. |
| Complete | Decision | The transaction guards definition files | Besides the statements `assertExclusiveWriter` names, a shared writer's transaction refuses any change to a `.sql` file or under `metadata/` and `metadata_dropped/`, other than creating directories and removing absent files, which startup does (`MetadataStorageFromInodeDBTransaction::commitWith`). `CREATE VIEW` and `CREATE DATABASE` reach the disk this way and fail with the same hint (`refused`). |
| Complete | Decision | Shared writers commit full parts | A packed part commits its files before its rename, outside the part's batch. A shared writer's insert of one throws `NOT_IMPLEMENTED` naming `min_bytes_for_full_part_storage`, which defaults to 0. |
| Complete | Decision | Startup cleanup stays off for shared writers | A part's temporary directory reaches InodeDB only in the batch that renames it: inserts and merges keep the part's transaction open until then. So a starting writer's cleanup finds no directory of another writer's running merge, and `startup_cleanup` passes with the cleanup forced on (`inodedb_shared_startup_cleanup`). The cleanup stays off because it also takes `delete_tmp_` directories, which another writer's part removal holds between its rename and its removal (`DataPartStorageOnDiskBase::remove`). |
| Complete | Decision | Deduplication needs a token | Two identical inserts without `insert_deduplication_token` both land in a `MergeTree` table with `non_replicated_deduplication_window = 100`, on the server's local disk and on `inodedb` alike (ClickHouse 26.10). Token files come only from inserts that carry a token. |
| Complete | Decision | A view's table deduplicates through its own tokens | In ClickHouse 26.10 a retried block that the source table deduplicates still reaches the source's views, whatever `deduplicate_blocks_in_dependent_materialized_views` says: after a kill between the source's commit and the view's, a retry with the setting at 0 still left 1,000 rows in each table. `view_retry` runs with the view table's tokens and without them. |
| Complete | Decision | Exclusive tables without deduplication keep nothing in InodeDB | With `non_replicated_deduplication_window` at 0, an exclusive writer loads no tokens and makes no coordination call, so its loads and inserts make the same InodeDB calls as before this phase (fork `bc86305ad`). |
| Complete | Decision | Coordination files follow removals | Removing a directory removes its mirror under `coord/`; renaming one moves nothing there. Atomic databases keep a table's data path through `RENAME TABLE`, which shared writers refuse. An exclusive writer that renames a table in an `Ordinary` database leaves its counter and tokens at the old path: the table starts with no tokens, and shared writers reconcile the counter at startup. |
| Complete | Decision | Merged-away sources wait for their merging writer | A shared writer removes the directories of parts it merged away, which it remembers in memory, and forgets other outdated parts. The sources of a merge whose writer died before `old_parts_lifetime` passed stay on the disk, outdated, until an exclusive writer loads the table and removes them (`PRODUCTION.md`, "Shared writers"). |
| Complete | Work | 7A: Writer modes | `inodedb_writer_mode` in `registerMetadataStorageFromInodeDB.cpp`; any other value, or `shared` with `inodedb_read_only`, is `INVALID_CONFIG_PARAMETER`. `owner` holds `exclusive {id}` or `shared {id}`: an exclusive writer replaces it, and a shared writer joins a shared owner at its version or replaces any other under its version (`MetadataStorageFromInodeDB::joinSharedOwner`); every batch requires that version. The id is 8 random hex digits per start, and a shared writer holds a reader lease. `modes`: 7 of 7, among them an exclusive writer fencing three shared ones with `TABLE_IS_READ_ONLY` and a revoked key fencing one shared writer while the others commit. |
| Complete | Work | 7B: Commit options and outcomes | `InodeDBCommitOptions` and `InodeDBCommitOutcome` in `DiskCommitTransactionOptions.h`. `MetadataStorageFromInodeDBTransaction::tryCommit` appends their ops and predicates to the transaction's batch and maps a skipped op or predicate to its outcome; a failed outcome keeps the transaction, so the next attempt commits the same files, and removes only the blobs that attempt spilled. Each outcome is tested through a server, as the fork's guidance prefers to gtests: counter conflict (`concurrent`, 682 retries), duplicate token (`exactly_once`, `lost_tokens`), schema changed (`definition`), claim lost (`leases`), fenced (`modes`). |
| Complete | Work | 7C: Coordination layout | `IInodeDBCoordination::coordPath`; the batch builder adds `RemoveAll` of `coord/{path}` with `IfExists` to each `RemoveDirectory` and `RemoveRecursive`. `drop`: a table's `block_number`, 3 tokens and 3 claims live under `coord/`, and `DROP TABLE ... SYNC` on an exclusive writer leaves nothing there (`build/test_shared_drop.log`). |
| Complete | Work | 7D: Block numbers | `InodeDBTableCoordinator::reconcileCounter`, `addCounterUpdate` and `addPartRename`; `MergeTreeSink::commitPartOnInodeDB` retries on `CounterConflict` and on a part directory already at its name. `concurrent`: 4 writers insert 40 times each, 682 commits retry with a new number, and no two inserted parts share a block number; `mode_changes` keeps them unique across shared, exclusive and shared again. |
| In Progress | Work | 7E: Deduplication tokens | `InodeDBTableCoordinator::addTokens` creates `coord/{table path}/tokens/{block id}` with `Create` in the part's batch in both modes. An exclusive writer keeps the `non_replicated_deduplication_window` newest tokens and removes those that leave the window in the same batch; shared writers expire tokens by age (`expireTokens`). A token another writer holds makes the batch apply nothing, and the insert reports the block as a duplicate (`duplicatesOf`). `exactly_once` 9 of 9, `lost_tokens` 2 of 2, `view_retry` 2 of 2, `drop` 2 of 2. Needs: parity's deduplication tests on fork `5c449f16e`. |
| Complete | Work | 7F: Schema condition | `setDefinition` reads the version of the table's `.sql` file at startup, and `addDefinitionCheck` adds `VersionIs` to every insert's and merge's batch; a failed check throws `ABORTED`. `definition`: 3 of 3. The test rewrites the definition in InodeDB under an admin key, as a control plane would, since an `ALTER` reaches shared writers only through an exclusive writer, which fences them first: the next insert commits nothing and leaves no rows, and the restarted writer inserts. |
| Complete | Work | 7G: Part-set refresh | `refreshDataPartsOnce` runs on shared writers every `refresh_parts_interval`, 1 s when unset. It reads the counter, lists, counts as vanished only parts held before the listing, skips parts the writer's own commits added meanwhile, and starts over after half of `old_parts_lifetime`; each refresh drops the disk cache's listings (`MetadataStorageFromInodeDB::refresh`), and every decision a stale cache entry could mislead is checked again in the batch. `modes` (a writer sees the others' parts), `concurrent` (each of 4 writers reads all 160,000 rows), `part_counts`. |
| Complete | Work | 7H: Merge claims | `InodeDBTableCoordinator::claim` at selection, `renewClaims` every third of `inodedb_claim_ttl_ms`, and `addMergeCommit` in `StorageMergeTree::commitSharedMergedPart`; an expired `held` claim is taken over under its version. `leases` 11 of 11, `merges` 7 of 7 (1,300 parts, two inserting and two merging writers), `overlap` 8 of 8. The layout and its uses for a control plane: `PRODUCTION.md`, "Shared writers". |
| Complete | Work | 7I: Cleanup rules | `clearOldPartsFromFilesystem`: a shared writer removes the directories of parts it merged away and then their claims, and forgets other outdated parts. Startup's `clearOldTemporaryDirectories` stays off (decision above), and later passes find no running merge's directory. `clearOldMutations`, `clearEmptyParts` and `clearUnusedPatchParts` run on exclusive writers; shared writers run `expireTokens` and `sweepClaims`, which removes other writers' `done` claims whose part is gone and `held` claims long past expiry. Each removal names a version or `IfExists`, so writers cleaning at once remove each file once. `merges`, `leases` and `kill_loop` end with a fresh exclusive writer's `CHECK TABLE` passing. |
| Complete | Work | 7J: Refused writes | `StorageMergeTree::assertExclusiveWriter` and the definition guard. `refused`: 12 statements fail naming exclusive mode (`ALTER ... ADD COLUMN`, `ALTER ... DELETE`, `ALTER ... UPDATE`, `DELETE FROM`, `DROP PARTITION`, `DETACH PARTITION`, `TRUNCATE`, `CREATE TABLE`, `CREATE VIEW`, `DROP TABLE`, `RENAME TABLE`, `CREATE DATABASE`), and the writer then inserts and runs `OPTIMIZE`. |
| Complete | Work | 7K: ProfileEvents | `InodeDBCommitConflicts`, `InodeDBCommitRetries`, `InodeDBClaimsTaken` and `InodeDBClaimsLost` in `src/Common/ProfileEvents.cpp`. `concurrent` reports retries, `merges` claims taken, `leases` claims lost, and `tests/bench.py --shared` commits and retries per insert. |
| Complete | Work | 7L: Tests and benchmarks | `tests/shared.py`, 18 scenarios; `tests/bench.py --shared`. |
| Complete | Risk | A reserved block number | Protection: 7D gives a number only in its part's batch. `reserved_number`: with it, the other writer merges blocks 1 and 2 while the late insert waits, and the late part commits as block 3. With `inodedb_shared_reserve_block_number`, the late part takes block 2 first, lands inside the other writer's `all_1_3_1`, and a fresh exclusive writer reads 200 of 300 rows. Patch parts take a number before they write (`StorageMergeTree.cpp:1590`) and store it in their data, so lightweight updates stay exclusive. |
| Complete | Risk | A listing that is no snapshot | Protection: the decision above. `listing`: 1,098 parts placed so the first page of 1,024 names ends at `2_10_10_0`; the merging writer lists one page per request (`inodedb_list_in_pages`) and pauses after the first, while another writer commits `2_1099_1099_0`, which sorts before that page's end, and `2_1100_1100_0`, after it. Limited to the counter, `OPTIMIZE ... PARTITION 2 FINAL` merges nothing and partition 2 keeps 3 rows; with `inodedb_shared_merge_past_refresh`, it merges blocks 10 and 1100 over the unseen 1099, and 2 rows remain. |
| Complete | Risk | Overlapping merges | Protection: claims outlive the merge. `overlap`: writer `b`'s view holds blocks 1 and 2 (`inodedb_shared_skip_refresh`) while `a` merges blocks 2 and 3 into `all_2_3_1`. `b`'s `OPTIMIZE` finds `a`'s `done` claim and merges nothing, and a fresh exclusive writer passes `CHECK TABLE`; with `inodedb_shared_release_claims_at_commit`, `b` commits `all_1_2_1`, which intersects `all_2_3_1`. |
| Complete | Risk | Startup cleanup | `startup_cleanup`: a writer detaches and attaches the table, running its startup, while another's merge waits before its commit; the merge commits with the cleanup off and with it forced on, 10 of 10 (decision above). |
| Complete | Risk | Lost deduplication tokens | Protection: 7E. `lost_tokens`: a writer killed after its part's commit; with tokens in the part's batch, the retry on another writer adds nothing (1,000 rows). With `inodedb_tokens_after_part` and the kill between the part's batch and the tokens', the retry commits the rows again (2,000), as the libch session observed. `exactly_once` covers a fence and a revoked key. |
| Complete | Risk | Source and view parts commit separately | `exactly_once`: a writer killed between the source's commit and the view's; the retry fills in the view once. `view_retry`: with tokens in the view's table, retries after a kill between the commits and after both leave 2,000 rows in each table; with the view table's window at 0, the retry after both doubles the view's rows (3,000). |
| Complete | Risk | In-memory part counts | Protection: shared writers refresh every `refresh_parts_interval`, 1 s unless set. `part_counts`: `a` commits 15 parts, and `b` refuses its 6th insert with "Too many parts (20 ...)" against `parts_to_throw_insert = 20`; with refreshes held (`inodedb_shared_skip_refresh`), `b` commits 10, and the table holds 25 parts. |
| Incomplete | Risk | A packed merged part on a shared writer | With `min_bytes_for_full_part_storage` above 0, a merge whose result is packed has no open transaction at its commit, and `commitTransactionWith` throws `LOGICAL_ERROR`. Needs: the `NOT_IMPLEMENTED` error a packed insert throws. |
| In Progress | Gate | Exclusive mode unchanged | On fork `5c449f16e`: features 18 of 18, fence 4 of 4, claims 3 of 3, replicas 10 of 10, discovery 21 of 21, crash 20 rounds, `demo/e2e.py` 28 of 28 (`build/test_*_p7.log`). Needs: parity, the gtests (`Metadata/*`, `InodeDBClientTest.*`, `ReadBufferFromFileView*`), and `tests/bench.py` on the drive and on tmpfs within noise of `BENCHMARKS.md`. |
| Complete | Gate | Concurrent inserts | `concurrent`: 4 shared writers insert 40 blocks of 1,000 rows each into a table with a materialized view and a projection. Every insert commits, a replica's counts stay whole multiples of 1,000 and never go back, every writer and the replica read 160,000 rows, the view's table and the projection count each row once, block numbers stay unique, and a fresh exclusive writer passes `CHECK TABLE`: 16 of 16. |
| Complete | Gate | Exactly once | `exactly_once`: a writer killed before its commit, between the source's and the view's commits, and after its insert returned; one whose key is revoked mid-insert; one fenced mid-insert by an exclusive writer. Each retry leaves each row once in the table and the view's table: 9 of 9. |
| Complete | Gate | Merges | `merges`: two writers insert 1,300 parts while two others merge; the merging writers merge and claim, no two active parts intersect on either, and a fresh exclusive writer reads every row once, logs no intersecting parts and passes `CHECK TABLE`: 7 of 7. |
| Complete | Gate | Leases | `leases`: two writers race to merge the same parts and one does; a killed holder's claims expire and another writer merges the parts with every row; a `SIGSTOP`ped holder's claims are taken over, and once resumed its merge commits nothing visible and counts its lost claims: 11 of 11. |
| Complete | Gate | Mode changes | `mode_changes`: three shared writers insert under load while an exclusive writer starts, takes the disk, and stops, and the shared writers restart: every acknowledged insert lands once, and block numbers stay unique: 6 of 6. |
| Complete | Gate | Differential | `differential`: one workload through one exclusive writer and through three shared writers gives equal answers for `MergeTree`, `ReplacingMergeTree`, `SummingMergeTree`, `AggregatingMergeTree`, `CollapsingMergeTree` and `VersionedCollapsingMergeTree`: 6 of 6. |
| Complete | Gate | Kill loop | `kill_loop`: 30 `SIGKILL`s of random writers among three, under four inserting threads, with 13,323 inserts acknowledged; a fresh exclusive writer reads each once and passes `CHECK TABLE`. |
| In Progress | Test | Benchmarks | `tests/bench.py --shared` runs 1, 2, 4 and 8 shared writers on one table and on a table each, and reports inserts per second, commits per insert and retries per insert. Needs: full runs on the drive and on tmpfs (`.dev/bench/results-shared.json`, `.dev/bench/results-shared-tmpfs.json`). |
| In Progress | Doc | 7M: Docs | `README.md`: the opening, the fence paragraph, "Shared writers", the trade-offs, the comparison table and the settings; `PRODUCTION.md`: the writers row, per-writer keys and "Shared writers" with the `coord/` layout and how a control plane lists, reserves and revokes claims; Needs: `BENCHMARKS.md`, "Shared writers", and measured insert rates for one table and a table each in the README's trade-offs. |
