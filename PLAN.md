# BigHouse Development Plan

## Overarching Goal

BigHouse runs ClickHouse like BigQuery. One persistent writer ingests into `MergeTree` tables stored on S3, and each query summons a fleet of short-lived ClickHouse servers that read the same tables as parallel replicas, answer, and exit. The foundation is a new object-storage `metadata_type`, `inodedb`, that keeps the disk's file tree in [InodeDB](../inodedb) and the blobs in S3, as [Custom Metadata for Object-Storage Disks](Custom%20Metadata%20for%20Object-Storage%20Disks.md) describes.

The `inodedb` metadata type matches an S3 disk with `local` metadata feature for feature. Every `MergeTree` workload that runs on `local` metadata runs on `inodedb` unchanged: hardlinks, mutations, lightweight deletes and updates, `FREEZE`, partition moves and copies, TTL moves, backups, appends, truncation, the `cache` and `encrypted` wrappers, and `ReplicatedMergeTree`. InodeDB has a real rename and real hardlinks, so the disk implements each operation directly, where `plain_rewritable` copies objects or refuses.

Each table has one writer. The demo runs on one machine with s3proxy standing in for S3.

## Layout

| Path | Contents |
| --- | --- |
| `ClickHouse/` | Submodule: ClickHouse `v26.10.2.103-stable`, shallow. BigHouse's ClickHouse changes go on a `bighouse` branch. |
| `Custom Metadata for Object-Storage Disks.md` | The design. |
| `PLAN.md` | This plan. |

Later phases add `inodedb/` (pinned InodeDB), `scripts/`, `tests/` and `demo/`.

## Implementation Principles

- **Parity with `local` is the bar.** Implement every method `MetadataStorageFromDisk` and its transaction implement (`ClickHouse/src/Disks/DiskObjectStorage/MetadataStorages/Local/MetadataStorageFromDisk.h`), with the same capability flags: `supportsEmptyFilesWithoutBlobs`, `areBlobPathsRandom`, `supportWritingWithAppend`, `supportsChmod` and `supportsStat`. A differential run against `local` decides parity, and a test that fails only on `inodedb` is a bug.
- **Keep ClickHouse changes contained.** New code lives in `ClickHouse/src/Disks/DiskObjectStorage/MetadataStorages/InodeDB/`. Every change outside that directory gets a ledger row naming the file and the reason, so rebasing onto a new ClickHouse release stays small.
- **InodeDB owns tree semantics.** The disk maps ClickHouse files onto inodes and values. When InodeDB lacks a capability, build it in `../inodedb` under that project's plan and tests.
- **One fenced batch per transaction.** Each metadata transaction commits as one `all_or_nothing` InodeDB batch carrying the writer's owner fence. Correctness lands first; staging, caching and inline files follow the parity gate.
- **Blobs outlive every reader.** A blob leaves S3 only after the commit that frees it, and only once no reader lease can reach it.
- **Everything runs locally.** s3proxy serves S3, an InodeDB server holds metadata, and ClickHouse comes from the submodule. Tests, CI and the demo use the same stack.

## Testing Strategy

- **Unit.** `ClickHouse/src/Disks/tests/gtest_metadata_inodedb_disk.cpp` runs the cases of `gtest_metadata_local_disk.cpp` against both metadata types, with InodeDB on its memory backend.
- **Parity.** ClickHouse's stateless tests in `--s3-storage` mode (`ClickHouse/tests/config/install.sh`), run once with `local` metadata and once with `inodedb`, both over s3proxy. `inodedb` must pass every test `local` passes; each exception gets a ledger row with its cause.
- **Features.** A scripted check per feature in the goal, run on both metadata types.
- **Crash.** `kill -9` the writer mid-insert, mid-merge, mid-mutation and mid-removal, restart it, and check `CHECK TABLE`, row counts, leftover `tmp_*` directories, and that no record names a deleted blob.
- **Replicas.** A writer and replicas: new parts appear and dropped parts vanish within one refresh, and queries outlive the writer's cleanup.
- **End to end.** The demo's query API returns the writer's answers at fleet sizes 1, 2 and 4.
- **Benchmarks.** Part commit rate and cold start against `local` metadata, recorded in `BENCHMARKS.md`.
- **CI.** GitHub Actions runs s3proxy and InodeDB as services, builds ClickHouse with a restored ccache, and runs each phase's tests as they land. The full stateless parity run runs on demand.

## Phase 0: Foundations

Goal:
A clean branch that builds ClickHouse from the submodule, runs InodeDB and s3proxy locally, and reproduces the baseline: `MergeTree` on an S3 disk with `local` metadata over s3proxy.

Scope:
- The `rebuild` branch, holding the design, this plan and the ClickHouse submodule.
- A ClickHouse fork, `danthegoodman1/ClickHouse`, with a `bighouse` branch cut from the pinned tag; the submodule points there.
- InodeDB pinned as an `inodedb/` submodule, so CI, the bridge and the demo build one revision.
- `scripts/clickhouse-build`: initializes ClickHouse's contrib submodules (`contrib/update-submodules.sh`), configures with clang-21, Ninja and ccache, and builds Debug and RelWithDebInfo into a git-ignored `build/`.
- `scripts/dev-up` and `scripts/dev-down`: s3proxy on `localhost:11111` with bucket `test` and credentials `clickhouse`/`clickhouse`, the endpoint ClickHouse's own test configs expect, and an InodeDB server on `localhost:7070`.
- A baseline smoke test on `local` metadata.
- A CI workflow that runs the baseline.

Out of scope:
- The `inodedb` metadata type, which Phase 2 builds.

Completion gate:
`scripts/clickhouse-build` produces `build/programs/clickhouse` from the pinned submodule, and with `scripts/dev-up` running, a `MergeTree` table on an S3 disk with `local` metadata over s3proxy takes inserts, merges and selects, locally and in CI.

Testing plan:
- `tests/smoke/local_metadata.sh`: create, insert, `OPTIMIZE FINAL`, select, drop; assert counts and that s3proxy holds the blobs.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| In Progress | Work | 0A: `rebuild` branch with the old project removed | Branch `rebuild` from `f89c479`; every tracked file staged for deletion. Needs: commit. |
| In Progress | Work | 0B: ClickHouse submodule at `v26.10.2.103-stable` | `.gitmodules` (`shallow = true`); `git submodule status` reports `c6428ebcc39f`, staged. Needs: commit. |
| Incomplete | Doc | Design doc line references | The design doc's line numbers come from an older tree; in the pinned tag `refreshDataPartsOnce` sits at `MergeTreeData.cpp:3535`, not 3206. Needs: references updated to the pinned tag. |
| Incomplete | Work | 0C: ClickHouse fork with a `bighouse` branch | Needs: approval to create `danthegoodman1/ClickHouse`, then the submodule URL and branch updated. |
| Incomplete | Work | 0D: InodeDB pinned as a submodule | Missing: `inodedb/` submodule at a commit of `github.com/danthegoodman1/inodedb`. |
| Incomplete | Work | 0E: `scripts/clickhouse-build` | Missing: script and a build log. |
| Incomplete | Work | 0F: `scripts/dev-up`, `scripts/dev-down` | Missing: scripts starting s3proxy and InodeDB on fixed ports. |
| Incomplete | Test | Baseline smoke on `local` metadata | Missing: `tests/smoke/local_metadata.sh` and a passing run. |
| Incomplete | Work | 0G: CI workflow | Missing: `.github/workflows/ci.yml` and a green run. |
| Incomplete | Risk | ClickHouse build time in CI | A cold build takes hours on a hosted runner. Needs: a measured cached build, or a self-hosted or larger runner. |
| Incomplete | Gate | Baseline runs locally and in CI | Missing: local run output and a CI run link. |

## Phase 1: InodeDB Client in ClickHouse

Goal:
ClickHouse code calls InodeDB through a C++ API: batches with conditions, stat, read, list, snapshots, `wait_for`, watch, leases, keys, and claiming and acknowledging freed files.

Scope:
- 1A: Choose the client. The recommendation is a C++ bridge over InodeDB's Rust client, which brings the client's cache, snapshots, leases, request-id retries and writer redirects. Its cost: ClickHouse's Rust workspace (`ClickHouse/rust/workspace`) vendors none of tokio, tonic, h2 or prost today, the build needs `ENABLE_RUST=1`, and `inodedb-proto` needs `protoc` at build time. The fallback is a C++ gRPC client over `contrib/grpc` with a thin cache.
- 1B: The bridge crate, `ClickHouse/rust/workspace/inodedb/`: one small tokio runtime per process, blocking calls with deadlines, and InodeDB errors mapped to ClickHouse error codes.
- 1C: Vendored dependencies through `ClickHouse/rust/vendor.sh`, so the build runs offline.
- 1D: `InodeDBClient`, the C++ wrapper the metadata type uses.

Completion gate:
`unit_tests_dbms --gtest_filter='InodeDBClient*'` passes against the server `scripts/dev-up` starts, from an offline build.

Testing plan:
- Gtests for each call, a failed condition, a writer redirect, a retried batch applied once, and a snapshot that holds its version while the writer moves on.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Decision | 1A: Rust bridge or C++ client | Needs: a spike that builds the bridge inside ClickHouse, with its binary-size and build-time cost. |
| Incomplete | Work | 1B: Bridge crate | Missing: crate and C++ header. |
| Incomplete | Work | 1C: Vendored dependencies | Missing: vendored crates and an offline build log. |
| Incomplete | Work | 1D: `InodeDBClient` wrapper | Missing: wrapper. |
| Incomplete | Test | Client gtests | Missing: tests and a passing run. |
| Incomplete | Gate | Offline build and gtests pass | Missing: build log and test output. |

## Phase 2: The `inodedb` Metadata Type at Parity

Goal:
An object-storage disk with `metadata_type` `inodedb` passes every test that an S3 disk with `local` metadata passes.

Scope:
- 2A: Registration. A `MetadataStorageType::InodeDB` value and its `metadataTypeFromString` entry (`ClickHouse/src/Disks/DiskType.h`), `registerMetadataStorageType("inodedb", ...)` in `MetadataStorageFactory.cpp`, and an `s3_inodedb` `<type>` shorthand. The disk reads `inodedb_endpoint`, `inodedb_key_file` and `inodedb_root` from its element.
- 2B: The data model. A directory is an InodeDB directory. A file is an InodeDB file whose value holds `DiskObjectStorageMetadata`'s serialized record: blob keys and sizes, total size, read-only flag and inline data. The reference count comes from the inode's `nlink`, and modification time and mode from its attributes. Files that `writeStringToFile` writes keep their bytes in the value.
- 2C: The transaction. Every method `MetadataStorageFromDiskTransaction` implements: `commit` and `tryCommit` as one batch, with a failed condition as the conflict outcome; `createDirectory`, `createDirectoryRecursive`, `removeDirectory`, `removeRecursive` with its predicate, `moveDirectory` (`Rename` with `NoReplace`), `moveFile`, `replaceFile` (`Rename` with `Replace`), `createHardLink` (`Link`), `unlinkFile`, `createMetadataFile`, `addBlobToMetadata`, `truncateFile`, `writeStringToFile`, `writeInlineDataToFile`, `setReadOnly`, `setLastModified`, `chmod`, `generateObjectKeyForPath` with random keys, and `getSubmittedForRemovalBlobs`.
- 2D: Reads within a transaction. An overlay lets later operations see earlier ones, and read-modify-write operations (`addBlobToMetadata`, `truncateFile`, `setReadOnly`) carry a version condition on the record they read.
- 2E: The storage. Every method `MetadataStorageFromDisk` implements: existence checks, `getFileSize`, `getLastModified`, `getLastChanged`, `stat` built from the inode, `listDirectory`, `iterateDirectory`, `readFileToString`, `readInlineDataToString`, `getSerializedMetadata`, `getHardlinkCount` with `local`'s counting convention, `getStorageObjects`, `isReadOnly` and `startup`.
- 2F: Blob removal. The removal-queue hooks (`getBlobsToRemove`, `recordAsRemoved`, `hasPendingRemovalBlobs`, `getDeadBlobsQueueEstimate`) run on InodeDB's `ClaimFreed` and `AckFreed`, so S3 deletes follow the commit that freed each blob, and the queue survives restarts as `local`'s `.metadata/blobs_to_remove.log` does. An append or truncate replaces a value that keeps most of its blobs, so a replaced value frees only the blobs its successor no longer names.
- 2G: Feature checks on both metadata types: mutations, lightweight `DELETE` and `UPDATE`, `FREEZE` and `UNFREEZE`, `ATTACH PARTITION FROM`, `REPLACE PARTITION`, `MOVE PARTITION TO TABLE`, TTL moves between a local disk and the `inodedb` disk, `BACKUP` and `RESTORE` to and from the disk, `Log`, `TinyLog` and `StripeLog` tables (append mode), `database_disk` on the disk, the `cache` and `encrypted` wrappers, `ReplicatedMergeTree` with each replica under its own root, and zero-copy replication, which upstream marks experimental.

Out of scope:
- Performance work, which Phase 4 takes up once parity holds.

Completion gate:
The parity gtests pass, the stateless parity run shows no failure unique to `inodedb` beyond ledger-listed exceptions, and every feature check passes on both metadata types.

Testing plan:
- `gtest_metadata_inodedb_disk.cpp` parameterized over both types.
- `tests/parity/run`: installs the `--s3-storage` test config with the default policy's disk switched between `local` and `inodedb`, runs `clickhouse-test` on each, and diffs the pass sets.
- `tests/features/`: one script per feature in 2G.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 2A: Registration and config | Missing: enum value, factory entry, shorthand, config parsing. |
| Incomplete | Decision | 2B: Value format | Recommendation: reuse `DiskObjectStorageMetadata` serialization and take the reference count from `nlink`. Needs: confirmation in the 2E tests. |
| Incomplete | Work | 2C: Transaction methods | Missing: implementation of every listed method. |
| Incomplete | Work | 2D: Transaction overlay and version conditions | Missing: overlay and a test where a later operation reads an earlier one. |
| Incomplete | Work | 2E: Storage methods | Missing: implementation of every listed method. |
| Incomplete | Work | 2F: Blob removal through `ClaimFreed` and `AckFreed` | Missing: implementation and a test that a blob stays in S3 until its acknowledgment. |
| Incomplete | Decision | `getLastChanged` | Only wrappers and one gtest call it. Recommendation: a change time in the value, written whenever the disk writes the value, with `getLastChanged` returning the later of it and the inode's `mtime`; directories return `mtime`. Attribute-only operations leave the value alone, since each rewrite frees the old value through the free queue. Needs: implementation and the gtest. |
| Incomplete | Decision | Records over 64 KiB | InodeDB caps a value at 64 KiB (`MAX_VALUE`, `inodedb/crates/core/src/types.rs:24`); a file with about a thousand blobs, as append-mode tables build, exceeds it. Recommendation: past a threshold well under the cap, write the record to S3 under a new random key, under a records prefix, before the commit, and store a pointer in the value. The object never changes, so readers cache it by key, and replacing the value frees it as it frees a blob. `getSerializedMetadata` resolves the pointer. Needs: implementation and a test that appends past the threshold, reads, truncates and drops. |
| Incomplete | Test | Parity gtests | Missing: test file and passing run. |
| Incomplete | Test | Stateless parity run | Missing: `tests/parity/run` and both result sets. |
| Incomplete | Test | 2G feature checks | Missing: `tests/features/` and results on both types. |
| Incomplete | Gate | No failure unique to `inodedb` | Missing: parity diff and the exception rows. |

## Phase 3: Durability, Fencing and Crash Safety

Goal:
The writer survives crashes, retries and takeovers with consistent metadata, losing no committed part and leaking no committed blob.

Scope:
- 3A: The owner fence. A starting writer bumps `<root>/.owner`, and every batch carries a `when` on that file's version; a fenced writer stops writing and reports it.
- 3B: Exactly-once commits. A batch retried after a timeout applies once, through InodeDB's request ids.
- 3C: Crash tests for insert, merge, mutation and removal.
- 3D: Freed-file claims that a crashed consumer leaves behind return after `claim_ttl_ms` and get deleted.

Completion gate:
A hundred kill-and-restart rounds per operation leave every table consistent, a fenced writer applies nothing, and a retried batch applies once.

Testing plan:
- `tests/crash/`: loops that kill the writer at random points, restart it, and check parts, row counts and blob references.
- A two-writer test: the second takes ownership, and the first's next commit fails.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 3A: Owner fence | Missing: implementation and the two-writer test. |
| Incomplete | Work | 3B: Exactly-once commits | Missing: a test that drops a response and retries. |
| Incomplete | Test | 3C: Crash loops | Missing: `tests/crash/` and results from 100 rounds per operation. |
| Incomplete | Test | 3D: Abandoned claims | Missing: a test that kills a consumer mid-claim. |
| Incomplete | Gate | Consistent after every crash round | Missing: crash loop results. |

## Phase 4: Commit Path Performance

Goal:
A part commits in one InodeDB round trip, reads come from the client cache, and a cold part loads in one store read.

Scope:
- 4A: Cached reads. Storage reads go through the client's cache, and the writer answers existence checks from it.
- 4B: Staging. Calls under `tmp_*` directories apply to a local staging area, and the rename out of staging sends the directory, every record, the modification time and any hardlinks as one batch. A rename into `delete_tmp_*` goes at once, and its cleanup batches.
- 4C: Orphan sweep. Staged files take keys under a per-server prefix, and startup deletes blobs there that no record names.
- 4D: Inline small files. `checksums.txt`, `columns.txt`, `count.txt` and small indexes and marks keep their bytes in the record. This changes `DiskObjectStorage`'s write and read paths.
- 4E: Benchmarks against `local` metadata.

Completion gate:
A part commit makes one InodeDB call under the parts lock, and `BENCHMARKS.md` reports insert rate and a 10,000-part cold start for both metadata types, with the parity run still clean.

Testing plan:
- A counter in `InodeDBClient` asserted by a test: one call per part commit.
- Crash loops from Phase 3 rerun with staging on; the orphan sweep leaves no unnamed blob under the staging prefix.
- The parity run rerun with 4A to 4D on.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 4A: Cached reads | Missing: implementation and a cache-hit test. |
| Incomplete | Work | 4B: Staging | Missing: implementation and the one-call test. |
| Incomplete | Work | 4C: Orphan sweep | Missing: implementation and a crash test. |
| Incomplete | Work | 4D: Inline small files | Missing: write and read path changes, listed with reasons, and a cold-load test. |
| Incomplete | Test | 4E: Benchmarks | Missing: `BENCHMARKS.md` with both metadata types. |
| Incomplete | Gate | One call per part, parity intact | Missing: the counter test, benchmarks and a parity diff. |

## Phase 5: Read Replicas

Goal:
Any number of read-only servers attach the writer's tables, see its committed state including drops, and finish their queries while the writer cleans up.

Scope:
- 5A: Read-only mode. A read key, `isReadOnly()` true, and `refresh` waiting on the latest InodeDB version.
- 5B: Version pinning. A server can pin one InodeDB version, so a fleet reads one state.
- 5C: Dropped parts. `MergeTreeData::refreshDataPartsOnce` removes parts that vanished from the listing. This changes `ClickHouse/src/Storages/MergeTree/MergeTreeData.cpp`.
- 5D: Reader leases. Replicas hold InodeDB leases, so freed blobs outlive their queries.
- 5E: Table discovery. `database_disk` on the `inodedb` disk, so replicas read table definitions from the shared tree.
- 5F: Scoped keys. Each replica gets a read-only key rooted at the tables' directory, with a TTL.

Completion gate:
A replica sees a new part and a dropped partition within one refresh, a long query on a replica returns the right answer while the writer merges and deletes its inputs, and an `ALTER` on the writer reaches the replica.

Testing plan:
- `tests/replicas/`: one writer and two replicas over s3proxy, with `old_parts_lifetime` set short.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 5A: Read-only mode | Missing: implementation and test. |
| Incomplete | Work | 5B: Version pinning | Missing: implementation and a test that two pinned servers list the same parts. |
| Incomplete | Work | 5C: Dropped parts on refresh | Missing: the `MergeTreeData.cpp` change and a `DROP PARTITION` test. |
| Incomplete | Work | 5D: Reader leases | Missing: implementation and the long-query test. |
| Incomplete | Work | 5E: Table discovery through `database_disk` | Missing: config and an `ALTER` test. |
| Incomplete | Work | 5F: Scoped read keys | Missing: key minting and a test that a replica's write fails. |
| Incomplete | Gate | Replicas track the writer and keep their queries | Missing: `tests/replicas/` results. |

## Phase 6: The BigHouse Query API

Goal:
`demo/` runs on one machine: s3proxy, InodeDB, a writer ClickHouse, and a query API that starts a fleet of ClickHouse servers for each query, runs it with parallel replicas, returns the result, and stops the fleet.

Scope:
- 6A: `demo/up` and `demo/down`. They start s3proxy, InodeDB and the writer, whose disk is read-write `inodedb` with `database_disk` on it, and load a generated dataset.
- 6B: Slots. A fixed set of slots, each a known set of ports on `127.0.0.1` for native, HTTP and interserver traffic. Each fleet's configs name exactly its slots as the cluster `bighouse`.
- 6C: The query API, a small stdlib-only Python HTTP service. `POST /query` with SQL and a fleet size takes free slots, pins the current InodeDB version, mints a read key, starts the servers, waits for their tables, runs the query on one with `enable_parallel_replicas = 1`, `max_parallel_replicas` set to the fleet size, `cluster_for_parallel_replicas = 'bighouse'` and `parallel_replicas_for_non_replicated_merge_tree = 1`, streams the result, stops the fleet and frees the slots. The response reports startup, query and teardown times.
- 6D: `demo/README.md`, a walkthrough.

Completion gate:
`demo/e2e` runs a query suite at fleet sizes 1, 2 and 4, each answer matching the writer's, while the writer keeps inserting; after each query no fleet process remains and every slot is free.

Testing plan:
- `demo/e2e` locally and in CI.
- Two concurrent queries take separate slots and both answer correctly.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 6A: `demo/up`, `demo/down`, dataset | Missing: scripts and dataset generator. |
| Incomplete | Work | 6B: Slots and fleet configs | Missing: slot table and config templates. |
| Incomplete | Work | 6C: Query API | Missing: service and its timing report. |
| Incomplete | Decision | Query API language | Recommendation: Python with the standard library only. Needs: confirmation. |
| Incomplete | Doc | 6D: Walkthrough | Missing: `demo/README.md`. |
| Incomplete | Test | Concurrent queries | Missing: test and result. |
| Incomplete | Gate | `demo/e2e` matches the writer at 1, 2 and 4 servers | Missing: local and CI run output. |
