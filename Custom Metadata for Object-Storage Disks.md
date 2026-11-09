# Custom Metadata for Object-Storage Disks

Oct 26, 2026 · @Dan

A new `metadata_type` for object-storage disks, with S3 holding the blobs and a transactional store holding the file tree, runs full `MergeTree` with one writer and read replicas. The store must make a directory rename atomic; the rest of the disk already exists in ClickHouse. Code references are paths in the ClickHouse source tree.

## What the metadata type decides

ClickHouse has no setting that makes a disk shared. Several servers configure disks over the same bucket and key prefix, and the disk's `metadata_type` decides what each server sees of the others' files.

| `metadata_type` | Where a file's list of blobs lives | Several servers over one bucket |
| --- | --- | --- |
| `local`, the default for S3, Azure, HDFS and local blob storage | files on each server's own disk, pointing at randomly named blobs | Each server sees only its own files. `ReplicatedMergeTree` replicas share blobs only through zero-copy replication (`allow_remote_fs_zero_copy_replication`), which upstream marks experimental. |
| `plain` | nowhere: a blob's key is its path | Every server sees the same names. ClickHouse treats the disk as write-once, for backups and static data. |
| `plain_rewritable` | a directory map kept in the bucket | It takes writes and can hold a `MergeTree` table, for one writer. |

## How a disk is composed

An object-storage disk is two parts chosen separately, a blob store and a metadata store, so a new metadata type pairs with the existing S3 client. The disk's creator (`ClickHouse/src/Disks/DiskObjectStorage/RegisterDiskObjectStorage.cpp:24`) builds it in three steps:

1. **Blob part.** `ObjectStorageFactory` reads `object_storage_type` (`s3`, `azure_blob_storage`, `hdfs`, `local`, `web`) and builds the client for blob reads, writes, deletes and listing.
2. **Metadata part.** `MetadataStorageFactory` reads `metadata_type` and looks it up in its registry. The type's creator receives the same config element and the blob client.
3. **Disk.** `DiskObjectStorage` joins the two: file and directory operations go to the metadata part, and blob I/O goes to the blob part.

Both parts read the same XML element and take the keys they know, so a new metadata type reads its own keys beside the S3 ones:

```xml
<my_disk>
    <type>object_storage</type>
    <object_storage_type>s3</object_storage_type>
    <endpoint>https://bucket.s3.amazonaws.com/prefix/</endpoint>
    <access_key_id>...</access_key_id>
    <secret_access_key>...</secret_access_key>
    <metadata_type>yourdb</metadata_type>
    <yourdb_endpoint>https://meta.example.com/</yourdb_endpoint>
    <yourdb_namespace>tenant-42</yourdb_namespace>
</my_disk>
```

The familiar disk types are shorthands for pairs:

| `<type>` | `object_storage_type` | `metadata_type` |
| --- | --- | --- |
| `s3` | `s3` | `local` |
| `s3_plain` | `s3` | `plain` |
| `s3_plain_rewritable` | `s3` | `plain_rewritable` |
| `s3_with_keeper` | `s3` | `keeper`, compiled only in Cloud builds |
| `object_storage` | as set | as set |

- **Without `metadata_type`,** the factory takes a hint from the type's name (`plain`, `with_keeper`), else the blob store's default: `local` for S3, Azure, HDFS and local, `web` for web.
- **Pairings.** `local`, `plain` and `plain_rewritable` work with any blob store; `web` and `web_index` need `object_storage_type = web`. A new type gets the blob client and may ignore it for metadata.
- **Layers.** The `cache` and `encrypted` disk types wrap a finished disk, so a filesystem cache can sit in front of the composed one.
- **In an OSS build,** `s3_with_keeper` is the same idea with Keeper as the store, and it fails with `unknown metadata storage type: keeper`.

## What the metadata layer stores

The metadata layer stores a file tree: directories, files, and for each file the blobs that hold its bytes. It knows nothing of parts, indexes or granules.

One file's record (`DiskObjectStorageMetadata`, `ClickHouse/src/Disks/DiskObjectStorage/MetadataStorages/DiskObjectStorageMetadata.h`) holds:

- the blobs that make up the file, each a key and a size, and the total size;
- a hardlink reference count;
- a read-only flag;
- an inline-data field for small contents, which in this tree only unit tests write.

Directories and modification times complete the tree. MergeTree maps onto it this way:

- **A part is a directory,** such as `store/<uuid prefix>/<uuid>/all_1_1_0/`. MergeTree learns which parts exist by listing the table's directory and parsing the names.
- **Every part file is an ordinary file** whose bytes live in S3: `primary.cidx`, the marks (`*.cmrk2`), skip indexes (`skp_idx_*`), `checksums.txt`, `columns.txt`, `count.txt`, `minmax_*.idx`, `partition.dat`, `ttl.txt`, and the column data (`*.bin`).

A wide part has two files per column plus about ten small ones, so a 20-column part takes about 50 records; a compact part takes about a dozen. A table of 10,000 wide parts holds around half a million records, and startup lists them all. Loading a part opens its small files through the disk, each a record lookup and then a small S3 read: about ten round trips to S3 per cold part.

## How MergeTree commits on a filesystem

MergeTree needs one atomic step, a directory rename, and builds every change on it, so it never needs a transaction across files.

| Change | How it lands | After a crash |
| --- | --- | --- |
| Insert | Writes `tmp_insert_<part>/`, then renames it to the part's name | Startup deletes `tmp_*` directories (`clearOldTemporaryDirectories`, `ClickHouse/src/Storages/MergeTree/MergeTreeData.cpp:4697`) |
| Merge | Writes the output in a temporary directory and renames it; the inputs stay | The name records the block range, so `all_1_5_1` covers `all_1_1_0` to `all_5_5_0`; startup keeps the covering parts active (`loadDataParts`, line 2771) |
| Mutation | Writes a new part, hardlinking unchanged files, then renames it | As for an insert |
| Removal | Renames the part to `delete_tmp_<part>`, then deletes it | Startup clears what is left |

Object-storage disks rely on the same step. `MetadataOperationsHolder::commit` (`ClickHouse/src/Disks/DiskObjectStorage/MetadataStorages/MetadataOperationsHolder.cpp:68`) runs its operations one at a time and rolls back only on an exception. A crash midway leaves partial metadata, which is safe because the rename comes last.

**When an insert returns.** A synchronous `INSERT` returns after every part it wrote is renamed and active. `MergeTreeSink::finishDelayedChunk` (`ClickHouse/src/Storages/MergeTree/MergeTreeSink.cpp:292`) finishes each part's files, commits the disk's metadata transaction, then calls `renameTempPartAndAdd` (line 449). The sink commits the last chunk's parts in `onFinish`, and the query answers after that.

- **Parts commit one at a time.** An insert that fails midway leaves the parts it already renamed. A plain `MergeTree` retry writes those rows again unless `non_replicated_deduplication_window` is set; it defaults to 0.
- **Renamed is not yet durable.** The rename may sit in the page cache until `fsync_after_insert` and `fsync_part_directory` are set.
- **Async inserts.** With `async_insert = 1` and the default `wait_for_async_insert = 1`, the client waits for the flush that renames the part; with `wait_for_async_insert = 0`, it returns once the rows are buffered.

On a disk with remote metadata, every insert also waits for the store to commit its rename, once per part.

## Building a metadata disk on a transactional store

Write one class pair, an `IMetadataStorage` and its transaction, backed by your store, and register it as a new `metadata_type`. The S3 client, reads, writes, the cache and blob removal stay as they are. ClickHouse Cloud's `keeper` metadata type is this design with Keeper as the store; its class is closed source.

&#91;embedded content: a metadata disk · what you build and what ClickHouse has\]

MergeTree calls the disk for files and directories; the disk sends tree operations to your store and blob I/O to S3.

### Steps

1. **Model the tree as inodes.** A directory entry maps a name to an inode id, and an inode holds a file's blobs, size, reference count, read-only flag and modification time. A directory rename then changes one entry.
2. **Implement the transaction,** `IMetadataTransaction`, about 25 methods: `createDirectory`, `writeStringToFile`, `createMetadataFile`, `addBlobToMetadata`, `moveDirectory`, `createHardLink`, `unlinkFile`, `removeRecursive`, `commit` and the rest. Commit the collected operations in one store transaction, or at least make the rename the last write.
3. **Implement the storage,** `IMetadataStorage`, about 55 methods, many with defaults: existence checks, sizes, listing, `readFileToString`, `getStorageObjects`, `isTransactional`, `refresh` and the removal queue.
4. **Register it** with `registerMetadataStorageType("yourdb", ...)` in `ClickHouse/src/Disks/DiskObjectStorage/MetadataStorages/MetadataStorageFactory.cpp`, reading its keys from the disk's element. A `<type>` shorthand beside `s3_with_keeper` is optional.
5. **Delete blobs after the metadata commit,** through the removal-queue hooks `getBlobsToRemove` and `recordAsRemoved`. A crash then leaves orphaned blobs, never a record that points at a deleted one.
6. **Fence the writer.** Keep an owner epoch in the store and check it in every commit, so a server that lost the table cannot write after another takes it over.
7. **Test with the stock suite.** Run the stateless tests with tables on the new disk, then kill the server mid-insert and mid-merge and check what startup finds.

### What the store must guarantee

| Guarantee | What MergeTree uses it for |
| --- | --- |
| Atomic compare-and-set on one key, or multi-key transactions if paths are keys | The rename that publishes a part |
| Durable before visible | A renamed part's records survive a crash, the role `fsync` before `rename` plays locally |
| Read-your-writes and consistent listing | Startup and refresh list the parts and read their files |
| Conditional writes | The owner epoch that fences a stale writer |

### The hard parts

- **Latency.** A part write touches 10 to 20 files, and startup reads every record. Each call is a round trip unless the implementation caches and batches, so performance work will dominate.
- **Small files.** Keeping `checksums.txt`, `columns.txt`, the index and the marks inline in your store would make a cold part load one store read. MergeTree writes every file as a blob, so that needs a change to the disk's write path.
- **Size.** The local implementation is about 1,600 lines. Keeper is the quickest first store, since its multi-op transactions already commit atomically.

## Committing once per part

A part can reach your store in one transaction, at its rename, if the metadata layer stages temporary directories locally. Today a part's records go in one batch, and the rename then makes three more calls under the table's parts lock:

1. `commitTransaction` sends the temporary directory and every file's record in one metadata commit.
2. `MergeTreeSink::commitPart` takes the parts lock and assigns the block number, and with it the part's final name.
3. `DataPartStorageOnDiskBase::rename` (`ClickHouse/src/Storages/MergeTree/DataPartStorageOnDiskBase.cpp:719`) calls `existsDirectory`, `setLastModified` and `moveDirectory`, each on its own.

The lock lets a table publish one part at a time, so the store's latency caps the table's part rate. At 5 ms a round trip, three calls hold the lock for 15 ms, about 65 parts a second; one call triples that.

Staging works because nothing under a temporary directory needs to be durable or visible. Startup deletes `tmp_*` and `delete_tmp_*` directories (`clearOldTemporaryDirectories`), and replica refreshes skip them. The layer:

- applies calls under `tmp_*` directories to a local staging area, and answers reads of them from it;
- answers existence checks from its cache, as the only writer;
- turns a rename out of staging into one store transaction: the directory, every record, its modification time and any hardlinks;
- sends a rename into `delete_tmp_*` at once, since removals must reach the store, and batches the cleanup that follows it.

The costs:

- **Orphaned blobs.** A crash loses staged records but not the blobs already uploaded for them. Give staged files keys under a per-server staging prefix (`generateObjectKeyForPath`), and at startup delete the blobs there that no record names.
- **Coupling.** The layer depends on MergeTree's temporary names.
- **Transaction size.** One transaction carries a whole part, about 50 records for a 20-column wide part.
- **Blob order.** Blobs a removal frees go only after the commit that drops their records.

In the diagrams below, the MergeTree column includes `DiskObjectStorage`, which uploads, reads and deletes the blobs.

Under each diagram, the transactions name paths from `table`, the table's directory, such as `store/3f2/3f2a9c1e-.../`. `k1` and `n1` stand for a blob key and its size.

### Insert

&#91;embedded content: insert · one part, staged until its rename\]

The layer answers the existence check from its cache and sends the directory, the records and the rename in one transaction, under the parts lock.

```text
One transaction, sent at the rename:
  mkdir  table/all_5_5_0/
  put    table/all_5_5_0/data.bin       = (blob k1, size n1)
  put    table/all_5_5_0/data.cmrk3     = (blob k2, size n2)
  put    table/all_5_5_0/checksums.txt  = (blob k3, size n3)
  ...    one put per file in the part
  mtime  table/all_5_5_0/
```

### Update

&#91;embedded content: update · the mutation entry, then one rewritten part\]

Lightweight `DELETE FROM` and `ALTER DELETE` follow the same sequence; `ALTER DELETE` rewrites every column, so it links fewer files. With `mutations_sync`, the client's OK waits for every part's commit.

```text
Transaction 1, sent when the entry is renamed:
  put    table/mutation_N.txt            = (blob k1, size n1)

Transaction 2, sent at each part's rename (column a unchanged, column b updated):
  mkdir  table/all_1_1_0_N/
  link   table/all_1_1_0_N/a.bin         -> record of table/all_1_1_0/a.bin, ref count + 1
  put    table/all_1_1_0_N/b.bin         = (blob k2, size n2)
  put    table/all_1_1_0_N/checksums.txt = (blob k3, size n3)
  ...    a link per unchanged file, a put per rewritten one
  mtime  table/all_1_1_0_N/
```

### Merge

&#91;embedded content: merge · five parts into one\]

Reading the inputs costs no store call when the layer caches records. The inputs stay in the store until the removal sequence takes them.

```text
One transaction, sent at the rename:
  mkdir  table/all_1_5_1/
  put    table/all_1_5_1/data.bin       = (blob k1, size n1)
  put    table/all_1_5_1/checksums.txt  = (blob k2, size n2)
  ...    one put per file in the merged part
  mtime  table/all_1_5_1/
```

### Removal

&#91;embedded content: removal · an outdated part\]

After the first commit, listings no longer show the part. A crash before the second leaves `delete_tmp_all_1_1_0`, which startup removes, and S3 loses a blob only after the commit that drops its record.

```text
Transaction 1, sent at the rename:
  rename table/all_1_1_0/ -> table/delete_tmp_all_1_1_0/

Transaction 2, sent with the rmdir:
  unlink table/delete_tmp_all_1_1_0/data.bin       frees k1 unless another name links it
  unlink table/delete_tmp_all_1_1_0/checksums.txt  frees k2 unless another name links it
  ...    one unlink per file
  rmdir  table/delete_tmp_all_1_1_0/

Then S3: delete k1, k2, ...
```

## The two interfaces

`IMetadataTransaction` queues the writes and `IMetadataStorage` answers the reads; both live in `ClickHouse/src/Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h`. A new type also needs a value in the `MetadataStorageType` enum and in `metadataTypeFromString` (`ClickHouse/src/Disks/DiskType.h`), which `getType` returns.

In the Default column, **required** means a pure virtual, **throws** means the default throws `NOT_IMPLEMENTED`, and anything else is a working default you may override.

### `IMetadataTransaction`

| Method | Default | What it does |
| --- | --- | --- |
| `commit(options)` | required | Applies every queued operation; throws on failure. |
| `tryCommit(options)` | required | The same, but returns a conflict as an outcome instead of throwing. `DiskObjectStorage::createDirectories` retries on it for transactional stores. |
| `createDirectory(path)` | throws | Creates one directory. |
| `createDirectoryRecursive(path)` | throws | Creates a directory and any missing parents. |
| `removeDirectory(path)` | throws | Removes an empty directory. |
| `removeRecursive(path, predicate)` | throws | Removes a subtree. The predicate decides, file by file, whether its blobs go too; zero-copy replication keeps shared ones. |
| `moveDirectory(from, to)` | throws | Renames a directory. Publishing a part (`tmp_insert_*` to its name) is this call, so it must be atomic. |
| `moveFile(from, to)` | throws | Renames a file. |
| `replaceFile(from, to)` | throws | Moves a file over an existing one in one step. |
| `createHardLink(from, to)` | throws | Gives a file a second name that shares its blobs and raises its reference count. Mutations and `FREEZE` rely on it. |
| `unlinkFile(path, if_exists, should_remove_objects)` | throws | Removes one name. When it was the last link and `should_remove_objects` is set, it queues the blobs for deletion. |
| `generateObjectKeyForPath(path)` | required | Chooses the blob key for a new file: random, or derived from the path for the plain types. |
| `createMetadataFile(path, objects)` | required | Creates a file's record pointing at blobs just uploaded. Every file the disk writes ends here. |
| `addBlobToMetadata(path, object)` | throws | Appends a blob to a file's list, for append-mode writes. |
| `truncateFile(path, size)` | throws | Cuts a file to a size, dropping the blobs past it. |
| `writeStringToFile(path, data)` | throws | Writes a small file whose bytes live in the metadata store itself; `FREEZE` writes its zero-copy info file this way. |
| `writeInlineDataToFile(path, data)` | throws | Stores bytes inline in a file's record; in this tree only unit tests call it. |
| `setReadOnly(path)` | throws | Marks a file read-only, the flag the record keeps. |
| `setLastModified(path, timestamp)` | throws | Sets a file's modification time. |
| `supportsChmod()` | required | Says whether `chmod` works. |
| `chmod(path, mode)` | throws | Changes a file's mode bits. |
| `getSubmittedForRemovalBlobs()` | required | Lists the blobs this transaction's removals freed; the disk deletes or waits on them after the commit. |
| `recordBlobsReplication(blob, missing_locations)` | throws when locations are missing | For disks spread over several locations: notes where a new blob is still missing. |

### `IMetadataStorage`

| Method | Default | What it does |
| --- | --- | --- |
| `createTransaction()` | required | Starts a transaction. |
| `startup()`, `shutdown()` | no-op | Connect to and disconnect from the store. |
| `getPath()` | required | The metadata's root path. |
| `getType()` | required | The type's `MetadataStorageType` value. |
| `getZooKeeperName()`, `getZooKeeperPath()` | empty | The Keeper connection and root a Keeper-backed store uses. |
| `existsFile`, `existsDirectory`, `existsFileOrDirectory` | required | Existence checks for a path. |
| `getFileSize(path)` | required | A file's size, from its record's total. |
| `getFileSizeIfExists(path)` | two calls | The size, or nothing when absent; override to save a round trip. |
| `getLastModified(path)` | required | A file's modification time. |
| `getLastModifiedIfExists(path)` | two calls | The time, or nothing when absent. |
| `getLastChanged(path)` | throws | A file's change time. |
| `supportsStat()`, `stat(path)` | required, throws | Whether POSIX `stat` works, and the call itself. |
| `supportsChmod()` | required | Says whether `chmod` works. |
| `listDirectory(path)` | required | The names in a directory. |
| `iterateDirectory(path)` | required | An iterator over a directory. MergeTree finds its parts with it at startup and on each refresh. |
| `isDirectoryEmpty(path)` | via iterator | Whether a directory holds anything. |
| `getHardlinkCount(path)` | required | A file's reference count, which MergeTree checks before it removes a part's files. |
| `getStorageObjects(path)` | required | The blobs behind a file, with keys and sizes. Every read calls it. |
| `getStorageObjectsIfExist(path)` | two calls | The blobs, or nothing when the file is absent. |
| `readFileToString(path)` | throws | Reads a file written with `writeStringToFile`. |
| `readInlineDataToString(path)` | throws | Reads a file's inline bytes. |
| `getSerializedMetadata(paths)` | throws | Returns files' serialized records. Zero-copy replication sends a part to another replica this way instead of its data. |
| `refresh(not_sooner_than_ms)` | no-op | Reloads state another server changed, at most that often. Read-only replicas call it before each parts refresh. |
| `isReadOnly()` | required | True makes the disk read-only; `refresh_parts_interval` requires it on replicas. |
| `isTransactional()` | false | True when commits can conflict and callers retry through `tryCommit`. |
| `isPlain()`, `isWriteOnce()` | false | Mark a key-is-path layout, and a disk that refuses rewrites. |
| `areBlobPathsRandom()` | required | Whether blob keys are random. The disk uses the page cache only then, or when read-only, since a reused key could serve stale bytes. |
| `supportsEmptyFilesWithoutBlobs()` | false | Whether an empty file can exist with no blob; otherwise the disk uploads an empty blob. |
| `supportWritingWithAppend()` | false | Whether `WriteMode::Append` works. |
| `getBlobsToRemove`, `recordAsRemoved`, `hasPendingRemovalBlobs`, `getDeadBlobsQueueEstimate` | empty | A persistent removal queue: blobs that commits freed, handed out for deletion and then recorded as gone. |
| `getBlobsToReplicate`, `recordAsReplicated`, `hasUnreplicatedBlobs`, `getMissingBlobsQueueEstimate` | empty | For disks over several locations: blobs to copy between them, and their progress. |
| `updateCache`, `updateCacheFromSerializedDescription`, `invalidateCache`, `dropCache` | no-op | For a store that caches records in memory: reload paths, apply another node's update, forget one path, clear everything. |
| `applyNewSettings(config, prefix, context)` | no-op | Applies a configuration reload. |

## Read replicas with one writer

Stock MergeTree already runs one writer and any number of read replicas on such a disk. Each replica configures the disk read-only and sets `refresh_parts_interval`, whose documentation names shared metadata as the requirement (`ClickHouse/src/Storages/MergeTree/registerStorageMergeTree.cpp:2428`).

Each refresh (`refreshDataPartsOnce`, `ClickHouse/src/Storages/MergeTree/MergeTreeData.cpp:3206`) works in three steps:

1. It asks the disk to reload its metadata (`refresh`).
2. It lists the table's directory.
3. It loads parts it has not seen, keeping only covering ones, so a merged part makes its inputs outdated on the replica.

Your store improves on `plain_rewritable` with real hardlinks. `plain_rewritable` copies the object when MergeTree asks for a hardlink (`MetadataStorageFromPlainRewritableObjectStorage.cpp:538`), so mutations, `ATTACH PARTITION FROM` and `FREEZE` copy data in S3; reference-counted hardlinks change only metadata.

| Gap in the replicas | What happens |
| --- | --- |
| Staleness | A replica sees a new part up to one refresh interval late. |
| Removals go unseen | The refresh adds parts and never notices vanished ones (comment at line 3201). After `DROP PARTITION`, `TRUNCATE` or a TTL that drops a whole part, replicas list the removed parts until restart, and reads fail once the blobs go. |
| Cleanup races queries | The writer deletes merged-away parts `old_parts_lifetime` (480 s) after they go outdated, whatever the replicas still read. |
| Schema changes | An `ALTER` on the writer must be repeated on every replica, for example through a `Replicated` database. |
| Listing cost | Every refresh lists the whole table, so its cost grows with the part count unless the store reports what changed. |
| One writer | Promoting a replica means a writable disk, a restart and a fenced old writer. |

Before replicas are correct after drops and long queries, MergeTree's refresh must drop parts that vanished, and the writer's cleanup must outlast every replica's queries.
