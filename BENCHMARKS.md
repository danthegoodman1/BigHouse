# Benchmarks

Two benchmarks ran on the machine below: `demo/bench` times queries through the query API at each fleet size, and `tests/bench.py` compares `inodedb` metadata with `local`.

## Machine

- Intel Core Ultra 9 285 (24 cores), 125 GiB RAM, Linux 7.0, governor `performance`, idle states deeper than C1 off.
- A Micron MTFDKBA2T0TGD NVMe drive under ext4, with a volatile write cache: `fdatasync` of an overwritten 4 KiB block takes 1.98 ms at p50 and 3.92 ms at p99.
- s3proxy 4.1.1 in Docker, on the filesystem backend on that drive. InodeDB on SQLite.
- ClickHouse from `ClickHouse/`, built `RelWithDebInfo` by `scripts/clickhouse-build`.

## Fleets

`demo/up`, `demo/load`, then `demo/bench`. The pool has 8 slots, and each query starts once the pool has replaced the last fleet's servers, so none waits for a boot. `SELECT count(), sum(duration_ms) FROM demo.events` over 20,000,000 rows, median of 10 runs, in ms:

| Servers | Assign | Attach | Query | Total |
| --- | --: | --: | --: | --: |
| 1 | 13 | 22 | 49 | 98 |
| 2 | 14 | 37 | 52 | 115 |
| 4 | 15 | 45 | 53 | 126 |
| 8 | 18 | 49 | 58 | 138 |

- **Assign** writes each server's `config.d/tenant.xml` and runs `SYSTEM RELOAD CONFIG`, which starts the read-only disk and its InodeDB lease. The servers assign at once.
- **Attach** lists the writer's databases in InodeDB and attaches them on the first server, which settles the set the fleet serves; then the other servers attach the same set at once. A fleet of one takes the first round alone.
- **Query** runs with parallel replicas. Every server shares this machine's 24 cores, so a larger fleet adds coordination and no CPU.
- The rest of each total, 12 to 14 ms, mints the query's read key with two `idb` calls and takes the servers from the pool.

Back to back, fleets of 4 on 8 slots took 276 ms a query, since each waits for servers the pool is still replacing; the last fleet's 4 servers were replaced 483 ms after its answer.

## Metadata

`tests/bench.py` gives one server three disks over the same s3proxy and runs the same work on each: `local` metadata, `inodedb` metadata, and `inodedb` with inline files off (`inodedb_blobs`). The inserts run twice more with `fsync_after_insert` and `fsync_part_directory` on: on the `local` disk (`local`, fsync on), and on the server's own drive (`default`, fsync on), which is what a durable insert costs there.

- **Inserts.** 200 inserts of 1,000 rows one after another, then 200 more from 8 threads, into a table with merges stopped. Inserts are synchronous (`async_insert = 0`), so each writes its own part.
- **10,000 parts.** 16 inserts at once write a table of 10,000 one-row partitions.
- **Cold start.** The server restarts with only that table attached; the time runs until `SELECT count()` answers.

The inserts and the cold starts run 3 rounds, with the disks in a different order each round, and the tables give medians. Whichever disk runs first runs slow: the first cold start follows 30,000 parts' writes, and in round 1 `local`'s took 2.86 s against its median of 2.30 s.

### InodeDB on the NVMe drive

`python3 tests/bench.py`

| Metadata | Inserts/s, 1 thread | Inserts/s, 8 threads | S3 writes per insert | InodeDB commits per insert | fsyncs per insert | 10,000 parts written (s) | Cold start (s) | S3 reads at load | InodeDB reads at load |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `local` | 325 | 1374 | 11.0 | 0.0 | 0.0 | 7.0 | 2.30 | 101,939 | 395 |
| `inodedb` | 131 | 189 | 0.0 | 1.0 | 0.0 | 50.4 | 1.29 | 1,411 | 20,283 |
| `inodedb_blobs` | 117 | 183 | 11.0 | 1.0 | 0.0 | 57.1 | 2.62 | 101,826 | 20,394 |
| `local`, fsync on | 325 | 1356 | 11.0 | 0.0 | 0.0 | | | | |
| `default`, fsync on | 35 | 84 | 0.0 | 0.0 | 14.0 | | | | |

### InodeDB on tmpfs

`python3 tests/bench.py --tmpfs` runs an InodeDB of its own with its store in `/dev/shm`, which stands in for a drive whose syncs take microseconds, such as the datacenter NVMe in InodeDB's own benchmarks (17 to 53 µs).

| Metadata | Inserts/s, 1 thread | Inserts/s, 8 threads | S3 writes per insert | InodeDB commits per insert | fsyncs per insert | 10,000 parts written (s) | Cold start (s) | S3 reads at load | InodeDB reads at load |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `local` | 325 | 1386 | 11.0 | 0.0 | 0.0 | 7.4 | 2.26 | 102,015 | 388 |
| `inodedb` | 337 | 1383 | 0.0 | 1.0 | 0.0 | 5.6 | 1.23 | 1,822 | 20,366 |
| `inodedb_blobs` | 270 | 1390 | 11.0 | 1.0 | 0.0 | 7.7 | 2.59 | 102,169 | 20,409 |
| `local`, fsync on | 317 | 1410 | 11.0 | 0.0 | 0.0 | | | | |
| `default`, fsync on | 35 | 84 | 0.0 | 0.0 | 14.0 | | | | |

### Reading the numbers

- **An insert makes one InodeDB commit.** It holds the part's directory and files, its modification time, and its rename to its final name: `MergeTreeSink` leaves the part's disk transaction open, and the rename joins it.
- **The commit waits for the store's sync.** On this drive that costs 2 ms. InodeDB groups concurrent commits into one sync: its own benchmark here commits 456 batches a second from one client, 3,499 from 16 and 9,041 from 128 (`cargo run --release -p inodedb-tests --example commits -- 3 sqlite`, store on the drive).
- **`local` metadata makes no insert durable.** It writes local files and syncs none of them, and on an object-storage disk `fsync_after_insert` and `fsync_part_directory` change nothing: with both on, it makes no fsync and the same S3 writes. MergeTree on this drive with both on makes 14 fsyncs per insert and takes 35 inserts a second, where `inodedb` makes each insert durable at 131.
- **One table commits one part at a time.** `MergeTreeSink::commitPart` renames each new part under the table's parts lock, and the part's commit goes with the rename, so one table's commits run in sequence. That caps one table near 190 inserts a second on this drive. With syncs that cost microseconds, the part's batch itself sets the pace: `inodedb` matches `local`, at 337 and 1,383 inserts a second against 325 and 1,386, and writes 10,000 parts in 5.6 s against 7.4 s.
- **Inline files make cold starts faster.** Loading a part reads its small files, which `inodedb` keeps inline: 10,000 parts load from 20,283 InodeDB reads and 1,411 S3 reads in 1.3 s, where `local` makes 101,939 S3 reads in 2.3 s. With inline files off, the load reads both and takes 2.6 s.
- **Small parts upload nothing.** Every file of a 1,000-row part fits within `inodedb_inline_file_max_bytes` (16 KiB), so `inodedb` writes no S3 objects for it.
- The `local` rows' InodeDB reads come from the server's two other disks, which start with it.
