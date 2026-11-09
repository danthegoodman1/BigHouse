# Benchmarks

`tests/bench.py` gives one server three disks over the same s3proxy and runs the same work on each: `local` metadata, `inodedb` metadata, and `inodedb` with inline files off (`inodedb_blobs`).

- **Inserts.** 200 inserts of 1,000 rows one after another, then 200 more from 8 threads, into a table with merges stopped. Inserts are synchronous (`async_insert = 0`), so each writes its own part.
- **10,000 parts.** 16 inserts at once write a table of 10,000 one-row partitions.
- **Cold start.** The server restarts with only that table attached; the time runs until `SELECT count()` answers.

## Machine

- Intel Core Ultra 9 285 (24 cores), 125 GiB RAM, Linux 7.0, governor `performance`, idle states deeper than C1 off.
- A Micron MTFDKBA2T0TGD NVMe drive under ext4, with a volatile write cache: `fdatasync` of an overwritten 4 KiB block takes 1.98 ms at p50 and 3.92 ms at p99.
- s3proxy 4.1.1 in Docker, on the filesystem backend on that drive. InodeDB on SQLite.
- ClickHouse from `ClickHouse/`, built `RelWithDebInfo` by `scripts/clickhouse-build`.

## InodeDB on the NVMe drive

`python3 tests/bench.py`

| Metadata | Inserts/s, 1 thread | Inserts/s, 8 threads | S3 writes per insert | InodeDB commits per insert | 10,000 parts written (s) | Cold start (s) | S3 reads at load | InodeDB reads at load |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `local` | 312 | 1269 | 11.0 | 0.0 | 5.5 | 2.33 | 107,141 | 1,620 |
| `inodedb` | 83 | 145 | 0.0 | 2.0 | 87.1 | 1.27 | 2,449 | 20,597 |
| `inodedb_blobs` | 77 | 142 | 11.0 | 2.0 | 90.8 | 2.59 | 102,260 | 20,526 |

## InodeDB on tmpfs

`python3 tests/bench.py --tmpfs` runs an InodeDB of its own with its store in `/dev/shm`, which stands in for a drive whose syncs take microseconds, such as the datacenter NVMe in InodeDB's own benchmarks (17 to 53 µs).

| Metadata | Inserts/s, 1 thread | Inserts/s, 8 threads | S3 writes per insert | InodeDB commits per insert | 10,000 parts written (s) | Cold start (s) | S3 reads at load | InodeDB reads at load |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `local` | 316 | 1123 | 11.0 | 0.0 | 4.0 | 2.30 | 107,135 | 1,620 |
| `inodedb` | 318 | 2182 | 0.0 | 2.0 | 4.8 | 1.20 | 1,672 | 20,530 |
| `inodedb_blobs` | 254 | 1468 | 11.0 | 2.0 | 6.6 | 2.51 | 102,156 | 20,614 |

## Reading the numbers

- **An insert makes two InodeDB commits.** The first holds the part's directory and files; the second renames the part to its final name and sets its modification time. `local` metadata makes neither: it writes local files and syncs none of them by default.
- **Each commit waits for the store's sync.** On this drive that costs 2 ms. InodeDB groups concurrent commits into one sync: its own benchmark here commits 456 batches a second from one client, 3,499 from 16 and 9,041 from 128 (`cargo run --release -p inodedb-tests --example commits -- 3 sqlite`, store on the drive).
- **One table renames one part at a time.** `MergeTreeSink::commitPart` renames each new part under the table's parts lock, so its rename commits run in sequence, each paying at least one sync. That caps one table near 145 inserts a second on this drive and stretches 10,000 parts to 87 s. With syncs that cost microseconds, `inodedb` matches `local` on one thread, outpaces it from 8 threads, and writes 10,000 parts in 4.8 s against 4.0 s.
- **Inline files make cold starts faster.** Loading a part reads its small files, which `inodedb` keeps inline: 10,000 parts load from 20,530 InodeDB reads and 1,672 S3 reads in 1.2 s, where `local` makes 107,135 S3 reads in 2.3 s. With inline files off, the load reads both and takes 2.5 s.
- **Small parts upload nothing.** Every file of a 1,000-row part fits within `inodedb_inline_file_max_bytes` (16 KiB), so `inodedb` writes no S3 objects for it.
- The `local` rows' InodeDB reads come from the server's two other disks, which start with it.
