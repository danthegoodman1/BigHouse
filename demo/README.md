# The BigHouse Demo

One machine plays every part: s3proxy stands in for S3, an InodeDB server holds the metadata, a persistent ClickHouse writer ingests, and the query API summons a fleet of ClickHouse servers for each query.

```
INSERT ─────► writer ──┬── metadata ──► InodeDB ◄── metadata, read key ──┐
                       └── blobs ─────► s3proxy ◄── blobs ───────────────┤
                                                                         │
POST /query ─► query API ─► fleet of read-only servers, one per slot ────┘
```

## Before you start

Build ClickHouse and InodeDB once; the first ClickHouse build takes a while.

```
scripts/clickhouse-build
scripts/inodedb-build
```

You need Docker for s3proxy and Python 3.11 or later; the scripts use only the standard library.

## 1. Start the writer

```
demo/up
```

`demo/up` runs `scripts/dev-up`, which starts s3proxy on `127.0.0.1:11111` and InodeDB on `127.0.0.1:7070`. Then it mints an InodeDB write key rooted at `bighouse/demo` and starts the writer, whose `bighouse` disk keeps blobs in s3proxy under `bighouse/demo/` and metadata in InodeDB under the key's root. The writer answers on `http://127.0.0.1:18200` and native port 19100, and its default storage policy is the `bighouse` disk, so every `MergeTree` table lands there.

## 2. Load data

```
demo/load            # 20 million rows; demo/load 1000000 for fewer
```

`demo/load` creates `demo.events (time, user_id, kind, url, duration_ms)`, partitioned by month, and fills it in 2-million-row inserts. Query the writer directly to compare answers later:

```
curl -s http://127.0.0.1:18200 --data-binary 'SELECT kind, count() FROM demo.events GROUP BY kind ORDER BY kind'
```

## 3. Start the query API

```
python3 -m bighouse.api --port 8080 --slots 16
```

The API reads `.dev/demo/config.json`, which `demo/up` writes. A slot is a fixed set of ports on `127.0.0.1`: slot `i` uses native port 20000+i, HTTP port 21000+i and interserver port 22000+i. The host runs at most one fleet server per slot.

## 4. Query

```
curl -s -D - 'http://127.0.0.1:8080/query?replicas=4' \
    --data-binary 'SELECT kind, count(), avg(duration_ms) FROM demo.events GROUP BY kind ORDER BY kind'
```

The answer matches the writer's. Two headers describe the fleet:

- `X-BigHouse-Replicas`: the fleet size.
- `X-BigHouse-Timings`: milliseconds to start the servers, attach the tables, run the query, and stop the fleet.

A query that finds too few free slots gets `503`.

## What happens per query

1. The API takes `replicas` free slots and mints an InodeDB read key rooted at `bighouse/demo`, valid for 15 minutes.
2. It starts one ClickHouse server per slot. Each gets a read-only `bighouse` disk with the read key and a cluster `bighouse` naming exactly the fleet's slots.
3. It reads `SHOW CREATE TABLE` from the writer for every `MergeTree` table and attaches each on every fleet server with its UUID, so each table resolves to the writer's directory in InodeDB.
4. It runs the query on the first server with `enable_parallel_replicas = 1`, `max_parallel_replicas` set to the fleet size, `cluster_for_parallel_replicas = 'bighouse'` and `parallel_replicas_for_non_replicated_merge_tree = 1`. That server coordinates, and every server reads a share of the marks from one snapshot of the parts.
5. It kills the fleet, deletes the servers' directories, frees the slots and revokes the key.

The writer keeps ingesting, merging and deleting old parts throughout. Each fleet server holds an InodeDB lease, so the writer deletes no blob a running query may still read.

## Check it end to end

```
python3 demo/e2e.py
```

`demo/e2e.py` serves the API on port 18080 with 8 slots and checks that:

- fleets of 1, 2 and 4 servers answer a query suite exactly as the writer does;
- fleets see whole insert batches only, while the writer inserts and merges;
- every server of a fleet of 4 reads a share of the query;
- two queries at once take separate slots and both answer;
- no fleet server or directory outlives its query.

## Stop

```
demo/down
scripts/dev-down           # scripts/dev-down --wipe also deletes the data in .dev/
```
