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

`demo/up` runs `scripts/dev-up`, which starts s3proxy on `127.0.0.1:11111` and InodeDB on `127.0.0.1:7070`. Then it mints an InodeDB write key rooted at `bighouse/demo` and starts the writer, whose `bighouse` disk keeps blobs in s3proxy under `bighouse/demo/` and metadata in InodeDB under the key's root. The writer answers on `http://127.0.0.1:18200` and native port 19100. Its default storage policy is the `bighouse` disk, so every `MergeTree` table lands there, and its `database_disk` is the same disk, so every database's definition lands there too.

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

The API reads `.dev/demo/config.json`, which `demo/up` writes. A slot is a fixed set of ports on `127.0.0.1`: slot `i` uses native port 20000+i, HTTP port 21000+i and interserver port 22000+i. The API boots a blank server in every slot as it starts, so queries find servers waiting, and the host runs at most one server per slot. Ctrl-C or SIGTERM stops the API and its servers.

## 4. Query

```
curl -s -D - 'http://127.0.0.1:8080/query?replicas=4' \
    --data-binary 'SELECT kind, count(), avg(duration_ms) FROM demo.events GROUP BY kind ORDER BY kind'
```

The answer matches the writer's. These headers describe the fleet:

- `X-BigHouse-Replicas`: the fleet size.
- `X-BigHouse-Timings`: milliseconds the query waited for servers still booting (`wait_ms`), assigned them to the disk (`assign_ms`), attached the databases (`attach_ms`) and ran (`query_ms`), and the total (`total_ms`).
- `X-BigHouse-Left-Out`: each of the writer's databases the fleet could not serve, with the reason. Present only when there is one.

A query that finds too few free slots gets `503`.

## What happens per query

Before any query arrives, each slot runs a blank server: it has no `bighouse` disk, no cluster and no credentials. Its boot config names `bighouse` as MergeTree's default storage policy, because MergeTree reads its defaults only at boot; the server looks the name up only when a table attaches.

1. The API takes `replicas` blank servers, waiting for any still booting, and mints an InodeDB read key rooted at `bighouse/demo`, valid for 15 minutes.
2. It assigns the servers to the disk. Each gets `config.d/tenant.xml`, holding a read-only `bighouse` disk under the read key, the `bighouse` policy and a cluster `bighouse` naming exactly the fleet's servers, and applies it with `SYSTEM RELOAD CONFIG`. The disk starts and takes an InodeDB lease.
3. It lists the writer's databases in InodeDB, reading each one's UUID from `metadata/<name>.sql`, and the first server attaches each with `ATTACH DATABASE ... SETTINGS disk = 'bighouse'`, which loads every table from its definition in InodeDB. The first server then leaves out any database holding something a fleet cannot serve as the writer would: a table whose data lies off the `bighouse` disk, a materialized view that refreshes on a schedule, or another engine. The other servers attach the databases it kept. The writer plays no part, so queries work while it is down.
4. It runs the query on the first server with `enable_parallel_replicas = 1`, `max_parallel_replicas` set to the fleet size, `cluster_for_parallel_replicas = 'bighouse'` and `parallel_replicas_for_non_replicated_merge_tree = 1`. That server coordinates, and every server reads a share of the marks from one snapshot of the parts.
5. It returns the answer. Meanwhile the pool stops each server with SIGTERM, which releases its lease, deletes the server's directory, revokes the key, and boots a blank server in the slot. A reload can add a disk but cannot remove one or change its key, so each server serves one query.

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
- a query that needs servers still booting waits for them;
- a fleet's servers stop once its query ends, and the pool then holds a fresh blank server in every slot;
- a fleet answers while the writer is down;
- no fleet server or directory outlives the pool.

## Stop

```
demo/down
scripts/dev-down           # scripts/dev-down --wipe also deletes the data in .dev/
```
