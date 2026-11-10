# Running BigHouse in Production

The demo runs every part on one machine. In production each part runs on its own, and a query takes the same steps: it takes blank servers, assigns them to the tenant's disk, attaches the tenant's databases, runs, and has the servers replaced.

| Part | Demo | Production |
| --- | --- | --- |
| Object storage | s3proxy | S3, with a prefix per disk |
| Metadata | One InodeDB server | InodeDB with standbys; the disk's client follows a standby's `NotWriter` answer to the writer |
| Writers | `demo-writer` | One exclusive writer per disk, or several shared ones, under a supervisor that restarts them and sends DDL through a brief exclusive writer |
| Fleet servers | Slots on `127.0.0.1` | Fleet hosts, each running a pool of slots |
| Query API | `bighouse/api.py`, one process | A scheduler that runs each query's steps as a durable workflow |

## Keys

An InodeDB key has a root, grants and an optional TTL. It mints keys only at or under its root, with grants and a TTL no wider than its own. A minted key expires with its minter at the latest, and revoking a key revokes every key it minted. InodeDB refuses a key once its TTL passes, by the clock of the server that checks it, and then deletes the key and the keys it minted.

- **Each writer's key:** `write`, rooted at the disk's directory, with no TTL. An exclusive writer that starts fences every other writer; shared writers commit side by side until one does. Give each shared writer a key of its own, so revoking one fences that writer alone.
- **The scheduler's key:** `admin`, rooted at the tenant's directory or at the one that holds every tenant's. The keys it mints reach nothing outside that root, and revoking it revokes them all, which cuts off every fleet still running.
- **Each query's key:** `read`, rooted at the disk's directory, with a TTL. The scheduler mints it as it assigns the servers and revokes it once they have stopped. The servers need it until then, to read and to release their InodeDB leases.

The TTL covers the case where nothing revokes the key, because the scheduler crashed or lost the workflow. Once it passes, an orphaned server can no longer read the tenant's metadata, and InodeDB's deletion of expired keys keeps one key per query from growing the store.

Make the TTL longer than the longest query plus the time to stop its servers, and bound queries below it: set `max_execution_time` under the TTL, with a constraint in the query user's profile so a query can't raise it. A query that outlives its key fails once InodeDB refuses its reads, and its servers can't release their leases. The demo mints 15-minute keys and stops queries at 14 minutes (`KEY_TTL_MINUTES` in `bighouse/api.py`).

## A query's steps

1. Take blank servers from the fleet hosts' pools, waiting for any still booting.
2. Mint the query's read key, and record its id and the servers in the workflow.
3. Assign each server: write `config.d/tenant.xml`, holding the read-only disk with the key and S3 credentials, the `bighouse` policy and a cluster naming exactly the fleet's servers, and run `SYSTEM RELOAD CONFIG`.
4. Attach the tenant's databases from InodeDB on one server, which leaves out those it cannot serve, then attach the same set on the rest.
5. Run the query on the first server with parallel replicas, and return the answer.
6. Stop each server with SIGTERM and delete its directory, then revoke the key and boot a blank server in each slot.

A workflow that resumes after a crash runs step 6 for the servers and key it recorded. The key's TTL bounds what even that misses. A host agent stops any fleet server that no workflow holds, since it keeps its slot's ports and the tenant's config until it stops. The demo's pool does this for its own slots as it boots them: it reads each server's pid from the server's directory, checks through a pidfd that the process still runs that server's config, and sends it SIGTERM.

## Fleet hosts

- **Blank boot config.** It holds no `bighouse` disk, cluster or credentials, and names `bighouse` as MergeTree's default storage policy (`<merge_tree><storage_policy>bighouse</storage_policy></merge_tree>`). MergeTree reads its defaults only at boot, and looks the name up only when a table attaches, by which time the reload has added the policy.
- **System logs.** Leave them off, or give each one `<storage_policy>default</storage_policy>`. A log table on the `bighouse` default fails: before assignment the policy is missing, and after it the disk is read-only.
- **One query per server.** A reload can add a disk but cannot remove one or change its key, and a server that attached a tenant's tables keeps their cached listings and definitions.
- **Stop with SIGTERM.** The disk then releases its InodeDB lease. InodeDB holds a freed blob while any lease might read it, across every disk in the store, so a killed server holds back blob deletion for every disk until its lease runs out, 30 s by default.
- **A query user confined to its fleet.** The demo runs queries as `default`, which has no password and can reach any server it can connect to through `remote()` and `url()`: other tenants' fleets on the host, and in the demo the writer. Give the cluster's servers a password of their own, and run tenant queries as a separate user without source grants (`remote`, `url`, `s3`, `file` and the like) and with the `max_execution_time` constraint.

## S3

- **One prefix per disk.** Fleet servers get read-only credentials scoped to it. Session credentials expire like the key (`<session_token>` on the disk).
- **A lifecycle rule with `AbortIncompleteMultipartUpload`.** ClickHouse uploads large blobs in parts, and a crash mid-upload leaves parts that no listing shows.
- **Uncommitted blobs.** BigHouse deletes only blobs that a commit freed. A writer that crashes or loses the disk leaves in S3 the blobs of parts it had not committed, a part waiting for the table's lock among them. `local` metadata leaves those of parts still being written.

## Shared writers

A disk with `<inodedb_writer_mode>shared</inodedb_writer_mode>` takes inserts and merges from several servers at once. Each shared writer inserts, merges and runs `OPTIMIZE`; it refuses DDL, mutations, lightweight updates and deletes, partition commands and `TRUNCATE`, naming exclusive mode. An exclusive writer, started for DDL and stopped after, fences every shared writer, and the supervisor restarts them once it stops; a restarted shared writer begins a new shared epoch. A shared writer leaves pending mutations to the next exclusive writer and merges around them, and the next exclusive writer also removes the source parts of merges whose writer died before `old_parts_lifetime` passed.

Every table keeps its coordination files in InodeDB under `coord/`, beside the disk's `tree/`, at the table's data path:

| Path | Holds |
|---|---|
| `coord/{table path}/block_number` | The table's highest committed block number, in decimal. Each insert's batch replaces it under its version. |
| `coord/{table path}/tokens/{block id}` | A deduplication token: the name of the part that took the block. Each insert's batch creates it. |
| `coord/{table path}/claims/{part name}` | A merge claim on a source part: `held {writer} {expiry ms} {result}` while the merge runs, then `done {writer} {result}`. |

A control plane with a write key rooted at the disk's directory can manage claims through InodeDB:

- **List** the merges in flight: `idb ls coord/{table path}/claims`, and `idb cat` each claim. Each writer logs its id at startup ("Joined the shared writers").
- **Reserve** parts for one writer: create `held {writer} {expiry ms} {result}` at each part's claim. Other writers leave those parts alone until the expiry, and the named writer may merge them.
- **Revoke** a merge: replace or remove its `held` claims. Its writer's commit requires each claim's version, so it commits nothing, and its uploaded blobs stay in S3 as orphans.

A `done` claim stays until its source part's directory is gone, and the merging writer removes it after the directory; touching one lets a writer with a stale view merge that source again, over the result. Expired `held` claims of dead writers go once another writer takes their parts over, or after twice the claim TTL.
