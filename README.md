# BigHouse

## Why (WIP)

You can get way more resources. EC2 the largest realistic machine you will use is 192 cores (*6a.metal), and that’s insanely expensive to run ($7.344/hr), especially paying for idle, boot, and setup time.

With fly.io you can spin up 30x 16vCPU machines (480 total cores), execute a query, and destroy the machines in 15 seconds. That query cost less than $0.09 to run (maybe an extra cent for R2 api calls).

With 30 saturated 2Gbit nics and 10:1 avg compression, that ends up being 2gbit * 10 ratio * 30 nics * 15 seconds = 9Tbs or 1.125TB/s. If the query processing itself took 10s, that’s 11.25TB of data. If that was run on BigQuery it would cost 11.25TB*$5/TB=$56.25, on TinyBird $0.07/GB*11,250GB = $787.5 (to be fair that’s absolutely not how you are supposed to use tinybird, but it is the closest thing clickhouse-wise).

## PoC

The PoC uses docker compose to create a ZK node and ClickHouse cluster, poll for expected cluster state, then:

```
bash run.sh
```

## Connect to fly internal network from your device

```
fly wireguard create personal iad idkpeer
```

Then use with wireguard app.
