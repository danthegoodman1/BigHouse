# BigHouse

## Why (WIP)

You can get way more resources. EC2 the largest realistic machine you will use is 192 cores (*6a.metal), and that’s insanely expensive to run ($7.344/hr), especially paying for idle, boot, and setup time.

With fly.io you can spin up 30x 16vCPU machines (480 total cores), execute a query, and destroy the machines in 15 seconds. That query cost less than $0.09 to run (maybe an extra cent for R2 api calls).

With 30 saturated 2Gbit nics and 10:1 avg compression, that ends up being 2gbit * 10 ratio * 30 nics * 15 seconds = 9Tbs or 1.125TB/s. If the query processing itself took 10s, that’s 11.25TB of data. If that was run on BigQuery it would cost 11.25TB*$5/TB=$56.25, on TinyBird $0.07/GB*11,250GB = $787.5 (to be fair that’s absolutely not how you are supposed to use tinybird, but it is the closest thing clickhouse-wise).

For 15 seconds that EC2 instance would cost $0.03 for 192 cores. Multiply by 2.5x to get to 480 cores and that's already $0.075, nearly the same cost, but you haven't considered the 30s first boot time (or 10s subsequent boot times), the cost of the disk, etc.

## How to run

Easiest way is to just use a test, it will download and execute temporal for you. You do need Taskfile though.

```
task single-test -- ./temporal -run TestQueryExecutorWorkflow
```

`.env` file should look like:

```ini
FLY_API_TOKEN=xxx
FLY_APP=xxx
ENV=local
DEBUG=1
PRETTY=1
TEMPORAL_URL=localhost:7233
```

## Primitive Performance Test

96 CSV files:
```
➜  dangoodman: ~ aws s3 ls --no-sign-request s3://altinity-clickhouse-data/nyc_taxi_rides/data/tripdata/ | wc -l                               4:57PM
      96
```

Single node:
```
SELECT count() FROM s3('https://s3.us-east-1.amazonaws.com/altinity-clickhouse-data/nyc_taxi_rides/data/tripdata/data-*.csv.gz', 'CSVWithNames',
'pickup_date Date, id UInt64, vendor_id String, tpep_pickup_datetime DateTime, tpep_dropoff_datetime DateTime, passenger_count UInt8, trip_distance Float32, pickup_longitude Float32, pickup_latitude Float32, rate_code_id String, store_and_fwd_flag String, dropoff_longitude Float32, dropoff_latitude Float32, payment_type LowCardinality(String), fare_amount Float32, extra String, mta_tax Float32, tip_amount Float32, tolls_amount Float32, improvement_surcharge Float32, total_amount Float32, pickup_location_id UInt16, dropoff_location_id UInt16, junk1 String, junk2 String',
'gzip')

Completed query in 2m21.991214525s
```

5 Nodes:
```
SELECT count() FROM s3Cluster('{cluster}', 'https://s3.us-east-1.amazonaws.com/altinity-clickhouse-data/nyc_taxi_rides/data/tripdata/data-*.csv.gz', 'CSVWithNames', 
	'pickup_date Date, id UInt64, vendor_id String, tpep_pickup_datetime DateTime, tpep_dropoff_datetime DateTime, passenger_count UInt8, trip_distance Float32, pickup_longitude Float32, pickup_latitude Float32, rate_code_id String, store_and_fwd_flag String, dropoff_longitude Float32, dropoff_latitude Float32, payment_type LowCardinality(String), fare_amount Float32, extra String, mta_tax Float32, tip_amount Float32, tolls_amount Float32, improvement_surcharge Float32, total_amount Float32, pickup_location_id UInt16, dropoff_location_id UInt16, junk1 String, junk2 String', 
	'gzip')

Completed query in 59.487185924s
```

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
