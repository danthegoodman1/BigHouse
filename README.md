# BigHouse

`urlCluster` is not in the docker image apparently, but `s3Cluster` is.

## PoC

The PoC uses docker compose to create a ZK node and ClickHouse cluster, poll for expected cluster state, then 

```
bash run.sh
```
