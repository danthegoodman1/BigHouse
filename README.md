# BigHouse

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