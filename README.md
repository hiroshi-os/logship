# logship

A small Kafka-shaped commit log in Go: segmented logs with a sparse index and
per-record CRC, keyed / round-robin partitioning, HTTP produce & fetch,
consumer groups (join / sync / heartbeat, range assignor, durable offsets),
and leader/follower replication with an ISR and high watermark.

**Transport: HTTP + JSON** on `:9092` (see [DESIGN.md](DESIGN.md)). No vendor
Kafka client libraries on the core path.

## 60-second path

```bash
docker compose up --build -d
# wait until healthy
for i in 1 2 3 4 5 6 7 8 9 10; do
  curl -sf http://127.0.0.1:9092/health && break
  sleep 1
done

curl -s http://127.0.0.1:9092/health
# {"ok":true,"id":1,"controller":1}

curl -s -X POST http://127.0.0.1:9092/topics \
  -H 'content-type: application/json' \
  -d '{"name":"orders","partitions":3,"replication_factor":3}'

curl -s -X POST http://127.0.0.1:9092/produce \
  -H 'content-type: application/json' \
  -d '{"topic":"orders","key":"user-1","value":"hello","acks":"1"}'

curl -s 'http://127.0.0.1:9092/fetch?topic=orders&partition=0&offset=0'
```

Consumer group (from a host with Go, talking to the published ports):

```bash
go run ./cmd/logship-cli consume orders --group workers --from-beginning --max 1
```

No Docker? `./scripts/local-cluster.sh` starts three brokers on
`127.0.0.1:9092-9094`. Stop with `./scripts/local-cluster.sh stop`.

## Layout

| Path | Role |
| --- | --- |
| `internal/log` | Segmented commit log, sparse index, CRC, HW |
| `internal/record` | On-disk record + Castagnoli CRC |
| `internal/routing` | FNV-1a keyed hash, atomic round-robin |
| `internal/assignor` | Range assignor |
| `internal/group` | Join / sync / heartbeat / offsets |
| `internal/broker` | HTTP API, controller, ISR, followers |
| `cmd/logship` | Broker process |
| `cmd/logship-cli` | Metadata / produce / fetch / consume |
| `cmd/bench` | Measured produce / consume ops/s |
| `scripts/chaos-kill-broker.sh` | Kill a broker, produce, restart |

## HTTP API

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/health` | Liveness + controller id |
| `GET` | `/metadata` | Brokers, topics, leaders, ISR, HW, LEO |
| `POST` | `/topics` | `{name, partitions, replication_factor}` |
| `POST` | `/produce` | `{topic, key, value, acks}` — proxies to leader |
| `GET` | `/fetch` | `topic, partition, offset, max_bytes` — consumers capped at HW |
| `POST` | `/groups/{g}/join` | `{member_id, topics}` |
| `POST` | `/groups/{g}/sync` | `{member_id, generation}` → assignment |
| `POST` | `/groups/{g}/heartbeat` | session keep-alive |
| `POST` | `/groups/{g}/leave` | drop member + rebalance |
| `POST` | `/groups/{g}/offsets` | durable commit |
| `GET` | `/groups/{g}/offsets` | last committed offsets |
| `GET` | `/internal/replica/fetch` | follower fetch (up to LEO) |
| `POST` | `/internal/replica/ack` | follower LEO → ISR / HW |

`acks=1` (default) returns after the leader append. `acks=all` waits until the
high watermark covers the offset.

## Replication (honest version)

Membership is **static** (`--peers`). The controller is the lowest live broker
id. Followers pull from the leader; the leader tracks an ISR and a high
watermark. This is **not** Raft / KRaft / ZooKeeper. Dual-leader writes are
possible on a network partition. Read [DESIGN.md](DESIGN.md) before using this
for anything that cannot lose the tail of a log.

## Chaos

```bash
# cluster must already be up; topic `orders` created
./scripts/chaos-kill-broker.sh broker-2
```

The script SIGKILLs `broker-2`, produces a canary to a surviving broker, and
starts the dead node again. Watch `/metadata` for ISR shrink and a new leader.

## Benches

```bash
go run ./cmd/bench --brokers 127.0.0.1:9092 --n 20000 --value-bytes 200 --acks 1
```

Results we actually measured are in [`benches/results.md`](benches/results.md).
No invented numbers.

## Tests

```bash
go test ./...
```

Unit coverage: record CRC, segment rotation + sparse index + recovery, range
assignor, keyed/keyless routing, group rebalance + durable offsets. An
in-process 3-broker produce/fetch test lives in `internal/broker`.

## Build

```bash
go build -o bin/logship ./cmd/logship
go build -o bin/logship-cli ./cmd/logship-cli
```
