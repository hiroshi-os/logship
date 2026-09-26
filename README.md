# logship

[![CI](https://github.com/hiroshi-os/logship/actions/workflows/ci.yml/badge.svg)](https://github.com/hiroshi-os/logship/actions/workflows/ci.yml)

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

## What is not guaranteed

Membership is **static** (`--peers`). The controller is the lowest live broker
id. Followers pull from the leader; the leader tracks an ISR and a high
watermark. That is the whole replication design. It is not a consensus log.
Read [DESIGN.md](DESIGN.md) before storing anything you cannot lose or
double-process.

- **Not a Kafka replacement.** The API is HTTP + JSON. There is no Kafka wire
  protocol, transactions, idempotent producer, compacted topics, or ACL.
- **No split-brain protection.** A network partition can elect two controllers
  and accept dual-leader writes to the same partition. Logs can diverge.
  This is not Raft, KRaft, or ZooKeeper.
- **No crash durability by default.** `fsync-every` defaults to 0, so appends
  stay in the OS page cache until a segment rotates or the process closes.
  `acks=1` returns after the leader append. It does not wait for followers
  or disk. A crash can drop the tail.
- **`acks=all` is not a disk flush and not a quorum election.** It waits until
  the high watermark covers the offset. The high watermark is the minimum log
  end of brokers currently in the ISR.
- **Unclean leader election.** If no ISR member is alive, the first live
  replica still becomes leader. That can drop the tail of the log.
- **At-least-once consume, not exactly-once.** Group membership lives in
  memory on the controller. Offsets are written locally and, when possible,
  appended to `__consumer_offsets`. A controller failover does not promise the
  new coordinator has those offsets, so consumers can reprocess.
- **The ISR is heartbeat and replica-ack state, not a commit quorum.** A dead
  broker is removed from the ISR. That does not fence a partitioned leader.
- **No cross-partition ordering, retention, auth, or quotas.** Segments stay
  on disk until the data directory is deleted.

## Chaos

```bash
# cluster must already be up; topic `orders` created
./scripts/chaos-kill-broker.sh broker-2
```

The script SIGKILLs `broker-2`, waits until that broker is not alive and is
out of the `orders` ISR, then produces to the partition it used to lead.
Under Docker it starts the dead node again. A host cluster
(`scripts/local-cluster.sh`) is left for you to restart.

## Benches

```bash
go run ./cmd/bench --brokers 127.0.0.1:9092 --n 20000 --value-bytes 200 --acks 1
```

Results we actually measured (3 local brokers, 4× Xeon, HTTP per record):

| acks | Routing | Produce ops/s | Consume ops/s |
| --- | --- | --- | --- |
| 1 | keyed | 19 286 | 169 956 |
| 1 | keyless | 11 270 | 200 581 |
| all | keyed | 43 | 170 816 |

Full command lines and notes: [`benches/results.md`](benches/results.md). No invented numbers.

## Tests

```bash
go test -race ./...
```

CI runs gofmt, `go vet`, `go test -race ./...`, and
`scripts/chaos-kill-broker.sh` against a local three-broker cluster.

Unit coverage: record CRC, segment rotation + sparse index + recovery, range
assignor, keyed/keyless routing, group rebalance + durable offsets. An
in-process 3-broker produce/fetch test, plus a broker-death ISR test, lives
in `internal/broker`.

## Build

```bash
go build -o bin/logship ./cmd/logship
go build -o bin/logship-cli ./cmd/logship-cli
```
