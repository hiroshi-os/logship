# Measured benches

Taken on 2026-09-13 against a **live 3-broker** logship cluster
(`127.0.0.1:9092-9094`, `scripts/local-cluster.sh`). Host: Linux 6.12,
4× Intel Xeon, 16 GiB RAM. `fsync-every=0` (page cache; `Sync` on rotate/close).
Each produce is one HTTP JSON request (no batching). **Not estimates.**

| Run | Topic | acks | Routing | Records | Value | Workers | Produce ops/s | Consume ops/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | bench1 | 1 | keyed FNV-1a | 20 000 / 20 000 | 200 B | 4 | **19 286** | **169 956** |
| 2 | bench2 | 1 | keyless RR | 10 000 / 10 000 | 200 B | 4 | **11 270** | **200 581** |
| 3 | bench3 | all | keyed FNV-1a | 2 000 / 2 000 | 200 B | 2 | **43** | **170 816** |

Reproduce:

```bash
./scripts/local-cluster.sh
go run ./cmd/bench --brokers 127.0.0.1:9092 --topic bench1 --n 20000 --value-bytes 200 --acks 1 --workers 4
go run ./cmd/bench --brokers 127.0.0.1:9092 --topic bench2 --n 10000 --value-bytes 200 --acks 1 --workers 4 --keyed=false
go run ./cmd/bench --brokers 127.0.0.1:9092 --topic bench3 --n 2000 --value-bytes 200 --acks all --workers 2
```

`acks=all` is slow on purpose: each produce waits until followers pull the
record over HTTP and the high watermark covers the offset. That is the
replication path, not a dummy sleep. Consume is a tight `GET /fetch` scan
capped at HW (large batches), which is why consume ops/s is much higher than
single-record produce.
