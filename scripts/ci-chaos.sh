#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
./scripts/local-cluster.sh stop || true
rm -rf tmp-cluster
./scripts/local-cluster.sh
cleanup() { ./scripts/local-cluster.sh stop || true; }
trap cleanup EXIT
ready=0
for _ in $(seq 1 50); do
  if curl -sf http://127.0.0.1:9092/health >/dev/null; then
    ready=1
    break
  fi
  sleep 0.2
done
if [ "$ready" != 1 ]; then
  echo "cluster did not become healthy" >&2
  exit 1
fi
created=0
for _ in $(seq 1 25); do
  if curl -sf -X POST http://127.0.0.1:9092/topics \
    -H 'content-type: application/json' \
    -d '{"name":"orders","partitions":3,"replication_factor":3}' >/dev/null; then
    created=1
    break
  fi
  sleep 0.2
done
if [ "$created" != 1 ]; then
  echo "create topic failed" >&2
  exit 1
fi
./scripts/chaos-kill-broker.sh broker-2
echo CHAOS_OK
