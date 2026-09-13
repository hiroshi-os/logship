#!/usr/bin/env bash
# Start a 3-broker cluster on the host (no Docker required).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
mkdir -p bin tmp-cluster
go build -o bin/logship ./cmd/logship

PEERS="1=127.0.0.1:9092,2=127.0.0.1:9093,3=127.0.0.1:9094"

start() {
  local id="$1" port="$2"
  local dir="$ROOT/tmp-cluster/broker-${id}"
  mkdir -p "$dir"
  echo "==> starting broker $id on :$port"
  ./bin/logship \
    --id="$id" \
    --bind="127.0.0.1:${port}" \
    --advertise="127.0.0.1:${port}" \
    --data="$dir" \
    --peers="$PEERS" \
    >"$ROOT/tmp-cluster/broker-${id}.out" 2>&1 &
  echo $! >"$ROOT/tmp-cluster/broker-${id}.pid"
}

if [ "${1:-}" = "stop" ]; then
  for id in 1 2 3; do
    pf="$ROOT/tmp-cluster/broker-${id}.pid"
    if [ -f "$pf" ]; then
      kill "$(cat "$pf")" 2>/dev/null || true
      rm -f "$pf"
    fi
  done
  echo "stopped"
  exit 0
fi

start 1 9092
start 2 9093
start 3 9094
echo "cluster up: 127.0.0.1:9092,9093,9094"
echo "logs under tmp-cluster/broker-*.out"
echo "stop with: $0 stop"
