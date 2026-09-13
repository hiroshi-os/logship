#!/usr/bin/env bash
# Chaos: kill one broker, show the cluster still accepts produce/fetch, then restart it.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

TARGET="${1:-broker-2}"
BROKER="${LOGSHIP_BROKERS:-127.0.0.1:9092}"

if command -v docker >/dev/null 2>&1 && docker compose ps --status running 2>/dev/null | grep -q broker; then
  MODE=docker
else
  MODE=local
fi

echo "==> mode=$MODE target=$TARGET"
echo "==> metadata before kill"
curl -sS "http://${BROKER}/metadata" | python3 -m json.tool | head -n 40

echo "==> producing canary-before"
curl -sS -X POST "http://${BROKER}/produce" \
  -H 'content-type: application/json' \
  -d '{"topic":"orders","key":"chaos","value":"before-kill","acks":"1"}'
echo

if [ "$MODE" = docker ]; then
  echo "==> docker compose kill $TARGET"
  docker compose kill "$TARGET"
else
  pidfile="$ROOT/tmp-cluster/${TARGET}.pid"
  if [ ! -f "$pidfile" ]; then
    echo "no pidfile $pidfile — pass a docker compose cluster or run scripts/local-cluster.sh" >&2
    exit 1
  fi
  pid="$(cat "$pidfile")"
  echo "==> kill -9 $pid ($TARGET)"
  kill -9 "$pid" || true
fi

sleep 4
echo "==> metadata after kill (controller / ISR should have moved)"
curl -sS "http://${BROKER}/metadata" | python3 -m json.tool | head -n 60

echo "==> producing canary-after (must succeed on remaining brokers)"
curl -sS -X POST "http://${BROKER}/produce" \
  -H 'content-type: application/json' \
  -d '{"topic":"orders","key":"chaos","value":"after-kill","acks":"1"}'
echo

if [ "$MODE" = docker ]; then
  echo "==> docker compose start $TARGET"
  docker compose start "$TARGET"
else
  echo "==> restart via scripts/local-cluster.sh (re-run the named broker only is left to the operator)"
  echo "    ./scripts/local-cluster.sh   # or start that id again"
fi

echo "==> chaos script finished"
