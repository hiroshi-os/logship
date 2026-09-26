#!/usr/bin/env bash
# Chaos: kill one broker, require the cluster to drop it from the ISR, produce
# to the partition it led, then restart it when running under Docker.
#
# The topic `orders` must already exist (replication factor >= 2). Default
# target is broker-2, talked to via broker-1 at $LOGSHIP_BROKERS.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

TARGET="${1:-broker-2}"
BROKER="${LOGSHIP_BROKERS:-127.0.0.1:9092}"
DEAD_ID="${TARGET#broker-}"
case "$DEAD_ID" in
  ''|*[!0-9]*)
    echo "target must look like broker-<id>, got $TARGET" >&2
    exit 1
    ;;
esac

if command -v docker >/dev/null 2>&1 && docker compose ps --status running 2>/dev/null | grep -q broker; then
  MODE=docker
else
  MODE=local
fi

echo "==> mode=$MODE target=$TARGET id=$DEAD_ID"

meta() {
  curl -sS --fail "http://${BROKER}/metadata"
}

# sed consumes the whole document so pipefail does not see SIGPIPE from head.
show_meta() {
  echo "==> metadata $1"
  meta | python3 -m json.tool | sed -n '1,80p'
}

# Exit 0 when every orders partition has a full-enough ISR that includes id
# and id leads at least one of them. Prints that partition id.
wait_victim_insync() {
  local id="$1"
  local i part
  for i in $(seq 1 80); do
    part="$(meta | python3 -c '
import json, sys
want = int(sys.argv[1])
meta = json.load(sys.stdin)
led = None
full = 0
parts = 0
for t in meta.get("topics") or []:
    if t.get("name") != "orders":
        continue
    for p in t.get("partitions") or []:
        parts += 1
        isr = p.get("isr") or []
        if want in isr and len(isr) >= 2:
            full += 1
        if p.get("leader") == want:
            led = p.get("id")
if parts > 0 and full == parts and led is not None:
    print(led)
    sys.exit(0)
sys.exit(1)
' "$id" || true)"
    if [ -n "$part" ]; then
      echo "$part"
      return 0
    fi
    sleep 0.25
  done
  echo "timed out waiting for broker $id to lead orders and sit in every ISR" >&2
  meta | python3 -m json.tool >&2 || true
  return 1
}

# Exit 0 only after id has stayed not-alive, not-leader, and out of every
# orders ISR for ~2s. A single metadata read can observe a transient shrink.
wait_victim_gone() {
  local id="$1"
  local i stable=0
  for i in $(seq 1 75); do
    if meta | python3 -c '
import json, sys
want = int(sys.argv[1])
meta = json.load(sys.stdin)
alive = None
for b in meta.get("brokers") or []:
    if b.get("id") == want:
        alive = bool(b.get("alive"))
if alive is not False:
    sys.exit(1)
for t in meta.get("topics") or []:
    if t.get("name") != "orders":
        continue
    for p in t.get("partitions") or []:
        if p.get("leader") == want or want in (p.get("isr") or []):
            sys.exit(1)
sys.exit(0)
' "$id"; then
      stable=$((stable + 1))
      if [ "$stable" -ge 10 ]; then
        return 0
      fi
    else
      stable=0
    fi
    sleep 0.2
  done
  echo "timed out waiting for broker $id to leave the ISR" >&2
  meta | python3 -m json.tool >&2 || true
  return 1
}

require_produce() {
  local value="$1"
  local partition="$2"
  local i body
  for i in $(seq 1 25); do
    body="$(curl -sS -X POST "http://${BROKER}/produce" \
      -H 'content-type: application/json' \
      -d "{\"topic\":\"orders\",\"partition\":${partition},\"key\":\"chaos\",\"value\":\"${value}\",\"acks\":\"1\"}" || true)"
    if python3 -c '
import json, sys
raw, want = sys.argv[1], int(sys.argv[2])
try:
    r = json.loads(raw)
except json.JSONDecodeError:
    sys.exit(1)
res = r.get("results") or []
if not res or "offset" not in res[0]:
    sys.exit(1)
if res[0].get("partition") != want:
    sys.exit(1)
' "$body" "$partition"; then
      echo "$body"
      return 0
    fi
    sleep 0.2
  done
  echo "produce failed for partition $partition: $body" >&2
  return 1
}

show_meta "before kill"
echo "==> waiting until broker $DEAD_ID is in the ISR and leads a partition"
PART="$(wait_victim_insync "$DEAD_ID")"
echo "==> broker $DEAD_ID leads orders partition $PART"
echo "==> producing canary-before"
require_produce "before-kill" "$PART"

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

echo "==> waiting for broker $DEAD_ID to drop out of the ISR"
wait_victim_gone "$DEAD_ID"
show_meta "after kill"

echo "==> producing canary-after on the failed-over partition"
require_produce "after-kill" "$PART"

if [ "$MODE" = docker ]; then
  echo "==> docker compose start $TARGET"
  docker compose start "$TARGET"
else
  echo "==> restart via scripts/local-cluster.sh (re-run the named broker only is left to the operator)"
  echo "    ./scripts/local-cluster.sh   # or start that id again"
fi

echo "==> chaos script finished"
