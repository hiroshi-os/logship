# logship design

logship is a small Kafka-shaped commit-log cluster: segmented logs, keyed
partitioning, produce/fetch, consumer groups, and leader/follower replication
with an ISR and a high watermark. It is an MVP, not a Kafka replacement.

## Wire API: HTTP + JSON

Produce, fetch, metadata, and the group protocol are **HTTP/JSON** on port 9092
(mapped to 9093/9094 for brokers 2 and 3). Internal replication uses the same
stack (`/internal/*`).

Why HTTP, not a binary TCP protocol:

- A 60-second `curl` path is part of the product requirement.
- Debugging a three-broker cluster with `wget` healthchecks is straightforward.
- The on-disk record format is still binary (CRC, offsets, length prefixes).
  The HTTP layer is a transport, not the log format.

A Kafka-compatible binary protocol would be the next step if clients needed
drop-in `franz-go` / `sarama` interop. That is explicitly out of scope; this
repo does not vendor Kafka client libraries on the core path.

## Commit log

Each partition is an append-only directory:

```
data/topics/<topic>/<partition>/
  00000000000000000000.log
  00000000000000000000.index
```

Record layout (big-endian):

```
crc32     uint32   Castagnoli over (size || payload)
size      uint32   bytes after `size`
magic     uint8    = 1
offset    int64
timestamp int64    unix millis
key_len   uint32
key       bytes
value_len uint32
value     bytes
```

The sparse index records `(relative_offset, file_position)` every
`index-interval` bytes (default 4 KiB). Lookup binary-searches the index and
scans forward. A corrupt or torn tail is truncated on recovery (CRC failure or
short read). Segments roll when the next append would exceed `segment-bytes`.

`fsync-every=N` (default 0) controls durability vs throughput. `0` means
writes hit the OS page cache and are `Sync`'d on segment rotate / process
close. That matches how many log systems behave under load; it is **not**
`acks=all` disk durability. Set `fsync-every=1` if you want each append
synced (benches will drop).

## Partition routing

- Non-empty key → FNV-1a 32-bit hash modulo `N`.
- Empty key → atomic round-robin on that broker.

Any broker accepts produce/fetch and **proxies** to the partition leader so
`curl` does not have to implement metadata. Clients that care about hops can
read `/metadata` and talk to the leader directly.

## Replication, ISR, high watermark

Each partition has an ordered replica list. The first replica is the preferred
leader. Followers pull `GET /fetch?replica=1` (up to LEO, not HW) and
`POST /internal/replica/ack` with their LEO.

- **LEO** — next offset the local log will assign.
- **ISR** — leader plus replicas that have acked within `replica.lag.time`
  (default 5s).
- **HW** — `min(LEO of ISR members)`. Consumers only see `[0, HW)`.
- **acks=1** — return after the leader append. HW still only moves with ISR.
  If the ISR is just the leader, HW advances immediately so a single-node or
  freshly created topic is usable.
- **acks=all** — block until `HW > produced offset` or `produce-timeout`.

On broker death the controller (below) shrinks ISR and moves leadership to the
first live replica. HW never retreats.

## Controller and membership — static, not consensus

Brokers are listed in `--peers` (`id=host:port,...`). That list is **static
membership**. Liveness is all-to-all HTTP heartbeats every 200ms. A broker is
dead after `session-timeout` (3s).

The **controller** is the lowest-ID live broker. It:

- owns `POST /topics` (assignment of replicas / preferred leaders)
- pushes `topics.json` snapshots to peers
- is the consumer-group coordinator
- reassigns leaders when a replica dies

There is **no Raft, no ZooKeeper, no KRaft**. This is a deliberate MVP cut.

### What static membership gets you

- Three `docker compose` processes become a cluster with no extra dependency.
- Leader failover is fast (heartbeat timeout + a tick).
- The code path is readable: one file of membership, one of replica fetch.

### What it does not get you

- **Split brain.** If the network partitions 1|2,3, both sides can elect a
  controller (`1` on the left, `2` on the right). Dual leaders can accept
  writes to the same partition. Kafka + Raft/ZK prevents this with an epoch
  and a consistent membership quorum. logship does not.
- **Controller state loss.** Group membership is in-memory on the controller.
  Offsets are fsynced to `offsets.json` *and* appended to `__consumer_offsets`,
  so a restart of the same node recovers offsets. A controller **failover**
  to a different node will replay `__consumer_offsets` only if that topic has
  been replicated to the new controller; otherwise consumers start from the
  new coordinator's empty map and must tolerate duplicate processing.
- **Unclean leader election.** If no ISR member is alive, we still pick the
  first live replica. That can lose the tail of the log. Kafka's
  `unclean.leader.election.enable=false` refuses this; we do not.

If you need those properties, replace the controller with Raft (or sit this
log behind an existing consensus layer) and fence produces with a leader
epoch. That is the honest upgrade path; faking consensus with extra heartbeats
would be worse.

## Consumer groups

States: `Empty → PreparingRebalance → CompletingRebalance → Stable`.

1. **Join** — empty `member_id` is assigned. Any join outside
   `PreparingRebalance` bumps `generation` and starts a rebalance (so a second
   member joining a one-member group forces the first to rejoin — same shape
   as Kafka).
2. **Sync** — coordinator runs the **range assignor** per topic (sorted
   members, contiguous partition slices; extras go to the first members).
   Members receive their assignment. When all have synced, the group is
   `Stable`.
3. **Heartbeat** — generation mismatch or a non-stable group returns
   `rebalance in progress`. Session timeout drops the member and rebalances.
4. **Offsets** — committed to `data/offsets.json` (rename-atomic) and, when
   possible, to the internal `__consumer_offsets` topic.

This is coordinator-side assignment (the group "leader" is informational).
Kafka's default is leader-assignor; range itself is identical.

## What was left out

- Transactions / idempotent producer
- Compacted topics
- Kafka wire protocol and ACL
- Disk-quota / retention by time (segments stay until you delete the dir)
- Exactly-once anything — at-least-once consume with durable offsets only
