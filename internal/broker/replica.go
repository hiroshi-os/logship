package broker

import (
	"context"
	"log"
	"strconv"
	"time"

	"github.com/hiroshi-os/logship/internal/protocol"
	"github.com/hiroshi-os/logship/internal/record"
)

func (b *Broker) noteReplica(topic string, partition, brokerID int, leo int64) {
	k := replicaKey(topic, partition)
	b.mu.Lock()
	defer b.mu.Unlock()
	m, ok := b.replicaLEO[k]
	if !ok {
		m = map[int]replicaState{}
		b.replicaLEO[k] = m
	}
	m[brokerID] = replicaState{leo: leo, lastSeen: time.Now()}
}

func (b *Broker) noteSelfLEO(topic string, partition int, leo int64) {
	b.noteReplica(topic, partition, b.cfg.ID, leo)
}

func (b *Broker) recomputeAllHW() {
	for _, t := range b.snapshotTopics() {
		for _, p := range t.Partitions {
			if p.Leader == b.cfg.ID {
				b.recomputeHW(t.Name, p.ID)
			}
		}
	}
}

func (b *Broker) recomputeHW(topic string, partition int) {
	pm, ok := b.partitionMeta(topic, partition)
	if !ok || pm.Leader != b.cfg.ID {
		return
	}
	lg, err := b.store.Open(topic, partition)
	if err != nil {
		return
	}
	k := replicaKey(topic, partition)
	b.mu.Lock()
	states := b.replicaLEO[k]
	now := time.Now()
	isr := []int{b.cfg.ID}
	for _, id := range pm.Replicas {
		if id == b.cfg.ID {
			continue
		}
		st, ok := states[id]
		if !ok {
			continue
		}
		if now.Sub(st.lastSeen) <= b.cfg.ReplicaLagTime {
			isr = append(isr, id)
		}
	}
	hw := lg.LEO()
	for _, id := range isr {
		if id == b.cfg.ID {
			continue
		}
		if st, ok := states[id]; ok && st.leo < hw {
			hw = st.leo
		}
	}
	changed := !sameInts(pm.ISR, isr)
	if changed {
		for i := range b.topics[topic].Partitions {
			if b.topics[topic].Partitions[i].ID == partition {
				b.topics[topic].Partitions[i].ISR = append([]int(nil), isr...)
			}
		}
		_ = b.persistTopicsLocked()
	}
	b.mu.Unlock()
	lg.SetHighWatermark(hw)
	if changed {
		go b.broadcastMetadata()
	}
}

func sameInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	ma := map[int]struct{}{}
	for _, x := range a {
		ma[x] = struct{}{}
	}
	for _, x := range b {
		if _, ok := ma[x]; !ok {
			return false
		}
	}
	return true
}

func (b *Broker) syncFollowers() {
	wanted := map[string]partitionMeta{}
	for _, t := range b.snapshotTopics() {
		for _, p := range t.Partitions {
			if contains(p.Replicas, b.cfg.ID) && p.Leader != b.cfg.ID {
				wanted[replicaKey(t.Name, p.ID)] = p
			}
		}
	}
	b.followMu.Lock()
	defer b.followMu.Unlock()
	for k, cancel := range b.followCancel {
		if _, ok := wanted[k]; !ok {
			cancel()
			delete(b.followCancel, k)
		}
	}
	for k, p := range wanted {
		if _, ok := b.followCancel[k]; ok {
			continue
		}
		ctx, cancel := context.WithCancel(context.Background())
		b.followCancel[k] = cancel
		topic, part := splitReplicaKey(k)
		leader := p.Leader
		b.wg.Add(1)
		go b.followLoop(ctx, topic, part, leader)
	}
}

func splitReplicaKey(k string) (string, int) {
	for i := len(k) - 1; i >= 0; i-- {
		if k[i] == '/' {
			n, _ := strconv.Atoi(k[i+1:])
			return k[:i], n
		}
	}
	return k, 0
}

func (b *Broker) followLoop(ctx context.Context, topic string, partition, leaderID int) {
	defer b.wg.Done()
	lg, err := b.store.Open(topic, partition)
	if err != nil {
		log.Printf("logship: open follower log %s/%d: %v", topic, partition, err)
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-b.stop:
			return
		default:
		}
		pm, ok := b.partitionMeta(topic, partition)
		if !ok || pm.Leader == b.cfg.ID || !contains(pm.Replicas, b.cfg.ID) {
			return
		}
		leaderID = pm.Leader
		addr := b.cluster.Addr(leaderID)
		if addr == "" {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		fr, err := b.cli.ReplicaFetch(addr, topic, partition, lg.LEO())
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		for _, wr := range fr.Records {
			rec := record.Record{
				Offset: wr.Offset, Timestamp: wr.Timestamp,
				Key: []byte(wr.Key), Value: []byte(wr.Value),
			}
			if rec.Offset < lg.LEO() {
				continue
			}
			if _, err := lg.AppendAt(rec); err != nil {
				log.Printf("logship: follower append %s/%d: %v", topic, partition, err)
				break
			}
		}
		lg.SetHighWatermark(fr.HighWatermark)
		_ = b.cli.ReplicaAck(addr, protocol.ReplicaAckRequest{
			BrokerID: b.cfg.ID, Topic: topic, Partition: partition, LEO: lg.LEO(),
		})
		if len(fr.Records) == 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(50 * time.Millisecond):
			}
		}
	}
}
