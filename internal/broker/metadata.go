package broker

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"

	"github.com/hiroshi-os/logship/internal/protocol"
)

type partitionMeta struct {
	ID       int   `json:"id"`
	Leader   int   `json:"leader"`
	Replicas []int `json:"replicas"`
	ISR      []int `json:"isr"`
}

type topicMeta struct {
	Name       string          `json:"name"`
	Partitions []partitionMeta `json:"partitions"`
}

func (b *Broker) metaPath() string {
	return filepath.Join(b.cfg.DataDir, "topics.json")
}

func (b *Broker) loadTopics() error {
	p := b.metaPath()
	raw, err := os.ReadFile(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var topics []topicMeta
	if err := json.Unmarshal(raw, &topics); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for i := range topics {
		t := topics[i]
		b.topics[t.Name] = &t
	}
	return nil
}

func (b *Broker) persistTopicsLocked() error {
	list := make([]topicMeta, 0, len(b.topics))
	for _, t := range b.topics {
		list = append(list, *t)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })
	raw, err := json.MarshalIndent(list, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(b.cfg.DataDir, 0o755); err != nil {
		return err
	}
	tmp := b.metaPath() + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, b.metaPath())
}

func (b *Broker) applyTopics(topics []topicMeta) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i := range topics {
		t := topics[i]
		cp := t
		b.topics[t.Name] = &cp
	}
	_ = b.persistTopicsLocked()
}

func (b *Broker) snapshotTopics() []topicMeta {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]topicMeta, 0, len(b.topics))
	for _, t := range b.topics {
		cp := *t
		cp.Partitions = append([]partitionMeta(nil), t.Partitions...)
		for i := range cp.Partitions {
			cp.Partitions[i].Replicas = append([]int(nil), t.Partitions[i].Replicas...)
			cp.Partitions[i].ISR = append([]int(nil), t.Partitions[i].ISR...)
		}
		out = append(out, cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (b *Broker) topic(name string) (*topicMeta, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	t, ok := b.topics[name]
	return t, ok
}

func (b *Broker) partitionMeta(name string, id int) (partitionMeta, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	t, ok := b.topics[name]
	if !ok {
		return partitionMeta{}, false
	}
	for _, p := range t.Partitions {
		if p.ID == id {
			return p, true
		}
	}
	return partitionMeta{}, false
}

func (b *Broker) assignReplicas(npart, rf, nbrokers int, ids []int) []partitionMeta {
	if rf < 1 {
		rf = 1
	}
	if rf > nbrokers {
		rf = nbrokers
	}
	out := make([]partitionMeta, npart)
	for i := 0; i < npart; i++ {
		reps := make([]int, rf)
		for r := 0; r < rf; r++ {
			reps[r] = ids[(i+r)%nbrokers]
		}
		out[i] = partitionMeta{
			ID:       i,
			Leader:   reps[0],
			Replicas: reps,
			ISR:      []int{reps[0]},
		}
	}
	return out
}

func (b *Broker) metadataPush() protocol.MetadataPush {
	snaps := b.snapshotTopics()
	topics := make([]protocol.TopicSnapshot, 0, len(snaps))
	for _, t := range snaps {
		ps := make([]protocol.PartitionInfo, 0, len(t.Partitions))
		for _, p := range t.Partitions {
			ps = append(ps, protocol.PartitionInfo{
				ID: p.ID, Leader: p.Leader, Replicas: p.Replicas, ISR: p.ISR,
			})
		}
		topics = append(topics, protocol.TopicSnapshot{Name: t.Name, Partitions: ps})
	}
	return protocol.MetadataPush{Controller: b.cluster.ControllerID(), Topics: topics}
}

func firstAlive(replicas []int, alive func(int) bool) int {
	for _, id := range replicas {
		if alive(id) {
			return id
		}
	}
	return replicas[0]
}
