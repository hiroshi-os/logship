// Package routing implements Kafka-style partition selection.
package routing

import (
	"hash/fnv"
	"sync/atomic"
)

// Partitioner maps a produce record onto a partition.
// Non-empty keys use FNV-1a 32-bit hash (stable, no extra deps).
// Empty / missing keys use atomic round-robin.
type Partitioner struct {
	rr uint64
}

// Partition returns an index in [0, numPartitions).
func (p *Partitioner) Partition(key []byte, numPartitions int) int {
	if numPartitions <= 0 {
		return 0
	}
	if len(key) == 0 {
		n := atomic.AddUint64(&p.rr, 1)
		return int((n - 1) % uint64(numPartitions))
	}
	h := fnv.New32a()
	_, _ = h.Write(key)
	return int(h.Sum32() % uint32(numPartitions))
}

// Hash32 is exported for tests and metadata debugging.
func Hash32(key []byte) uint32 {
	h := fnv.New32a()
	_, _ = h.Write(key)
	return h.Sum32()
}
