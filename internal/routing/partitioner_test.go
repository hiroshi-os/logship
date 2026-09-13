package routing

import "testing"

func TestKeyedIsDeterministic(t *testing.T) {
	var p Partitioner
	a := p.Partition([]byte("user-42"), 8)
	b := p.Partition([]byte("user-42"), 8)
	if a != b {
		t.Fatalf("keyed hash not stable: %d vs %d", a, b)
	}
	if a < 0 || a >= 8 {
		t.Fatalf("out of range %d", a)
	}
}

func TestKeyedSpreads(t *testing.T) {
	var p Partitioner
	seen := map[int]bool{}
	for i := 0; i < 200; i++ {
		k := []byte{byte(i), byte(i >> 8)}
		seen[p.Partition(k, 7)] = true
	}
	if len(seen) < 5 {
		t.Fatalf("poor key spread: %v", seen)
	}
}

func TestKeylessRoundRobin(t *testing.T) {
	var p Partitioner
	counts := make([]int, 3)
	for i := 0; i < 30; i++ {
		counts[p.Partition(nil, 3)]++
	}
	for i, c := range counts {
		if c != 10 {
			t.Fatalf("rr partition %d got %d", i, c)
		}
	}
}

func TestZeroPartitions(t *testing.T) {
	var p Partitioner
	if p.Partition([]byte("x"), 0) != 0 {
		t.Fatal("expected 0")
	}
}
