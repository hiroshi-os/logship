package group

import (
	"testing"
	"time"
)

func parts(topic string) []int {
	if topic == "orders" {
		return []int{0, 1, 2, 3, 4, 5}
	}
	return nil
}

func TestJoinSyncRangeAssign(t *testing.T) {
	c := New(t.TempDir(), time.Second, 50*time.Millisecond, parts)
	j1, err := c.Join("g", "", []string{"orders"})
	if err != nil {
		t.Fatal(err)
	}
	j2, err := c.Join("g", "", []string{"orders"})
	if err != nil {
		t.Fatal(err)
	}
	// Second joiner starts a new generation; the first member must rejoin.
	j1, err = c.Join("g", j1.MemberID, []string{"orders"})
	if err != nil {
		t.Fatal(err)
	}
	if j1.Generation != j2.Generation {
		t.Fatalf("generation %d vs %d", j1.Generation, j2.Generation)
	}
	a1, _, _, err := c.Sync("g", j1.MemberID, j1.Generation)
	if err != nil {
		t.Fatal(err)
	}
	a2, _, st, err := c.Sync("g", j2.MemberID, j2.Generation)
	if err != nil {
		t.Fatal(err)
	}
	if st != "Stable" {
		t.Fatalf("state %s", st)
	}
	n1 := len(a1["orders"])
	n2 := len(a2["orders"])
	if n1+n2 != 6 {
		t.Fatalf("coverage %d+%d", n1, n2)
	}
	seen := map[int]bool{}
	for _, p := range append(append([]int{}, a1["orders"]...), a2["orders"]...) {
		if seen[p] {
			t.Fatalf("overlap on %d", p)
		}
		seen[p] = true
	}
}

func TestRebalanceOnJoin(t *testing.T) {
	c := New(t.TempDir(), time.Second, 50*time.Millisecond, parts)
	j1, _ := c.Join("g", "", []string{"orders"})
	_, _, _, _ = c.Sync("g", j1.MemberID, j1.Generation)
	j2, _ := c.Join("g", "", []string{"orders"})
	if j2.Generation <= j1.Generation {
		t.Fatalf("expected generation bump, %d -> %d", j1.Generation, j2.Generation)
	}
	_, err := c.Heartbeat("g", j1.MemberID, j1.Generation)
	if err != ErrRebalanceInProgress {
		t.Fatalf("expected rebalance error, got %v", err)
	}
	j1b, err := c.Join("g", j1.MemberID, []string{"orders"})
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, err = c.Sync("g", j1b.MemberID, j1b.Generation)
	if err != nil {
		t.Fatal(err)
	}
	_, _, st, err := c.Sync("g", j2.MemberID, j2.Generation)
	if err != nil {
		t.Fatal(err)
	}
	if st != "Stable" {
		t.Fatalf("state %s", st)
	}
}

func TestOffsetCommitDurable(t *testing.T) {
	dir := t.TempDir()
	c := New(dir, time.Second, 50*time.Millisecond, parts)
	if err := c.Commit("g", []struct {
		Topic     string
		Partition int
		Offset    int64
	}{{"orders", 0, 42}}); err != nil {
		t.Fatal(err)
	}
	c2 := New(dir, time.Second, 50*time.Millisecond, parts)
	offs := c2.FetchOffsets("g", "orders")
	if len(offs) != 1 || offs[0].Offset != 42 || offs[0].Partition != 0 {
		t.Fatalf("offsets %#v", offs)
	}
}

func TestHeartbeatStable(t *testing.T) {
	c := New(t.TempDir(), time.Second, 50*time.Millisecond, parts)
	j, _ := c.Join("g", "", []string{"orders"})
	_, _, _, _ = c.Sync("g", j.MemberID, j.Generation)
	st, err := c.Heartbeat("g", j.MemberID, j.Generation)
	if err != nil || st != "Stable" {
		t.Fatalf("hb %s %v", st, err)
	}
}

func TestExpireRemovesDeadMember(t *testing.T) {
	c := New(t.TempDir(), 30*time.Millisecond, 10*time.Millisecond, parts)
	j1, _ := c.Join("g", "", []string{"orders"})
	j2, _ := c.Join("g", "", []string{"orders"})
	_, _, _, _ = c.Sync("g", j1.MemberID, j1.Generation)
	_, _, _, _ = c.Sync("g", j2.MemberID, j2.Generation)
	// stop heartbeating j2
	time.Sleep(50 * time.Millisecond)
	c.Expire()
	_, n, ids := c.DebugGroup("g")
	if len(ids) > 1 {
		t.Fatalf("expected j2 dropped, members=%v gen=%d", ids, n)
	}
}
