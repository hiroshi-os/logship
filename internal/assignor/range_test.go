package assignor

import (
	"reflect"
	"testing"
)

func TestRangeEvenSplit(t *testing.T) {
	got := RangeAssign([]string{"c", "a", "b"}, map[string][]int{"orders": {0, 1, 2, 3, 4, 5}})
	want := map[string]map[string][]int{
		"a": {"orders": {0, 1}},
		"b": {"orders": {2, 3}},
		"c": {"orders": {4, 5}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v want %#v", got, want)
	}
}

func TestRangeUneven(t *testing.T) {
	got := RangeAssign([]string{"m2", "m1"}, map[string][]int{"t": {0, 1, 2, 3, 4}})
	// sorted members m1, m2; extra goes to first → m1 gets 3, m2 gets 2
	if !reflect.DeepEqual(got["m1"]["t"], []int{0, 1, 2}) {
		t.Fatalf("m1 got %v", got["m1"]["t"])
	}
	if !reflect.DeepEqual(got["m2"]["t"], []int{3, 4}) {
		t.Fatalf("m2 got %v", got["m2"]["t"])
	}
}

func TestRangePerTopicIndependent(t *testing.T) {
	got := RangeAssign([]string{"x", "y"}, map[string][]int{
		"a": {0, 1},
		"b": {0},
	})
	if len(got["x"]["a"])+len(got["y"]["a"]) != 2 {
		t.Fatal("topic a not fully assigned")
	}
	if len(got["x"]["b"])+len(got["y"]["b"]) != 1 {
		t.Fatal("topic b not fully assigned")
	}
}

func TestRangeSingleMember(t *testing.T) {
	got := RangeAssign([]string{"only"}, map[string][]int{"t": {2, 0, 1}})
	if !reflect.DeepEqual(got["only"]["t"], []int{0, 1, 2}) {
		t.Fatalf("got %v", got["only"]["t"])
	}
}

func TestRangeNoMembers(t *testing.T) {
	got := RangeAssign(nil, map[string][]int{"t": {0}})
	if len(got) != 0 {
		t.Fatalf("expected empty, got %#v", got)
	}
}

func TestRangeCoversAllNoOverlap(t *testing.T) {
	members := []string{"a", "b", "c"}
	parts := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	got := RangeAssign(members, map[string][]int{"t": parts})
	seen := map[int]string{}
	for m, topics := range got {
		for _, p := range topics["t"] {
			if other, ok := seen[p]; ok {
				t.Fatalf("partition %d assigned to %s and %s", p, other, m)
			}
			seen[p] = m
		}
	}
	if len(seen) != 10 {
		t.Fatalf("coverage %d", len(seen))
	}
}
