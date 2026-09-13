package broker

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/hiroshi-os/logship/internal/client"
	"github.com/hiroshi-os/logship/internal/cluster"
	"github.com/hiroshi-os/logship/internal/protocol"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func startBroker(t *testing.T, id int, addr string, peers []cluster.Peer) *Broker {
	t.Helper()
	cfg := Config{
		ID:              id,
		Bind:            addr,
		Advertise:       addr,
		DataDir:         t.TempDir(),
		Peers:           peers,
		SessionTimeout:  2 * time.Second,
		GroupSession:    time.Second,
		RebalanceWait:   50 * time.Millisecond,
		ReplicaLagTime:  2 * time.Second,
		FsyncEvery:      0,
		MaxSegmentBytes: 1 << 16,
		IndexInterval:   256,
		MinISR:          1,
		ProduceTimeout:  3 * time.Second,
	}
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func TestThreeBrokerProduceFetch(t *testing.T) {
	a1, a2, a3 := freeAddr(t), freeAddr(t), freeAddr(t)
	peers := []cluster.Peer{
		{ID: 1, Addr: a1}, {ID: 2, Addr: a2}, {ID: 3, Addr: a3},
	}
	_ = startBroker(t, 1, a1, peers)
	_ = startBroker(t, 2, a2, peers)
	_ = startBroker(t, 3, a3, peers)
	time.Sleep(400 * time.Millisecond)

	cli := client.New([]string{a1, a2, a3})
	if err := cli.CreateTopic("orders", 3, 3); err != nil {
		t.Fatal(err)
	}
	time.Sleep(400 * time.Millisecond)

	for i := 0; i < 12; i++ {
		resp, err := cli.Produce(a1, protocol.ProduceRequest{
			Topic: "orders",
			Key:   fmt.Sprintf("user-%d", i),
			Value: fmt.Sprintf("msg-%d", i),
			Acks:  "1",
		})
		if err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
		if len(resp.Results) != 1 {
			t.Fatalf("results %#v", resp)
		}
	}

	// Wait for HW / replica catch-up.
	deadline := time.Now().Add(3 * time.Second)
	var total int
	for time.Now().Before(deadline) {
		total = 0
		for p := 0; p < 3; p++ {
			fr, err := cli.Fetch(a1, "orders", p, 0, 1<<20, false)
			if err != nil {
				t.Fatal(err)
			}
			total += len(fr.Records)
		}
		if total == 12 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if total != 12 {
		t.Fatalf("fetched %d records, want 12", total)
	}
}

func TestConsumerGroupRoundTrip(t *testing.T) {
	addr := freeAddr(t)
	_ = startBroker(t, 1, addr, []cluster.Peer{{ID: 1, Addr: addr}})
	cli := client.New([]string{addr})
	if err := cli.CreateTopic("t", 4, 1); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		if _, err := cli.Produce(addr, protocol.ProduceRequest{
			Topic: "t", Key: fmt.Sprintf("k%d", i), Value: "v", Acks: "1",
		}); err != nil {
			t.Fatal(err)
		}
	}
	j1, err := cli.Join("cg", protocol.JoinRequest{Topics: []string{"t"}})
	if err != nil {
		t.Fatal(err)
	}
	j2, err := cli.Join("cg", protocol.JoinRequest{Topics: []string{"t"}})
	if err != nil {
		t.Fatal(err)
	}
	j1, err = cli.Join("cg", protocol.JoinRequest{MemberID: j1.MemberID, Topics: []string{"t"}})
	if err != nil {
		t.Fatal(err)
	}
	s1, err := cli.Sync("cg", protocol.SyncRequest{MemberID: j1.MemberID, Generation: j1.Generation})
	if err != nil {
		t.Fatal(err)
	}
	s2, err := cli.Sync("cg", protocol.SyncRequest{MemberID: j2.MemberID, Generation: j2.Generation})
	if err != nil {
		t.Fatal(err)
	}
	n := len(s1.Assignment["t"]) + len(s2.Assignment["t"])
	if n != 4 {
		t.Fatalf("assignment coverage %d (%v / %v)", n, s1.Assignment, s2.Assignment)
	}
	if err := cli.CommitOffsets("cg", protocol.OffsetCommitRequest{
		MemberID: j1.MemberID, Generation: j1.Generation,
		Offsets: []protocol.OffsetCommit{{Topic: "t", Partition: s1.Assignment["t"][0], Offset: 2}},
	}); err != nil {
		t.Fatal(err)
	}
	of, err := cli.FetchOffsets("cg", "t")
	if err != nil {
		t.Fatal(err)
	}
	if len(of.Offsets) == 0 || of.Offsets[0].Offset != 2 {
		t.Fatalf("offsets %#v", of)
	}
}
