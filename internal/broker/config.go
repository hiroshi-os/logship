package broker

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/hiroshi-os/logship/internal/cluster"
)

type Config struct {
	ID              int
	Bind            string
	Advertise       string
	DataDir         string
	Peers           []cluster.Peer
	SessionTimeout  time.Duration
	GroupSession    time.Duration
	RebalanceWait   time.Duration
	ReplicaLagTime  time.Duration
	FsyncEvery      int
	MaxSegmentBytes int64
	IndexInterval   int64
	MinISR          int
	ProduceTimeout  time.Duration
}

func ParsePeers(s string) ([]cluster.Peer, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	var out []cluster.Peer
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		idStr, addr, ok := strings.Cut(part, "=")
		if !ok {
			return nil, fmt.Errorf("peer %q: want id=host:port", part)
		}
		id, err := strconv.Atoi(idStr)
		if err != nil {
			return nil, fmt.Errorf("peer id %q: %w", idStr, err)
		}
		out = append(out, cluster.Peer{ID: id, Addr: addr})
	}
	return out, nil
}
