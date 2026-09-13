package cluster

import (
	"sort"
	"sync"
	"time"
)

// Peer is a statically configured broker.
type Peer struct {
	ID   int
	Addr string
}

// Cluster tracks static membership and liveness via heartbeats.
// The controller is the lowest-ID broker that has been seen recently.
type Cluster struct {
	mu             sync.RWMutex
	Self           Peer
	Peers          map[int]Peer
	lastSeen       map[int]time.Time
	SessionTimeout time.Duration
}

func New(self Peer, peers []Peer, session time.Duration) *Cluster {
	if session <= 0 {
		session = 3 * time.Second
	}
	c := &Cluster{
		Self:           self,
		Peers:          map[int]Peer{},
		lastSeen:       map[int]time.Time{},
		SessionTimeout: session,
	}
	for _, p := range peers {
		c.Peers[p.ID] = p
	}
	c.Peers[self.ID] = self
	c.lastSeen[self.ID] = time.Now()
	return c
}

func (c *Cluster) MarkSeen(id int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastSeen[id] = time.Now()
}

func (c *Cluster) IsAlive(id int) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if id == c.Self.ID {
		return true
	}
	t, ok := c.lastSeen[id]
	if !ok {
		return false
	}
	return time.Since(t) < c.SessionTimeout
}

func (c *Cluster) AliveIDs() []int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var ids []int
	now := time.Now()
	for id := range c.Peers {
		if id == c.Self.ID {
			ids = append(ids, id)
			continue
		}
		if t, ok := c.lastSeen[id]; ok && now.Sub(t) < c.SessionTimeout {
			ids = append(ids, id)
		}
	}
	sort.Ints(ids)
	return ids
}

func (c *Cluster) ControllerID() int {
	ids := c.AliveIDs()
	if len(ids) == 0 {
		return c.Self.ID
	}
	return ids[0]
}

func (c *Cluster) IsController() bool {
	return c.ControllerID() == c.Self.ID
}

func (c *Cluster) Addr(id int) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Peers[id].Addr
}

func (c *Cluster) All() []Peer {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]Peer, 0, len(c.Peers))
	for _, p := range c.Peers {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (c *Cluster) OtherPeers() []Peer {
	var out []Peer
	for _, p := range c.All() {
		if p.ID != c.Self.ID {
			out = append(out, p)
		}
	}
	return out
}
