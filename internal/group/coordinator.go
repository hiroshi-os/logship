// Package group implements consumer-group join/sync/heartbeat, range assignment, and offsets.
package group

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/hiroshi-os/logship/internal/assignor"
)

var (
	ErrUnknownMember       = errors.New("unknown member")
	ErrGenerationMismatch  = errors.New("generation mismatch")
	ErrRebalanceInProgress = errors.New("rebalance in progress")
	ErrUnknownGroup        = errors.New("unknown group")
)

type State int

const (
	Empty State = iota
	PreparingRebalance
	CompletingRebalance
	Stable
)

func (s State) String() string {
	switch s {
	case Empty:
		return "Empty"
	case PreparingRebalance:
		return "PreparingRebalance"
	case CompletingRebalance:
		return "CompletingRebalance"
	case Stable:
		return "Stable"
	default:
		return "Unknown"
	}
}

type Member struct {
	ID            string
	Topics        []string
	LastHeartbeat time.Time
	Joined        bool
	Synced        bool
	Assignment    map[string][]int
}

type Group struct {
	ID         string
	State      State
	Generation int
	Members    map[string]*Member
	Leader     string
}

type JoinResult struct {
	MemberID   string
	Generation int
	LeaderID   string
	Members    []string
	State      string
}

type Coordinator struct {
	mu              sync.Mutex
	groups          map[string]*Group
	offsets         map[string]int64 // group/topic/partition
	offsetPath      string
	sessionTimeout  time.Duration
	rebalanceWait   time.Duration
	partitionsOf    func(topic string) []int
	onOffsetPersist func(group, topic string, partition int, offset int64)
}

func New(dataDir string, session, rebalance time.Duration, partitionsOf func(string) []int) *Coordinator {
	if session <= 0 {
		session = 10 * time.Second
	}
	if rebalance <= 0 {
		rebalance = 2 * time.Second
	}
	c := &Coordinator{
		groups:         map[string]*Group{},
		offsets:        map[string]int64{},
		offsetPath:     filepath.Join(dataDir, "offsets.json"),
		sessionTimeout: session,
		rebalanceWait:  rebalance,
		partitionsOf:   partitionsOf,
	}
	_ = c.loadOffsets()
	return c
}

func (c *Coordinator) SetOffsetHook(fn func(group, topic string, partition int, offset int64)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onOffsetPersist = fn
}

func offsetKey(group, topic string, partition int) string {
	return group + "/" + topic + "/" + strconv.Itoa(partition)
}

func (c *Coordinator) loadOffsets() error {
	b, err := os.ReadFile(c.offsetPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return json.Unmarshal(b, &c.offsets)
}

func (c *Coordinator) persistOffsetsLocked() error {
	if err := os.MkdirAll(filepath.Dir(c.offsetPath), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(c.offsets, "", "  ")
	if err != nil {
		return err
	}
	tmp := c.offsetPath + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, c.offsetPath)
}

func newMemberID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return "m-" + hex.EncodeToString(b[:])
}

func (c *Coordinator) group(id string) *Group {
	g, ok := c.groups[id]
	if !ok {
		g = &Group{ID: id, State: Empty, Members: map[string]*Member{}}
		c.groups[id] = g
	}
	return g
}

func (c *Coordinator) Join(groupID, memberID string, topics []string) (JoinResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	g := c.group(groupID)
	if memberID == "" {
		memberID = newMemberID()
	}
	m, exists := g.Members[memberID]
	if !exists {
		m = &Member{ID: memberID}
		g.Members[memberID] = m
	}
	m.Topics = topics
	m.LastHeartbeat = time.Now()
	m.Synced = false

	if g.State != PreparingRebalance {
		c.beginRebalanceLocked(g)
	}
	m.Joined = true
	if g.State == PreparingRebalance {
		c.maybeCompleteJoinLocked(g)
	}

	var members []string
	if g.Leader == memberID {
		for id := range g.Members {
			members = append(members, id)
		}
		sort.Strings(members)
	}
	return JoinResult{
		MemberID:   memberID,
		Generation: g.Generation,
		LeaderID:   g.Leader,
		Members:    members,
		State:      g.State.String(),
	}, nil
}

func (c *Coordinator) beginRebalanceLocked(g *Group) {
	g.State = PreparingRebalance
	g.Generation++
	g.Leader = ""
	for _, m := range g.Members {
		m.Joined = false
		m.Synced = false
		m.Assignment = nil
	}
	// The caller just joined; mark them after this if needed — Join sets Joined=true after?
	// We are called from Join after Joined=true, so reset then re-set:
}

func (c *Coordinator) maybeCompleteJoinLocked(g *Group) {
	now := time.Now()
	all := true
	for id, m := range g.Members {
		if now.Sub(m.LastHeartbeat) > c.sessionTimeout {
			delete(g.Members, id)
			continue
		}
		if !m.Joined {
			all = false
		}
	}
	if !all && len(g.Members) > 0 {
		// Still waiting; Expire() will finish after rebalanceWait via generation start time.
		return
	}
	c.finishAssignmentLocked(g)
}

func (c *Coordinator) finishAssignmentLocked(g *Group) {
	if len(g.Members) == 0 {
		g.State = Empty
		g.Leader = ""
		return
	}
	ids := make([]string, 0, len(g.Members))
	subs := map[string]struct{}{}
	for id, m := range g.Members {
		ids = append(ids, id)
		for _, t := range m.Topics {
			subs[t] = struct{}{}
		}
	}
	sort.Strings(ids)
	g.Leader = ids[0]
	tp := map[string][]int{}
	for t := range subs {
		if c.partitionsOf != nil {
			tp[t] = c.partitionsOf(t)
		}
	}
	asg := assignor.RangeAssign(ids, tp)
	for id, topics := range asg {
		if m := g.Members[id]; m != nil {
			m.Assignment = topics
			m.Synced = false
		}
	}
	g.State = CompletingRebalance
}

func (c *Coordinator) Sync(groupID, memberID string, generation int) (map[string][]int, int, string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	g, ok := c.groups[groupID]
	if !ok {
		return nil, 0, "", ErrUnknownGroup
	}
	if generation != g.Generation {
		return nil, g.Generation, g.State.String(), ErrGenerationMismatch
	}
	m, ok := g.Members[memberID]
	if !ok {
		return nil, g.Generation, g.State.String(), ErrUnknownMember
	}
	if g.State == PreparingRebalance {
		return nil, g.Generation, g.State.String(), ErrRebalanceInProgress
	}
	m.LastHeartbeat = time.Now()
	m.Synced = true
	if g.State == CompletingRebalance {
		all := true
		for _, mem := range g.Members {
			if !mem.Synced {
				all = false
				break
			}
		}
		if all {
			g.State = Stable
		}
	}
	asg := m.Assignment
	if asg == nil {
		asg = map[string][]int{}
	}
	return asg, g.Generation, g.State.String(), nil
}

func (c *Coordinator) Heartbeat(groupID, memberID string, generation int) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	g, ok := c.groups[groupID]
	if !ok {
		return "", ErrUnknownGroup
	}
	m, ok := g.Members[memberID]
	if !ok {
		return g.State.String(), ErrUnknownMember
	}
	if generation != g.Generation || g.State != Stable {
		return g.State.String(), ErrRebalanceInProgress
	}
	m.LastHeartbeat = time.Now()
	return g.State.String(), nil
}

func (c *Coordinator) Leave(groupID, memberID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	g, ok := c.groups[groupID]
	if !ok {
		return
	}
	delete(g.Members, memberID)
	c.beginRebalanceLocked(g)
	// mark remaining as needing rejoin; none have Joined=true
	c.maybeCompleteJoinLocked(g)
}

func (c *Coordinator) Commit(groupID string, offsets []struct {
	Topic     string
	Partition int
	Offset    int64
}) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, o := range offsets {
		c.offsets[offsetKey(groupID, o.Topic, o.Partition)] = o.Offset
		if c.onOffsetPersist != nil {
			c.onOffsetPersist(groupID, o.Topic, o.Partition, o.Offset)
		}
	}
	return c.persistOffsetsLocked()
}

func (c *Coordinator) FetchOffsets(groupID, topic string) []struct {
	Topic     string
	Partition int
	Offset    int64
} {
	c.mu.Lock()
	defer c.mu.Unlock()
	prefix := groupID + "/"
	var out []struct {
		Topic     string
		Partition int
		Offset    int64
	}
	for k, v := range c.offsets {
		if len(k) < len(prefix) || k[:len(prefix)] != prefix {
			continue
		}
		rest := k[len(prefix):]
		// rest = topic/partition — topic may contain slashes; split last
		i := lastSlash(rest)
		if i < 0 {
			continue
		}
		t := rest[:i]
		p, err := atoi(rest[i+1:])
		if err != nil {
			continue
		}
		if topic != "" && t != topic {
			continue
		}
		out = append(out, struct {
			Topic     string
			Partition int
			Offset    int64
		}{t, p, v})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Topic != out[j].Topic {
			return out[i].Topic < out[j].Topic
		}
		return out[i].Partition < out[j].Partition
	})
	return out
}

func lastSlash(s string) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '/' {
			return i
		}
	}
	return -1
}

func atoi(s string) (int, error) { return strconv.Atoi(s) }

// Expire drops dead members and completes delayed joins. Call from a ticker.
func (c *Coordinator) Expire() {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	for _, g := range c.groups {
		changed := false
		for id, m := range g.Members {
			if now.Sub(m.LastHeartbeat) > c.sessionTimeout {
				delete(g.Members, id)
				changed = true
			}
		}
		if changed {
			c.beginRebalanceLocked(g)
			// After a member death we wait for remaining members to rejoin via heartbeat error.
			// If nobody is left, go Empty.
			if len(g.Members) == 0 {
				g.State = Empty
				continue
			}
		}
		if g.State == PreparingRebalance {
			// Complete once every remaining member has rejoined, or after rebalanceWait
			// from the last generation bump — we approximate: if all joined OR none pending for wait.
			allJoined := true
			oldestHB := now
			for _, m := range g.Members {
				if !m.Joined {
					allJoined = false
				}
				if m.LastHeartbeat.Before(oldestHB) {
					oldestHB = m.LastHeartbeat
				}
			}
			if allJoined || now.Sub(oldestHB) > c.rebalanceWait {
				// drop members that never rejoined
				for id, m := range g.Members {
					if !m.Joined && now.Sub(m.LastHeartbeat) > c.rebalanceWait {
						delete(g.Members, id)
					}
				}
				c.finishAssignmentLocked(g)
			}
		}
	}
}

func (c *Coordinator) ApplyOffset(group, topic string, partition int, offset int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.offsets[offsetKey(group, topic, partition)] = offset
	_ = c.persistOffsetsLocked()
}

func (c *Coordinator) SnapshotOffsets() map[string]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := make(map[string]int64, len(c.offsets))
	for k, v := range c.offsets {
		cp[k] = v
	}
	return cp
}

func (c *Coordinator) DebugGroup(id string) (State, int, []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	g, ok := c.groups[id]
	if !ok {
		return Empty, 0, nil
	}
	var ids []string
	for mid := range g.Members {
		ids = append(ids, mid)
	}
	sort.Strings(ids)
	return g.State, g.Generation, ids
}

func (c *Coordinator) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return fmt.Sprintf("groups=%d offsets=%d", len(c.groups), len(c.offsets))
}
