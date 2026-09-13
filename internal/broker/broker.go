package broker

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/hiroshi-os/logship/internal/client"
	"github.com/hiroshi-os/logship/internal/cluster"
	"github.com/hiroshi-os/logship/internal/group"
	"github.com/hiroshi-os/logship/internal/protocol"
	"github.com/hiroshi-os/logship/internal/routing"
	"github.com/hiroshi-os/logship/internal/store"
)

const offsetsTopic = "__consumer_offsets"

type replicaState struct {
	leo      int64
	lastSeen time.Time
}

// Broker is a single logship node: commit logs, replication, and group coordinator.
type Broker struct {
	cfg         Config
	cluster     *cluster.Cluster
	store       *store.Store
	groups      *group.Coordinator
	partitioner routing.Partitioner
	cli         *client.Client

	mu         sync.RWMutex
	topics     map[string]*topicMeta
	replicaLEO map[string]map[int]replicaState

	httpServer *http.Server
	ln         net.Listener

	stop chan struct{}
	wg   sync.WaitGroup

	followMu     sync.Mutex
	followCancel map[string]context.CancelFunc
}

func New(cfg Config) (*Broker, error) {
	if cfg.Bind == "" {
		cfg.Bind = "0.0.0.0:9092"
	}
	if cfg.DataDir == "" {
		cfg.DataDir = "data"
	}
	if cfg.SessionTimeout == 0 {
		cfg.SessionTimeout = 3 * time.Second
	}
	if cfg.GroupSession == 0 {
		cfg.GroupSession = 10 * time.Second
	}
	if cfg.RebalanceWait == 0 {
		cfg.RebalanceWait = 2 * time.Second
	}
	if cfg.ReplicaLagTime == 0 {
		cfg.ReplicaLagTime = 5 * time.Second
	}
	if cfg.MaxSegmentBytes == 0 {
		cfg.MaxSegmentBytes = 1 << 20
	}
	if cfg.IndexInterval == 0 {
		cfg.IndexInterval = 4096
	}
	if cfg.MinISR <= 0 {
		cfg.MinISR = 1
	}
	if cfg.ProduceTimeout == 0 {
		cfg.ProduceTimeout = 5 * time.Second
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, err
	}
	self := cluster.Peer{ID: cfg.ID, Addr: cfg.Advertise}
	cl := cluster.New(self, cfg.Peers, cfg.SessionTimeout)
	st := store.New(cfg.DataDir, cfg.FsyncEvery, cfg.MaxSegmentBytes, cfg.IndexInterval)
	if err := st.LoadExisting(); err != nil {
		return nil, err
	}
	b := &Broker{
		cfg:          cfg,
		cluster:      cl,
		store:        st,
		topics:       map[string]*topicMeta{},
		replicaLEO:   map[string]map[int]replicaState{},
		stop:         make(chan struct{}),
		followCancel: map[string]context.CancelFunc{},
		cli:          client.New(nil),
	}
	b.groups = group.New(cfg.DataDir, cfg.GroupSession, cfg.RebalanceWait, b.topicPartitions)
	b.groups.SetOffsetHook(b.replicateOffset)
	if err := b.loadTopics(); err != nil {
		return nil, err
	}
	return b, nil
}

func (b *Broker) topicPartitions(topic string) []int {
	t, ok := b.topic(topic)
	if !ok {
		return nil
	}
	out := make([]int, len(t.Partitions))
	for i, p := range t.Partitions {
		out[i] = p.ID
	}
	return out
}

func (b *Broker) Addr() string {
	if b.ln != nil {
		return b.ln.Addr().String()
	}
	return b.cfg.Advertise
}

func (b *Broker) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", b.handleHealth)
	mux.HandleFunc("GET /metadata", b.handleMetadata)
	mux.HandleFunc("POST /topics", b.handleCreateTopic)
	mux.HandleFunc("POST /produce", b.handleProduce)
	mux.HandleFunc("GET /fetch", b.handleFetch)
	mux.HandleFunc("POST /groups/{group}/join", b.handleJoin)
	mux.HandleFunc("POST /groups/{group}/sync", b.handleSync)
	mux.HandleFunc("POST /groups/{group}/heartbeat", b.handleHeartbeat)
	mux.HandleFunc("POST /groups/{group}/leave", b.handleLeave)
	mux.HandleFunc("POST /groups/{group}/offsets", b.handleCommitOffsets)
	mux.HandleFunc("GET /groups/{group}/offsets", b.handleFetchOffsets)
	mux.HandleFunc("POST /internal/heartbeat", b.handlePeerHeartbeat)
	mux.HandleFunc("POST /internal/metadata", b.handleMetadataPush)
	mux.HandleFunc("GET /internal/replica/fetch", b.handleReplicaFetch)
	mux.HandleFunc("POST /internal/replica/ack", b.handleReplicaAck)

	ln, err := net.Listen("tcp", b.cfg.Bind)
	if err != nil {
		return err
	}
	b.ln = ln
	if b.cfg.Advertise == "" {
		b.cfg.Advertise = ln.Addr().String()
		b.cluster.Self.Addr = b.cfg.Advertise
		b.cluster.Peers[b.cfg.ID] = b.cluster.Self
	}
	b.httpServer = &http.Server{Handler: mux}

	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		if err := b.httpServer.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("logship: http serve: %v", err)
		}
	}()

	b.wg.Add(1)
	go b.loop()

	b.syncFollowers()
	return nil
}

func (b *Broker) Close() error {
	close(b.stop)
	b.followMu.Lock()
	for _, cancel := range b.followCancel {
		cancel()
	}
	b.followMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if b.httpServer != nil {
		_ = b.httpServer.Shutdown(ctx)
	}
	b.wg.Wait()
	_ = b.store.Close()
	return nil
}

func (b *Broker) loop() {
	defer b.wg.Done()
	t := time.NewTicker(200 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-b.stop:
			return
		case <-t.C:
			b.tick()
		}
	}
}

func (b *Broker) tick() {
	b.cluster.MarkSeen(b.cfg.ID)
	for _, p := range b.cluster.OtherPeers() {
		if err := b.cli.PeerHeartbeat(p.Addr, b.cfg.ID); err != nil {
			continue
		}
	}
	b.groups.Expire()
	if b.cluster.IsController() {
		b.controllerTick()
		b.ensureOffsetsTopic()
	}
	b.recomputeAllHW()
	b.syncFollowers()
}

func (b *Broker) controllerTick() {
	changed := false
	b.mu.Lock()
	for _, t := range b.topics {
		for i := range t.Partitions {
			p := &t.Partitions[i]
			aliveISR := p.ISR[:0]
			for _, id := range p.ISR {
				if b.cluster.IsAlive(id) {
					aliveISR = append(aliveISR, id)
				} else {
					changed = true
				}
			}
			p.ISR = append([]int(nil), aliveISR...)
			if !b.cluster.IsAlive(p.Leader) {
				p.Leader = firstAlive(p.Replicas, b.cluster.IsAlive)
				if !contains(p.ISR, p.Leader) {
					p.ISR = append([]int{p.Leader}, p.ISR...)
				}
				changed = true
			}
			if len(p.ISR) == 0 {
				p.ISR = []int{p.Leader}
				changed = true
			}
		}
	}
	if changed {
		_ = b.persistTopicsLocked()
	}
	b.mu.Unlock()
	if changed {
		b.broadcastMetadata()
	}
}

func (b *Broker) ensureOffsetsTopic() {
	if _, ok := b.topic(offsetsTopic); ok {
		return
	}
	_ = b.createTopicLocked(offsetsTopic, 1, min(3, len(b.cluster.All())))
}

func (b *Broker) broadcastMetadata() {
	push := b.metadataPush()
	for _, p := range b.cluster.OtherPeers() {
		_ = b.cli.PushMetadata(p.Addr, push)
	}
}

func (b *Broker) createTopicLocked(name string, npart, rf int) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.topics[name]; ok {
		return nil
	}
	ids := make([]int, 0, len(b.cluster.All()))
	for _, p := range b.cluster.All() {
		ids = append(ids, p.ID)
	}
	if npart <= 0 {
		npart = 1
	}
	parts := b.assignReplicas(npart, rf, len(ids), ids)
	b.topics[name] = &topicMeta{Name: name, Partitions: parts}
	_ = b.persistTopicsLocked()
	go b.broadcastMetadata()
	return nil
}

func (b *Broker) replicateOffset(groupID, topic string, partition int, offset int64) {
	if topic == offsetsTopic {
		return
	}
	key := groupID + "/" + topic + "/" + strconv.Itoa(partition)
	val := strconv.FormatInt(offset, 10)
	_, _ = b.produceLocal(protocol.ProduceRequest{
		Topic: offsetsTopic,
		Key:   key,
		Value: val,
		Acks:  "1",
	})
}

func (b *Broker) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (b *Broker) writeErr(w http.ResponseWriter, status int, msg string) {
	b.writeJSON(w, status, protocol.ErrorBody{Error: msg})
}

func contains(xs []int, v int) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func replicaKey(topic string, partition int) string {
	return topic + "/" + strconv.Itoa(partition)
}
