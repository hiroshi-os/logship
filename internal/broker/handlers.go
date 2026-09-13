package broker

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/hiroshi-os/logship/internal/client"
	"github.com/hiroshi-os/logship/internal/protocol"
	"github.com/hiroshi-os/logship/internal/record"
)

func (b *Broker) handleHealth(w http.ResponseWriter, _ *http.Request) {
	b.writeJSON(w, http.StatusOK, map[string]any{
		"ok":         true,
		"id":         b.cfg.ID,
		"controller": b.cluster.ControllerID(),
	})
}

func (b *Broker) handleMetadata(w http.ResponseWriter, _ *http.Request) {
	b.writeJSON(w, http.StatusOK, b.publicMetadata())
}

func (b *Broker) publicMetadata() protocol.Metadata {
	md := protocol.Metadata{Controller: b.cluster.ControllerID()}
	for _, p := range b.cluster.All() {
		md.Brokers = append(md.Brokers, protocol.BrokerInfo{
			ID: p.ID, Addr: p.Addr, Alive: b.cluster.IsAlive(p.ID),
		})
	}
	for _, t := range b.snapshotTopics() {
		ti := protocol.TopicInfo{Name: t.Name}
		for _, p := range t.Partitions {
			hw, leo := int64(0), int64(0)
			if lg, ok := b.store.Get(t.Name, p.ID); ok {
				hw = lg.HighWatermark()
				leo = lg.LEO()
			}
			ti.Partitions = append(ti.Partitions, protocol.PartitionInfo{
				ID: p.ID, Leader: p.Leader, Replicas: p.Replicas, ISR: p.ISR, HW: hw, LEO: leo,
			})
		}
		md.Topics = append(md.Topics, ti)
	}
	return md
}

func (b *Broker) handleCreateTopic(w http.ResponseWriter, r *http.Request) {
	var req protocol.CreateTopicRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		b.writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.Name == "" {
		b.writeErr(w, http.StatusBadRequest, "name required")
		return
	}
	if !b.cluster.IsController() {
		addr := b.cluster.Addr(b.cluster.ControllerID())
		if addr == "" || addr == b.cfg.Advertise {
			b.writeErr(w, http.StatusServiceUnavailable, "controller unavailable")
			return
		}
		fwd := client.New([]string{addr})
		fwd.HTTP = b.cli.HTTP
		if err := fwd.CreateTopic(req.Name, req.Partitions, req.ReplicationFactor); err != nil {
			b.writeErr(w, http.StatusBadGateway, err.Error())
			return
		}
		b.writeJSON(w, http.StatusOK, map[string]string{"status": "forwarded"})
		return
	}
	if _, ok := b.topic(req.Name); ok {
		b.writeJSON(w, http.StatusOK, map[string]string{"status": "exists"})
		return
	}
	if err := b.createTopicLocked(req.Name, req.Partitions, req.ReplicationFactor); err != nil {
		b.writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	b.writeJSON(w, http.StatusOK, map[string]any{"status": "created", "topic": req.Name})
}

func (b *Broker) handleProduce(w http.ResponseWriter, r *http.Request) {
	var req protocol.ProduceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		// allow topic/key/value query for the 60s curl path
		req.Topic = r.URL.Query().Get("topic")
		req.Key = r.URL.Query().Get("key")
		req.Value = r.URL.Query().Get("value")
		req.Acks = r.URL.Query().Get("acks")
		if req.Topic == "" {
			b.writeErr(w, http.StatusBadRequest, "invalid produce body")
			return
		}
	}
	resp, err := b.produceLocal(req)
	if err != nil {
		if strings.HasPrefix(err.Error(), "not_leader:") {
			parts := strings.Split(err.Error(), ":")
			leader, _ := strconv.Atoi(parts[1])
			addr := b.cluster.Addr(leader)
			if addr != "" && addr != b.cfg.Advertise {
				proxied, perr := b.cli.Produce(addr, req)
				if perr == nil {
					b.writeJSON(w, http.StatusOK, proxied)
					return
				}
			}
			b.writeJSON(w, http.StatusConflict, protocol.ErrorBody{
				Error: "not_leader", Leader: leader, LeaderAddr: addr,
			})
			return
		}
		if strings.Contains(err.Error(), "unknown topic") {
			b.writeErr(w, http.StatusNotFound, err.Error())
			return
		}
		b.writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	b.writeJSON(w, http.StatusOK, resp)
}

func (b *Broker) produceLocal(req protocol.ProduceRequest) (protocol.ProduceResponse, error) {
	t, ok := b.topic(req.Topic)
	if !ok {
		return protocol.ProduceResponse{}, errUnknownTopic(req.Topic)
	}
	recs := req.Records
	if len(recs) == 0 {
		recs = []protocol.ProduceRecord{{Key: req.Key, Value: req.Value, Partition: req.Partition}}
	}
	acks := req.Acks
	if acks == "" {
		acks = "1"
	}
	out := protocol.ProduceResponse{Topic: req.Topic}
	for _, rec := range recs {
		pid := rec.Partition
		if req.Partition > 0 && rec.Partition == 0 && req.Key == rec.Key {
			pid = req.Partition
		}
		if pid <= 0 && rec.Partition == 0 && req.Partition <= 0 {
			pid = b.partitioner.Partition([]byte(rec.Key), len(t.Partitions))
		}
		if pid < 0 || pid >= len(t.Partitions) {
			pid = b.partitioner.Partition([]byte(rec.Key), len(t.Partitions))
		}
		pm, ok := b.partitionMeta(req.Topic, pid)
		if !ok {
			return out, errUnknownTopic(req.Topic)
		}
		if pm.Leader != b.cfg.ID {
			return out, errNotLeader(pm.Leader)
		}
		lg, err := b.store.Open(req.Topic, pid)
		if err != nil {
			return out, err
		}
		off, err := lg.Append([]byte(rec.Key), []byte(rec.Value))
		if err != nil {
			return out, err
		}
		b.noteSelfLEO(req.Topic, pid, lg.LEO())
		b.recomputeHW(req.Topic, pid)
		if acks == "all" || acks == "-1" {
			if err := lg.WaitHighWatermark(off, b.cfg.ProduceTimeout); err != nil {
				return out, err
			}
		} else {
			// acks=1: visible on the leader immediately for replica fetch;
			// consumers still wait for HW. If ISR is only the leader, bump HW.
			b.maybeSoloHW(req.Topic, pid, lg)
		}
		out.Results = append(out.Results, protocol.ProduceResult{Partition: pid, Offset: off})
	}
	return out, nil
}

func (b *Broker) maybeSoloHW(topic string, partition int, lg interface {
	SetHighWatermark(int64)
	LEO() int64
}) {
	pm, ok := b.partitionMeta(topic, partition)
	if !ok {
		return
	}
	if len(pm.ISR) == 1 && pm.ISR[0] == b.cfg.ID {
		lg.SetHighWatermark(lg.LEO())
	}
}

func (b *Broker) handleFetch(w http.ResponseWriter, r *http.Request) {
	b.serveFetch(w, r, r.URL.Query().Get("replica") == "1")
}

func (b *Broker) handleReplicaFetch(w http.ResponseWriter, r *http.Request) {
	b.serveFetch(w, r, true)
}

func (b *Broker) serveFetch(w http.ResponseWriter, r *http.Request, replica bool) {
	q := r.URL.Query()
	topic := q.Get("topic")
	part, _ := strconv.Atoi(q.Get("partition"))
	off, _ := strconv.ParseInt(q.Get("offset"), 10, 64)
	maxBytes, _ := strconv.Atoi(q.Get("max_bytes"))
	if maxBytes <= 0 {
		maxBytes = 1 << 20
	}
	pm, ok := b.partitionMeta(topic, part)
	if !ok {
		b.writeErr(w, http.StatusNotFound, "unknown topic/partition")
		return
	}
	if pm.Leader != b.cfg.ID {
		addr := b.cluster.Addr(pm.Leader)
		if addr != "" && !replica {
			fr, err := b.cli.Fetch(addr, topic, part, off, maxBytes, false)
			if err == nil {
				b.writeJSON(w, http.StatusOK, fr)
				return
			}
		}
		b.writeJSON(w, http.StatusConflict, protocol.ErrorBody{
			Error: "not_leader", Leader: pm.Leader, LeaderAddr: addr,
		})
		return
	}
	lg, err := b.store.Open(topic, part)
	if err != nil {
		b.writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	maxOff := lg.HighWatermark()
	if replica {
		maxOff = lg.LEO()
	}
	recs, err := lg.Read(off, maxBytes, maxOff)
	if err != nil {
		b.writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	b.writeJSON(w, http.StatusOK, protocol.FetchResponse{
		Topic: topic, Partition: part,
		HighWatermark: lg.HighWatermark(), LogEndOffset: lg.LEO(),
		Records: toWire(recs),
	})
}

func toWire(recs []record.Record) []protocol.WireRecord {
	out := make([]protocol.WireRecord, len(recs))
	for i, r := range recs {
		out[i] = protocol.WireRecord{
			Offset: r.Offset, Timestamp: r.Timestamp,
			Key: string(r.Key), Value: string(r.Value),
		}
	}
	return out
}

func (b *Broker) handleJoin(w http.ResponseWriter, r *http.Request) {
	if !b.forwardGroup(w, r) {
		return
	}
	var req protocol.JoinRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	res, err := b.groups.Join(r.PathValue("group"), req.MemberID, req.Topics)
	if err != nil {
		b.writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	b.writeJSON(w, http.StatusOK, protocol.JoinResponse{
		MemberID: res.MemberID, Generation: res.Generation,
		LeaderID: res.LeaderID, Members: res.Members, State: res.State,
	})
}

func (b *Broker) handleSync(w http.ResponseWriter, r *http.Request) {
	if !b.forwardGroup(w, r) {
		return
	}
	var req protocol.SyncRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	asg, gen, st, err := b.groups.Sync(r.PathValue("group"), req.MemberID, req.Generation)
	if err != nil {
		b.writeJSON(w, http.StatusConflict, protocol.HeartbeatResponse{Error: err.Error(), State: st})
		return
	}
	b.writeJSON(w, http.StatusOK, protocol.SyncResponse{Generation: gen, Assignment: asg, State: st})
}

func (b *Broker) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if !b.forwardGroup(w, r) {
		return
	}
	var req protocol.HeartbeatRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	st, err := b.groups.Heartbeat(r.PathValue("group"), req.MemberID, req.Generation)
	if err != nil {
		b.writeJSON(w, http.StatusConflict, protocol.HeartbeatResponse{Error: err.Error(), State: st})
		return
	}
	b.writeJSON(w, http.StatusOK, protocol.HeartbeatResponse{State: st})
}

func (b *Broker) handleLeave(w http.ResponseWriter, r *http.Request) {
	if !b.forwardGroup(w, r) {
		return
	}
	var req protocol.LeaveRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	b.groups.Leave(r.PathValue("group"), req.MemberID)
	b.writeJSON(w, http.StatusOK, map[string]string{"status": "left"})
}

func (b *Broker) handleCommitOffsets(w http.ResponseWriter, r *http.Request) {
	if !b.forwardGroup(w, r) {
		return
	}
	var req protocol.OffsetCommitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		b.writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	var offs []struct {
		Topic     string
		Partition int
		Offset    int64
	}
	for _, o := range req.Offsets {
		offs = append(offs, struct {
			Topic     string
			Partition int
			Offset    int64
		}{o.Topic, o.Partition, o.Offset})
	}
	if err := b.groups.Commit(r.PathValue("group"), offs); err != nil {
		b.writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	b.writeJSON(w, http.StatusOK, map[string]string{"status": "committed"})
}

func (b *Broker) handleFetchOffsets(w http.ResponseWriter, r *http.Request) {
	if !b.forwardGroup(w, r) {
		return
	}
	offs := b.groups.FetchOffsets(r.PathValue("group"), r.URL.Query().Get("topic"))
	resp := protocol.OffsetFetchResponse{}
	for _, o := range offs {
		resp.Offsets = append(resp.Offsets, protocol.OffsetCommit{
			Topic: o.Topic, Partition: o.Partition, Offset: o.Offset,
		})
	}
	b.writeJSON(w, http.StatusOK, resp)
}

func (b *Broker) handlePeerHeartbeat(w http.ResponseWriter, r *http.Request) {
	var req protocol.HeartbeatPeerRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.BrokerID != 0 {
		b.cluster.MarkSeen(req.BrokerID)
	}
	b.writeJSON(w, http.StatusOK, map[string]any{"id": b.cfg.ID, "controller": b.cluster.ControllerID()})
}

func (b *Broker) handleMetadataPush(w http.ResponseWriter, r *http.Request) {
	var push protocol.MetadataPush
	if err := json.NewDecoder(r.Body).Decode(&push); err != nil {
		b.writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	var topics []topicMeta
	for _, t := range push.Topics {
		tm := topicMeta{Name: t.Name}
		for _, p := range t.Partitions {
			tm.Partitions = append(tm.Partitions, partitionMeta{
				ID: p.ID, Leader: p.Leader, Replicas: p.Replicas, ISR: p.ISR,
			})
		}
		topics = append(topics, tm)
	}
	b.applyTopics(topics)
	b.syncFollowers()
	b.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (b *Broker) handleReplicaAck(w http.ResponseWriter, r *http.Request) {
	var req protocol.ReplicaAckRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		b.writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	b.noteReplica(req.Topic, req.Partition, req.BrokerID, req.LEO)
	b.recomputeHW(req.Topic, req.Partition)
	b.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// forwardGroup proxies group APIs to the controller (the group coordinator).
// Returns false if the request was already handled (forwarded or error).
func (b *Broker) forwardGroup(w http.ResponseWriter, r *http.Request) bool {
	if b.cluster.IsController() {
		return true
	}
	addr := b.cluster.Addr(b.cluster.ControllerID())
	if addr == "" {
		b.writeErr(w, http.StatusServiceUnavailable, "controller unavailable")
		return false
	}
	body, _ := io.ReadAll(r.Body)
	url := "http://" + addr + r.URL.Path
	if r.URL.RawQuery != "" {
		url += "?" + r.URL.RawQuery
	}
	req, err := http.NewRequest(r.Method, url, strings.NewReader(string(body)))
	if err != nil {
		b.writeErr(w, http.StatusBadGateway, err.Error())
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := b.cli.HTTP.Do(req)
	if err != nil {
		b.writeErr(w, http.StatusBadGateway, err.Error())
		return false
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
	return false
}

type topicError string

func (e topicError) Error() string { return string(e) }

func errUnknownTopic(name string) error { return topicError("unknown topic: " + name) }

type notLeaderError struct{ leader int }

func (e notLeaderError) Error() string { return "not_leader:" + strconv.Itoa(e.leader) }

func errNotLeader(leader int) error { return notLeaderError{leader: leader} }
