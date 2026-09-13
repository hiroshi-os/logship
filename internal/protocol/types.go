// Package protocol is the HTTP JSON contract shared by brokers and clients.
package protocol

type ErrorBody struct {
	Error      string `json:"error"`
	Leader     int    `json:"leader,omitempty"`
	LeaderAddr string `json:"leader_addr,omitempty"`
}

type CreateTopicRequest struct {
	Name              string `json:"name"`
	Partitions        int    `json:"partitions"`
	ReplicationFactor int    `json:"replication_factor"`
}

type BrokerInfo struct {
	ID    int    `json:"id"`
	Addr  string `json:"addr"`
	Alive bool   `json:"alive"`
}

type PartitionInfo struct {
	ID       int   `json:"id"`
	Leader   int   `json:"leader"`
	Replicas []int `json:"replicas"`
	ISR      []int `json:"isr"`
	HW       int64 `json:"hw"`
	LEO      int64 `json:"leo"`
}

type TopicInfo struct {
	Name       string          `json:"name"`
	Partitions []PartitionInfo `json:"partitions"`
}

type Metadata struct {
	Controller int          `json:"controller"`
	Brokers    []BrokerInfo `json:"brokers"`
	Topics     []TopicInfo  `json:"topics"`
}

type ProduceRequest struct {
	Topic     string          `json:"topic"`
	Partition int             `json:"partition"`
	Key       string          `json:"key"`
	Value     string          `json:"value"`
	Acks      string          `json:"acks"`
	Records   []ProduceRecord `json:"records,omitempty"`
}

type ProduceRecord struct {
	Key       string `json:"key"`
	Value     string `json:"value"`
	Partition int    `json:"partition"`
}

type ProduceResponse struct {
	Topic   string          `json:"topic"`
	Results []ProduceResult `json:"results"`
}

type ProduceResult struct {
	Partition int   `json:"partition"`
	Offset    int64 `json:"offset"`
}

type WireRecord struct {
	Offset    int64  `json:"offset"`
	Timestamp int64  `json:"timestamp"`
	Key       string `json:"key"`
	Value     string `json:"value"`
}

type FetchResponse struct {
	Topic         string       `json:"topic"`
	Partition     int          `json:"partition"`
	HighWatermark int64        `json:"high_watermark"`
	LogEndOffset  int64        `json:"log_end_offset"`
	Records       []WireRecord `json:"records"`
}

type JoinRequest struct {
	MemberID string   `json:"member_id"`
	Topics   []string `json:"topics"`
}

type JoinResponse struct {
	MemberID   string   `json:"member_id"`
	Generation int      `json:"generation"`
	LeaderID   string   `json:"leader_id"`
	Members    []string `json:"members,omitempty"`
	State      string   `json:"state"`
}

type SyncRequest struct {
	MemberID   string `json:"member_id"`
	Generation int    `json:"generation"`
}

type SyncResponse struct {
	Generation int              `json:"generation"`
	Assignment map[string][]int `json:"assignment"`
	State      string           `json:"state"`
}

type HeartbeatRequest struct {
	MemberID   string `json:"member_id"`
	Generation int    `json:"generation"`
}

type HeartbeatResponse struct {
	Error string `json:"error,omitempty"`
	State string `json:"state"`
}

type LeaveRequest struct {
	MemberID string `json:"member_id"`
}

type OffsetCommitRequest struct {
	MemberID   string         `json:"member_id"`
	Generation int            `json:"generation"`
	Offsets    []OffsetCommit `json:"offsets"`
}

type OffsetCommit struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
}

type OffsetFetchResponse struct {
	Offsets []OffsetCommit `json:"offsets"`
}

type ReplicaAckRequest struct {
	BrokerID  int    `json:"broker_id"`
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	LEO       int64  `json:"leo"`
}

type HeartbeatPeerRequest struct {
	BrokerID int `json:"broker_id"`
}

type TopicSnapshot struct {
	Name       string          `json:"name"`
	Partitions []PartitionInfo `json:"partitions"`
}

type MetadataPush struct {
	Controller int             `json:"controller"`
	Topics     []TopicSnapshot `json:"topics"`
}
