package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/hiroshi-os/logship/internal/protocol"
)

type Client struct {
	Brokers []string
	HTTP    *http.Client
}

func New(brokers []string) *Client {
	return &Client{
		Brokers: brokers,
		HTTP:    &http.Client{Timeout: 8 * time.Second},
	}
}

func (c *Client) pick() string {
	if len(c.Brokers) == 0 {
		return ""
	}
	return c.Brokers[0]
}

func (c *Client) doJSON(method, rawURL string, in any) (int, []byte, error) {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return 0, nil, err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, rawURL, body)
	if err != nil {
		return 0, nil, err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return resp.StatusCode, b, err
}

func (c *Client) any(method, path string, in any, out any) error {
	var last error
	for _, b := range c.Brokers {
		code, raw, err := c.doJSON(method, "http://"+b+path, in)
		if err != nil {
			last = err
			continue
		}
		if code >= 400 {
			var eb protocol.ErrorBody
			_ = json.Unmarshal(raw, &eb)
			if eb.Error == "" {
				eb.Error = string(raw)
			}
			last = fmt.Errorf("http %d: %s", code, eb.Error)
			continue
		}
		if out != nil {
			if err := json.Unmarshal(raw, out); err != nil {
				return err
			}
		}
		return nil
	}
	if last == nil {
		last = fmt.Errorf("no brokers")
	}
	return last
}

func (c *Client) Health(addr string) error {
	_, _, err := c.doJSON(http.MethodGet, "http://"+addr+"/health", nil)
	return err
}

func (c *Client) Metadata() (protocol.Metadata, error) {
	var md protocol.Metadata
	err := c.any(http.MethodGet, "/metadata", nil, &md)
	return md, err
}

func (c *Client) CreateTopic(name string, partitions, rf int) error {
	return c.any(http.MethodPost, "/topics", protocol.CreateTopicRequest{
		Name: name, Partitions: partitions, ReplicationFactor: rf,
	}, nil)
}

func (c *Client) Produce(addr string, req protocol.ProduceRequest) (protocol.ProduceResponse, error) {
	if addr == "" {
		addr = c.pick()
	}
	var out protocol.ProduceResponse
	code, raw, err := c.doJSON(http.MethodPost, "http://"+addr+"/produce", req)
	if err != nil {
		return out, err
	}
	if code >= 400 {
		var eb protocol.ErrorBody
		_ = json.Unmarshal(raw, &eb)
		return out, fmt.Errorf("%s", eb.Error)
	}
	err = json.Unmarshal(raw, &out)
	return out, err
}

func (c *Client) Fetch(addr, topic string, partition int, offset int64, maxBytes int, replica bool) (protocol.FetchResponse, error) {
	if addr == "" {
		addr = c.pick()
	}
	q := url.Values{
		"topic":     {topic},
		"partition": {fmt.Sprintf("%d", partition)},
		"offset":    {fmt.Sprintf("%d", offset)},
		"max_bytes": {fmt.Sprintf("%d", maxBytes)},
	}
	if replica {
		q.Set("replica", "1")
	}
	var out protocol.FetchResponse
	code, raw, err := c.doJSON(http.MethodGet, "http://"+addr+"/fetch?"+q.Encode(), nil)
	if err != nil {
		return out, err
	}
	if code >= 400 {
		return out, fmt.Errorf("fetch %d: %s", code, raw)
	}
	err = json.Unmarshal(raw, &out)
	return out, err
}

func (c *Client) Join(group string, req protocol.JoinRequest) (protocol.JoinResponse, error) {
	var out protocol.JoinResponse
	err := c.any(http.MethodPost, "/groups/"+url.PathEscape(group)+"/join", req, &out)
	return out, err
}

func (c *Client) Sync(group string, req protocol.SyncRequest) (protocol.SyncResponse, error) {
	var out protocol.SyncResponse
	err := c.any(http.MethodPost, "/groups/"+url.PathEscape(group)+"/sync", req, &out)
	return out, err
}

func (c *Client) Heartbeat(group string, req protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
	var out protocol.HeartbeatResponse
	err := c.any(http.MethodPost, "/groups/"+url.PathEscape(group)+"/heartbeat", req, &out)
	return out, err
}

func (c *Client) Leave(group string, memberID string) error {
	return c.any(http.MethodPost, "/groups/"+url.PathEscape(group)+"/leave", protocol.LeaveRequest{MemberID: memberID}, nil)
}

func (c *Client) CommitOffsets(group string, req protocol.OffsetCommitRequest) error {
	return c.any(http.MethodPost, "/groups/"+url.PathEscape(group)+"/offsets", req, nil)
}

func (c *Client) FetchOffsets(group, topic string) (protocol.OffsetFetchResponse, error) {
	var out protocol.OffsetFetchResponse
	path := "/groups/" + url.PathEscape(group) + "/offsets"
	if topic != "" {
		path += "?topic=" + url.QueryEscape(topic)
	}
	err := c.any(http.MethodGet, path, nil, &out)
	return out, err
}

func (c *Client) PeerHeartbeat(addr string, id int) error {
	code, _, err := c.doJSON(http.MethodPost, "http://"+addr+"/internal/heartbeat", protocol.HeartbeatPeerRequest{BrokerID: id})
	if err != nil {
		return err
	}
	if code >= 400 {
		return fmt.Errorf("heartbeat status %d", code)
	}
	return nil
}

func (c *Client) PushMetadata(addr string, push protocol.MetadataPush) error {
	code, raw, err := c.doJSON(http.MethodPost, "http://"+addr+"/internal/metadata", push)
	if err != nil {
		return err
	}
	if code >= 400 {
		return fmt.Errorf("metadata push %d: %s", code, raw)
	}
	return nil
}

func (c *Client) ReplicaAck(addr string, req protocol.ReplicaAckRequest) error {
	code, raw, err := c.doJSON(http.MethodPost, "http://"+addr+"/internal/replica/ack", req)
	if err != nil {
		return err
	}
	if code >= 400 {
		return fmt.Errorf("ack %d: %s", code, raw)
	}
	return nil
}

func (c *Client) ReplicaFetch(addr, topic string, partition int, offset int64) (protocol.FetchResponse, error) {
	return c.Fetch(addr, topic, partition, offset, 1<<20, true)
}
