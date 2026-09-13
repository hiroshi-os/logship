package store

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/hiroshi-os/logship/internal/log"
)

// Store owns per-partition commit logs under data/topics/<topic>/<partition>.
type Store struct {
	mu     sync.RWMutex
	root   string
	logCfg log.Config
	logs   map[string]*log.Log
}

func New(root string, fsyncEvery int, maxSeg, idxInt int64) *Store {
	cfg := log.Config{
		MaxSegmentBytes: maxSeg,
		IndexInterval:   idxInt,
		FsyncEvery:      fsyncEvery,
	}
	return &Store{root: root, logCfg: cfg, logs: map[string]*log.Log{}}
}

func key(topic string, partition int) string {
	return topic + "/" + strconv.Itoa(partition)
}

func (s *Store) Open(topic string, partition int) (*log.Log, error) {
	k := key(topic, partition)
	s.mu.Lock()
	defer s.mu.Unlock()
	if l, ok := s.logs[k]; ok {
		return l, nil
	}
	cfg := s.logCfg
	cfg.Dir = filepath.Join(s.root, "topics", topic, strconv.Itoa(partition))
	l, err := log.Open(cfg)
	if err != nil {
		return nil, err
	}
	s.logs[k] = l
	return l, nil
}

func (s *Store) Get(topic string, partition int) (*log.Log, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	l, ok := s.logs[key(topic, partition)]
	return l, ok
}

// LoadExisting opens any partition directories already on disk.
func (s *Store) LoadExisting() error {
	root := filepath.Join(s.root, "topics")
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	topics, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	for _, t := range topics {
		if !t.IsDir() {
			continue
		}
		parts, err := os.ReadDir(filepath.Join(root, t.Name()))
		if err != nil {
			return err
		}
		for _, p := range parts {
			if !p.IsDir() {
				continue
			}
			n, err := strconv.Atoi(p.Name())
			if err != nil {
				continue
			}
			if _, err := s.Open(t.Name(), n); err != nil {
				return fmt.Errorf("open %s/%d: %w", t.Name(), n, err)
			}
		}
	}
	return nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var err error
	for _, l := range s.logs {
		if e := l.Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
}

func ParseKey(k string) (topic string, partition int, ok bool) {
	i := strings.LastIndex(k, "/")
	if i < 0 {
		return "", 0, false
	}
	n, err := strconv.Atoi(k[i+1:])
	if err != nil {
		return "", 0, false
	}
	return k[:i], n, true
}
