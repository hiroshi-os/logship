// Package log implements a segmented, CRC-protected commit log with a sparse index.
package log

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hiroshi-os/logship/internal/record"
)

const (
	DefaultMaxSegmentBytes int64 = 1 << 20
	DefaultIndexInterval   int64 = 4096
)

// Config controls on-disk layout and durability.
type Config struct {
	Dir             string
	MaxSegmentBytes int64
	IndexInterval   int64
	// FsyncEvery, when > 0, calls fsync after that many appends. 0 means
	// rely on periodic flush / Close (page cache). 1 is fully synchronous.
	FsyncEvery int
}

// Log is a durable append-only sequence of records identified by monotonic offsets.
type Log struct {
	mu       sync.RWMutex
	hwCond   *sync.Cond
	cfg      Config
	segments []*segment
	active   *segment
	hw       int64 // high watermark: next offset visible to consumers
	appends  int
}

// Open creates or recovers a partition log in dir.
func Open(cfg Config) (*Log, error) {
	if cfg.MaxSegmentBytes <= 0 {
		cfg.MaxSegmentBytes = DefaultMaxSegmentBytes
	}
	if cfg.IndexInterval <= 0 {
		cfg.IndexInterval = DefaultIndexInterval
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, err
	}
	l := &Log{cfg: cfg}
	l.hwCond = sync.NewCond(&l.mu)
	if err := l.recover(); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *Log) recover() error {
	bases, err := listSegmentBases(l.cfg.Dir)
	if err != nil {
		return err
	}
	if len(bases) == 0 {
		s, err := createSegment(l.cfg.Dir, 0, l.cfg.IndexInterval)
		if err != nil {
			return err
		}
		l.segments = []*segment{s}
		l.active = s
		return nil
	}
	for i, base := range bases {
		s, err := openSegment(l.cfg.Dir, base, l.cfg.IndexInterval, i == len(bases)-1)
		if err != nil {
			return fmt.Errorf("open segment %d: %w", base, err)
		}
		l.segments = append(l.segments, s)
	}
	l.active = l.segments[len(l.segments)-1]
	l.hw = 0
	return nil
}

func listSegmentBases(dir string) ([]int64, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var bases []int64
	for _, e := range ents {
		name := e.Name()
		if !strings.HasSuffix(name, ".log") {
			continue
		}
		n, err := strconv.ParseInt(strings.TrimSuffix(name, ".log"), 10, 64)
		if err != nil {
			continue
		}
		bases = append(bases, n)
	}
	sort.Slice(bases, func(i, j int) bool { return bases[i] < bases[j] })
	return bases, nil
}

// Append assigns the next offset and writes rec (timestamp filled if zero).
func (l *Log) Append(key, value []byte) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	rec := record.Record{
		Offset:    l.active.nextOffset,
		Timestamp: time.Now().UnixMilli(),
		Key:       key,
		Value:     value,
	}
	return l.appendLocked(rec)
}

// AppendAt writes a record with a leader-assigned offset (follower / recovery path).
func (l *Log) AppendAt(rec record.Record) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if rec.Offset != l.active.nextOffset {
		return 0, fmt.Errorf("log: unexpected offset want %d got %d", l.active.nextOffset, rec.Offset)
	}
	return l.appendLocked(rec)
}

func (l *Log) appendLocked(rec record.Record) (int64, error) {
	if l.active.size > 0 && l.active.size+int64(record.EncodedSize(rec)) > l.cfg.MaxSegmentBytes {
		if err := l.rotateLocked(); err != nil {
			return 0, err
		}
		if rec.Offset != l.active.nextOffset {
			rec.Offset = l.active.nextOffset
		}
	}
	off, err := l.active.append(rec)
	if err != nil {
		return 0, err
	}
	l.appends++
	if l.cfg.FsyncEvery > 0 && l.appends%l.cfg.FsyncEvery == 0 {
		if err := l.active.flush(); err != nil {
			return 0, err
		}
	}
	return off, nil
}

func (l *Log) rotateLocked() error {
	if err := l.active.flush(); err != nil {
		return err
	}
	s, err := createSegment(l.cfg.Dir, l.active.nextOffset, l.cfg.IndexInterval)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	l.active = s
	return nil
}

// Read returns records in [from, min(LEO, maxOffset)) totaling up to maxBytes.
func (l *Log) Read(from int64, maxBytes int, maxOffset int64) ([]record.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if maxBytes <= 0 {
		maxBytes = 1 << 20
	}
	if maxOffset < 0 {
		maxOffset = l.active.nextOffset
	}
	if from >= l.active.nextOffset || from >= maxOffset {
		return nil, nil
	}
	var out []record.Record
	var n int
	for _, s := range l.segments {
		if s.nextOffset <= from {
			continue
		}
		if s.baseOffset >= maxOffset {
			break
		}
		recs, err := s.readFrom(from, maxBytes-n, maxOffset)
		if err != nil {
			return nil, err
		}
		out = append(out, recs...)
		for _, r := range recs {
			n += record.EncodedSize(r)
		}
		if n >= maxBytes {
			break
		}
		if len(recs) > 0 {
			from = recs[len(recs)-1].Offset + 1
		} else if s.nextOffset > from {
			from = s.nextOffset
		}
	}
	return out, nil
}

// LEO is the next offset that will be assigned (log end offset).
func (l *Log) LEO() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.active.nextOffset
}

// HighWatermark is the next offset consumers may read (exclusive).
func (l *Log) HighWatermark() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.hw
}

// SetHighWatermark advances (never retreats) the consumer-visible offset.
func (l *Log) SetHighWatermark(hw int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if hw > l.active.nextOffset {
		hw = l.active.nextOffset
	}
	if hw > l.hw {
		l.hw = hw
		l.hwCond.Broadcast()
	}
}

// WaitHighWatermark blocks until HW > offset or timeout.
func (l *Log) WaitHighWatermark(offset int64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	l.mu.Lock()
	defer l.mu.Unlock()
	for l.hw <= offset {
		if timeout <= 0 {
			return fmt.Errorf("log: hw wait timeout")
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return fmt.Errorf("log: hw wait timeout")
		}
		timer := time.AfterFunc(remaining, func() {
			l.mu.Lock()
			l.hwCond.Broadcast()
			l.mu.Unlock()
		})
		l.hwCond.Wait()
		timer.Stop()
		if time.Now().After(deadline) && l.hw <= offset {
			return fmt.Errorf("log: hw wait timeout")
		}
	}
	return nil
}

// Dir returns the partition data directory.
func (l *Log) Dir() string { return l.cfg.Dir }

// Close flushes and closes all segments.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var err error
	for _, s := range l.segments {
		if e := s.flush(); e != nil && err == nil {
			err = e
		}
		if e := s.close(); e != nil && err == nil {
			err = e
		}
	}
	return err
}

// BaseDir is a helper for tests listing files.
func BaseDir(dir string) string { return filepath.Clean(dir) }
