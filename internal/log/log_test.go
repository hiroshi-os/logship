package log

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func tempLog(t *testing.T, maxSeg, idxInt int64) *Log {
	t.Helper()
	dir := t.TempDir()
	l, err := Open(Config{
		Dir:             dir,
		MaxSegmentBytes: maxSeg,
		IndexInterval:   idxInt,
		FsyncEvery:      1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

func TestAppendAndRead(t *testing.T) {
	l := tempLog(t, 1<<20, 4096)
	for i := 0; i < 100; i++ {
		off, err := l.Append([]byte("k"), []byte{byte(i)})
		if err != nil {
			t.Fatal(err)
		}
		if off != int64(i) {
			t.Fatalf("offset %d want %d", off, i)
		}
	}
	l.SetHighWatermark(l.LEO())
	recs, err := l.Read(0, 1<<20, l.HighWatermark())
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 100 {
		t.Fatalf("got %d records", len(recs))
	}
	if recs[50].Offset != 50 || recs[50].Value[0] != 50 {
		t.Fatalf("record 50 mismatch: %+v", recs[50])
	}
}

func TestReadFromMiddle(t *testing.T) {
	l := tempLog(t, 1<<20, 64)
	for i := 0; i < 50; i++ {
		if _, err := l.Append(nil, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	l.SetHighWatermark(50)
	recs, err := l.Read(40, 1<<20, 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 10 || recs[0].Offset != 40 {
		t.Fatalf("got %d recs starting %+v", len(recs), recs)
	}
}

func TestSegmentRotationAndSparseIndex(t *testing.T) {
	l := tempLog(t, 400, 80)
	for i := 0; i < 40; i++ {
		if _, err := l.Append([]byte("key"), bytes.Repeat([]byte("v"), 20)); err != nil {
			t.Fatal(err)
		}
	}
	if len(l.segments) < 2 {
		t.Fatalf("expected rotation, segments=%d", len(l.segments))
	}
	logs, _ := filepath.Glob(filepath.Join(l.Dir(), "*.log"))
	idxs, _ := filepath.Glob(filepath.Join(l.Dir(), "*.index"))
	if len(logs) < 2 || len(idxs) < 2 {
		t.Fatalf("files logs=%d indexes=%d", len(logs), len(idxs))
	}
	l.SetHighWatermark(l.LEO())
	recs, err := l.Read(0, 1<<20, l.LEO())
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 40 {
		t.Fatalf("cross-segment read got %d", len(recs))
	}
	mid, err := l.Read(25, 1<<20, l.LEO())
	if err != nil {
		t.Fatal(err)
	}
	if len(mid) == 0 || mid[0].Offset != 25 {
		t.Fatalf("sparse lookup failed: %+v", mid)
	}
}

func TestRecoverAfterReopen(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(Config{Dir: dir, MaxSegmentBytes: 300, IndexInterval: 64, FsyncEvery: 1})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if _, err := l.Append([]byte("k"), []byte("recover-me")); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l2, err := Open(Config{Dir: dir, MaxSegmentBytes: 300, IndexInterval: 64, FsyncEvery: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	if l2.LEO() != 20 {
		t.Fatalf("LEO after recover %d", l2.LEO())
	}
	off, err := l2.Append([]byte("k"), []byte("next"))
	if err != nil {
		t.Fatal(err)
	}
	if off != 20 {
		t.Fatalf("next offset %d", off)
	}
	l2.SetHighWatermark(l2.LEO())
	recs, err := l2.Read(0, 1<<20, l2.LEO())
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 21 {
		t.Fatalf("recovered %d records", len(recs))
	}
}

func TestConsumerCappedAtHW(t *testing.T) {
	l := tempLog(t, 1<<20, 4096)
	for i := 0; i < 10; i++ {
		if _, err := l.Append(nil, []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	// HW still 0 — consumer read should be empty.
	recs, err := l.Read(0, 1<<20, l.HighWatermark())
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 0 {
		t.Fatalf("expected no consumer-visible records, got %d", len(recs))
	}
	l.SetHighWatermark(6)
	recs, err = l.Read(0, 1<<20, l.HighWatermark())
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 6 {
		t.Fatalf("hw-capped read got %d", len(recs))
	}
}

func TestWaitHighWatermark(t *testing.T) {
	l := tempLog(t, 1<<20, 4096)
	off, err := l.Append(nil, []byte("v"))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- l.WaitHighWatermark(off, time.Second)
	}()
	time.Sleep(20 * time.Millisecond)
	l.SetHighWatermark(off + 1)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("wait did not unblock")
	}
}

func TestConcurrentRead(t *testing.T) {
	l := tempLog(t, 1<<20, 64)
	for i := 0; i < 200; i++ {
		if _, err := l.Append([]byte("k"), []byte("vvvvvvvv")); err != nil {
			t.Fatal(err)
		}
	}
	l.SetHighWatermark(l.LEO())
	errc := make(chan error, 8)
	for g := 0; g < 8; g++ {
		go func() {
			for i := 0; i < 20; i++ {
				recs, err := l.Read(int64(i*5), 4096, l.LEO())
				if err != nil {
					errc <- err
					return
				}
				if len(recs) == 0 || recs[0].Offset != int64(i*5) {
					errc <- fmt.Errorf("want offset %d got %v", i*5, recs)
					return
				}
			}
			errc <- nil
		}()
	}
	for g := 0; g < 8; g++ {
		if err := <-errc; err != nil {
			t.Fatal(err)
		}
	}
}

func TestCRCOnDisk(t *testing.T) {
	l := tempLog(t, 1<<20, 4096)
	if _, err := l.Append([]byte("k"), []byte("good")); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	// Flip a payload byte in the log file.
	logs, err := filepath.Glob(filepath.Join(l.Dir(), "*.log"))
	if err != nil || len(logs) == 0 {
		t.Fatal(err)
	}
	data, err := os.ReadFile(logs[0])
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xff
	if err := os.WriteFile(logs[0], data, 0o644); err != nil {
		t.Fatal(err)
	}
	l2, err := Open(Config{Dir: l.Dir(), FsyncEvery: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	// Torn/corrupt tail is truncated on recover — LEO should be 0.
	if l2.LEO() != 0 {
		t.Fatalf("expected corrupt tail truncated, LEO=%d", l2.LEO())
	}
}
