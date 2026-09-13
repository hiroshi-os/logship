package log

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/hiroshi-os/logship/internal/record"
)

type segment struct {
	baseOffset      int64
	dir             string
	logFile         *os.File
	idx             *sparseIndex
	size            int64
	nextOffset      int64
	bytesSinceIndex int64
	indexInterval   int64
}

func segmentPaths(dir string, base int64) (logPath, indexPath string) {
	name := fmt.Sprintf("%020d", base)
	return filepath.Join(dir, name+".log"), filepath.Join(dir, name+".index")
}

func createSegment(dir string, base int64, indexInterval int64) (*segment, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	logPath, indexPath := segmentPaths(dir, base)
	lf, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}
	idx, err := openIndex(indexPath)
	if err != nil {
		_ = lf.Close()
		return nil, err
	}
	return &segment{
		baseOffset:    base,
		dir:           dir,
		logFile:       lf,
		idx:           idx,
		nextOffset:    base,
		indexInterval: indexInterval,
	}, nil
}

func openSegment(dir string, base int64, indexInterval int64, active bool) (*segment, error) {
	logPath, indexPath := segmentPaths(dir, base)
	lf, err := os.OpenFile(logPath, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	st, err := lf.Stat()
	if err != nil {
		_ = lf.Close()
		return nil, err
	}
	idx, err := openIndex(indexPath)
	if err != nil {
		_ = lf.Close()
		return nil, err
	}
	s := &segment{
		baseOffset:    base,
		dir:           dir,
		logFile:       lf,
		idx:           idx,
		size:          st.Size(),
		indexInterval: indexInterval,
	}
	if err := s.recover(active); err != nil {
		_ = s.close()
		return nil, err
	}
	return s, nil
}

func (s *segment) recover(scanAll bool) error {
	if s.size == 0 {
		s.nextOffset = s.baseOffset
		return nil
	}
	if !scanAll && len(s.idx.entries) > 0 {
		last := s.idx.entries[len(s.idx.entries)-1]
		// Scan from last index entry to find the true end.
		if _, err := s.logFile.Seek(int64(last.Position), io.SeekStart); err != nil {
			return err
		}
		off := s.baseOffset + int64(last.RelOffset)
		pos := int64(last.Position)
		for {
			rec, err := record.Decode(s.logFile)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				// Truncate torn write.
				if err := s.logFile.Truncate(pos); err != nil {
					return err
				}
				s.size = pos
				break
			}
			off = rec.Offset + 1
			pos += int64(record.EncodedSize(rec))
		}
		s.nextOffset = off
		s.size = pos
		if _, err := s.logFile.Seek(0, io.SeekEnd); err != nil {
			return err
		}
		return nil
	}
	if _, err := s.logFile.Seek(0, io.SeekStart); err != nil {
		return err
	}
	var pos int64
	next := s.baseOffset
	s.idx.entries = s.idx.entries[:0]
	if err := s.idx.file.Truncate(0); err != nil {
		return err
	}
	if _, err := s.idx.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	var since int64
	for {
		rec, err := record.Decode(s.logFile)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			if err := s.logFile.Truncate(pos); err != nil {
				return err
			}
			break
		}
		if since == 0 || since >= s.indexInterval {
			if err := s.idx.append(uint32(rec.Offset-s.baseOffset), uint32(pos)); err != nil {
				return err
			}
			since = 0
		}
		sz := int64(record.EncodedSize(rec))
		pos += sz
		since += sz
		next = rec.Offset + 1
	}
	s.nextOffset = next
	s.size = pos
	s.bytesSinceIndex = since
	_, err := s.logFile.Seek(0, io.SeekEnd)
	return err
}

func (s *segment) append(rec record.Record) (int64, error) {
	if rec.Offset != s.nextOffset {
		return 0, fmt.Errorf("segment: offset mismatch want %d got %d", s.nextOffset, rec.Offset)
	}
	pos := s.size
	enc := record.Encode(rec)
	if _, err := s.logFile.Write(enc); err != nil {
		return 0, err
	}
	if s.bytesSinceIndex == 0 || s.bytesSinceIndex >= s.indexInterval {
		if err := s.idx.append(uint32(rec.Offset-s.baseOffset), uint32(pos)); err != nil {
			return 0, err
		}
		s.bytesSinceIndex = 0
	}
	s.bytesSinceIndex += int64(len(enc))
	s.size += int64(len(enc))
	s.nextOffset++
	return rec.Offset, nil
}

func (s *segment) readFrom(fromOffset int64, maxBytes int, maxOffset int64) ([]record.Record, error) {
	if fromOffset < s.baseOffset || fromOffset >= s.nextOffset {
		return nil, nil
	}
	if maxOffset <= fromOffset {
		return nil, nil
	}
	rel := uint32(fromOffset - s.baseOffset)
	pos := int64(s.idx.lookup(rel))
	var out []record.Record
	var n int
	for pos < s.size {
		rec, sz, err := record.DecodeAt(s.logFile, pos)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		pos += int64(sz)
		if rec.Offset < fromOffset {
			continue
		}
		if rec.Offset >= maxOffset {
			break
		}
		if n > 0 && n+sz > maxBytes {
			break
		}
		out = append(out, rec)
		n += sz
		if n >= maxBytes {
			break
		}
	}
	return out, nil
}

func (s *segment) contains(offset int64) bool {
	return offset >= s.baseOffset && offset < s.nextOffset
}

func (s *segment) flush() error {
	if err := s.logFile.Sync(); err != nil {
		return err
	}
	return s.idx.file.Sync()
}

func (s *segment) close() error {
	var err error
	if s.logFile != nil {
		err = s.logFile.Close()
	}
	if s.idx != nil {
		if e := s.idx.close(); e != nil && err == nil {
			err = e
		}
	}
	return err
}
