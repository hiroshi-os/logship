package log

import (
	"encoding/binary"
	"io"
	"os"
	"sort"
)

// indexEntry maps a relative offset to a byte position in the companion .log file.
type indexEntry struct {
	RelOffset uint32
	Position  uint32
}

// sparseIndex is a Kafka-style sparse offset index. Not every record is indexed;
// lookup returns the nearest preceding position and the caller scans forward.
type sparseIndex struct {
	path    string
	file    *os.File
	entries []indexEntry
}

func openIndex(path string) (*sparseIndex, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	idx := &sparseIndex{path: path, file: f}
	if err := idx.load(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return idx, nil
}

func (i *sparseIndex) load() error {
	st, err := i.file.Stat()
	if err != nil {
		return err
	}
	n := int(st.Size() / 8)
	if n == 0 {
		return nil
	}
	buf := make([]byte, n*8)
	if _, err := i.file.ReadAt(buf, 0); err != nil && err != io.EOF {
		return err
	}
	i.entries = make([]indexEntry, n)
	for k := 0; k < n; k++ {
		i.entries[k] = indexEntry{
			RelOffset: binary.BigEndian.Uint32(buf[k*8 : k*8+4]),
			Position:  binary.BigEndian.Uint32(buf[k*8+4 : k*8+8]),
		}
	}
	return nil
}

func (i *sparseIndex) append(relOffset uint32, position uint32) error {
	var buf [8]byte
	binary.BigEndian.PutUint32(buf[0:4], relOffset)
	binary.BigEndian.PutUint32(buf[4:8], position)
	if _, err := i.file.Write(buf[:]); err != nil {
		return err
	}
	i.entries = append(i.entries, indexEntry{RelOffset: relOffset, Position: position})
	return nil
}

// lookup returns the byte position of the greatest indexed offset <= relOffset.
func (i *sparseIndex) lookup(relOffset uint32) uint32 {
	if len(i.entries) == 0 {
		return 0
	}
	n := sort.Search(len(i.entries), func(j int) bool {
		return i.entries[j].RelOffset > relOffset
	})
	if n == 0 {
		// No entry at or before the target — scan from the start of the segment.
		return 0
	}
	return i.entries[n-1].Position
}

func (i *sparseIndex) close() error {
	if i.file == nil {
		return nil
	}
	return i.file.Close()
}
