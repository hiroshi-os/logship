// Package record defines the on-disk and wire record format with a CRC32 checksum.
package record

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"time"
)

const (
	Magic      byte = 1
	headerSize      = 4 + 4 + 1 + 8 + 8 + 4 + 4 // crc, size, magic, offset, ts, keylen, vallen
)

var (
	ErrCorrupt  = errors.New("record: crc mismatch or truncated payload")
	ErrBadMagic = errors.New("record: unsupported magic")
	crcTable    = crc32.MakeTable(crc32.Castagnoli)
)

// Record is a single commit-log entry. Offset is assigned by the partition leader.
type Record struct {
	Offset    int64
	Timestamp int64
	Key       []byte
	Value     []byte
}

// Encode writes a length-prefixed, CRC-protected record.
//
// Layout:
//
//	crc32     uint32  // Castagnoli over everything after this field
//	size      uint32  // number of bytes after size (magic..value)
//	magic     uint8
//	offset    int64
//	timestamp int64
//	key_len   uint32
//	key       bytes
//	value_len uint32
//	value     bytes
func Encode(r Record) []byte {
	if r.Timestamp == 0 {
		r.Timestamp = time.Now().UnixMilli()
	}
	keyLen := len(r.Key)
	valLen := len(r.Value)
	size := 1 + 8 + 8 + 4 + keyLen + 4 + valLen
	buf := make([]byte, 8+size)
	binary.BigEndian.PutUint32(buf[4:8], uint32(size))
	buf[8] = Magic
	binary.BigEndian.PutUint64(buf[9:17], uint64(r.Offset))
	binary.BigEndian.PutUint64(buf[17:25], uint64(r.Timestamp))
	binary.BigEndian.PutUint32(buf[25:29], uint32(keyLen))
	copy(buf[29:29+keyLen], r.Key)
	off := 29 + keyLen
	binary.BigEndian.PutUint32(buf[off:off+4], uint32(valLen))
	copy(buf[off+4:], r.Value)
	crc := crc32.Checksum(buf[4:], crcTable)
	binary.BigEndian.PutUint32(buf[0:4], crc)
	return buf
}

// Decode consumes one record from r.
func Decode(r io.Reader) (Record, error) {
	var hdr [8]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return Record{}, err
	}
	wantCRC := binary.BigEndian.Uint32(hdr[0:4])
	size := binary.BigEndian.Uint32(hdr[4:8])
	if size < 1+8+8+4+4 || size > 64<<20 {
		return Record{}, fmt.Errorf("%w: implausible size %d", ErrCorrupt, size)
	}
	payload := make([]byte, size)
	if _, err := io.ReadFull(r, payload); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return Record{}, ErrCorrupt
		}
		return Record{}, err
	}
	crcInput := make([]byte, 4+len(payload))
	copy(crcInput[:4], hdr[4:8])
	copy(crcInput[4:], payload)
	got := crc32.Checksum(crcInput, crcTable)
	if got != wantCRC {
		return Record{}, fmt.Errorf("%w: crc", ErrCorrupt)
	}
	if payload[0] != Magic {
		return Record{}, ErrBadMagic
	}
	rec := Record{
		Offset:    int64(binary.BigEndian.Uint64(payload[1:9])),
		Timestamp: int64(binary.BigEndian.Uint64(payload[9:17])),
	}
	keyLen := binary.BigEndian.Uint32(payload[17:21])
	if int(21+keyLen+4) > len(payload) {
		return Record{}, ErrCorrupt
	}
	rec.Key = append([]byte(nil), payload[21:21+keyLen]...)
	valOff := 21 + int(keyLen)
	valLen := binary.BigEndian.Uint32(payload[valOff : valOff+4])
	if valOff+4+int(valLen) != len(payload) {
		return Record{}, ErrCorrupt
	}
	rec.Value = append([]byte(nil), payload[valOff+4:valOff+4+int(valLen)]...)
	return rec, nil
}

// EncodedSize returns the on-disk size of rec.
func EncodedSize(r Record) int {
	return 8 + 1 + 8 + 8 + 4 + len(r.Key) + 4 + len(r.Value)
}

// CRCOf returns the Castagnoli CRC stored for rec (used by tests).
func CRCOf(r Record) uint32 {
	enc := Encode(r)
	return binary.BigEndian.Uint32(enc[:4])
}
