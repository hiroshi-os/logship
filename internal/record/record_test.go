package record

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	in := Record{Offset: 42, Timestamp: 1_700_000_000_000, Key: []byte("user-1"), Value: []byte("hello")}
	enc := Encode(in)
	out, err := Decode(bytes.NewReader(enc))
	if err != nil {
		t.Fatal(err)
	}
	if out.Offset != in.Offset || out.Timestamp != in.Timestamp {
		t.Fatalf("meta mismatch: %+v vs %+v", out, in)
	}
	if !bytes.Equal(out.Key, in.Key) || !bytes.Equal(out.Value, in.Value) {
		t.Fatalf("payload mismatch: %+v vs %+v", out, in)
	}
}

func TestEmptyKeyAndValue(t *testing.T) {
	in := Record{Offset: 0, Timestamp: 1, Key: nil, Value: nil}
	out, err := Decode(bytes.NewReader(Encode(in)))
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Key) != 0 || len(out.Value) != 0 {
		t.Fatalf("expected empty payloads, got %+v", out)
	}
}

func TestCRCDetectsCorruption(t *testing.T) {
	enc := Encode(Record{Offset: 7, Timestamp: 9, Key: []byte("k"), Value: []byte("payload")})
	enc[len(enc)-1] ^= 0xff
	_, err := Decode(bytes.NewReader(enc))
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}
}

func TestCRCDetectsHeaderTamper(t *testing.T) {
	enc := Encode(Record{Offset: 1, Timestamp: 2, Key: []byte("a"), Value: []byte("b")})
	enc[0] ^= 0xff
	_, err := Decode(bytes.NewReader(enc))
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}
}

func TestDecodeTruncated(t *testing.T) {
	enc := Encode(Record{Offset: 1, Timestamp: 2, Key: []byte("k"), Value: []byte("v")})
	_, err := Decode(bytes.NewReader(enc[:5]))
	if err == nil {
		t.Fatal("expected error on truncated record")
	}
}

func TestCRCStable(t *testing.T) {
	r := Record{Offset: 3, Timestamp: 4, Key: []byte("k"), Value: []byte("v")}
	if CRCOf(r) != CRCOf(r) {
		t.Fatal("CRC not deterministic")
	}
	if CRCOf(r) == 0 {
		t.Fatal("CRC unexpectedly zero")
	}
}

func TestEncodedSizeMatchesBytes(t *testing.T) {
	r := Record{Offset: 9, Timestamp: 8, Key: []byte("abc"), Value: []byte("xyz")}
	if EncodedSize(r) != len(Encode(r)) {
		t.Fatalf("size %d != encoded %d", EncodedSize(r), len(Encode(r)))
	}
}

func TestDecodeAtMatchesDecode(t *testing.T) {
	in := Record{Offset: 5, Timestamp: 6, Key: []byte("k"), Value: []byte("v")}
	enc := Encode(in)
	out, n, err := DecodeAt(bytes.NewReader(enc), 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(enc) {
		t.Fatalf("n=%d want %d", n, len(enc))
	}
	if out.Offset != in.Offset || string(out.Value) != "v" {
		t.Fatalf("%+v", out)
	}
}

func TestDecodeEOF(t *testing.T) {
	_, err := Decode(bytes.NewReader(nil))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}
}
