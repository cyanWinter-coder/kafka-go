package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go/compress"
	"github.com/segmentio/kafka-go/compress/gzip"
	"github.com/segmentio/kafka-go/compress/zstd"
)

// testRecord describes a record to encode into a v2 record batch for tests.
type testRecord struct {
	offsetDelta    int64
	timestampDelta int64
	key            []byte // nil encodes a null key
	value          []byte // nil encodes a null value (tombstone)
	headers        []Header
}

func appendTestVarInt(b []byte, i int64) []byte {
	x := uint64((i << 1) ^ (i >> 63)) // zig-zag
	for x >= 0x80 {
		b = append(b, byte(x)|0x80)
		x >>= 7
	}
	return append(b, byte(x))
}

func encodeTestRecord(r testRecord) []byte {
	body := []byte{0} // record attributes
	body = appendTestVarInt(body, r.timestampDelta)
	body = appendTestVarInt(body, r.offsetDelta)
	body = appendTestVarNullBytes(body, r.key)
	body = appendTestVarNullBytes(body, r.value)
	body = appendTestVarInt(body, int64(len(r.headers)))
	for _, h := range r.headers {
		body = appendTestVarInt(body, int64(len(h.Key)))
		body = append(body, h.Key...)
		body = appendTestVarNullBytes(body, h.Value)
	}
	out := appendTestVarInt(nil, int64(len(body)))
	return append(out, body...)
}

func appendTestVarNullBytes(b, value []byte) []byte {
	if value == nil {
		return appendTestVarInt(b, -1)
	}
	b = appendTestVarInt(b, int64(len(value)))
	return append(b, value...)
}

func buildTestV2Batch(baseOffset int64, attributes int16, numRecords int32, recordsSection []byte) []byte {
	buf := make([]byte, 0, recordBatchHeaderSizeV2+12+len(recordsSection))

	var scratch [8]byte
	binary.BigEndian.PutUint64(scratch[:], uint64(baseOffset))
	buf = append(buf, scratch[:8]...)
	buf = append(buf, 0, 0, 0, 0) // batch length placeholder
	binary.BigEndian.PutUint32(scratch[:], ^uint32(0))
	buf = append(buf, scratch[:4]...) // partition leader epoch (-1)
	buf = append(buf, 2)              // magic
	buf = append(buf, 0, 0, 0, 0)     // crc placeholder
	binary.BigEndian.PutUint16(scratch[:], uint16(attributes))
	buf = append(buf, scratch[:2]...)
	buf = append(buf, 0, 0, 0, 0)             // last offset delta
	buf = append(buf, 0, 0, 0, 0, 0, 0, 0, 0) // first timestamp
	buf = append(buf, 0, 0, 0, 0, 0, 0, 0, 0) // max timestamp
	buf = append(buf, 0, 0, 0, 0, 0, 0, 0, 0) // producer id
	buf = append(buf, 0, 0)                   // producer epoch
	buf = append(buf, 0, 0, 0, 0)             // base sequence
	binary.BigEndian.PutUint32(scratch[:], uint32(numRecords))
	buf = append(buf, scratch[:4]...)
	buf = append(buf, recordsSection...)

	batchLength := int32(len(buf) - 12)
	binary.BigEndian.PutUint32(buf[8:12], uint32(batchLength))

	crc := crc32.Checksum(buf[21:], crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(buf[17:21], crc)

	return buf
}

func encodeTestRecords(records ...testRecord) []byte {
	var out []byte
	for _, r := range records {
		out = append(out, encodeTestRecord(r)...)
	}
	return out
}

func readTestRecordSet(t *testing.T, batch []byte) (RecordSet, error) {
	t.Helper()
	var rs RecordSet
	d := &decoder{reader: bytes.NewReader(batch), remain: len(batch)}
	err := rs.readFromVersion2(d)
	return rs, err
}

// snapshotRecord copies a record, since RecordReader implementations may reuse
// the memory backing the records they return.
type snapshotRecord struct {
	offset  int64
	time    time.Time
	key     []byte
	value   []byte
	headers []Header
}

func readAllTestRecords(t *testing.T, records RecordReader) []snapshotRecord {
	t.Helper()
	var out []snapshotRecord
	for {
		r, err := records.ReadRecord()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("unexpected error reading records: %v", err)
		}
		s := snapshotRecord{
			offset: r.Offset,
			time:   r.Time,
			key:    readTestBytes(t, r.Key),
			value:  readTestBytes(t, r.Value),
		}
		for _, h := range r.Headers {
			s.headers = append(s.headers, Header{Key: h.Key, Value: append([]byte(nil), h.Value...)})
		}
		out = append(out, s)
	}
}

func readTestBytes(t *testing.T, b Bytes) []byte {
	t.Helper()
	if b == nil {
		return nil
	}
	v, err := ReadAll(b)
	if err != nil {
		t.Fatalf("failed to read bytes: %v", err)
	}
	return v
}

func TestRecordBatchV2OffsetsAndHoles(t *testing.T) {
	const baseOffset = 100
	records := []testRecord{
		{offsetDelta: 0, key: []byte("a"), value: []byte("value-a")},
		// Offsets 1 and 2 were compacted away: the batch contains a hole.
		{offsetDelta: 3, key: []byte("d"), value: nil}, // tombstone
		{offsetDelta: 4, key: nil, value: []byte("value-e"), headers: []Header{
			{Key: "h", Value: []byte("v")},
		}},
	}
	batch := buildTestV2Batch(baseOffset, 0, int32(len(records)), encodeTestRecords(records...))

	rs, err := readTestRecordSet(t, batch)
	if err != nil {
		t.Fatalf("failed to read record batch: %v", err)
	}
	if rs.Version != 2 {
		t.Errorf("unexpected version: %d", rs.Version)
	}

	found := readAllTestRecords(t, rs.Records)
	if len(found) != len(records) {
		t.Fatalf("expected %d records, got %d", len(records), len(found))
	}

	for i, want := range records {
		got := found[i]
		if wantOffset := baseOffset + want.offsetDelta; got.offset != wantOffset {
			t.Errorf("record %d: expected offset %d, got %d", i, wantOffset, got.offset)
		}
		if !bytes.Equal(got.key, want.key) {
			t.Errorf("record %d: expected key %q, got %q", i, want.key, got.key)
		}
		if !bytes.Equal(got.value, want.value) {
			t.Errorf("record %d: expected value %q, got %q", i, want.value, got.value)
		}
		if len(got.headers) != len(want.headers) {
			t.Errorf("record %d: expected %d headers, got %d", i, len(want.headers), len(got.headers))
			continue
		}
		for j, h := range want.headers {
			if got.headers[j].Key != h.Key || !bytes.Equal(got.headers[j].Value, h.Value) {
				t.Errorf("record %d header %d mismatch: %+v != %+v", i, j, got.headers[j], h)
			}
		}
	}

	if last := rs.Records.(*RecordBatch); last.BaseOffset != baseOffset {
		t.Errorf("expected base offset %d, got %d", baseOffset, last.BaseOffset)
	}
}

func TestRecordBatchV2CompressedOffsetsAndHoles(t *testing.T) {
	const baseOffset = 42
	records := []testRecord{
		{offsetDelta: 0, key: []byte("a"), value: []byte("value-a")},
		{offsetDelta: 2, key: []byte("c"), value: nil}, // tombstone after a hole
		{offsetDelta: 5, key: []byte("f"), value: []byte("value-f")},
	}

	for _, codec := range []struct {
		name  string
		codec compress.Codec
		attr  int16
	}{
		{name: "gzip", codec: new(gzip.Codec), attr: int16(compress.Gzip)},
		{name: "zstd", codec: new(zstd.Codec), attr: int16(compress.Zstd)},
	} {
		t.Run(codec.name, func(t *testing.T) {
			var compressed bytes.Buffer
			w := codec.codec.NewWriter(&compressed)
			if _, err := w.Write(encodeTestRecords(records...)); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}

			batch := buildTestV2Batch(baseOffset, codec.attr, int32(len(records)), compressed.Bytes())
			rs, err := readTestRecordSet(t, batch)
			if err != nil {
				t.Fatalf("failed to read compressed record batch: %v", err)
			}

			found := readAllTestRecords(t, rs.Records)
			if len(found) != len(records) {
				t.Fatalf("expected %d records, got %d", len(records), len(found))
			}
			for i, want := range records {
				got := found[i]
				if wantOffset := baseOffset + want.offsetDelta; got.offset != wantOffset {
					t.Errorf("record %d: expected offset %d, got %d", i, wantOffset, got.offset)
				}
				if !bytes.Equal(got.key, want.key) {
					t.Errorf("record %d: expected key %q, got %q", i, want.key, got.key)
				}
				if !bytes.Equal(got.value, want.value) {
					t.Errorf("record %d: expected value %q, got %q", i, want.value, got.value)
				}
			}
		})
	}
}

func TestRecordBatchV2NegativeOffsetDelta(t *testing.T) {
	records := encodeTestRecords(testRecord{offsetDelta: 0, key: []byte("a")})
	// Overwrite the offset delta of the single record with -1 by encoding the
	// record manually.
	body := []byte{0}
	body = appendTestVarInt(body, 0)  // timestamp delta
	body = appendTestVarInt(body, -1) // negative offset delta
	body = appendTestVarInt(body, 1)
	body = append(body, 'a')
	body = appendTestVarInt(body, -1)
	body = appendTestVarInt(body, 0)
	badRecord := appendTestVarInt(nil, int64(len(body)))
	badRecord = append(badRecord, body...)
	batch := buildTestV2Batch(10, 0, 1, badRecord)
	_ = records

	_, err := readTestRecordSet(t, batch)
	if err == nil || !strings.Contains(err.Error(), "negative offset delta") {
		t.Fatalf("expected a negative offset delta error, got %v", err)
	}
}

func TestRecordBatchV2CRCMismatch(t *testing.T) {
	batch := buildTestV2Batch(0, 0, 1, encodeTestRecords(testRecord{value: []byte("value")}))
	batch[len(batch)-1] ^= 0xff // corrupt the record data, CRC no longer matches

	_, err := readTestRecordSet(t, batch)
	if err == nil || !strings.Contains(err.Error(), "crc32 checksum mismatch") {
		t.Fatalf("expected a crc32 checksum mismatch error, got %v", err)
	}
}

func TestRecordBatchV2TrailingBytes(t *testing.T) {
	records := encodeTestRecords(testRecord{value: []byte("value")})
	records = append(records, 0x00) // extra byte not accounted for by any record
	batch := buildTestV2Batch(0, 0, 1, records)

	_, err := readTestRecordSet(t, batch)
	if err == nil || !strings.Contains(err.Error(), "trailing bytes") {
		t.Fatalf("expected a trailing bytes error, got %v", err)
	}
}

func TestRecordBatchV2TruncatedTail(t *testing.T) {
	encoded := encodeTestRecords(
		testRecord{key: []byte("key-0"), value: []byte("value-0")},
		testRecord{offsetDelta: 1, key: []byte("key-1"), value: []byte("value-1")},
		testRecord{offsetDelta: 2, key: []byte("key-2"), value: []byte("value-2")},
	)
	// Cut the records section in the middle of the third record while still
	// declaring three records: the batch is truncated.
	truncated := encoded[:len(encoded)-5]
	batch := buildTestV2Batch(7, 0, 3, truncated)

	rs, err := readTestRecordSet(t, batch)
	if err != nil {
		t.Fatalf("expected the records decoded before the truncation to be readable, got %v", err)
	}

	records := rs.Records
	for i, want := range []string{"value-0", "value-1"} {
		r, err := records.ReadRecord()
		if err != nil {
			t.Fatalf("record %d: unexpected error: %v", i, err)
		}
		if got := readTestBytes(t, r.Value); !bytes.Equal(got, []byte(want)) {
			t.Errorf("record %d: expected value %q, got %q", i, want, got)
		}
		if wantOffset := int64(7 + i); r.Offset != wantOffset {
			t.Errorf("record %d: expected offset %d, got %d", i, wantOffset, r.Offset)
		}
	}

	// The iterator must report the truncation and stay at the same position
	// instead of advancing to unknown data or reporting io.EOF.
	_, err1 := records.ReadRecord()
	if err1 == nil || errors.Is(err1, io.EOF) {
		t.Fatalf("expected a truncation error, got %v", err1)
	}
	_, err2 := records.ReadRecord()
	if err2 == nil || err2.Error() != err1.Error() {
		t.Fatalf("expected the same error to be returned again, got %v then %v", err1, err2)
	}
}

func TestRecordBatchV2InvalidRecordCount(t *testing.T) {
	records := encodeTestRecords(testRecord{value: []byte("value")})

	for _, tc := range []struct {
		name  string
		count int32
	}{
		{name: "negative", count: -1},
		{name: "too large", count: 1 << 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			batch := buildTestV2Batch(0, 0, tc.count, records)
			_, err := readTestRecordSet(t, batch)
			if err == nil || !strings.Contains(err.Error(), "invalid record count") {
				t.Fatalf("expected an invalid record count error, got %v", err)
			}
		})
	}
}

func TestRecordBatchV2InvalidLengths(t *testing.T) {
	for _, tc := range []struct {
		name    string
		records []byte
		message string
	}{
		{
			name:    "negative record length",
			records: append(appendTestVarInt(nil, -1), make([]byte, recordMinSizeV2)...),
			message: "invalid record length",
		},
		{
			name:    "zero record length",
			records: append(appendTestVarInt(nil, 0), make([]byte, recordMinSizeV2)...),
			message: "invalid record length",
		},
		{
			name: "key length exceeds record",
			records: func() []byte {
				body := []byte{0}
				body = appendTestVarInt(body, 0)
				body = appendTestVarInt(body, 0)
				body = appendTestVarInt(body, 1<<20) // key length larger than record
				rec := appendTestVarInt(nil, int64(len(body)))
				return append(rec, body...)
			}(),
			message: "invalid key length",
		},
		{
			name: "value length exceeds record",
			records: func() []byte {
				body := []byte{0}
				body = appendTestVarInt(body, 0)
				body = appendTestVarInt(body, 0)
				body = appendTestVarInt(body, -1)    // null key
				body = appendTestVarInt(body, 1<<20) // value length larger than record
				rec := appendTestVarInt(nil, int64(len(body)))
				return append(rec, body...)
			}(),
			message: "invalid value length",
		},
		{
			name: "negative header count",
			records: func() []byte {
				body := []byte{0}
				body = appendTestVarInt(body, 0)
				body = appendTestVarInt(body, 0)
				body = appendTestVarInt(body, -1)
				body = appendTestVarInt(body, -1)
				body = appendTestVarInt(body, -2) // negative header count
				rec := appendTestVarInt(nil, int64(len(body)))
				return append(rec, body...)
			}(),
			message: "invalid header count",
		},
		{
			name: "header key length exceeds record",
			records: func() []byte {
				body := []byte{0}
				body = appendTestVarInt(body, 0)
				body = appendTestVarInt(body, 0)
				body = appendTestVarInt(body, -1)
				body = appendTestVarInt(body, -1)
				body = appendTestVarInt(body, 1)     // one header
				body = appendTestVarInt(body, 1<<20) // header key length
				rec := appendTestVarInt(nil, int64(len(body)))
				return append(rec, body...)
			}(),
			message: "invalid record header key length",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			batch := buildTestV2Batch(0, 0, 1, tc.records)
			_, err := readTestRecordSet(t, batch)
			if err == nil || !strings.Contains(err.Error(), tc.message) {
				t.Fatalf("expected a %q error, got %v", tc.message, err)
			}
		})
	}
}

func TestRecordBatchV2InvalidBatchLength(t *testing.T) {
	// A batch length smaller than the batch header cannot describe a valid
	// batch and must be reported instead of being silently ignored.
	batch := buildTestV2Batch(0, 0, 0, nil)
	binary.BigEndian.PutUint32(batch[8:12], uint32(recordBatchHeaderSizeV2-1))

	_, err := readTestRecordSet(t, batch)
	if err == nil || !strings.Contains(err.Error(), "invalid record batch length") {
		t.Fatalf("expected an invalid record batch length error, got %v", err)
	}
}

func TestRecordBatchV2TruncatedBatchIsDiscarded(t *testing.T) {
	// Preserve the historical behavior for responses that were truncated
	// before the end of the batch: the partial batch is discarded without
	// reporting an error.
	batch := buildTestV2Batch(0, 0, 1, encodeTestRecords(testRecord{value: []byte("value")}))
	partial := batch[:len(batch)-3]

	var rs RecordSet
	d := &decoder{reader: bytes.NewReader(partial), remain: len(partial)}
	if err := rs.readFromVersion2(d); err != nil {
		t.Fatalf("expected a truncated batch to be discarded, got %v", err)
	}
	if rs.Records != nil {
		t.Errorf("expected no records, got %v", rs.Records)
	}
	if d.remain != 0 {
		t.Errorf("expected the truncated batch to be discarded, %d bytes remain", d.remain)
	}
}

func TestRecordBatchV2UnsupportedCompression(t *testing.T) {
	// Compression code 5 has no registered codec.
	batch := buildTestV2Batch(0, 5, 0, nil)
	if _, err := readTestRecordSet(t, batch); err == nil || !strings.Contains(err.Error(), "unsupported compression codec") {
		t.Fatalf("expected an unsupported compression codec error, got %v", err)
	}
}

func TestOptimizedRecordReaderStickyError(t *testing.T) {
	boom := errors.New("boom")
	reader := &optimizedRecordReader{
		records: []optimizedRecord{{offset: 1}},
		err:     boom,
	}

	if _, err := reader.ReadRecord(); err != nil {
		t.Fatalf("expected the first record to be read, got %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := reader.ReadRecord(); !errors.Is(err, boom) {
			t.Fatalf("call %d: expected the sticky error, got %v", i, err)
		}
	}
}

func TestDecoderReadBounds(t *testing.T) {
	d := &decoder{reader: bytes.NewReader([]byte("abc")), remain: 3}

	if b := d.read(10); b != nil {
		t.Errorf("expected no bytes to be read, got %q", b)
	}
	if !errors.Is(d.err, io.ErrUnexpectedEOF) {
		t.Errorf("expected an io.ErrUnexpectedEOF, got %v", d.err)
	}

	d = &decoder{reader: bytes.NewReader([]byte("abc")), remain: 3}
	if b := d.read(-1); b != nil {
		t.Errorf("expected no bytes to be read, got %q", b)
	}
	if !errors.Is(d.err, io.ErrUnexpectedEOF) {
		t.Errorf("expected an io.ErrUnexpectedEOF, got %v", d.err)
	}
}

// TestRecordBatchV2CorruptInputDoesNotPanic corrupts a valid batch at many
// positions and ensures that decoding never panics.
func TestRecordBatchV2CorruptInputDoesNotPanic(t *testing.T) {
	records := encodeTestRecords(
		testRecord{key: []byte("key-0"), value: []byte("value-0")},
		testRecord{offsetDelta: 1, key: []byte("key-1"), value: []byte("value-1")},
		testRecord{offsetDelta: 2, value: []byte("value-2")},
	)
	valid := buildTestV2Batch(3, 0, int32(3), records)

	rng := rand.New(rand.NewSource(1))
	batch := make([]byte, len(valid))
	for i := 0; i < 2000; i++ {
		copy(batch, valid)
		for j := 0; j < 1+rng.Intn(4); j++ {
			batch[rng.Intn(len(batch))] = byte(rng.Intn(256))
		}

		func() {
			defer func() {
				if p := recover(); p != nil {
					t.Fatalf("decoding corrupt input panicked: %v (batch %x)", p, batch)
				}
			}()

			var rs RecordSet
			d := &decoder{reader: bytes.NewReader(batch), remain: len(batch)}
			if err := rs.readFromVersion2(d); err != nil {
				return
			}
			if rs.Records == nil {
				return
			}
			for n := 0; n < 16; n++ {
				if _, err := rs.Records.ReadRecord(); err != nil {
					break
				}
			}
		}()
	}
}

func TestRecordBatchV2Timestamps(t *testing.T) {
	const firstTimestamp = 1600000000000
	records := []testRecord{
		{offsetDelta: 0, timestampDelta: 0, value: []byte("a")},
		{offsetDelta: 1, timestampDelta: 1500, value: []byte("b")},
	}
	batch := buildTestV2Batch(0, 0, 2, encodeTestRecords(records...))
	// Patch the first timestamp in the batch header.
	binary.BigEndian.PutUint64(batch[27:35], firstTimestamp)
	binary.BigEndian.PutUint64(batch[35:43], firstTimestamp+1500)
	// The CRC covers the timestamps, recompute it.
	binary.BigEndian.PutUint32(batch[17:21], crc32.Checksum(batch[21:], crc32.MakeTable(crc32.Castagnoli)))

	rs, err := readTestRecordSet(t, batch)
	if err != nil {
		t.Fatalf("failed to read record batch: %v", err)
	}

	found := readAllTestRecords(t, rs.Records)
	if len(found) != len(records) {
		t.Fatalf("expected %d records, got %d", len(records), len(found))
	}
	for i, want := range records {
		if wantTime := makeTime(firstTimestamp + want.timestampDelta); !found[i].time.Equal(wantTime) {
			t.Errorf("record %d: expected timestamp %v, got %v", i, wantTime, found[i].time)
		}
	}
}
