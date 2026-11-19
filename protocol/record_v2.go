package protocol

import (
	"fmt"
	"hash/crc32"
	"io"
	"time"
)

const (
	// Size in bytes of the record batch header which follows the base offset
	// and batch length fields in the v2 record batch format:
	//
	//   partition leader epoch (4) + magic (1) + crc (4) + attributes (2) +
	//   last offset delta (4) + first timestamp (8) + max timestamp (8) +
	//   producer id (8) + producer epoch (2) + base sequence (4) +
	//   record count (4)
	recordBatchHeaderSizeV2 = 49

	// Minimum size in bytes of a v2 record, including its length prefix:
	// length (>=1) + attributes (1) + timestamp delta (>=1) +
	// offset delta (>=1) + key length (>=1) + value length (>=1) +
	// header count (>=1).
	recordMinSizeV2 = 7

	// Minimum size in bytes of the body of a v2 record, i.e. everything but
	// its length prefix.
	recordBodyMinSizeV2 = recordMinSizeV2 - 1
)

func (rs *RecordSet) readFromVersion2(d *decoder) error {
	baseOffset := d.readInt64()
	batchLength := d.readInt32()

	if d.err != nil || int(batchLength) > d.remain {
		// The response was truncated before the end of the batch; keep the
		// historical behavior of discarding the partial batch.
		d.discardAll()
		return nil
	}

	// A batch length smaller than its header, including negative values,
	// cannot be valid: report it instead of letting it desynchronize the
	// decoding of the message set.
	if batchLength < recordBatchHeaderSizeV2 {
		return fmt.Errorf("invalid record batch length: %d", batchLength)
	}

	dec := &decoder{
		reader: d,
		remain: int(batchLength),
	}

	partitionLeaderEpoch := dec.readInt32()
	magicByte := dec.readInt8()
	crc := dec.readInt32()

	dec.setCRC(crc32.MakeTable(crc32.Castagnoli))

	attributes := dec.readInt16()
	lastOffsetDelta := dec.readInt32()
	firstTimestamp := dec.readInt64()
	maxTimestamp := dec.readInt64()
	producerID := dec.readInt64()
	producerEpoch := dec.readInt16()
	baseSequence := dec.readInt32()
	numRecords := dec.readInt32()
	reader := io.Reader(dec)

	// unused
	_ = lastOffsetDelta
	_ = maxTimestamp

	if dec.err != nil {
		return dec.err
	}

	if compression := Attributes(attributes).Compression(); compression != 0 {
		codec := compression.Codec()
		if codec == nil {
			return fmt.Errorf("unsupported compression codec (%d)", compression)
		}
		decompressor := codec.NewReader(reader)
		defer decompressor.Close()
		reader = decompressor
	}

	buffer := newPageBuffer()
	defer buffer.unref()

	_, err := buffer.ReadFrom(reader)
	if err != nil {
		return err
	}
	if dec.crc32 != uint32(crc) {
		return fmt.Errorf("crc32 checksum mismatch (computed=%d found=%d)", dec.crc32, uint32(crc))
	}

	recordsLength := buffer.Len()
	dec.reader = buffer
	dec.remain = recordsLength

	// Each record takes at least recordMinSizeV2 bytes, so a count which is
	// negative or larger than what the records section can hold is corrupt.
	// Checking before allocating guards against panics and against huge
	// allocations driven by a malicious count.
	if numRecords < 0 || int64(numRecords) > int64(recordsLength)/recordMinSizeV2 {
		return fmt.Errorf("invalid record count %d in record batch of %d bytes", numRecords, recordsLength)
	}

	records := make([]optimizedRecord, numRecords)
	// These are two lazy allocators that will be used to optimize allocation of
	// page references for keys and values.
	//
	// By default, no memory is allocated and on first use, numRecords page refs
	// are allocated in a contiguous memory space, and the allocators return
	// pointers into those arrays for each page ref that get requested.
	//
	// The reasoning is that kafka partitions typically have records of a single
	// form, which either have no keys, no values, or both keys and values.
	// Using lazy allocators adapts nicely to these patterns to only allocate
	// the memory that is needed by the program, while still reducing the number
	// of malloc calls made by the program.
	//
	// Using a single allocator for both keys and values keeps related values
	// close by in memory, making access to the records more friendly to CPU
	// caches.
	alloc := pageRefAllocator{size: int(numRecords)}
	// Following the same reasoning that kafka partitions will typically have
	// records with repeating formats, we expect to either find records with
	// no headers, or records which always contain headers.
	//
	// To reduce the memory footprint when records have no headers, the Header
	// slices are lazily allocated in a separate array.
	headers := ([][]Header)(nil)

	// tailErr carries a decode error found after some records were decoded
	// successfully; it is reported by the record reader once the valid records
	// have been consumed so the reader never advances into an unknown position.
	var tailErr error

	for i := range records {
		r := &records[i]

		recordLength := dec.readVarInt()
		if dec.err != nil {
			// The batch was truncated in the middle of the records section.
			// Keep the records decoded so far.
			records = records[:i]
			tailErr = fmt.Errorf("record batch truncated: %w", dontExpectEOF(dec.err))
			break
		}

		if recordLength < recordBodyMinSizeV2 {
			return fmt.Errorf("invalid record length %d at index %d in record batch", recordLength, i)
		}

		if recordLength > int64(dec.remain) {
			// The last record of the batch extends past the end of the
			// records section: the batch is truncated.
			records = records[:i]
			tailErr = fmt.Errorf("record batch truncated: record of %d bytes at index %d exceeds the %d remaining bytes",
				recordLength, i, dec.remain)
			break
		}

		// Decode the record body from a sub-decoder bounded by the record
		// length prefix, so a malformed record cannot misalign the decoding of
		// the records which follow it.
		rec := &decoder{reader: dec, remain: int(recordLength)}

		_ = rec.readInt8() // record attributes (unused)
		timestampDelta := rec.readVarInt()
		offsetDelta := rec.readVarInt()

		if rec.err == nil && offsetDelta < 0 {
			rec.setError(fmt.Errorf("invalid negative offset delta %d at index %d in record batch", offsetDelta, i))
		}

		keyLength := rec.readVarInt()
		keyOffset := int64(recordsLength - dec.remain)
		if rec.err == nil && (keyLength < -1 || keyLength > int64(rec.remain)) {
			rec.setError(fmt.Errorf("invalid key length %d at index %d in record batch", keyLength, i))
		}
		if keyLength > 0 {
			rec.discard(int(keyLength))
		}

		valueLength := rec.readVarInt()
		valueOffset := int64(recordsLength - dec.remain)
		if rec.err == nil && (valueLength < -1 || valueLength > int64(rec.remain)) {
			rec.setError(fmt.Errorf("invalid value length %d at index %d in record batch", valueLength, i))
		}
		if valueLength > 0 {
			rec.discard(int(valueLength))
		}

		numHeaders := rec.readVarInt()
		if rec.err == nil && (numHeaders < 0 || 2*numHeaders > int64(rec.remain)) {
			// Each header takes at least two bytes for the key and value
			// length prefixes.
			rec.setError(fmt.Errorf("invalid header count %d at index %d in record batch", numHeaders, i))
		}

		var recordHeaders []Header
		if numHeaders > 0 && rec.err == nil {
			recordHeaders = make([]Header, numHeaders)

			for j := range recordHeaders {
				recordHeaders[j] = Header{
					Key:   readRecordHeaderKey(rec),
					Value: readRecordHeaderValue(rec),
				}
			}
		}

		if rec.err != nil {
			return rec.err
		}

		// Skip any bytes left in the record to remain aligned on the record
		// boundaries described by the length prefixes.
		rec.discardAll()

		r.offset = baseOffset + offsetDelta
		r.timestamp = firstTimestamp + timestampDelta

		if numHeaders > 0 {
			if headers == nil {
				headers = make([][]Header, numRecords)
			}
			headers[i] = recordHeaders
		}

		if keyLength >= 0 {
			r.keyRef = alloc.newPageRef()
			buffer.refTo(r.keyRef, keyOffset, keyOffset+keyLength)
		}

		if valueLength >= 0 {
			r.valueRef = alloc.newPageRef()
			buffer.refTo(r.valueRef, valueOffset, valueOffset+valueLength)
		}
	}

	if tailErr == nil && dec.remain != 0 {
		return fmt.Errorf("record batch contains %d trailing bytes after %d records", dec.remain, numRecords)
	}

	// Note: it's unclear whether kafka 0.11+ still truncates the responses,
	// all attempts I made at constructing a test to trigger a truncation have
	// failed. I kept this code here as a safeguard but it may never execute.
	if tailErr != nil && len(records) == 0 {
		return tailErr
	}

	*rs = RecordSet{
		Version:    magicByte,
		Attributes: Attributes(attributes),
		Records: &optimizedRecordReader{
			records: records,
			headers: headers,
			err:     tailErr,
		},
	}

	if rs.Attributes.Control() {
		rs.Records = &ControlBatch{
			Attributes:           rs.Attributes,
			PartitionLeaderEpoch: partitionLeaderEpoch,
			BaseOffset:           baseOffset,
			ProducerID:           producerID,
			ProducerEpoch:        producerEpoch,
			BaseSequence:         baseSequence,
			Records:              rs.Records,
		}
	} else {
		rs.Records = &RecordBatch{
			Attributes:           rs.Attributes,
			PartitionLeaderEpoch: partitionLeaderEpoch,
			BaseOffset:           baseOffset,
			ProducerID:           producerID,
			ProducerEpoch:        producerEpoch,
			BaseSequence:         baseSequence,
			Records:              rs.Records,
		}
	}

	return nil
}

// readRecordHeaderKey reads a record header key from d, validating its length
// against the number of bytes remaining in the record to prevent panics and
// excessive allocations on corrupt input.
func readRecordHeaderKey(d *decoder) string {
	n := d.readVarInt()
	if d.err != nil {
		return ""
	}
	if n > int64(d.remain) {
		d.setError(fmt.Errorf("invalid record header key length: %d", n))
		return ""
	}
	if n < 0 {
		return ""
	}
	return bytesToString(d.read(int(n)))
}

// readRecordHeaderValue reads a record header value from d, validating its
// length against the number of bytes remaining in the record to prevent panics
// and excessive allocations on corrupt input.
func readRecordHeaderValue(d *decoder) []byte {
	n := d.readVarInt()
	if d.err != nil {
		return nil
	}
	if n > int64(d.remain) {
		d.setError(fmt.Errorf("invalid record header value length: %d", n))
		return nil
	}
	if n < 0 {
		return nil
	}
	return d.read(int(n))
}

func (rs *RecordSet) writeToVersion2(buffer *pageBuffer, bufferOffset int64) error {
	records := rs.Records
	numRecords := int32(0)

	e := &encoder{writer: buffer}
	e.writeInt64(0)                    // base offset                         |  0 +8
	e.writeInt32(0)                    // placeholder for record batch length |  8 +4
	e.writeInt32(-1)                   // partition leader epoch              | 12 +3
	e.writeInt8(2)                     // magic byte                          | 16 +1
	e.writeInt32(0)                    // placeholder for crc32 checksum      | 17 +4
	e.writeInt16(int16(rs.Attributes)) // attributes                          | 21 +2
	e.writeInt32(0)                    // placeholder for lastOffsetDelta     | 23 +4
	e.writeInt64(0)                    // placeholder for firstTimestamp      | 27 +8
	e.writeInt64(0)                    // placeholder for maxTimestamp        | 35 +8
	e.writeInt64(-1)                   // producer id                         | 43 +8
	e.writeInt16(-1)                   // producer epoch                      | 51 +2
	e.writeInt32(-1)                   // base sequence                       | 53 +4
	e.writeInt32(0)                    // placeholder for numRecords          | 57 +4

	var compressor io.WriteCloser
	if compression := rs.Attributes.Compression(); compression != 0 {
		if codec := compression.Codec(); codec != nil {
			compressor = codec.NewWriter(buffer)
			e.writer = compressor
		}
	}

	currentTimestamp := timestamp(time.Now())
	lastOffsetDelta := int32(0)
	firstTimestamp := int64(0)
	maxTimestamp := int64(0)

	err := forEachRecord(records, func(i int, r *Record) error {
		t := timestamp(r.Time)
		if t == 0 {
			t = currentTimestamp
		}
		if i == 0 {
			firstTimestamp = t
		}
		if t > maxTimestamp {
			maxTimestamp = t
		}

		timestampDelta := t - firstTimestamp
		offsetDelta := int64(i)
		lastOffsetDelta = int32(offsetDelta)

		length := 1 + // attributes
			sizeOfVarInt(timestampDelta) +
			sizeOfVarInt(offsetDelta) +
			sizeOfVarNullBytesIface(r.Key) +
			sizeOfVarNullBytesIface(r.Value) +
			sizeOfVarInt(int64(len(r.Headers)))

		for _, h := range r.Headers {
			length += sizeOfVarString(h.Key) + sizeOfVarNullBytes(h.Value)
		}

		e.writeVarInt(int64(length))
		e.writeInt8(0) // record attributes (unused)
		e.writeVarInt(timestampDelta)
		e.writeVarInt(offsetDelta)

		if err := e.writeVarNullBytesFrom(r.Key); err != nil {
			return err
		}

		if err := e.writeVarNullBytesFrom(r.Value); err != nil {
			return err
		}

		e.writeVarInt(int64(len(r.Headers)))

		for _, h := range r.Headers {
			e.writeVarString(h.Key)
			e.writeVarNullBytes(h.Value)
		}

		numRecords++
		return nil
	})

	if err != nil {
		return err
	}

	if compressor != nil {
		if err := compressor.Close(); err != nil {
			return err
		}
	}

	if numRecords == 0 {
		return ErrNoRecord
	}

	b2 := packUint32(uint32(lastOffsetDelta))
	b3 := packUint64(uint64(firstTimestamp))
	b4 := packUint64(uint64(maxTimestamp))
	b5 := packUint32(uint32(numRecords))

	buffer.WriteAt(b2[:], bufferOffset+23)
	buffer.WriteAt(b3[:], bufferOffset+27)
	buffer.WriteAt(b4[:], bufferOffset+35)
	buffer.WriteAt(b5[:], bufferOffset+57)

	totalLength := buffer.Size() - bufferOffset
	batchLength := totalLength - 12

	checksum := uint32(0)
	crcTable := crc32.MakeTable(crc32.Castagnoli)

	buffer.pages.scan(bufferOffset+21, bufferOffset+totalLength, func(chunk []byte) bool {
		checksum = crc32.Update(checksum, crcTable, chunk)
		return true
	})

	b0 := packUint32(uint32(batchLength))
	b1 := packUint32(checksum)

	buffer.WriteAt(b0[:], bufferOffset+8)
	buffer.WriteAt(b1[:], bufferOffset+17)
	return nil
}
