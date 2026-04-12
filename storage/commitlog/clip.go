package commitlog

import (
	"encoding/binary"
	"io"
)

const (
	batchLogOverhead    = 12
	batchHeaderReadSize = 61
	batchRecordCountOff = 57
)

// ClipRegionToOffset trims the region to include only complete RecordBatches
// whose lastOffset < maxVisible. maxVisible is exclusive (HWM = first
// uncommitted offset; LEO = next offset to be assigned).
//
// Reads only batch headers (61 bytes each) — record bodies stay on disk so
// the zero-copy sendfile path is preserved.
func ClipRegionToOffset(reg *RecordsRegion, maxVisible int64) (*RecordsRegion, error) {
	if reg == nil || reg.Size == 0 || reg.File == nil {
		return reg, nil
	}

	pos := int64(0)
	visibleSize := int64(0)

	var hdr [batchHeaderReadSize]byte

	for pos < reg.Size {
		if reg.Size-pos < batchHeaderReadSize {
			return nil, io.ErrUnexpectedEOF
		}

		if _, err := reg.File.ReadAt(hdr[:], reg.FileOffset+pos); err != nil {
			return nil, err
		}

		baseOffset := int64(binary.BigEndian.Uint64(hdr[:8]))
		batchLen := int(binary.BigEndian.Uint32(hdr[8:12]))
		recordCount := int(binary.BigEndian.Uint32(hdr[batchRecordCountOff : batchRecordCountOff+4]))

		if batchLen < batchLogOverhead || recordCount <= 0 {
			return nil, io.ErrUnexpectedEOF
		}

		lastOffset := baseOffset + int64(recordCount) - 1
		if lastOffset >= maxVisible {
			break
		}

		batchTotal := int64(batchLogOverhead + batchLen)
		if pos+batchTotal > reg.Size {
			break
		}

		pos += batchTotal
		visibleSize = pos
	}

	if visibleSize == reg.Size {
		return reg, nil
	}

	result := *reg
	result.Size = visibleSize
	return &result, nil
}
