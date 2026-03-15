package common

import (
	"fmt"

	"github.com/klauspost/compress/snappy"
)

const (
	CompressionNone   int16 = 0
	CompressionGzip   int16 = 1
	CompressionSnappy int16 = 2
	CompressionLz4    int16 = 3
	CompressionZstd   int16 = 4

	compressionMask int16 = 0x07
)

func Decompress(attributes int16, data []byte) ([]byte, error) {
	//todo: complete supported decompresses
	codec := attributes & compressionMask
	switch codec {
	case CompressionNone:
		return data, nil
	case CompressionGzip:
		panic("gzip compression not yet supported")
	case CompressionSnappy:
		return DecompressSnappy(data)
	case CompressionLz4:
		panic("lz4 compression not yet supported")
	case CompressionZstd:
		panic("zstd compression not yet supported")
	default:
		return nil, fmt.Errorf("unknown compression codec %d", codec)
	}
}

func DecompressSnappy(data []byte) ([]byte, error) {
	out, err := snappy.Decode(nil, data)
	if err != nil {
		return nil, fmt.Errorf("snappy: decompress: %w", err)
	}
	return out, nil
}
