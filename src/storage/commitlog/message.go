package commitlog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"slices"
	"time"
)

type Message struct {
	Offset    int64
	Timestamp time.Time
	Key       []byte
	Value     []byte
	Headers   map[string][]byte
}

// SerializeMessage serializes a Message to the log file in a deterministic binary format.
//
// On-disk layout (big-endian):
//
//	[TotalSize: 4B] [CRC32: 4B] [Offset: 8B] [Timestamp: 8B]
//	[KeyLen: 4B] [Key: variable] [ValueLen: 4B] [Value: variable]
//	[HeaderCount: 4B] [Headers: variable (sorted by key)]
//
// CRC32 covers everything after the CRC field itself.
func SerializeMessage(message Message) ([]byte, error) {

	headerSize := 4 // 4B for header count

	for k, v := range message.Headers {
		headerSize += 4 + len(k) + 4 + len(v) // key length + key + value length + value
	}

	payloadSize := 8 + 8 + 4 + len(message.Key) + 4 + len(message.Value) + headerSize // Offset + Timestamp + KeyLen + Key + ValueLen + Value + Headers

	totalSize := 4 + payloadSize        // CRC32 + Payload
	buf := make([]byte, 0, 4+totalSize) // prefix totalSize + totalSize
	buffer := bytes.NewBuffer(buf)

	if binary.Write(buffer, binary.BigEndian, uint32(totalSize)) != nil {
		return nil, fmt.Errorf("could not write headers to commitlog")
	}

	// Placeholder for CRC32, will be overwritten later
	crcIndex := buffer.Len()
	if binary.Write(buffer, binary.BigEndian, uint32(0)) != nil {
		return nil, fmt.Errorf("could not write headers to commitlog")
	}

	//write offset
	if binary.Write(buffer, binary.BigEndian, uint64(message.Offset)) != nil {
		return nil, fmt.Errorf("could not write offset to commitlog")
	}

	//write timestamp
	if binary.Write(buffer, binary.BigEndian, uint64(message.Timestamp.UnixNano())) != nil {
		return nil, fmt.Errorf("could not write timestamp to commitlog")
	}

	//write message key
	if binary.Write(buffer, binary.BigEndian, uint32(len(message.Key))) != nil {
		return nil, fmt.Errorf("could not write key length to commitlog")
	}
	if _, err := buffer.Write(message.Key); err != nil {
		return nil, fmt.Errorf("could not write key to commitlog: %v", err)
	}

	//write message value
	if binary.Write(buffer, binary.BigEndian, uint32(len(message.Value))) != nil {
		return nil, fmt.Errorf("could not write value length to commitlog")
	}
	if _, err := buffer.Write(message.Value); err != nil {
		return nil, fmt.Errorf("could not write value to commitlog: %v", err)
	}

	//write headers
	if binary.Write(buffer, binary.BigEndian, uint32(len(message.Headers))) != nil {
		return nil, fmt.Errorf("could not write header count to commitlog")
	}

	sortHeaderKeys := make([]string, 0, len(message.Headers))
	for k := range message.Headers {
		sortHeaderKeys = append(sortHeaderKeys, k)
	}
	slices.Sort(sortHeaderKeys)

	for _, key := range sortHeaderKeys {
		value := message.Headers[key]
		if binary.Write(buffer, binary.BigEndian, uint32(len(key))) != nil {
			return nil, fmt.Errorf("could not write header key length to commitlog")
		}
		if _, err := buffer.Write([]byte(key)); err != nil {
			return nil, fmt.Errorf("could not write header key to commitlog: %v", err)
		}

		if binary.Write(buffer, binary.BigEndian, uint32(len(value))) != nil {
			return nil, fmt.Errorf("could not write header value length to commitlog")
		}
		if _, err := buffer.Write(value); err != nil {
			return nil, fmt.Errorf("could not write header value to commitlog: %v", err)
		}
	}

	// Calculate CRC32 of the payload (everything after the CRC field)
	crcData := buffer.Bytes()[crcIndex+4:] // Payload starts after CRC field
	crc := crc32.ChecksumIEEE(crcData)
	binary.BigEndian.PutUint32(buf[crcIndex:crcIndex+4], crc)

	return buffer.Bytes(), nil
}

// DeserializeMessage reads a single message from the log file at the current position and returns it.
// On-disk layout (big-endian):
//
//	[TotalSize: 4B] [CRC32: 4B] [Offset: 8B] [Timestamp: 8B]
//	[KeyLen: 4B] [Key: variable] [ValueLen: 4B] [Value: variable]
//	[HeaderCount: 4B] [Headers: variable (sorted by key)]
//
// returns the deserialized Message, the total number of bytes read, and an error if any.
func DeserializeMessage(reader io.Reader) (*Message, int, error) {
	var size uint32
	if err := binary.Read(reader, binary.BigEndian, &size); err != nil {
		return nil, 0, fmt.Errorf("could not read message size: %v", err)
	}

	buff := make([]byte, size)
	if _, err := io.ReadFull(reader, buff); err != nil {
		return nil, 0, fmt.Errorf("could not read message payload: %v", err)
	}
	buffer := bytes.NewReader(buff)
	byteRead := 4 // size field already read

	var crc uint32
	if err := binary.Read(buffer, binary.BigEndian, &crc); err != nil {
		return nil, 0, fmt.Errorf("could not read message CRC: %v", err)
	}
	byteRead += 4

	payload := buff[4:] // Payload starts after CRC field
	if crc32.ChecksumIEEE(payload) != crc {
		return nil, 0, fmt.Errorf("CRC mismatch: expected %d, got %d", crc, crc32.ChecksumIEEE(payload))
	}

	var offset uint64
	if err := binary.Read(buffer, binary.BigEndian, &offset); err != nil {
		return nil, 0, fmt.Errorf("could not read message offset: %v", err)
	}
	byteRead += 8

	var timestamp uint64
	if err := binary.Read(buffer, binary.BigEndian, &timestamp); err != nil {
		return nil, 0, fmt.Errorf("could not read message timestamp: %v", err)
	}
	byteRead += 8

	var keyLen uint32
	if err := binary.Read(buffer, binary.BigEndian, &keyLen); err != nil {
		return nil, 0, fmt.Errorf("could not read message key length: %v", err)
	}
	byteRead += 4

	key := make([]byte, keyLen)
	if _, err := buffer.Read(key); err != nil {
		return nil, 0, fmt.Errorf("could not read message key: %v", err)
	}
	byteRead += int(keyLen)

	var valueLen uint32
	if err := binary.Read(buffer, binary.BigEndian, &valueLen); err != nil {
		return nil, 0, fmt.Errorf("could not read message value length: %v", err)
	}
	byteRead += 4

	value := make([]byte, valueLen)
	if _, err := buffer.Read(value); err != nil {
		return nil, 0, fmt.Errorf("could not read message value: %v", err)
	}
	byteRead += int(valueLen)

	var headerCount uint32
	if err := binary.Read(buffer, binary.BigEndian, &headerCount); err != nil {
		return nil, 0, fmt.Errorf("could not read message header count: %v", err)
	}
	byteRead += 4

	headers := make(map[string][]byte, headerCount)
	for i := uint32(0); i < headerCount; i++ {
		var keyHeaderLen uint32
		if err := binary.Read(buffer, binary.BigEndian, &keyHeaderLen); err != nil {
			return nil, 0, fmt.Errorf("could not read header key length: %v", err)
		}
		byteRead += 4

		keyBytes := make([]byte, keyHeaderLen)
		if _, err := buffer.Read(keyBytes); err != nil {
			return nil, 0, fmt.Errorf("could not read header key: %v", err)
		}
		byteRead += int(keyHeaderLen)

		var valueHeaderLen uint32
		if err := binary.Read(buffer, binary.BigEndian, &valueHeaderLen); err != nil {
			return nil, 0, fmt.Errorf("could not read header value length: %v", err)
		}
		byteRead += 4

		valueBytes := make([]byte, valueHeaderLen)
		if _, err := buffer.Read(valueBytes); err != nil {
			return nil, 0, fmt.Errorf("could not read header value: %v", err)
		}
		byteRead += int(valueHeaderLen)

		headers[string(keyBytes)] = valueBytes
	}

	return &Message{
		Offset:    int64(offset),
		Timestamp: time.Unix(0, int64(timestamp)),
		Key:       key,
		Value:     value,
		Headers:   headers,
	}, byteRead, nil
}
