package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	MaxPayloadSize = 1024 * 1024 * 100
)

func WriteFraming(conn io.Writer, header *ResponseHeader, req Response) error {
	enc := NewWriter(256)
	if err := header.Encode(enc); err != nil {
		return err
	}
	if err := req.Encode(enc); err != nil {
		return err
	}
	payload := enc.Bytes()
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(payload)))

	if _, err := conn.Write(lenBuf); err != nil {
		return err
	}
	if _, err := conn.Write(payload); err != nil {
		return err
	}
	return nil
}

func ReadFraming(conn io.Reader) ([]byte, error) {
	var lenBuff [4]byte
	_, err := io.ReadFull(conn, lenBuff[:])
	if err != nil {
		return nil, err
	}

	payloadLen := binary.BigEndian.Uint32(lenBuff[:])
	if payloadLen == 0 {
		return nil, nil
	}
	if payloadLen > MaxPayloadSize {
		return nil, fmt.Errorf("payload too large: %d", payloadLen)
	}

	payload := make([]byte, payloadLen)
	_, err = io.ReadFull(conn, payload)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("payload read error: %s", err))
	}
	return payload, nil
}
