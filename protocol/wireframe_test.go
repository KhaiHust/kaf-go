package protocol

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

// Mock Response implementation for testing
type mockResponse struct {
	data          []byte
	shouldFailEnc bool
}

func (m *mockResponse) Encode(w *Writer) error {
	if m.shouldFailEnc {
		return errors.New("mock encode error")
	}
	w.buff = append(w.buff, m.data...)
	return nil
}

func (m *mockResponse) Decode(r *Reader) error {
	return nil
}

func (m *mockResponse) ApiKey() int16 {
	return 1
}

// Mock io.Writer that fails
type failingWriter struct {
	failOnWrite int
	writeCount  int
}

func (f *failingWriter) Write(p []byte) (n int, err error) {
	f.writeCount++
	if f.writeCount == f.failOnWrite {
		return 0, errors.New("write error")
	}
	return len(p), nil
}

// Mock io.Reader that fails
type failingReader struct {
	failOnRead int
	readCount  int
}

func (f *failingReader) Read(p []byte) (n int, err error) {
	f.readCount++
	if f.readCount >= f.failOnRead {
		return 0, errors.New("read error")
	}
	return 0, nil
}

// Mock io.Reader that returns partial data
type partialReader struct {
	data []byte
	pos  int
}

func (p *partialReader) Read(buf []byte) (n int, err error) {
	if p.pos >= len(p.data) {
		return 0, io.EOF
	}
	n = copy(buf, p.data[p.pos:])
	p.pos += n
	if p.pos >= len(p.data) {
		err = io.EOF
	}
	return n, err
}

func TestWriteFraming_Success(t *testing.T) {
	var buf bytes.Buffer

	header := &ResponseHeader{
		CorrelationId: 123,
		ApiVersion:    2,
		ApiKey:        1,
	}

	resp := &mockResponse{
		data: []byte{0x01, 0x02, 0x03},
	}

	err := WriteFraming(&buf, header, resp)
	if err != nil {
		t.Fatalf("WriteFraming() error = %v, want nil", err)
	}

	// Check that data was written
	if buf.Len() == 0 {
		t.Error("WriteFraming() wrote no data")
	}

	// First 4 bytes should be the length
	data := buf.Bytes()
	if len(data) < 4 {
		t.Fatalf("WriteFraming() wrote %d bytes, want at least 4", len(data))
	}

	// Read length prefix
	payloadLen := uint32(data[0])<<24 | uint32(data[1])<<16 | uint32(data[2])<<8 | uint32(data[3])

	// Total length should be 4 (length prefix) + payloadLen
	expectedTotal := 4 + int(payloadLen)
	if len(data) != expectedTotal {
		t.Errorf("WriteFraming() wrote %d bytes, want %d", len(data), expectedTotal)
	}

	// Verify payload length matches actual payload
	actualPayload := data[4:]
	if len(actualPayload) != int(payloadLen) {
		t.Errorf("Payload length = %d, want %d", len(actualPayload), payloadLen)
	}
}

func TestWriteFraming_EmptyRequest(t *testing.T) {
	var buf bytes.Buffer

	header := &ResponseHeader{
		CorrelationId: 0,
		ApiVersion:    0,
		ApiKey:        0,
	}

	resp := &mockResponse{
		data: []byte{},
	}

	err := WriteFraming(&buf, header, resp)
	if err != nil {
		t.Fatalf("WriteFraming() error = %v, want nil", err)
	}

	// Should still write length prefix + header
	if buf.Len() < 4 {
		t.Errorf("WriteFraming() wrote %d bytes, want at least 4", buf.Len())
	}
}

func TestWriteFraming_HeaderEncodeError(t *testing.T) {
	// Create a failing writer that will fail on first write
	fw := &failingWriter{failOnWrite: 1}

	header := &ResponseHeader{
		ApiKey:        1,
		ApiVersion:    2,
		CorrelationId: 123,
	}

	resp := &mockResponse{
		data: []byte{0x01},
	}

	err := WriteFraming(fw, header, resp)
	if err == nil {
		t.Error("WriteFraming() error = nil, want error")
	}
}

func TestWriteFraming_RequestEncodeError(t *testing.T) {
	var buf bytes.Buffer

	header := &ResponseHeader{
		ApiKey:        1,
		ApiVersion:    2,
		CorrelationId: 123,
	}

	resp := &mockResponse{
		data:          []byte{0x01},
		shouldFailEnc: true,
	}

	err := WriteFraming(&buf, header, resp)
	if err == nil {
		t.Error("WriteFraming() error = nil, want error")
	}

	if !strings.Contains(err.Error(), "mock encode error") {
		t.Errorf("WriteFraming() error = %v, want 'mock encode error'", err)
	}
}

func TestWriteFraming_WriteLengthError(t *testing.T) {
	fw := &failingWriter{failOnWrite: 1}

	header := &ResponseHeader{
		ApiKey:        1,
		ApiVersion:    2,
		CorrelationId: 123,
	}

	resp := &mockResponse{
		data: []byte{0x01, 0x02},
	}

	err := WriteFraming(fw, header, resp)
	if err == nil {
		t.Error("WriteFraming() error = nil, want error")
	}
}

func TestWriteFraming_WritePayloadError(t *testing.T) {
	fw := &failingWriter{failOnWrite: 2}

	header := &ResponseHeader{
		ApiKey:        1,
		ApiVersion:    2,
		CorrelationId: 123,
	}

	resp := &mockResponse{
		data: []byte{0x01, 0x02},
	}

	err := WriteFraming(fw, header, resp)
	if err == nil {
		t.Error("WriteFraming() error = nil, want error")
	}
}

func TestReadFraming_Success(t *testing.T) {
	// Create a valid framed message
	payload := []byte{0x01, 0x02, 0x03, 0x04, 0x05}

	var buf bytes.Buffer
	// Write length prefix
	lenBuf := make([]byte, 4)
	lenBuf[0] = 0
	lenBuf[1] = 0
	lenBuf[2] = 0
	lenBuf[3] = 5 // length = 5
	buf.Write(lenBuf)
	// Write payload
	buf.Write(payload)

	result, err := ReadFraming(&buf)
	if err != nil {
		t.Fatalf("ReadFraming() error = %v, want nil", err)
	}

	if !bytes.Equal(result, payload) {
		t.Errorf("ReadFraming() = %v, want %v", result, payload)
	}
}

func TestReadFraming_ZeroLength(t *testing.T) {
	var buf bytes.Buffer
	// Write zero length
	lenBuf := make([]byte, 4)
	buf.Write(lenBuf)

	result, err := ReadFraming(&buf)
	if err != nil {
		t.Fatalf("ReadFraming() error = %v, want nil", err)
	}

	if result != nil {
		t.Errorf("ReadFraming() = %v, want nil", result)
	}
}

func TestReadFraming_LargePayload(t *testing.T) {
	// Create a payload that's exactly at the limit
	size := MaxPayloadSize

	var buf bytes.Buffer
	lenBuf := make([]byte, 4)
	lenBuf[0] = byte(size >> 24)
	lenBuf[1] = byte(size >> 16)
	lenBuf[2] = byte(size >> 8)
	lenBuf[3] = byte(size)
	buf.Write(lenBuf)

	// Write large payload
	payload := make([]byte, size)
	for i := 0; i < size; i++ {
		payload[i] = byte(i % 256)
	}
	buf.Write(payload)

	result, err := ReadFraming(&buf)
	if err != nil {
		t.Fatalf("ReadFraming() error = %v, want nil", err)
	}

	if len(result) != size {
		t.Errorf("ReadFraming() length = %d, want %d", len(result), size)
	}

	if !bytes.Equal(result, payload) {
		t.Error("ReadFraming() payload mismatch")
	}
}

func TestReadFraming_PayloadTooLarge(t *testing.T) {
	var buf bytes.Buffer

	// Write length that's too large
	size := MaxPayloadSize + 1
	lenBuf := make([]byte, 4)
	lenBuf[0] = byte(size >> 24)
	lenBuf[1] = byte(size >> 16)
	lenBuf[2] = byte(size >> 8)
	lenBuf[3] = byte(size)
	buf.Write(lenBuf)

	result, err := ReadFraming(&buf)
	if err == nil {
		t.Error("ReadFraming() error = nil, want error")
	}

	if result != nil {
		t.Errorf("ReadFraming() = %v, want nil", result)
	}

	if !strings.Contains(err.Error(), "payload too large") {
		t.Errorf("ReadFraming() error = %v, want 'payload too large'", err)
	}
}

func TestReadFraming_ReadLengthError(t *testing.T) {
	// Create a reader that returns EOF immediately
	buf := bytes.NewReader([]byte{})

	result, err := ReadFraming(buf)
	if err == nil {
		t.Error("ReadFraming() error = nil, want error")
	}

	if result != nil {
		t.Errorf("ReadFraming() = %v, want nil", result)
	}
}

func TestReadFraming_ReadLengthPartial(t *testing.T) {
	// Only 2 bytes instead of 4
	buf := bytes.NewReader([]byte{0x00, 0x01})

	result, err := ReadFraming(buf)
	if err == nil {
		t.Error("ReadFraming() error = nil, want error")
	}

	if result != nil {
		t.Errorf("ReadFraming() = %v, want nil", result)
	}
}

func TestReadFraming_ReadPayloadError(t *testing.T) {
	var buf bytes.Buffer

	// Write length = 10
	lenBuf := make([]byte, 4)
	lenBuf[3] = 10
	buf.Write(lenBuf)

	// Only write 5 bytes instead of 10
	buf.Write([]byte{0x01, 0x02, 0x03, 0x04, 0x05})

	result, err := ReadFraming(&buf)
	if err == nil {
		t.Error("ReadFraming() error = nil, want error")
		return
	}

	if result != nil {
		t.Errorf("ReadFraming() = %v, want nil", result)
	}

	if !strings.Contains(err.Error(), "payload read error") {
		t.Errorf("ReadFraming() error = %v, want 'payload read error'", err)
	}
}

func TestReadFraming_MultipleFrames(t *testing.T) {
	var buf bytes.Buffer

	// Write first frame
	payload1 := []byte{0x01, 0x02, 0x03}
	lenBuf1 := make([]byte, 4)
	lenBuf1[3] = 3
	buf.Write(lenBuf1)
	buf.Write(payload1)

	// Write second frame
	payload2 := []byte{0x04, 0x05}
	lenBuf2 := make([]byte, 4)
	lenBuf2[3] = 2
	buf.Write(lenBuf2)
	buf.Write(payload2)

	// Read first frame
	result1, err := ReadFraming(&buf)
	if err != nil {
		t.Fatalf("ReadFraming() first frame error = %v, want nil", err)
	}
	if !bytes.Equal(result1, payload1) {
		t.Errorf("ReadFraming() first frame = %v, want %v", result1, payload1)
	}

	// Read second frame
	result2, err := ReadFraming(&buf)
	if err != nil {
		t.Fatalf("ReadFraming() second frame error = %v, want nil", err)
	}
	if !bytes.Equal(result2, payload2) {
		t.Errorf("ReadFraming() second frame = %v, want %v", result2, payload2)
	}
}

func TestWriteFraming_ReadFraming_Roundtrip(t *testing.T) {
	var buf bytes.Buffer

	header := &ResponseHeader{
		CorrelationId: 999,
	}

	respData := []byte{0xAA, 0xBB, 0xCC, 0xDD}
	resp := &mockResponse{
		data: respData,
	}

	// Write frame
	err := WriteFraming(&buf, header, resp)
	if err != nil {
		t.Fatalf("WriteFraming() error = %v", err)
	}

	// Read frame
	payload, err := ReadFraming(&buf)
	if err != nil {
		t.Fatalf("ReadFraming() error = %v", err)
	}

	// Decode header
	reader := NewReader(payload)
	decodedHeader := &ResponseHeader{}
	err = decodedHeader.Decode(reader)
	if err != nil {
		t.Fatalf("Header.Decode() error = %v", err)
	}

	// Verify header - only CorrelationId is in ResponseHeader
	if decodedHeader.CorrelationId != header.CorrelationId {
		t.Errorf("CorrelationId = %d, want %d", decodedHeader.CorrelationId, header.CorrelationId)
	}

	// Verify remaining data is the response data
	remaining := payload[reader.pos:]
	if !bytes.Equal(remaining, respData) {
		t.Errorf("Response data = %v, want %v", remaining, respData)
	}
}

func TestMaxPayloadSize_Constant(t *testing.T) {
	expected := 1024 * 1024 * 100
	if MaxPayloadSize != expected {
		t.Errorf("MaxPayloadSize = %d, want %d", MaxPayloadSize, expected)
	}
}

func TestWriteFraming_LargePayload(t *testing.T) {
	var buf bytes.Buffer

	header := &ResponseHeader{
		ApiKey:        1,
		ApiVersion:    1,
		CorrelationId: 1,
	}

	// Create a large payload
	largeData := make([]byte, 1024*1024) // 1 MB
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	resp := &mockResponse{
		data: largeData,
	}

	err := WriteFraming(&buf, header, resp)
	if err != nil {
		t.Fatalf("WriteFraming() error = %v, want nil", err)
	}

	// Verify we can read it back
	payload, err := ReadFraming(&buf)
	if err != nil {
		t.Fatalf("ReadFraming() error = %v", err)
	}

	if len(payload) < len(largeData) {
		t.Errorf("Payload length = %d, want at least %d", len(payload), len(largeData))
	}
}

func TestReadFraming_EmptyReader(t *testing.T) {
	buf := bytes.NewReader([]byte{})

	result, err := ReadFraming(buf)
	if err == nil {
		t.Error("ReadFraming() error = nil, want error")
	}
	if result != nil {
		t.Errorf("ReadFraming() = %v, want nil", result)
	}
}

func TestWriteFraming_NilClientId(t *testing.T) {
	var buf bytes.Buffer

	header := &ResponseHeader{
		ApiKey:        1,
		ApiVersion:    2,
		CorrelationId: 123,
	}

	resp := &mockResponse{
		data: []byte{0x01, 0x02},
	}

	err := WriteFraming(&buf, header, resp)
	if err != nil {
		t.Fatalf("WriteFraming() error = %v, want nil", err)
	}

	// Should handle response gracefully
	if buf.Len() == 0 {
		t.Error("WriteFraming() wrote no data")
	}
}

func TestReadFraming_ExactlyMaxPayloadSize(t *testing.T) {
	var buf bytes.Buffer

	// Write exactly MaxPayloadSize
	size := MaxPayloadSize
	lenBuf := make([]byte, 4)
	lenBuf[0] = byte(size >> 24)
	lenBuf[1] = byte(size >> 16)
	lenBuf[2] = byte(size >> 8)
	lenBuf[3] = byte(size)
	buf.Write(lenBuf)

	// Write the payload
	payload := make([]byte, size)
	buf.Write(payload)

	result, err := ReadFraming(&buf)
	if err != nil {
		t.Fatalf("ReadFraming() error = %v, want nil", err)
	}

	if len(result) != size {
		t.Errorf("ReadFraming() length = %d, want %d", len(result), size)
	}
}
