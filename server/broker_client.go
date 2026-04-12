package server

import (
	"encoding/binary"
	"net"
	"sync"

	"github.com/KhaiHust/kaf-go/common"
	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
	"github.com/KhaiHust/kaf-go/protocol/fetch"
)

type BrokerClient struct {
	brokerId          int32
	addr              string
	conn              net.Conn
	mu                sync.Mutex
	nextCorrelationId int32
}

func NewBrokerClient(conn net.Conn, brokerId int32, add string) *BrokerClient {
	return &BrokerClient{
		brokerId:          brokerId,
		addr:              add,
		mu:                sync.Mutex{},
		conn:              conn,
		nextCorrelationId: 1,
	}
}

func (bc *BrokerClient) Connect() error {
	// open TCP
	conn, err := net.Dial("tcp", bc.addr)
	if err != nil {
		return err
	}
	bc.conn = conn
	return nil
}

func (bc *BrokerClient) Fetch(req *fetch.FetchRequest) (*fetch.FetchResponse, error) {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	w := protocol.NewWriter(512)

	var requestHeader = &protocol.RequestHeader{
		ApiKey:        constant.ApiKeyFetch,
		ApiVersion:    18,
		CorrelationId: bc.nextCorrelationId,
		ClientId:      common.StringPtr("broker-client"),
	}

	if err := requestHeader.Encode(w); err != nil {
		return nil, err
	}

	if err := req.Encode(w); err != nil {
		return nil, err
	}

	payload := w.Bytes()
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))

	if _, err := bc.conn.Write(lenBuf[:]); err != nil {
		return nil, err
	}
	if _, err := bc.conn.Write(payload); err != nil {
		return nil, err
	}

	bc.nextCorrelationId++

	respFraming, err := protocol.ReadFraming(bc.conn)
	if err != nil {
		return nil, err
	}

	r := protocol.NewReader(respFraming)
	var responseHeader protocol.ResponseHeader
	responseHeader.ApiKey = constant.ApiKeyFetch
	responseHeader.ApiVersion = 18
	if err = responseHeader.Decode(r); err != nil {
		return nil, err
	}

	var fetchResp fetch.FetchResponse
	if err = fetchResp.Decode(r); err != nil {
		return nil, err
	}

	return &fetchResp, nil
}

func (bc *BrokerClient) Close() error {
	if bc.conn != nil {
		return bc.conn.Close()
	}
	return nil
}
