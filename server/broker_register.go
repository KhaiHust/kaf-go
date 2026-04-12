package server

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/KhaiHust/kaf-go/common"
	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
	brokerproto "github.com/KhaiHust/kaf-go/protocol/broker"
	"github.com/KhaiHust/kaf-go/protocol/types"
	"github.com/gofrs/uuid/v5"
)

type BrokerRegistrar struct {
	server         *Server
	controllerAddr string
	conn           net.Conn
	correlationId  int32
	stopCh         chan struct{}
}

func NewBrokerRegistrar(server *Server, controllerAddr string) *BrokerRegistrar {
	return &BrokerRegistrar{
		server:         server,
		controllerAddr: controllerAddr,
		stopCh:         make(chan struct{}),
	}
}

func (br *BrokerRegistrar) RegisterWithRetry() {
	backoff := time.Second
	for {
		if err := br.register(); err != nil {
			slog.Warn("broker registration failed, retrying",
				"err", err, "backoff", backoff)
			select {
			case <-br.stopCh:
				return
			case <-time.After(backoff):
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		go br.heartbeatLoop()
		return
	}
}

func (br *BrokerRegistrar) register() error {
	conn, err := net.Dial("tcp", br.controllerAddr)
	if err != nil {
		return fmt.Errorf("dial controller %s: %w", br.controllerAddr, err)
	}
	br.conn = conn

	host, port := br.ownHostPort()
	incarnationId, _ := uuid.NewV4()

	req := &brokerproto.BrokerRegistrationRequest{
		BrokerId:      br.server.brokerID,
		ClusterId:     types.CompactString(br.server.clusterID),
		IncarnationId: incarnationId,
		Listeners: []brokerproto.BrokerListener{
			{
				Name:             "PLAINTEXT",
				Host:             types.CompactString(host),
				Port:             port,
				SecurityProtocol: 0,
			},
		},
		Features: []brokerproto.BrokerFeature{},
	}

	var regResp brokerproto.BrokerRegistrationResponse
	if err := br.sendRequest(constant.ApiKeyBrokerRegistration, req, &regResp); err != nil {
		return err
	}
	if regResp.ErrorCode != 0 {
		return constant.CreateError(regResp.ErrorCode)
	}

	atomic.StoreInt64(&br.server.brokerEpoch, regResp.BrokerEpoch)
	slog.Info("registered with controller",
		"brokerId", br.server.brokerID,
		"epoch", regResp.BrokerEpoch,
	)
	return nil
}

func (br *BrokerRegistrar) heartbeatLoop() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-br.stopCh:
			return
		case <-ticker.C:
			if err := br.sendHeartbeat(); err != nil {
				slog.Warn("heartbeat failed", "err", err)
			}
		}
	}
}

func (br *BrokerRegistrar) sendHeartbeat() error {
	var metadataOffset int64
	if br.server.metadataFetcher != nil {
		metadataOffset = br.server.metadataFetcher.CurrentOffset()
	}
	req := &brokerproto.BrokerHeartbeatRequest{
		BrokerId:              br.server.brokerID,
		BrokerEpoch:           atomic.LoadInt64(&br.server.brokerEpoch),
		CurrentMetadataOffset: metadataOffset,
		WantFence:             false,
		WantShutDown:          false,
	}

	var hbResp brokerproto.BrokerHeartbeatResponse
	if err := br.sendRequest(constant.ApiKeyBrokerHeartbeat, req, &hbResp); err != nil {
		return err
	}
	if hbResp.ErrorCode != 0 {
		return constant.CreateError(hbResp.ErrorCode)
	}
	if hbResp.ShouldShutDown {
		slog.Info("controller requested shutdown")
		br.server.Stop()
	}
	return nil
}

func (br *BrokerRegistrar) sendRequest(apiKey int16, req protocol.IEncoder, respBody protocol.IDecoder) error {
	cid := atomic.AddInt32(&br.correlationId, 1)

	w := protocol.NewWriter(256)
	hdr := &protocol.RequestHeader{
		ApiKey:        apiKey,
		ApiVersion:    0,
		CorrelationId: cid,
		ClientId:      common.StringPtr("broker-registrar"),
	}
	if err := hdr.Encode(w); err != nil {
		return err
	}
	if err := req.Encode(w); err != nil {
		return err
	}

	payload := w.Bytes()
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))
	if _, err := br.conn.Write(lenBuf[:]); err != nil {
		return err
	}
	if _, err := br.conn.Write(payload); err != nil {
		return err
	}

	framing, err := protocol.ReadFraming(br.conn)
	if err != nil {
		return err
	}
	r := protocol.NewReader(framing)
	var respHdr protocol.ResponseHeader
	if err = respHdr.Decode(r); err != nil {
		return err
	}
	return respBody.Decode(r)
}

func (br *BrokerRegistrar) Stop() {
	close(br.stopCh)
	if br.conn != nil {
		br.conn.Close()
	}
}

func (br *BrokerRegistrar) ownHostPort() (string, int32) {
	addr := br.server.addr
	parts := strings.SplitN(addr, ":", 2)
	host := parts[0]
	if host == "" || host == "0.0.0.0" {
		host = "localhost"
	}
	port := int32(9092)
	if len(parts) == 2 {
		if p, err := strconv.Atoi(parts[1]); err == nil {
			port = int32(p)
		}
	}
	return host, port
}
