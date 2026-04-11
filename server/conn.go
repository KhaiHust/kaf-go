package server

import (
	"fmt"
	"log/slog"
	"net"

	"github.com/KhaiHust/kaf-go/api"
	"github.com/KhaiHust/kaf-go/constant"
	"github.com/KhaiHust/kaf-go/protocol"
)

type Conn struct {
	NetConn net.Conn
	Server  *Server
}

func NewConn(conn net.Conn, server *Server) *Conn {
	return &Conn{NetConn: conn, Server: server}
}

func (c *Conn) handle() {
	defer func() {
		c.NetConn.Close()
		slog.Info("connection closed")
	}()

	for {
		framing, err := protocol.ReadFraming(c.NetConn)
		if err != nil || framing == nil {
			return
		}

		slog.Info("raw data received",
			"bytes", fmt.Sprintf("%x", framing),
			"length", len(framing),
		)

		reader := protocol.NewReader(framing)
		var requestHeader protocol.RequestHeader
		if err = requestHeader.Decode(reader); err != nil {
			slog.Error("decode request header failed: %v", err)
			return
		}

		apiKey := requestHeader.ApiKey
		apiVersion := requestHeader.ApiVersion
		slog.Info(fmt.Sprintf("api key: %v, version: %v", apiKey, apiVersion))
		c.HandleRequest(apiKey, &requestHeader, reader)
	}
}

func (c *Conn) HandleRequest(apiKey int16, header *protocol.RequestHeader, reader *protocol.Reader) {
	slog.Info(fmt.Sprintf("api key: %v", apiKey))
	//var resp []byte
	switch apiKey {
	case constant.ApiKeyProduce:
		err := api.HandleProduceApiKeys(c.NetConn, c.Server, header, reader)
		if err != nil {
			slog.Error("handle produce failed: %v", err)
		}
	case constant.ApiKeyFetch:
		err := api.HandleFetchApiKeys(c.NetConn, c.Server, header, reader)
		if err != nil {
			slog.Error("handle fetch failed: %v", err)
		}
	case constant.ApiKeyListOffsets:
		err := api.HandleListOffsetsApi(c.NetConn, header, reader, c.Server.topicStore)
		if err != nil {
			slog.Error("handle list offsets failed: %v", err)
		}
	case constant.ApiKeyOffsetCommit:
		err := api.HandleOffsetCommitApi(c.NetConn, header, reader, c.Server.groupStore)
		if err != nil {
			slog.Error("handle offset commit failed: %v", err)
		}
	case constant.ApiKeyOffsetFetch:
		err := api.HandleOffsetFetchApi(c.NetConn, header, reader, c.Server.groupStore)
		if err != nil {
			slog.Error("handle offset fetch failed: %v", err)
		}
	case constant.ApiFindCoordinator:
		err := api.HandleFindCoordinatorApi(c.NetConn, header, reader)
		if err != nil {
			slog.Error("handle find coordinator failed: %v", err)
		}
	case constant.ApiKeyJoinGroup:
		err := api.HandleJoinGroupApi(c.NetConn, header, reader, c.Server.groupStore)
		if err != nil {
			slog.Error("handle join group failed: %v", err)
		}

	case constant.ApiKeyLeaveGroup:
		err := api.HandleLeaveGroupApi(c.NetConn, header, reader, c.Server.groupStore)
		if err != nil {
			slog.Error("handle leave group failed: %v", err)
		}
	case constant.ApiKeyHeartbeat:
		err := api.HandleHeartbeatApi(c.NetConn, header, reader, c.Server.groupStore)
		if err != nil {
			slog.Error("handle heartbeat failed: %v", err)
		}
	case constant.ApiKeySyncGroup:
		err := api.HandleSyncGroupApi(c.NetConn, header, reader, c.Server.groupStore)
		if err != nil {
			slog.Error("handle sync group failed: %v", err)
		}

	case constant.ApiKeyApiVersions:
		responseHeader, response, err := api.HandleApiVersionApi(header, reader)
		if err != nil {
			slog.Error("handle api versions failed: %v", err)
			return
		}
		if err := protocol.WriteFraming(c.NetConn, responseHeader, response); err != nil {
			slog.Error("write response failed: %v", err)
		}

	case constant.ApiKeyMetaData:
		err := api.HandleMetaData(c.NetConn, c.Server, header, reader)
		if err != nil {
			slog.Error("handle meta data failed: %v", err)
		}
	case constant.ApiKeyCreateTopics:
		err := api.HandleCreateTopics(c.NetConn, c.Server, header, reader)
		if err != nil {
			slog.Error("handle create topics failed: %v", err)
		}
	default:
		slog.Error("api key not supported: %v", apiKey)
	}

}
