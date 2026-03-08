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
	conn   net.Conn
	server *Server
}

func NewConn(conn net.Conn, server *Server) *Conn {
	return &Conn{conn: conn, server: server}
}

func (c *Conn) handle() {
	defer func() {
		c.conn.Close()
		slog.Info("connection closed")
	}()

	for {
		framing, err := protocol.ReadFraming(c.conn)
		if err != nil || framing == nil {
			return
		}
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
	case constant.ApiKeyApiVersions:
		responseHeader, response, err := api.HandleApiVersionApi(header, reader)
		if err != nil {
			slog.Error("handle api versions failed: %v", err)
			return
		}
		if err := protocol.WriteFraming(c.conn, responseHeader, response); err != nil {
			slog.Error("write response failed: %v", err)
		}

	case constant.ApiKeyMetaData:
		err := api.HandleMetaData(c.conn, header, reader)
		if err != nil {
			slog.Error("handle meta data failed: %v", err)
		}
	case constant.ApiKeyCreateTopics:
		err := api.HandleCreateTopics(c.conn, header, reader)
		if err != nil {
			slog.Error("handle create topics failed: %v", err)
		}

	default:
		slog.Error("api key not supported: %v", apiKey)
	}

}
