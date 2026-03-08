package server

import (
	"fmt"
	"log/slog"
	"net"
	"sync"

	"github.com/KhaiHust/kaf-go/api"
)

type Server struct {
	addr      string
	logDir    string
	brokerIDs []int32
	store     *api.TopicStore
	listener  net.Listener
	wg        *sync.WaitGroup
	quit      chan struct{}
}

func NewServer(addr, logDir string, brokerIDs []int32) *Server {
	return &Server{
		addr:      addr,
		logDir:    logDir,
		brokerIDs: brokerIDs,
		store:     api.NewTopicStore(),
		quit:      make(chan struct{}),
		wg:        new(sync.WaitGroup),
	}
}

func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.listener = listener
	s.wg.Add(1)
	go s.acceptLoop()
	return nil
}

func (s *Server) Stop() {
	s.listener.Close()
	close(s.quit)
	s.wg.Wait()
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			slog.Info("accept loop stopping")
			return
		}
		c := NewConn(conn, s)
		slog.Info(fmt.Sprintf("accept loop starting, addr: %s", conn.RemoteAddr().String()))
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			c.handle()
		}()
	}
}
