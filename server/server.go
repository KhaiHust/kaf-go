package server

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/KhaiHust/kaf-go/config"
	"github.com/KhaiHust/kaf-go/storage"
	"github.com/KhaiHust/kaf-go/storage/commitlog"
)

type Server struct {
	addr       string
	logDir     string
	brokerIDs  []int32
	topicStore *storage.TopicStore
	listener   net.Listener
	wg         *sync.WaitGroup
	quit       chan struct{}
}

func NewServer(addr, logDir string, brokerIDs []int32) *Server {
	return &Server{
		addr:       addr,
		logDir:     logDir,
		brokerIDs:  brokerIDs,
		topicStore: storage.NewTopicStore(),
		quit:       make(chan struct{}),
		wg:         new(sync.WaitGroup),
	}
}

func (s *Server) Start() error {
	if err := s.recoverTopic(); err != nil {
		return err
	}

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

func (s *Server) recoverTopic() error {
	topicEntries, err := os.ReadDir(s.logDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}

	recovered := make(map[string]*storage.TopicMeta) // topicName - TopicMetadata

	for _, topicEntry := range topicEntries {
		if !topicEntry.IsDir() {
			continue
		}

		topicDir := filepath.Join(s.logDir, topicEntry.Name())

		meta, err := storage.LoadTopicMeta(topicDir)
		if err != nil {
			slog.Warn("load topic meta failed, ignore it")
			continue
		}

		if _, ok := recovered[topicEntry.Name()]; ok {
			continue
		}

		if err := s.topicStore.AddTopicMetadata(meta); err != nil {
			slog.Warn("add topic meta failed, ignore it")
			continue
		}

		partitionEntries, err := os.ReadDir(topicDir)
		if err != nil {
			return err
		}

		for _, partEntry := range partitionEntries {
			if !partEntry.IsDir() {
				continue // skip meta.json and other files
			}

			partitionDir := filepath.Join(topicDir, partEntry.Name())
			partitionIndex := parsePartitionIndex(partEntry.Name(), meta.Name)

			commitLog, err := commitlog.NewCommitLog(partitionDir, defaultCommitLogConfig())
			if err != nil {
				return fmt.Errorf("failed to recover commitlog %s: %w", partitionDir, err)
			}

			s.topicStore.AddCommitLog(meta.Name, partitionIndex, commitLog)

			slog.Info("recovered partition",
				"topic", meta.Name,
				"partition", partitionIndex,
				"newestOffset", commitLog.NewestOffset(),
			)
		}

		recovered[topicEntry.Name()] = meta

		slog.Info("recovered topic", "topic", meta.Name, "numPartitions", meta.NumPartitions)
	}
	return nil
}

func parsePartitionIndex(dirName string, topicName string) int32 {
	// remove topicName prefix + "-"
	// "topicName-partitionIndex" → "partitionIndex"
	suffix := strings.TrimPrefix(dirName, topicName+"-")
	idx, err := strconv.Atoi(suffix)
	if err != nil {
		slog.Warn("failed to parse partition index", "dir", dirName)
		return 0
	}
	return int32(idx)
}

func defaultCommitLogConfig() *config.CommitLogConfig {
	return &config.CommitLogConfig{
		SegmentMaxBytes: 1024 * 1024 * 100,
		IndexInterval:   4 * 1024,
		RetentionBytes:  1024 * 1024 * 100,
		RetentionTime:   7 * 24 * time.Hour,
	}

}
