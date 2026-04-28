package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/KhaiHust/kaf-go/api"
	"github.com/KhaiHust/kaf-go/config"
	"github.com/KhaiHust/kaf-go/coordinator"
	"github.com/KhaiHust/kaf-go/metrics"
	metadatapkg "github.com/KhaiHust/kaf-go/protocol/metadata"
	"github.com/KhaiHust/kaf-go/storage"
	"github.com/KhaiHust/kaf-go/storage/commitlog"
	"github.com/gofrs/uuid/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Server struct {
	addr                  string
	advertisedHost        string // host this broker advertises in Metadata responses; empty → "localhost"
	logDir                string
	brokerID              int32
	metricsPort           int
	metricsServer         *http.Server
	topicStore            *storage.TopicStore
	groupStore            *coordinator.GroupStore
	partitionStateStore   *coordinator.PartitionStateStore
	replicaFetcherManager *ReplicaFetcherManager
	brokerRegistry        *BrokerRegistry
	metadataLog           *coordinator.MetadataLog
	brokerRegistrar       *BrokerRegistrar
	metadataFetcher       *MetadataFetcher
	isrManager            *ISRManager
	pidManager            *coordinator.PidManager
	clusterID             string
	brokerEpoch           int64 // atomic; set after registration with controller
	listener              net.Listener
	wg                    *sync.WaitGroup
	quit                  chan struct{}
}

func NewServer(addr, logDir string, brokerID int32, advertisedHost string, metricsPort int) *Server {
	s := &Server{
		addr:                addr,
		advertisedHost:      advertisedHost,
		logDir:              logDir,
		brokerID:            brokerID,
		metricsPort:         metricsPort,
		topicStore:          storage.NewTopicStore(),
		groupStore:          coordinator.NewGroupStore(),
		partitionStateStore: coordinator.NewPartitionStateStore(),
		brokerRegistry:      NewBrokerRegistry(),
		metadataLog:         coordinator.NewMetadataLog(),
		pidManager:          coordinator.NewPidManager(),
		quit:                make(chan struct{}),
		wg:                  new(sync.WaitGroup),
	}
	s.replicaFetcherManager = NewReplicaFetcherManager(s)
	// Register this broker in its own registry so IsController() works immediately.
	s.brokerRegistry.RegisterBroker(brokerID, s.AdvertisedHost(), parsePort(addr))
	return s
}

// AdvertisedHost returns the host this broker advertises in Metadata / FindCoordinator
// responses and BrokerRegistration to the controller. Falls back to "localhost" when
// unset so single-host development workflows keep working.
func (s *Server) AdvertisedHost() string {
	if s.advertisedHost != "" {
		return s.advertisedHost
	}
	return "localhost"
}

func parsePort(addr string) int32 {
	_, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return 9092
	}
	port, _ := strconv.Atoi(portStr)
	return int32(port)
}

func (s *Server) Start() error {
	if err := s.recoverTopic(); err != nil {
		return err
	}
	if err := s.initMetadataLog(); err != nil {
		return err
	}
	s.recoverPidManager()

	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.listener = listener
	s.wg.Add(1)
	go s.acceptLoop()

	if !s.IsController() && s.brokerRegistrar != nil {
		go s.brokerRegistrar.RegisterWithRetry()
	}
	if !s.IsController() && s.metadataFetcher != nil {
		go s.metadataFetcher.Run()
	}

	s.isrManager = NewISRManager(s)
	go s.isrManager.Run()

	s.startMetricsServer()

	return nil
}

func (s *Server) Stop() {
	if s.isrManager != nil {
		s.isrManager.Stop()
	}
	if s.metricsServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = s.metricsServer.Shutdown(ctx)
		cancel()
	}
	s.listener.Close()
	close(s.quit)
	s.wg.Wait()
}

// startMetricsServer exposes /metrics on s.metricsPort. metricsPort==0 disables.
func (s *Server) startMetricsServer() {
	if s.metricsPort == 0 {
		return
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	s.metricsServer = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.metricsPort),
		Handler: mux,
	}
	go func() {
		slog.Info("metrics: listening", "port", s.metricsPort)
		if err := s.metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Warn("metrics server error", "err", err)
		}
	}()
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

			var topicId uuid.UUID
			if topicId, err = uuid.FromString(meta.TopicId); err != nil {
				return err
			}
			s.topicStore.AddCommitLog(topicId, partitionIndex, commitLog)

			if err = s.partitionStateStore.SetLeader(meta.Name, partitionIndex, s.brokerID); err != nil {
				return err
			}

			if ps := s.partitionStateStore.GetPartitionState(meta.Name, partitionIndex); ps != nil && ps.Idempotence != nil {
				psm := coordinator.NewProducerStateManager(commitLog.Dir(), ps.Idempotence)
				psm.SetLabels(meta.Name, partitionIndex)
				ps.ProducerStateManager = psm
				topicName := meta.Name
				partIdx := partitionIndex
				cl := commitLog
				cl.SetSegmentRollHook(func(newBase int64) {
					psm.OnSegmentRoll(newBase)
					metrics.ActiveSegments.WithLabelValues(topicName, metrics.FormatPartition(partIdx)).Set(float64(cl.NumSegments()))
				})
				metrics.ActiveSegments.WithLabelValues(topicName, metrics.FormatPartition(partIdx)).Set(float64(cl.NumSegments()))
				if rerr := psm.Recover(commitLogReader{commitLog}); rerr != nil {
					slog.Warn("recover producer state failed", "topic", meta.Name, "partition", partitionIndex, "err", rerr)
				}
			}

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

func (s *Server) GetTopicStore() *storage.TopicStore {
	return s.topicStore
}
func (s *Server) GetGroupStore() *coordinator.GroupStore {
	return s.groupStore
}
func (s *Server) GetPartitionStateStore() *coordinator.PartitionStateStore {
	return s.partitionStateStore
}

func (s *Server) GetBrokerID() int32 {
	return s.brokerID
}

func (s *Server) GetLogDir() string {
	if s.logDir == "" {
		return api.LogStorageDataFolder
	}
	return s.logDir
}

func (s *Server) GetAllBrokerIDs() []int32 {
	brokerInfos := s.brokerRegistry.All()
	brokerIds := make([]int32, 0, len(brokerInfos))
	for _, info := range brokerInfos {
		brokerIds = append(brokerIds, info.BrokerId)
	}
	sort.Slice(brokerIds, func(i, j int) bool { return brokerIds[i] < brokerIds[j] })
	return brokerIds
}

func (s *Server) GetBrokerAddr(id int32) (host string, port int32) {
	return s.brokerRegistry.GetBrokerAddr(id)
}

func (s *Server) StartFollowerFetch(topicName string, partition, leaderID int32) {
	s.replicaFetcherManager.AddFetcher(topicName, partition, leaderID)
}

func (s *Server) clusterId() string {
	return s.clusterID
}

func (s *Server) IsController() bool {
	return s.brokerID == s.brokerRegistry.LowestBrokerID()
}

func (s *Server) GetBrokerRegistry() api.IBrokerRegistry {
	return s.brokerRegistry
}

func (s *Server) GetMetadataLog() api.IMetadataLog {
	return s.metadataLog
}

func (s *Server) GetPidManager() *coordinator.PidManager {
	return s.pidManager
}

func (s *Server) recoverPidManager() {
	var maxSeen int64
	if err := s.metadataLog.Replay(func(value []byte) {
		if rec, ok := metadatapkg.Decode(value).(*metadatapkg.ProducerIdsRecord); ok {
			if rec.NextProducerId > maxSeen {
				maxSeen = rec.NextProducerId
			}
		}
	}); err != nil {
		slog.Warn("recoverPidManager: replay failed", "err", err)
	}
	if maxSeen > 0 {
		s.pidManager.SeedFromLog(maxSeen)
		slog.Info("recoverPidManager: seeded", "nextProducerId", maxSeen)
	}
	s.pidManager.Configure(s.brokerID, atomic.LoadInt64(&s.brokerEpoch), s.metadataLog)
}

func (s *Server) initMetadataLog() error {
	dir := filepath.Join(s.logDir, "__cluster_metadata", "__cluster_metadata-0")
	cl, err := commitlog.NewCommitLog(dir, defaultCommitLogConfig())
	if err != nil {
		return fmt.Errorf("init metadata log: %w", err)
	}
	s.metadataLog.Init(cl)

	metaTopicID := metadatapkg.ClusterMetadataTopicID
	_ = s.topicStore.RegisterTopic(metaTopicID, "__cluster_metadata", 1)
	s.topicStore.AddCommitLog(metaTopicID, 0, cl)

	controllerID := s.brokerRegistry.LowestBrokerID()
	s.partitionStateStore.SetPartitionState(
		"__cluster_metadata", 0, controllerID, 0,
		[]int32{controllerID}, []int32{controllerID},
	)
	if ps := s.partitionStateStore.GetPartitionState("__cluster_metadata", 0); ps != nil {
		s.metadataLog.SetPartitionState(ps)
	}
	slog.Info("metadata log initialized", "dir", dir, "leader", controllerID)
	return nil
}

func (s *Server) ApplyMetadataRecord(value []byte) {
	rec := metadatapkg.Decode(value)
	if rec == nil {
		slog.Warn("metadata: unknown record type", "typeByte", value[0])
		return
	}
	switch r := rec.(type) {
	case *metadatapkg.BrokerRecord:
		s.brokerRegistry.RegisterBroker(r.BrokerId, r.Host, r.Port)
		slog.Info("metadata: broker registered", "brokerId", r.BrokerId, "host", r.Host, "port", r.Port)

	case *metadatapkg.TopicRecord:
		_ = s.topicStore.RegisterTopic(r.TopicId, r.Name, r.NumPartitions)
		minISR := r.MinInsyncReplicas
		if minISR <= 0 {
			minISR = 1
		}

		topicDir := filepath.Join(s.logDir, r.Name)
		meta := storage.TopicMeta{
			Name:              r.Name,
			TopicId:           r.TopicId.String(),
			NumPartitions:     r.NumPartitions,
			ReplicationFactor: 1,
			MinInsyncReplicas: minISR,
		}
		_ = storage.SaveTopicMeta(topicDir, meta)
		s.topicStore.SetTopicMeta(r.TopicId, &meta)
		slog.Info("metadata: topic registered", "topic", r.Name, "partitions", r.NumPartitions, "minISR", minISR)

	case *metadatapkg.ProducerIdsRecord:
		s.pidManager.SeedFromLog(r.NextProducerId)
		slog.Info("metadata: producer ids leased",
			"brokerId", r.BrokerId, "brokerEpoch", r.BrokerEpoch, "nextProducerId", r.NextProducerId)

	case *metadatapkg.PartitionRecord:
		topicData, err := s.topicStore.GetTopic(r.TopicId)
		if err != nil {
			slog.Warn("metadata: partition record for unknown topic", "topicId", r.TopicId)
			return
		}
		topicName := string(topicData.Name)
		s.partitionStateStore.SetPartitionState(
			topicName, r.PartitionId, r.Leader, r.LeaderEpoch, r.Replicas, r.ISR,
		)

		if r.Leader == s.brokerID {
			if err := s.createPartitionLog(topicName, r.TopicId, r.PartitionId); err != nil {
				slog.Error("metadata: failed to create partition log",
					"topic", topicName, "partition", r.PartitionId, "err", err)
			}
		} else {
			s.replicaFetcherManager.AddFetcher(topicName, r.PartitionId, r.Leader)
		}
	}
}

// createPartitionLog creates the commit log directory and registers it in the topic store.
func (s *Server) createPartitionLog(topicName string, topicId uuid.UUID, partitionId int32) error {
	dir := filepath.Join(s.logDir, topicName, fmt.Sprintf("%s-%d", topicName, partitionId))
	cl, err := commitlog.NewCommitLog(dir, defaultCommitLogConfig())
	if err != nil {
		return fmt.Errorf("create partition log %s/%d: %w", topicName, partitionId, err)
	}
	s.topicStore.AddCommitLog(topicId, partitionId, cl)
	_ = s.partitionStateStore.SetLeader(topicName, partitionId, s.brokerID)

	if ps := s.partitionStateStore.GetPartitionState(topicName, partitionId); ps != nil && ps.Idempotence != nil {
		psm := coordinator.NewProducerStateManager(cl.Dir(), ps.Idempotence)
		psm.SetLabels(topicName, partitionId)
		ps.ProducerStateManager = psm
		clRef := cl
		topic := topicName
		part := partitionId
		clRef.SetSegmentRollHook(func(newBase int64) {
			psm.OnSegmentRoll(newBase)
			metrics.ActiveSegments.WithLabelValues(topic, metrics.FormatPartition(part)).Set(float64(clRef.NumSegments()))
		})
		metrics.ActiveSegments.WithLabelValues(topic, metrics.FormatPartition(part)).Set(float64(clRef.NumSegments()))
		if rerr := psm.Recover(commitLogReader{cl}); rerr != nil {
			slog.Warn("recover producer state failed", "topic", topicName, "partition", partitionId, "err", rerr)
		}
	}

	slog.Info("metadata: partition log created (this broker is leader)",
		"topic", topicName, "partition", partitionId)
	return nil
}

func (s *Server) SetPeers(peers map[int32]string) {
	for id, addr := range peers {
		if id == s.brokerID {
			continue
		}
		host, portStr, err := net.SplitHostPort(addr)
		if err != nil {
			slog.Error("failed to parse peer addr", "addr", addr, "error", err)
			continue
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			slog.Error("failed to parse peer port", "port", portStr, "error", err)
			continue
		}
		s.brokerRegistry.RegisterBroker(id, host, int32(port))
		if err := s.replicaFetcherManager.RegisterBrokerClient(id, addr); err != nil {
			slog.Warn("failed to dial peer for replica fetching, will retry on demand",
				"brokerId", id, "addr", addr, "err", err)
		}
	}

	if !s.IsController() {
		controllerID := s.brokerRegistry.LowestBrokerID()
		controllerAddr := s.brokerRegistry.Addr(controllerID)
		s.brokerRegistrar = NewBrokerRegistrar(s, controllerAddr)
		s.metadataFetcher = NewMetadataFetcher(s, controllerAddr)
	}
}

type commitLogReader struct {
	cl commitlog.ICommitLog
}

func (r commitLogReader) Dir() string {
	return r.cl.Dir()
}

func (r commitLogReader) ActiveSegmentBaseOffset() int64 {
	return r.cl.ActiveSegmentBaseOffset()
}

func (r commitLogReader) WalkBatchHeadersFrom(fromOffset int64, fn func(coordinator.SnapshotBatchHeader) error) error {
	return r.cl.WalkBatchHeadersFrom(fromOffset, func(h commitlog.BatchHeader) error {
		return fn(coordinator.SnapshotBatchHeader{
			BaseOffset:      h.BaseOffset,
			LastOffsetDelta: h.LastOffsetDelta,
			ProducerId:      h.ProducerId,
			ProducerEpoch:   h.ProducerEpoch,
			BaseSequence:    h.BaseSequence,
		})
	})
}
