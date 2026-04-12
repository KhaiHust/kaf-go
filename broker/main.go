package main

import (
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	server2 "github.com/KhaiHust/kaf-go/server"
)

func parsePeers(s string) map[int32]string {
	result := make(map[int32]string)
	if s == "" {
		return result
	}
	for _, pair := range strings.Split(s, ",") {
		parts := strings.SplitN(strings.TrimSpace(pair), "=", 2)
		if len(parts) != 2 {
			log.Fatalf("invalid peer format %q, want id=host:port", pair)
		}
		id, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil {
			log.Fatalf("invalid peer broker id %q: %v", parts[0], err)
		}
		result[int32(id)] = strings.TrimSpace(parts[1])
	}
	return result
}

func main() {
	brokerConfig := struct{ BrokerId, Port int32 }{BrokerId: 1, Port: 9092}

	brokerId := flag.Int("broker-id", int(brokerConfig.BrokerId), "Broker ID")
	port := flag.Int("port", int(brokerConfig.Port), "Broker port")
	logDir := flag.String("log-dir", "var/log", "Directory for log storage")
	peersFlag := flag.String("peers", "", `Peer brokers, e.g. "2=localhost:9093,3=localhost:9094"`)
	flag.Parse()

	peers := parsePeers(*peersFlag)

	server := server2.NewServer(fmt.Sprintf(":%d", *port), *logDir, int32(*brokerId))
	server.SetPeers(peers)

	err := server.Start()
	if err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	// Wait for interrupt signal to gracefully shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	slog.Info("Shutting down server...")
	server.Stop()
	slog.Info("Server stopped gracefully")
}
