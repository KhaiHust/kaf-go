package main

import (
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/KhaiHust/kaf-go/config"
	server2 "github.com/KhaiHust/kaf-go/server"
)

func main() {
	brokerConfig := config.BrokerConfig{
		BrokerId: 1,
		Host:     "localhost",
		Port:     9092,
	}

	brokerId := flag.Int("broker-id", int(brokerConfig.BrokerId), "Broker ID")
	//host := flag.String("host", brokerConfig.Host, "Broker host")
	port := flag.Int("port", int(brokerConfig.Port), "Broker port")
	logDir := flag.String("log-dir", "var/log", "Directory for log storage")
	flag.Parse()

	server := server2.NewServer(fmt.Sprintf(":%d", *port), *logDir, int32(*brokerId))
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
