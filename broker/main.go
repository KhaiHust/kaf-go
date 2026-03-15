package main

import (
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	server2 "github.com/KhaiHust/kaf-go/server"
)

func main() {
	server := server2.NewServer(":9092", "var/log", []int32{1})
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
