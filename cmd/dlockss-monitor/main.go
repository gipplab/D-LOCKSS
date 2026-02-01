// Command dlockss-monitor runs a minimal observability server (metrics/status endpoints).
// It does not run a full D-LOCKSS node; use it to scrape metrics from nodes or as a placeholder
// for a future dashboard that aggregates multiple nodes.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"dlockss/internal/api"
	"dlockss/internal/config"
	"dlockss/internal/telemetry"
)

func main() {
	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	metrics := telemetry.NewMetricsManager()
	apiServer := api.NewAPIServer(config.APIPort, metrics)
	apiServer.Start()

	log.Printf("[Monitor] Observability server on :%d (/metrics, /status, /health)", config.APIPort)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Printf("[Monitor] Shutting down...")
	cancel()
	_ = apiServer.Shutdown(context.Background())
	os.Exit(0)
}
