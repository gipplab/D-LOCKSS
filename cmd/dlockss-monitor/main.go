// Command dlockss-monitor runs the D-LOCKSS network monitor.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"dlockss/internal/monitor"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))

	geoipDB := flag.String("geoip-db", "", "Path to a MaxMind/DB-IP .mmdb GeoIP database file")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg := monitor.DefaultMonitorConfig()
	if v := os.Getenv("DLOCKSS_MONITOR_NODE_CLEANUP_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			cfg.NodeCleanupTimeout = d
			slog.Info("node cleanup timeout from env", "timeout", cfg.NodeCleanupTimeout)
		}
	}
	if v := os.Getenv("DLOCKSS_MONITOR_BOOTSTRAP_SHARD_DEPTH"); v != "" {
		if d, err := strconv.Atoi(v); err == nil && d >= 0 && d <= 12 {
			cfg.BootstrapShardDepth = d
			slog.Info("bootstrap shard depth from env", "depth", cfg.BootstrapShardDepth)
		}
	}
	if v := os.Getenv("DLOCKSS_PUBSUB_TOPIC_PREFIX"); v != "" {
		cfg.PubsubTopicPrefix = v
		slog.Info("pubsub topic prefix from env", "prefix", cfg.PubsubTopicPrefix)
	}

	geoDBPath := *geoipDB
	if geoDBPath == "" {
		geoDBPath = os.Getenv("DLOCKSS_MONITOR_GEOIP_DB")
	}
	saiaAPIKey := os.Getenv("SAIA_API_KEY")

	m := monitor.NewMonitor(cfg, geoDBPath, saiaAPIKey)
	defer m.Close()

	h, err := monitor.StartLibP2P(ctx, m)
	if err != nil {
		log.Fatalf("P2P error: %v", err)
	}
	defer h.Close()

	mux := http.NewServeMux()
	m.RegisterRoutes(mux)

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", monitor.WebUIPort),
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 20 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go m.RunStatusLogger(ctx)

	go func() {
		slog.Info("monitor started", "url", fmt.Sprintf("http://localhost:%d", monitor.WebUIPort), "peer_id", h.ID().String())
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("http server error", "error", err)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down gracefully")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("http shutdown error", "error", err)
	}
}
