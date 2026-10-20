// Command dlockss-monitor runs the D-LOCKSS network monitor.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"dlockss/internal/keywords"
	"dlockss/internal/monitor"
)

func main() {
	topicFlag := flag.String("topic", "", "archive topic name (overrides DLOCKSS_TOPIC_NAME env var)")
	prefixFlag := flag.String("prefix", "", "pubsub topic prefix / protocol version (overrides DLOCKSS_PUBSUB_TOPIC_PREFIX env var)")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	m := monitor.NewMonitor(monitorConfig(*topicFlag, *prefixFlag))

	kwCfg := keywords.ConfigFromEnv()
	if kwCfg.APIKey != "" {
		slog.Info("keyword indexing configured", "model", kwCfg.Model, "data_dir", kwCfg.DataDir)
	}
	m.SetKeywords(keywords.NewStore(kwCfg))
	m.StartKeywordIndexer(ctx)

	h, err := monitor.StartLibP2P(ctx, m)
	if err != nil {
		slog.Error("p2p error", "error", err)
		os.Exit(1)
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

// monitorConfig applies environment overrides to the defaults, then the
// command-line flags, which take precedence.
func monitorConfig(topicFlag, prefixFlag string) monitor.Config {
	cfg := monitor.DefaultConfig()
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
	if v := os.Getenv("DLOCKSS_TOPIC_NAME"); v != "" {
		cfg.TopicName = v
		slog.Info("topic name from env", "topic", cfg.TopicName)
	}
	if topicFlag != "" {
		cfg.TopicName = topicFlag
		slog.Info("topic name from flag", "topic", cfg.TopicName)
	}
	if prefixFlag != "" {
		cfg.PubsubTopicPrefix = prefixFlag
		slog.Info("pubsub topic prefix from flag", "prefix", cfg.PubsubTopicPrefix)
	}
	return cfg
}
