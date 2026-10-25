// Command dlockss-monitor observes a D-LOCKSS network as a silent PROBE and
// serves a dashboard on port 8080: nodes, shards, replication health, and an
// optional LLM-backed keyword search over the preserved content.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/internal/config"
	"dlockss/internal/keywords"
	"dlockss/internal/monitor"
)

func main() {
	topicFlag := flag.String("topic", "", "topic name (overrides DLOCKSS_TOPIC_NAME)")
	prefixFlag := flag.String("prefix", "", "pubsub topic prefix (overrides DLOCKSS_PUBSUB_TOPIC_PREFIX)")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	topicName := firstNonEmpty(*topicFlag, os.Getenv("DLOCKSS_TOPIC_NAME"), config.DefaultTopicName)
	topicPrefix := firstNonEmpty(*prefixFlag, os.Getenv("DLOCKSS_PUBSUB_TOPIC_PREFIX"), config.DefaultPubsubVersion)

	nodeTimeout := 350 * time.Second
	if v := os.Getenv("DLOCKSS_MONITOR_NODE_CLEANUP_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			nodeTimeout = d
		} else {
			slog.Warn("invalid DLOCKSS_MONITOR_NODE_CLEANUP_TIMEOUT, using default", "value", v)
		}
	}
	dataDir := keywords.DataDir()
	net, err := monitor.NewNet(ctx, dataDir)
	if err != nil {
		slog.Error("could not start the monitor host", "err", err)
		os.Exit(1)
	}
	defer net.Host.Close()
	slog.Info("monitor started", "peer_id", net.Host.ID(), "topic", topicName, "prefix", topicPrefix)

	state := monitor.NewState(net.Host.ID(), nodeTimeout)
	topicOf := func(shardID string) string { return topicPrefix + "-" + topicName + "-shard-" + shardID }
	subs := monitor.NewSubscriber(net, state, topicOf)
	subs.Bootstrap(ctx)
	go subs.RunDynamic(ctx)
	go state.RunCleanup(ctx)
	go net.RunPeerstoreGC(ctx,
		func(p peer.ID) bool {
			for _, n := range state.Nodes() {
				if n.PeerID == p {
					return true
				}
			}
			return false
		},
		subs.TopicPeers,
	)

	settings := keywords.LoadSettings(dataDir)
	store := keywords.NewStore(dataDir)
	indexer := keywords.NewIndexer(store, settings, state.UniqueCIDs)
	go indexer.Run(ctx)

	server := monitor.NewServer(state, net, indexer, topicPrefix, topicName)
	slog.Info("dashboard listening", "addr", "http://localhost:8080")
	if err := server.Run(ctx); err != nil && ctx.Err() == nil {
		slog.Error("http server failed", "err", err)
		os.Exit(1)
	}
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
