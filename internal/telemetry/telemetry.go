package telemetry

import (
	"context"
	"log"
	"time"

	"dlockss/internal/config"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
)

// No dedicated telemetry topic - nodes broadcast on their shard topics

type TelemetryClient struct {
	host      host.Host
	ps        *pubsub.PubSub
	metrics   *MetricsManager
	shardInfo ShardInfoProvider
	publisher ShardPublisher // Interface to publish to shard
}

type ShardPublisher interface {
	PublishToShardCBOR(data []byte, shardID string)
}

func NewTelemetryClient(h host.Host, ps *pubsub.PubSub, metrics *MetricsManager) *TelemetryClient {
	// monitorIDStr is ignored - nodes are agnostic of monitor
	// Nodes broadcast telemetry on their current shard topic, monitor listens on all shards

	if ps == nil {
		log.Printf("[Telemetry] PubSub not available, telemetry disabled")
		return nil
	}

	tc := &TelemetryClient{
		host:    h,
		ps:      ps,
		metrics: metrics,
	}

	log.Printf("[Telemetry] Telemetry client initialized (will broadcast on shard topics)")

	return tc
}

// SetShardPublisher sets the shard publisher (called after initialization to break cycle)
func (tc *TelemetryClient) SetShardPublisher(sp ShardPublisher, sip ShardInfoProvider) {
	tc.publisher = sp
	tc.shardInfo = sip
}

func (tc *TelemetryClient) Start(ctx context.Context) {
	log.Printf("[Telemetry] Starting telemetry client (pubsub-based, monitor-agnostic)")
	go tc.runLoop(ctx)
}

func (tc *TelemetryClient) runLoop(ctx context.Context) {
	ticker := time.NewTicker(config.TelemetryInterval) // Report at configured interval (default: 2 minutes)
	defer ticker.Stop()

	// Try to discover monitor immediately on startup
	tc.pushTelemetry()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tc.pushTelemetry()
		}
	}
}

func (tc *TelemetryClient) pushTelemetry() {
	if tc.metrics == nil {
		return
	}

	// 1. Gather Data (Reuse StatusResponse)
	status := tc.metrics.GetStatus()
	// status.PeerID = tc.host.ID().String() // Unused write

	// Log telemetry data for debugging
	if config.VerboseLogging {
		log.Printf("[Telemetry] Sending status: pinned=%d, known=%d, shard=%s, peers=%d",
			status.Storage.PinnedFiles, status.Storage.KnownFiles, status.CurrentShard, status.PeersInShard)
	}

	// 2. Marshal
	/*
	data, err := json.Marshal(status)
	if err != nil {
		log.Printf("[Telemetry] Marshal error: %v", err)
		return
	}
	*/

	// 3. Broadcast on current shard topic (monitor listens on all shards)
	// DISABLED: JSON telemetry causes CBOR decoding errors on other nodes and is not
	// currently parsed by the monitor (which uses heartbeats/ingests).
	// Re-enable only if using a separate topic or if monitor parses JSON.
	/*
	if tc.publisher == nil {
		log.Printf("[Telemetry] ShardPublisher not available, cannot broadcast telemetry")
		return
	}

	// Get current shard and publish telemetry on that shard's topic
	currentShard := status.CurrentShard
	if tc.shardInfo != nil {
		// Use fresh shard info if available, though metrics.GetStatus() should have it
		currentShard, _ = tc.shardInfo.GetShardInfo()
	}

	// Publish to current shard topic - monitor listens on all shard topics
	tc.publisher.PublishToShardCBOR(data, currentShard)

	log.Printf("[Telemetry] Successfully broadcasted status on shard %s (monitor-agnostic)", currentShard)
	*/
}
