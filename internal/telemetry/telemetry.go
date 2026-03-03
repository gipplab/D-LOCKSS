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

// pushTelemetry gathers and logs local node status.
// Publishing over pubsub is disabled: JSON telemetry causes CBOR decoding
// errors on other nodes and is not currently parsed by the monitor.
func (tc *TelemetryClient) pushTelemetry() {
	if tc.metrics == nil {
		return
	}

	status := tc.metrics.GetStatus()

	if config.VerboseLogging {
		log.Printf("[Telemetry] Sending status: pinned=%d, known=%d, shard=%s, peers=%d",
			status.Storage.PinnedFiles, status.Storage.KnownFiles, status.CurrentShard, status.PeersInShard)
	}
}
