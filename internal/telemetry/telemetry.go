package telemetry

import (
	"context"
	"log/slog"
	"time"

	"dlockss/internal/config"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
)

// No dedicated telemetry topic - nodes broadcast on their shard topics

type TelemetryClient struct {
	cfg       *config.Config
	host      host.Host
	ps        *pubsub.PubSub
	metrics   *MetricsManager
	shardInfo ShardInfoProvider
	publisher ShardPublisher // Interface to publish to shard
}

type ShardPublisher interface {
	PublishToShardCBOR(data []byte, shardID string)
}

func NewTelemetryClient(cfg *config.Config, h host.Host, ps *pubsub.PubSub, metrics *MetricsManager) *TelemetryClient {
	if ps == nil {
		slog.Warn("pubsub not available, telemetry disabled")
		return nil
	}

	tc := &TelemetryClient{
		cfg:     cfg,
		host:    h,
		ps:      ps,
		metrics: metrics,
	}

	slog.Info("telemetry client initialized")

	return tc
}

// SetShardPublisher sets the shard publisher (called after initialization to break cycle)
func (tc *TelemetryClient) SetShardPublisher(sp ShardPublisher, sip ShardInfoProvider) {
	tc.publisher = sp
	tc.shardInfo = sip
}

func (tc *TelemetryClient) Start(ctx context.Context) {
	slog.Info("starting telemetry client")
	go tc.runLoop(ctx)
}

func (tc *TelemetryClient) runLoop(ctx context.Context) {
	ticker := time.NewTicker(tc.cfg.TelemetryInterval)
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

	slog.Debug("sending telemetry status",
		"pinned", status.Storage.PinnedFiles, "known", status.Storage.KnownFiles,
		"shard", status.CurrentShard, "peers", status.PeersInShard)
}
