package shard

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ipfs/go-cid"
	pubsub "github.com/libp2p/go-libp2p-pubsub"

	"dlockss/internal/config"
	"dlockss/pkg/schema"
)

// replicationOps is the narrow interface the replicationManager uses to
// interact with the rest of the shard package.
type replicationOps interface {
	replicationContext() context.Context
	replicationConfig() *config.Config
	getCurrentShard() string
	getPinnedManifests() []string
	isPinned(key string) bool
	isLegacyManifest(cidStr string) bool
	publishCBOR(data []byte, shardID string)
	ensureCluster(ctx context.Context, shardID string) error
	clusterPinIfAbsent(ctx context.Context, shardID string, c cid.Cid) error
	clusterTriggerSync(shardID string)
	ipfsPinRecursive(ctx context.Context, c cid.Cid) error
	replicationSigner() MessageAuthenticator
}

type replicationManager struct {
	ops replicationOps

	mu       sync.Mutex
	cooldown map[string]time.Time
	sem      chan struct{}
}

func newReplicationManager(ops replicationOps, maxConcurrent int) *replicationManager {
	if maxConcurrent < 1 {
		maxConcurrent = 1
	}
	return &replicationManager{
		ops:      ops,
		cooldown: make(map[string]time.Time),
		sem:      make(chan struct{}, maxConcurrent),
	}
}

func (rm *replicationManager) pruneCooldown() {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	cutoff := time.Now().Add(-2 * replicationRequestCooldownDuration)
	for cidStr, lastSent := range rm.cooldown {
		if lastSent.Before(cutoff) {
			delete(rm.cooldown, cidStr)
		}
	}
}

func (rm *replicationManager) runChecker() {
	cfg := rm.ops.replicationConfig()
	if cfg.Replication.CheckInterval <= 0 {
		return
	}
	ctx := rm.ops.replicationContext()
	ticker := time.NewTicker(rootReplicationCheckInterval)
	defer ticker.Stop()

	var lastCheck time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			currentShard := rm.ops.getCurrentShard()

			interval := cfg.Replication.CheckInterval
			if currentShard == "" {
				interval = rootReplicationCheckInterval
			}
			if time.Since(lastCheck) < interval {
				continue
			}
			lastCheck = time.Now()

			manifests := rm.ops.getPinnedManifests()
			if len(manifests) == 0 {
				continue
			}

			rm.pruneCooldown()
			rm.sendReplicationRequests(ctx, cfg, currentShard, manifests)
		}
	}
}

func (rm *replicationManager) sendReplicationRequests(ctx context.Context, cfg *config.Config, currentShard string, manifests []string) {
	maxConc := cfg.Replication.MaxConcurrentReplicationChecks
	if maxConc < 1 {
		maxConc = 1
	}
	sem := make(chan struct{}, maxConc)
	var wg sync.WaitGroup
	var sentThisCycle int32

	for _, manifestCIDStr := range manifests {
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case sem <- struct{}{}:
		}
		if atomic.LoadInt32(&sentThisCycle) >= maxReplicationRequestsPerCycle {
			<-sem
			continue
		}
		wg.Add(1)
		go func(manifestCIDStr string) {
			defer wg.Done()
			defer func() { <-sem }()
			c, err := cid.Decode(manifestCIDStr)
			if err != nil {
				return
			}
			if atomic.LoadInt32(&sentThisCycle) >= maxReplicationRequestsPerCycle {
				return
			}
			rm.mu.Lock()
			lastSent := rm.cooldown[manifestCIDStr]
			if time.Since(lastSent) < replicationRequestCooldownDuration {
				rm.mu.Unlock()
				return
			}
			rm.cooldown[manifestCIDStr] = time.Now()
			rm.mu.Unlock()

			signer := rm.ops.replicationSigner()
			if signer == nil {
				return
			}
			rr := &schema.ReplicationRequest{
				SignedEnvelope: schema.SignedEnvelope{Type: schema.MessageTypeReplicationRequest, ManifestCID: c},
			}
			if err := signer.SignProtocolMessage(rr); err != nil {
				slog.Error("failed to sign ReplicationRequest", "manifest", manifestCIDStr, "error", err)
				return
			}
			b, err := rr.MarshalCBOR()
			if err != nil {
				return
			}
			rm.ops.publishCBOR(b, currentShard)
			atomic.AddInt32(&sentThisCycle, 1)
			slog.Debug("ReplicationRequest sent", "manifest", manifestCIDStr, "shard", currentShard)
		}(manifestCIDStr)
	}
	wg.Wait()
}

func (rm *replicationManager) handleRequest(msg *pubsub.Message, rr *schema.ReplicationRequest, shardID string) {
	signer := rm.ops.replicationSigner()
	if signer == nil {
		return
	}
	ctx := rm.ops.replicationContext()
	cfg := rm.ops.replicationConfig()

	logPrefix := fmt.Sprintf("ReplicationRequest (Shard %s)", shardID)
	if signer.ShouldDropMessage(msg.GetFrom(), rr.SenderID, rr.Timestamp, rr.Nonce, rr.Sig, rr.MarshalCBORForSigning, logPrefix) {
		slog.Warn("ReplicationRequest rejected", "manifest", rr.ManifestCID.String(), "from", msg.GetFrom().String(), "shard", shardID)
		return
	}
	manifestCIDStr := rr.ManifestCID.String()
	c := rr.ManifestCID

	if rm.ops.isLegacyManifest(manifestCIDStr) {
		slog.Info("ignoring legacy manifest in ReplicationRequest", "manifest", manifestCIDStr)
		return
	}

	if rm.ops.isPinned(manifestCIDStr) {
		if err := rm.ops.ensureCluster(ctx, shardID); err != nil {
			slog.Error("ReplicationRequest: failed to ensure cluster for shard", "shard", shardID, "error", err)
			return
		}
		rm.ops.clusterTriggerSync(shardID)
		return
	}
	if !cfg.Replication.AutoReplicationEnabled {
		return
	}
	select {
	case rm.sem <- struct{}{}:
	default:
		slog.Debug("auto-replication skipped, concurrency limit reached", "manifest", manifestCIDStr)
		return
	}
	go func() {
		defer func() { <-rm.sem }()
		fetchCtx, cancelFetch := context.WithTimeout(ctx, cfg.Replication.AutoReplicationTimeout)
		if err := rm.ops.ipfsPinRecursive(fetchCtx, c); err != nil {
			cancelFetch()
			slog.Error("auto-replication: failed to fetch/pin", "manifest", manifestCIDStr, "error", err)
			return
		}
		cancelFetch()
		if err := rm.ops.ensureCluster(ctx, shardID); err != nil {
			slog.Error("auto-replication: failed to ensure cluster for shard", "shard", shardID, "error", err)
			return
		}
		if err := rm.ops.clusterPinIfAbsent(ctx, shardID, c); err != nil {
			slog.Error("auto-replication: failed to write CRDT pin", "manifest", manifestCIDStr, "error", err)
		}
		rm.ops.clusterTriggerSync(shardID)
		slog.Info("auto-replication: fetched and pinned", "manifest", manifestCIDStr, "shard", shardID)
	}()
}
