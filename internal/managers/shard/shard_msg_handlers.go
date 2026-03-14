package shard

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/internal/common"
	"dlockss/pkg/schema"
)

// isAuthorizedIngestor returns true if the peer is allowed to publish ingest
// messages. When the allowlist is empty the topic is open to all.
func (sm *ShardManager) isAuthorizedIngestor(senderID peer.ID) bool {
	if len(sm.ingestAllowlist) == 0 {
		return true
	}
	_, ok := sm.ingestAllowlist[senderID]
	return ok
}

// IsLocalNodeIngestor returns true if the local node is authorized to ingest
// files into this topic (i.e. it is on the allowlist, or no allowlist is set).
func (sm *ShardManager) IsLocalNodeIngestor() bool {
	return sm.isAuthorizedIngestor(sm.h.ID())
}

// handleIngestMessage verifies IngestMessage and pins if on current shard and responsible.
func (sm *ShardManager) handleIngestMessage(msg *pubsub.Message, im *schema.IngestMessage, shardID string) {
	if sm.signer == nil {
		return
	}
	logPrefix := fmt.Sprintf("IngestMessage (Shard %s)", shardID)
	if sm.signer.ShouldDropMessage(msg.GetFrom(), im.SenderID, im.Timestamp, im.Nonce, im.Sig, im.MarshalCBORForSigning, logPrefix) {
		slog.Warn("IngestMessage rejected", "from", msg.GetFrom().String(), "shard", shardID)
		return
	}
	if !sm.isAuthorizedIngestor(im.SenderID) {
		slog.Warn("IngestMessage from unauthorized peer", "sender", im.SenderID, "shard", shardID)
		return
	}
	key := im.ManifestCID.String()

	checkCtx, checkCancel := context.WithTimeout(sm.ctx, 5*time.Second)
	legacy := common.IsLegacyManifest(checkCtx, sm.ipfsClient, key)
	checkCancel()
	if legacy {
		slog.Info("ignoring legacy manifest", "manifest", key)
		return
	}

	sm.storageMgr.AddKnownFile(key)

	sm.mu.RLock()
	currentShard := sm.currentShard
	sm.mu.RUnlock()
	if shardID != currentShard {
		return
	}

	resolveCtx, cancel := context.WithTimeout(sm.ctx, 5*time.Second)
	payloadCIDStr, _ := common.GetPayloadCIDForShardAssignment(resolveCtx, sm.ipfsClient, key)
	cancel()
	if payloadCIDStr != key && !sm.AmIResponsibleFor(payloadCIDStr) {
		return
	}

	if err := sm.clusterMgr.PinIfAbsent(sm.ctx, shardID, im.ManifestCID, -1, -1); err != nil {
		slog.Error("failed to pin ingested file to cluster", "manifest", key, "error", err)
	}
	sm.clusterMgr.TriggerSync(shardID)
}
