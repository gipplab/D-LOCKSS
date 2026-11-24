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

// isAuthorizedIngestor reports whether the peer is allowed to introduce files.
// When no origin gate is configured the topic is open to all.
func (sm *ShardManager) isAuthorizedIngestor(senderID peer.ID) bool {
	if sm.origin == nil {
		return true
	}
	return sm.origin.AllowsOrigin(senderID)
}

// IsLocalNodeIngestor reports whether the local node is authorized to ingest
// files into this topic: it is on the allowlist, or no allowlist is set.
func (sm *ShardManager) IsLocalNodeIngestor() bool {
	return sm.isAuthorizedIngestor(sm.nodeID)
}

// handleIngestMessage verifies an IngestMessage and pins the file if it was
// published on the current shard and the shard is responsible for it.
func (sm *ShardManager) handleIngestMessage(msg *pubsub.Message, im *schema.IngestMessage, shardID string) {
	if sm.signer == nil {
		return
	}
	logPrefix := fmt.Sprintf("IngestMessage (Shard %s)", shardID)
	node, ok := sm.signer.Authenticate(msg.GetFrom(), im, logPrefix)
	if !ok {
		slog.Warn("IngestMessage rejected", "from", msg.GetFrom().String(), "shard", shardID)
		return
	}
	sm.peers.RecordSeen(shardID, node)
	if !sm.isAuthorizedIngestor(node) {
		slog.Warn("IngestMessage from unauthorized node", "node", node, "shard", shardID)
		return
	}
	if !sm.allowsManifestCID(sm.ctx, im.ManifestCID) {
		slog.Warn("IngestMessage refused: untrusted data origin", "node", node, "manifest", im.ManifestCID.String(), "shard", shardID)
		return
	}
	key := im.ManifestCID.String()
	if sm.isLegacyManifest(key) {
		slog.Info("ignoring legacy manifest", "manifest", key)
		return
	}

	sm.storageMgr.AddKnownFile(key)
	if shardID != sm.getCurrentShard() {
		return
	}

	resolveCtx, cancel := context.WithTimeout(sm.ctx, 5*time.Second)
	payloadCIDStr, _ := common.GetPayloadCIDForShardAssignment(resolveCtx, sm.ipfsClient, key)
	cancel()
	if payloadCIDStr != key && !sm.AmIResponsibleFor(payloadCIDStr) {
		return
	}

	if err := sm.clusterMgr.PinIfAbsent(sm.ctx, shardID, im.ManifestCID); err != nil {
		slog.Error("failed to pin ingested file to cluster", "manifest", key, "error", err)
	}
	sm.clusterMgr.TriggerSync(shardID)
}
