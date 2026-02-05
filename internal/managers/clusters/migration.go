package clusters

import (
	"context"
	"log"

	"dlockss/internal/common"

	"github.com/ipfs/go-cid"
)

// MigratePins moves all pins that are now responsible in the destination shard
// from the source shard cluster to the destination shard cluster.
func (cm *ClusterManager) MigratePins(ctx context.Context, sourceShardID, destShardID string) error {
	log.Printf("[ClusterMigration] Starting migration from %s -> %s", sourceShardID, destShardID)

	// 1. Get all pins from the source cluster (CRDT state)
	pins, err := cm.ListPins(ctx, sourceShardID)
	if err != nil {
		log.Printf("[ClusterMigration] Source shard %s not found or error: %v", sourceShardID, err)
		return nil
	}

	allocations := make([]cid.Cid, 0, len(pins))
	for _, pin := range pins {
		allocations = append(allocations, pin.Cid.Cid)
	}

	log.Printf("[ClusterMigration] Found %d pins in source shard %s", len(allocations), sourceShardID)

	destDepth := len(destShardID)

	for _, c := range allocations {
		// 2. Check responsibility: Does this CID belong to the NEW shard?
		// We need to check if the CID's hash prefix matches the destShardID.
		// Note: This checks the ManifestCID itself. Ideally we check PayloadCID but
		// we might not have it handy without fetching the block.
		// For migration, we assume the key used for sharding is the one we have.
		// If D-LOCKSS uses PayloadCID for sharding, we need to resolve it.
		// However, `common.GetPayloadCIDForShardAssignment` requires IPFS access.
		// We have `cm.ipfsClient`.

		key := c.String()
		payloadCIDStr := common.GetPayloadCIDForShardAssignment(ctx, cm.ipfsClient, key)
		stableHex := common.KeyToStableHex(payloadCIDStr)
		targetPrefix := common.GetHexBinaryPrefix(stableHex, destDepth)

		if targetPrefix != destShardID {
			// This pin belongs to the OTHER sibling (e.g. we moved to "00", this belongs to "01")
			// We do NOT migrate it. It will be dropped when we leave the old shard.
			continue
		}

		// 3. Pin to the new cluster
		// We use default replication factors for now (-1).
		if err := cm.Pin(ctx, destShardID, c, -1, -1); err != nil {
			log.Printf("[ClusterMigration] Failed to migrate pin %s: %v", c, err)
			continue
		}

		// 4. Unpin from old?
		// We leave it for now; it will disappear when we close the old cluster.
		// Explicit unpinning might cause churn if we are still syncing with peers who haven't split yet.
	}

	log.Printf("[ClusterMigration] Migration finished from %s -> %s", sourceShardID, destShardID)
	return nil
}
