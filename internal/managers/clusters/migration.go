package clusters

import (
	"context"
	"fmt"
	"log"

	"dlockss/internal/common"

	"github.com/ipfs/go-cid"
)

// MigratePins moves all pins that are now responsible in the destination shard
// from the source shard cluster to the destination shard cluster.
func (cm *ClusterManager) MigratePins(ctx context.Context, sourceShardID, destShardID string) error {
	log.Printf("[ClusterMigration] Starting migration from %s -> %s", sourceShardID, destShardID)

	pins, err := cm.ListPins(ctx, sourceShardID)
	if err != nil {
		return fmt.Errorf("source shard %s not found or error: %w", sourceShardID, err)
	}

	allocations := make([]cid.Cid, 0, len(pins))
	for _, pin := range pins {
		allocations = append(allocations, pin.Cid.Cid)
	}

	log.Printf("[ClusterMigration] Found %d pins in source shard %s", len(allocations), sourceShardID)

	destDepth := len(destShardID)

	migrated := 0
	for _, c := range allocations {
		key := c.String()
		payloadCIDStr := common.GetPayloadCIDForShardAssignment(ctx, cm.ipfsClient, key)
		stableHex := common.KeyToStableHex(payloadCIDStr)
		targetPrefix := common.GetHexBinaryPrefix(stableHex, destDepth)

		if targetPrefix != destShardID {
			continue
		}
		if err := cm.Pin(ctx, destShardID, c, -1, -1); err != nil {
			log.Printf("[ClusterMigration] Failed to migrate pin %s to dest: %v", c, err)
			continue
		}
		if err := cm.Unpin(ctx, sourceShardID, c); err != nil {
			log.Printf("[ClusterMigration] Failed to unpin %s from source: %v", c, err)
		}
		migrated++
	}

	log.Printf("[ClusterMigration] Migration finished from %s -> %s (%d pins migrated)", sourceShardID, destShardID, migrated)
	return nil
}
