package clusters

import (
	"context"
	"log"

	"github.com/ipfs/go-cid"
)

// MigratePins moves all pins that are now responsible in the destination shard
// from the source shard cluster to the destination shard cluster.
func (cm *ClusterManager) MigratePins(ctx context.Context, sourceShardID, destShardID string) error {
	log.Printf("[ClusterMigration] Starting migration from %s -> %s", sourceShardID, destShardID)

	// 1. Get all allocations (pins) from the source cluster
	// In a real implementation, we would query the local state of the source CRDT.
	// allocations, err := cm.clusters[sourceShardID].Consensus.ListPins()

	// Placeholder: Retrieve pins from source (mocked)
	allocations := []cid.Cid{}

	for _, c := range allocations {
		// 2. Check responsibility: Does this CID belong to the NEW shard?
		// We need access to the common.GetPayloadCIDForShardAssignment logic here.
		// For now, we assume ALL pins from the old shard that match the new prefix should be moved.

		// 3. Pin to the new cluster
		// We use default replication factors for now, or preserve from old pin.
		if err := cm.Pin(ctx, destShardID, c, 5, 10); err != nil {
			log.Printf("[ClusterMigration] Failed to migrate pin %s: %v", c, err)
			continue
		}

		// 4. Unpin from the old cluster?
		// OPTION A: Immediate unpin. Good for cleanup, bad if revert needed.
		// OPTION B: Let the old cluster "die" naturally when we LeaveShard.
		// Since CRDT state is local, we don't strictly need to unpin from the old one
		// if we are about to destroy the entire old cluster instance.
		// However, if the old cluster persists (e.g. overlap period), we should unpin
		// to stop tracking it there.

		if err := cm.Unpin(ctx, sourceShardID, c); err != nil {
			log.Printf("[ClusterMigration] Failed to cleanup old pin %s: %v", c, err)
		}
	}

	log.Printf("[ClusterMigration] Migration finished from %s -> %s", sourceShardID, destShardID)
	return nil
}
