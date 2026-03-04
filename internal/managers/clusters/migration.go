package clusters

import (
	"context"
	"fmt"
	"log/slog"

	"dlockss/internal/common"

	"github.com/ipfs/go-cid"
)

// MigratePins moves all pins that are now responsible in the destination shard
// from the source shard cluster to the destination shard cluster.
func (cm *ClusterManager) MigratePins(ctx context.Context, sourceShardID, destShardID string) error {
	slog.Info("starting pin migration", "source", sourceShardID, "dest", destShardID)

	pins, err := cm.ListPins(ctx, sourceShardID)
	if err != nil {
		return fmt.Errorf("source shard %s not found or error: %w", sourceShardID, err)
	}

	allocations := make([]cid.Cid, 0, len(pins))
	for _, pin := range pins {
		allocations = append(allocations, pin.Cid.Cid)
	}

	slog.Info("found pins in source shard", "count", len(allocations), "source", sourceShardID)

	destDepth := len(destShardID)

	migrated := 0
	var failures int
	for _, c := range allocations {
		key := c.String()
		payloadCIDStr, _ := common.GetPayloadCIDForShardAssignment(ctx, cm.ipfsClient, key)
		stableHex := common.KeyToStableHex(payloadCIDStr)
		targetPrefix, err := common.GetHexBinaryPrefix(stableHex, destDepth)
		if err != nil {
			slog.Error("failed to compute target prefix", "cid", key, "error", err)
			failures++
			continue
		}

		if targetPrefix != destShardID {
			continue
		}
		if err := cm.Pin(ctx, destShardID, c, -1, -1); err != nil {
			slog.Error("failed to migrate pin to dest", "cid", c, "error", err)
			failures++
			continue
		}
		if err := cm.Unpin(ctx, sourceShardID, c); err != nil {
			slog.Error("failed to unpin from source", "cid", c, "error", err)
			failures++
		}
		migrated++
	}

	slog.Info("pin migration finished", "source", sourceShardID, "dest", destShardID, "migrated", migrated, "failures", failures)
	if failures > 0 {
		return fmt.Errorf("pin migration had %d failures", failures)
	}
	return nil
}
