package clusters

import (
	"context"
	"fmt"
	"log/slog"

	"dlockss/internal/common"
)

// MigratePins moves every pin of the source shard whose payload now maps to
// the destination shard: it is pinned in the destination cluster and then
// unpinned from the source cluster.
func (cm *ClusterManager) MigratePins(ctx context.Context, sourceShardID, destShardID string) error {
	slog.Info("starting pin migration", "source", sourceShardID, "dest", destShardID)

	pins, err := cm.ListPins(ctx, sourceShardID)
	if err != nil {
		return fmt.Errorf("list pins of source shard %s: %w", sourceShardID, err)
	}
	slog.Info("found pins in source shard", "count", len(pins), "source", sourceShardID)

	destDepth := len(destShardID)
	var migrated, failures int
	for _, pin := range pins {
		c := pin.Cid.Cid
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
