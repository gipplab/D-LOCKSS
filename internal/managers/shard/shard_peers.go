package shard

import (
	"context"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/internal/config"
)

func (sm *ShardManager) getShardPeerCount() int {
	sm.mu.RLock()
	currentShard := sm.currentShard
	sub, exists := sm.shardSubs[currentShard]
	sm.mu.RUnlock()

	if exists && sub.topic != nil {
		activeCount := sm.countActivePeers(currentShard, true, config.SeenPeersWindow)
		if activeCount > 0 {
			return activeCount
		}
		// Fallback: no role data yet, use mesh as lower bound (may overcount PASSIVE/PROBE)
		meshPeers := sub.topic.ListPeers()
		return len(meshPeers) + 1
	}

	if sm.clusterMgr != nil {
		count, err := sm.clusterMgr.GetPeerCount(sm.ctx, currentShard)
		if err == nil {
			return count
		}
	}
	return 0
}

// getShardPeerCountForSplit returns ACTIVE peer count for split decisions.
// Uses only role-based counts (HEARTBEAT/JOIN); avoids mesh fallback because the mesh
// can include the monitor and other non-ACTIVE subscribers, which would overcount
// and trigger premature splits (e.g. 9 real nodes + monitor = 10, split when we shouldn't).
func (sm *ShardManager) getShardPeerCountForSplit() int {
	sm.mu.RLock()
	currentShard := sm.currentShard
	sub, exists := sm.shardSubs[currentShard]
	sm.mu.RUnlock()

	if exists && sub.topic != nil {
		activeCount := sm.countActivePeers(currentShard, true, config.SeenPeersWindow)
		if activeCount > 0 {
			return activeCount
		}
		// No role data yet: return 0 to avoid splitting on phantom peers (monitor in mesh).
		// Split will proceed once HEARTBEATs establish accurate ACTIVE counts.
		return 0
	}
	if sm.clusterMgr != nil {
		count, err := sm.clusterMgr.GetPeerCount(sm.ctx, currentShard)
		if err == nil {
			return count
		}
	}
	return 0
}

func (sm *ShardManager) GetShardInfo() (string, int) {
	sm.mu.RLock()
	currentShard := sm.currentShard
	sm.mu.RUnlock()
	return currentShard, sm.getShardPeerCount()
}

func (sm *ShardManager) GetHost() host.Host {
	return sm.h
}

func (sm *ShardManager) GetShardPeers() []peer.ID {
	sm.mu.RLock()
	currentShard := sm.currentShard
	sub, exists := sm.shardSubs[currentShard]
	sm.mu.RUnlock()

	if !exists || sub.topic == nil {
		return nil
	}
	return sub.topic.ListPeers()
}

func (sm *ShardManager) GetPeersForShard(shardID string) []peer.ID {
	sm.mu.RLock()
	sub, exists := sm.shardSubs[shardID]
	sm.mu.RUnlock()

	if !exists || sub.topic == nil {
		return nil
	}

	// Return only ACTIVE peers (exclude PASSIVE and PROBE for replication)
	sm.mu.RLock()
	roles, ok := sm.seenPeerRoles[shardID]
	cutoff := time.Now().Add(-350 * time.Second)
	sm.mu.RUnlock()

	if ok {
		var active []peer.ID
		sm.mu.RLock()
		for p, info := range roles {
			if info.Role == RoleActive && info.LastSeen.After(cutoff) && p != sm.h.ID() {
				active = append(active, p)
			}
		}
		sm.mu.RUnlock()
		return active
	}

	// Fallback: no role data, use mesh+seen (may include PASSIVE/PROBE)
	meshPeers := sub.topic.ListPeers()
	seen := make(map[peer.ID]struct{}, len(meshPeers))
	for _, p := range meshPeers {
		seen[p] = struct{}{}
	}
	sm.mu.RLock()
	if seenMap, ok := sm.seenPeers[shardID]; ok {
		for p, lastSeen := range seenMap {
			if lastSeen.After(cutoff) {
				seen[p] = struct{}{}
			}
		}
	}
	sm.mu.RUnlock()
	all := make([]peer.ID, 0, len(seen))
	for p := range seen {
		if p != sm.h.ID() {
			all = append(all, p)
		}
	}
	return all
}

func (sm *ShardManager) GetShardPeerCount(shardID string) int {
	sm.mu.RLock()
	currentShard := sm.currentShard
	sub, exists := sm.shardSubs[shardID]
	sm.mu.RUnlock()

	if !exists || sub.topic == nil {
		return 0
	}
	includeSelf := (shardID == currentShard)
	activeCount := sm.countActivePeers(shardID, includeSelf, 350*time.Second)
	if activeCount > 0 {
		return activeCount
	}
	meshPeers := sub.topic.ListPeers()
	n := len(meshPeers)
	if includeSelf {
		n++
	}
	return n
}

func getSiblingShard(shardID string) string {
	if shardID == "" {
		return ""
	}
	parent := shardID[:len(shardID)-1]
	lastBit := shardID[len(shardID)-1]
	otherBit := '0' + (1 - (lastBit - '0'))
	return parent + string(byte(otherBit))
}

func (sm *ShardManager) generateDeeperShards(currentShard string, maxDepth int) []string {
	if maxDepth <= 0 {
		return nil
	}

	var shards []string
	queue := []string{currentShard}
	maxShardLength := len(currentShard) + maxDepth

	for len(queue) > 0 {
		shard := queue[0]
		queue = queue[1:]

		child0 := shard + "0"
		child1 := shard + "1"

		if len(child0) <= maxShardLength {
			shards = append(shards, child0, child1)
			if len(child0) < maxShardLength {
				queue = append(queue, child0, child1)
			}
		}
	}

	return shards
}

func (sm *ShardManager) probeShard(shardID string, probeTimeout time.Duration) int {
	sm.mu.RLock()
	sub, alreadyJoined := sm.shardSubs[shardID]
	sm.mu.RUnlock()

	if alreadyJoined && sub.topic != nil {
		return sm.getProbePeerCount(shardID, 350*time.Second)
	}
	return sm.probeShardSilently(shardID, probeTimeout)
}

func (sm *ShardManager) probeShardSilently(shardID string, probeTimeout time.Duration) int {
	sm.mu.Lock()
	t, fromCache := sm.probeTopicCache[shardID]
	if fromCache {
		delete(sm.probeTopicCache, shardID)
	}
	sm.mu.Unlock()

	if !fromCache {
		topicName := fmt.Sprintf("%s-creative-commons-shard-%s", config.PubsubTopicPrefix, shardID)
		var err error
		t, err = sm.ps.Join(topicName)
		if err != nil {
			return 0
		}
	}

	psSub, err := t.Subscribe()
	if err != nil {
		if !fromCache {
			_ = t.Close()
		}
		return 0
	}
	defer psSub.Cancel()

	// Publish PROBE so others know we're a prober (not counted)
	probeMsg := []byte("PROBE:" + sm.h.ID().String())
	_ = t.Publish(sm.ctx, probeMsg)

	// Process incoming messages to collect HEARTBEAT/JOIN/PROBE role info
	deadline := time.Now().Add(probeTimeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(sm.ctx, 500*time.Millisecond)
		msg, err := psSub.Next(ctx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			continue
		}
		if msg.GetFrom() == sm.h.ID() {
			continue
		}
		sm.processTextProtocolForProbe(msg, shardID)
	}

	activeCount := sm.countActivePeers(shardID, false, 350*time.Second)
	if activeCount > 0 {
		sm.mu.Lock()
		if old := sm.probeTopicCache[shardID]; old != nil && old != t {
			_ = old.Close()
		}
		const maxProbeCache = 4
		if len(sm.probeTopicCache) >= maxProbeCache && sm.probeTopicCache[shardID] == nil {
			for k, v := range sm.probeTopicCache {
				_ = v.Close()
				delete(sm.probeTopicCache, k)
				break
			}
		}
		sm.probeTopicCache[shardID] = t
		sm.mu.Unlock()
		return activeCount
	}

	// No ACTIVE peers: return 0 instead of mesh count. The mesh can include the monitor
	// or other non-ACTIVE subscribers; trusting it would allow phantom "join existing"
	// when the child is empty, bypassing the create threshold (14).
	sm.mu.Lock()
	if old := sm.probeTopicCache[shardID]; old != nil && old != t {
		_ = old.Close()
	}
	const maxProbeCache = 4
	if len(sm.probeTopicCache) >= maxProbeCache && sm.probeTopicCache[shardID] == nil {
		for k, v := range sm.probeTopicCache {
			_ = v.Close()
			delete(sm.probeTopicCache, k)
			break
		}
	}
	sm.probeTopicCache[shardID] = t
	sm.mu.Unlock()
	return 0
}

func (sm *ShardManager) getProbePeerCount(shardID string, activeWindow time.Duration) int {
	sm.mu.RLock()
	_, observerOnly := sm.observerOnlyShards[shardID]
	sm.mu.RUnlock()
	activeCount := sm.countActivePeers(shardID, !observerOnly, activeWindow)
	if activeCount > 0 {
		return activeCount
	}
	// No ACTIVE peers: return 0 instead of mesh count. The mesh can include the monitor
	// or other non-ACTIVE subscribers; trusting it would allow phantom "join existing".
	return 0
}
