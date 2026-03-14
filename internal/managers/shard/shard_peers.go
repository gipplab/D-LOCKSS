package shard

import (
	"context"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// getShardPeerCount returns the number of active peers in the current shard.
// When useMeshFallback is true, falls back to the mesh peer list if no
// role-based counts are available. Split decisions should pass false to avoid
// counting non-ACTIVE subscribers (e.g. the monitor).
func (sm *ShardManager) getShardPeerCount(useMeshFallback bool) int {
	sm.mu.RLock()
	currentShard := sm.currentShard
	sub, exists := sm.shardSubs[currentShard]
	sm.mu.RUnlock()

	if exists && sub.topic != nil {
		activeCount := sm.peers.CountActive(currentShard, true, currentShard, sm.cfg.Sharding.SeenPeersWindow)
		if activeCount > 0 {
			return activeCount
		}
		if useMeshFallback {
			return len(sub.topic.ListPeers()) + 1
		}
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

func (sm *ShardManager) GetShardInfo() string {
	sm.mu.RLock()
	currentShard := sm.currentShard
	sm.mu.RUnlock()
	return currentShard
}

// PeerID returns the local peer's ID.
func (sm *ShardManager) PeerID() peer.ID {
	return sm.h.ID()
}

func (sm *ShardManager) GetPeersForShard(shardID string) []peer.ID {
	sm.mu.RLock()
	sub, exists := sm.shardSubs[shardID]
	sm.mu.RUnlock()

	if !exists || sub.topic == nil {
		return nil
	}

	if sm.peers.HasRoles(shardID) {
		return sm.peers.GetActiveForShard(shardID, sm.cfg.Sharding.SeenPeersWindow)
	}

	meshPeers := sub.topic.ListPeers()
	seen := make(map[peer.ID]struct{}, len(meshPeers))
	for _, p := range meshPeers {
		if p != sm.h.ID() {
			seen[p] = struct{}{}
		}
	}
	for p := range sm.peers.GetSeenPeers(shardID, sm.cfg.Sharding.SeenPeersWindow) {
		seen[p] = struct{}{}
	}
	all := make([]peer.ID, 0, len(seen))
	for p := range seen {
		all = append(all, p)
	}
	return all
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

func (sm *ShardManager) probeShard(shardID string, probeTimeout time.Duration) int {
	sm.mu.RLock()
	sub, alreadyJoined := sm.shardSubs[shardID]
	sm.mu.RUnlock()

	if alreadyJoined && sub.topic != nil {
		return sm.getProbePeerCount(shardID, sm.cfg.Sharding.SeenPeersWindow)
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
		topicName := sm.shardTopicName(shardID)
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
	probeMsg := []byte(msgPrefixProbe + sm.h.ID().String())
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

	activeCount := sm.peers.CountActive(shardID, false, "", sm.cfg.Sharding.SeenPeersWindow)
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
	currentShard := sm.currentShard
	_, observerOnly := sm.observerOnlyShards[shardID]
	sm.mu.RUnlock()
	activeCount := sm.peers.CountActive(shardID, !observerOnly, currentShard, activeWindow)
	if activeCount > 0 {
		return activeCount
	}
	return 0
}
