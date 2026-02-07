package shard

import (
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// getShardPeerCount returns peer count for the CURRENT shard.
// It may use seenPeers (recent heartbeats) to avoid undercounting when mesh is slow to update.
func (sm *ShardManager) getShardPeerCount() int {
	sm.mu.RLock()
	currentShard := sm.currentShard
	sub, exists := sm.shardSubs[currentShard]
	sm.mu.RUnlock()

	if exists && sub.topic != nil {
		meshPeers := sub.topic.ListPeers()
		meshCount := len(meshPeers) + 1

		sm.mu.RLock()
		seenCount := 0
		if seenMap, ok := sm.seenPeers[currentShard]; ok {
			cutoff := time.Now().Add(-350 * time.Second)
			for _, lastSeen := range seenMap {
				if lastSeen.After(cutoff) {
					seenCount++
				}
			}
			seenCount++
		}
		sm.mu.RUnlock()

		if seenCount > meshCount {
			return seenCount
		}
		return meshCount
	}

	if sm.clusterMgr != nil {
		count, err := sm.clusterMgr.GetPeerCount(sm.ctx, currentShard)
		if err == nil {
			return count
		}
	}
	return 0
}

// getShardPeerCountForSplit returns the current mesh size only (peers actually in the topic now).
// Used for split decisions so we never split on stale counts: seenPeers can include nodes that
// have already left the shard (up to 350s), which would otherwise make e.g. 11 report 12 and
// split when only 2–3 nodes remain.
func (sm *ShardManager) getShardPeerCountForSplit() int {
	sm.mu.RLock()
	currentShard := sm.currentShard
	sub, exists := sm.shardSubs[currentShard]
	sm.mu.RUnlock()

	if exists && sub.topic != nil {
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

// GetShardInfo returns current shard ID and peer count.
func (sm *ShardManager) GetShardInfo() (string, int) {
	sm.mu.RLock()
	currentShard := sm.currentShard
	sm.mu.RUnlock()
	return currentShard, sm.getShardPeerCount()
}

func (sm *ShardManager) GetHost() host.Host {
	return sm.h
}

// GetShardPeers returns the list of peers in the current shard (mesh peers from pubsub).
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

// GetPeersForShard returns the list of peers in the given shard (mesh peers from pubsub).
func (sm *ShardManager) GetPeersForShard(shardID string) []peer.ID {
	sm.mu.RLock()
	sub, exists := sm.shardSubs[shardID]
	sm.mu.RUnlock()

	if !exists || sub.topic == nil {
		return nil
	}
	return sub.topic.ListPeers()
}

// GetShardPeerCount returns the peer count for a specific shard.
func (sm *ShardManager) GetShardPeerCount(shardID string) int {
	sm.mu.RLock()
	sub, exists := sm.shardSubs[shardID]
	sm.mu.RUnlock()

	if !exists || sub.topic == nil {
		return 0
	}
	return len(sub.topic.ListPeers())
}

// getSiblingShard returns the sibling shard ID (same parent, other branch). E.g. "0"→"1", "010"→"011".
// Empty shard has no sibling; returns "".
func getSiblingShard(shardID string) string {
	if shardID == "" {
		return ""
	}
	parent := shardID[:len(shardID)-1]
	lastBit := shardID[len(shardID)-1]
	otherBit := '0' + (1 - (lastBit - '0'))
	return parent + string(byte(otherBit))
}

// generateDeeperShards generates all possible deeper shards in the branch starting from the current shard.
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

// probeShard checks how many peers are in a shard without becoming a member.
func (sm *ShardManager) probeShard(shardID string, probeTimeout time.Duration) int {
	sm.mu.RLock()
	sub, alreadyJoined := sm.shardSubs[shardID]
	sm.mu.RUnlock()

	if alreadyJoined && sub.topic != nil {
		return sm.getProbePeerCount(shardID, sub.topic, 350*time.Second)
	}
	return sm.probeShardSilently(shardID, probeTimeout)
}

// probeShardSilently subscribes to a shard topic to observe the mesh, then unsubscribes.
func (sm *ShardManager) probeShardSilently(shardID string, probeTimeout time.Duration) int {
	topicName := fmt.Sprintf("dlockss-creative-commons-shard-%s", shardID)
	t, err := sm.ps.Join(topicName)
	if err != nil {
		return 0
	}
	psSub, err := t.Subscribe()
	if err != nil {
		_ = t.Close()
		return 0
	}
	defer psSub.Cancel()

	time.Sleep(probeTimeout)

	meshPeers := t.ListPeers()
	n := len(meshPeers)
	if n == 0 {
		time.Sleep(2 * time.Second)
		meshPeers = t.ListPeers()
		if len(meshPeers) > n {
			n = len(meshPeers)
		}
	}

	sm.mu.Lock()
	if old := sm.probeTopicCache[shardID]; old != nil {
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
	return n
}

// getProbePeerCount returns peer count for a shard when probing (observer or mesh).
func (sm *ShardManager) getProbePeerCount(shardID string, topic interface{ ListPeers() []peer.ID }, activeWindow time.Duration) int {
	meshPeers := topic.ListPeers()
	sm.mu.RLock()
	_, observerOnly := sm.observerOnlyShards[shardID]
	sm.mu.RUnlock()
	meshCount := len(meshPeers)
	if !observerOnly {
		meshCount++
	}

	sm.mu.RLock()
	seenCount := 0
	if seenMap, exists := sm.seenPeers[shardID]; exists {
		cutoff := time.Now().Add(-activeWindow)
		for _, lastSeen := range seenMap {
			if lastSeen.After(cutoff) {
				seenCount++
			}
		}
		if !observerOnly {
			seenCount++
		}
	}
	sm.mu.RUnlock()

	if seenCount > meshCount {
		return seenCount
	}
	return meshCount
}
