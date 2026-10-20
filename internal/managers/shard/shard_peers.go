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

// activePeersInCurrentShard returns the ACTIVE peers (excluding self) seen in the current shard.
func (sm *ShardManager) activePeersInCurrentShard() []peer.ID {
	return sm.activePeersInShard(sm.getCurrentShard())
}

// activePeersInShard returns the ACTIVE peers (excluding self) recorded for a
// shard, from membership or from probing it.
func (sm *ShardManager) activePeersInShard(shardID string) []peer.ID {
	return sm.peers.GetActiveForShard(shardID, sm.cfg.Sharding.SeenPeersWindow)
}

func (sm *ShardManager) GetShardInfo() string {
	return sm.getCurrentShard()
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

	// PROBE tells members this node is only probing, so they do not count it.
	_ = t.Publish(sm.ctx, []byte(msgPrefixProbe+sm.h.ID().String()))

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

	// Only ACTIVE peers count, never the mesh size. The mesh can include the
	// monitor or other non-ACTIVE subscribers; trusting it would allow a
	// phantom "join existing" for an empty child, bypassing the create threshold.
	activeCount := sm.peers.CountActive(shardID, false, "", sm.cfg.Sharding.SeenPeersWindow)

	sm.mu.Lock()
	sm.cacheProbeTopicLocked(shardID, t)
	sm.mu.Unlock()
	return activeCount
}

func (sm *ShardManager) getProbePeerCount(shardID string, activeWindow time.Duration) int {
	sm.mu.RLock()
	currentShard := sm.currentShard
	_, observerOnly := sm.observerOnlyShards[shardID]
	sm.mu.RUnlock()
	return sm.peers.CountActive(shardID, !observerOnly, currentShard, activeWindow)
}
