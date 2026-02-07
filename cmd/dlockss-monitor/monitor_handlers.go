package main

import (
	"context"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/pkg/schema"
)

func (m *Monitor) handleIngestMessage(im *schema.IngestMessage, senderID peer.ID, shardID string, ip string) {
	now := time.Now()
	peerIDStr := senderID.String()

	m.mu.Lock()
	defer m.mu.Unlock()

	nodeState, exists := m.nodes[peerIDStr]
	if !exists {
		log.Printf("[Monitor] New node discovered via IngestMessage: %s (shard: %s)", peerIDStr, shardID)
		nodeState = &NodeState{
			PeerID:         peerIDStr,
			CurrentShard:   shardID,
			PinnedFiles:    0,
			KnownFiles:     0,
			LastSeen:       now,
			ShardHistory:   []ShardHistoryEntry{{ShardID: shardID, FirstSeen: now}},
			IPAddress:      ip,
			announcedFiles: make(map[string]time.Time),
		}
		m.nodes[peerIDStr] = nodeState
		m.nodeFiles[peerIDStr] = make(map[string]time.Time)
		m.treeDirty = true
	}

	nodeState.LastSeen = now
	manifestCIDStr := im.ManifestCID.String()

	if nodeState.announcedFiles == nil {
		nodeState.announcedFiles = make(map[string]time.Time)
	}
	nodeState.announcedFiles[manifestCIDStr] = now

	if m.nodeFiles[peerIDStr] == nil {
		m.nodeFiles[peerIDStr] = make(map[string]time.Time)
	}
	m.nodeFiles[peerIDStr][manifestCIDStr] = now

	nodeState.KnownFiles = len(nodeState.announcedFiles)
	if n := len(nodeState.announcedFiles); n > nodeState.PinnedFiles {
		nodeState.PinnedFiles = n
	}
	m.uniqueCIDs[manifestCIDStr] = now

	if m.manifestReplication[manifestCIDStr] == nil {
		m.manifestReplication[manifestCIDStr] = make(map[string]time.Time)
	}
	m.manifestReplication[manifestCIDStr][peerIDStr] = now
	m.setPeerShardLastSeenUnlocked(peerIDStr, shardID, now)

	if m.ps != nil {
		m.ensureShardSubscriptionUnlocked(context.Background(), shardID)
	}

	if ip != "" && ip != nodeState.IPAddress {
		nodeState.IPAddress = ip
		nodeState.Region = ""
		select {
		case m.geoQueue <- geoRequest{ip: ip, peerID: peerIDStr}:
		default:
		}
	}
}

func (m *Monitor) setPeerShardLastSeenUnlocked(peerIDStr, shardID string, t time.Time) {
	if m.peerShardLastSeen[peerIDStr] == nil {
		m.peerShardLastSeen[peerIDStr] = make(map[string]time.Time)
	}
	m.peerShardLastSeen[peerIDStr][shardID] = t
}

func (m *Monitor) handleHeartbeat(senderID peer.ID, shardID string, ip string, pinnedCount int) {
	now := time.Now()
	peerIDStr := senderID.String()

	m.mu.Lock()
	defer m.mu.Unlock()

	m.setPeerShardLastSeenUnlocked(peerIDStr, shardID, now)

	nodeState, exists := m.nodes[peerIDStr]
	if !exists {
		log.Printf("[Monitor] New node discovered via heartbeat: %s (shard: %s, pinned: %d)", peerIDStr, shardLogLabel(shardID), pinnedCount)
		nodeState = &NodeState{
			PeerID:         peerIDStr,
			CurrentShard:   shardID,
			PinnedFiles:    pinnedCount,
			KnownFiles:     0,
			LastSeen:       now,
			ShardHistory:   []ShardHistoryEntry{{ShardID: shardID, FirstSeen: now}},
			IPAddress:      ip,
			announcedFiles: make(map[string]time.Time),
		}
		m.nodes[peerIDStr] = nodeState
		m.treeDirty = true
	} else {
		nodeState.LastSeen = now
		if pinnedCount >= 0 {
			nodeState.PinnedFiles = pinnedCount
			if pinnedCount == 0 {
				removedFromManifests := 0
				for manifest, peers := range m.manifestReplication {
					if _, had := peers[peerIDStr]; had {
						delete(peers, peerIDStr)
						removedFromManifests++
						if len(peers) == 0 {
							delete(m.manifestReplication, manifest)
						}
					}
				}
				if removedFromManifests > 0 {
					log.Printf("[Monitor] UNPIN_ALL peer=%s shard=%s (pinned=0, removed from %d manifests)",
						peerIDStr, shardID, removedFromManifests)
				}
			}
		}
		if nodeState.CurrentShard == "" {
			nodeState.CurrentShard = shardID
			nodeState.ShardHistory = append(nodeState.ShardHistory, ShardHistoryEntry{ShardID: shardID, FirstSeen: now})
			m.treeDirty = true
		} else {
			m.updateNodeShardLocked(nodeState, shardID, now)
		}
		if ip != "" && ip != nodeState.IPAddress {
			nodeState.IPAddress = ip
			nodeState.Region = ""
		}
	}

	if ip != "" && nodeState.Region == "" {
		select {
		case m.geoQueue <- geoRequest{ip: ip, peerID: peerIDStr}:
		default:
		}
	}
}

func (m *Monitor) handleLeaveShard(peerID peer.ID, shardID string) {
	peerIDStr := peerID.String()
	m.mu.Lock()
	defer m.mu.Unlock()
	node, exists := m.nodes[peerIDStr]
	if !exists {
		return
	}
	if node.CurrentShard == shardID {
		node.CurrentShard = ""
		m.treeDirty = true
	}
}

func (m *Monitor) updateNodeShardLocked(node *NodeState, newShard string, timestamp time.Time) {
	if len(node.ShardHistory) == 0 {
		return
	}
	lastShard := node.ShardHistory[len(node.ShardHistory)-1].ShardID

	if lastShard != newShard {
		m.treeDirty = true
		log.Printf("[Monitor] SHARD_MOVE peer=%s from=%s to=%s",
			node.PeerID, shardLogLabel(lastShard), shardLogLabel(newShard))
		if len(newShard) > len(lastShard) && strings.HasPrefix(newShard, lastShard) {
			log.Printf("[Monitor] Detected shard split: %s -> %s (node: %s)", shardLogLabel(lastShard), newShard, node.PeerID)
			m.splitEvents = append(m.splitEvents, ShardSplitEvent{
				ParentShard: lastShard,
				ChildShard:  newShard,
				Timestamp:   timestamp,
			})
			if m.ps != nil {
				m.ensureShardSubscriptionUnlocked(context.Background(), newShard)
			}
			if lastShard == "" && (newShard == "0" || newShard == "1") {
				siblingShard := "1"
				if newShard == "1" {
					siblingShard = "0"
				}
				m.ensureShardSubscriptionUnlocked(context.Background(), siblingShard)
			}
		} else {
			if m.ps != nil {
				m.ensureShardSubscriptionUnlocked(context.Background(), newShard)
			}
		}
		node.CurrentShard = newShard
		node.ShardHistory = append(node.ShardHistory, ShardHistoryEntry{ShardID: newShard, FirstSeen: timestamp})

		peerIDStr := node.PeerID
		depth := 0
		for _, n := range m.nodes {
			shard := n.CurrentShard
			if shard == "" && len(n.ShardHistory) > 0 {
				shard = n.ShardHistory[len(n.ShardHistory)-1].ShardID
			}
			if len(shard) > depth {
				depth = len(shard)
			}
		}
		removed := 0
		for manifest, peers := range m.manifestReplication {
			if _, had := peers[peerIDStr]; !had {
				continue
			}
			if targetShardForManifest(manifest, depth) != newShard {
				delete(peers, peerIDStr)
				removed++
				if len(peers) == 0 {
					delete(m.manifestReplication, manifest)
				}
			}
		}
		if removed > 0 {
			log.Printf("[Monitor] Shard move: removed peer %s from %d manifests (now only in shard %s)",
				peerIDStr, removed, shardLogLabel(newShard))
		}
	}
}

func (m *Monitor) getPinnedInShardForNode(peerIDStr string, nodeShard string) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	depth := m.replicationNetworkDepthUnlocked()
	cutoff := time.Now().Add(-ReplicationAnnounceTTL)
	if m.peerShardLastSeen[peerIDStr] != nil {
		if last := m.peerShardLastSeen[peerIDStr][nodeShard]; last.Before(cutoff) {
			return 0
		}
	}
	count := 0
	for manifest, peers := range m.manifestReplication {
		if _, ok := peers[peerIDStr]; !ok {
			continue
		}
		if targetShardForManifest(manifest, depth) != nodeShard {
			continue
		}
		count++
	}
	return count
}

func (m *Monitor) ensureMinPinnedForPeer(peerIDStr string, min int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	node, ok := m.nodes[peerIDStr]
	if !ok {
		return
	}
	if node.PinnedFiles < min {
		node.PinnedFiles = min
	}
}

func (m *Monitor) getShardMembership() map[string][]string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	shardToPeers := make(map[string][]string)
	for peerIDStr, node := range m.nodes {
		shard := node.CurrentShard
		if shard == "" && len(node.ShardHistory) > 0 {
			shard = node.ShardHistory[len(node.ShardHistory)-1].ShardID
		}
		short := peerIDStr
		shardToPeers[shard] = append(shardToPeers[shard], short)
	}
	for shard := range shardToPeers {
		sort.Strings(shardToPeers[shard])
	}
	return shardToPeers
}
