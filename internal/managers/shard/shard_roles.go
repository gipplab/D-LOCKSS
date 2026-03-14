package shard

import (
	"bytes"
	"strings"
	"sync"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

type peerRole string

const (
	roleActive     peerRole = "ACTIVE"
	rolePassive    peerRole = "PASSIVE"
	roleProbe      peerRole = "PROBE"
	roleReplicator peerRole = "REPLICATOR"
)

type peerRoleInfo struct {
	role     peerRole
	lastSeen time.Time
}

type peerTracker struct {
	mu     sync.RWMutex
	selfID peer.ID
	seen   map[string]map[peer.ID]time.Time    // shard → peer → lastSeen
	roles  map[string]map[peer.ID]peerRoleInfo // shard → peer → role
}

func newPeerTracker(selfID peer.ID) *peerTracker {
	return &peerTracker{
		selfID: selfID,
		seen:   make(map[string]map[peer.ID]time.Time),
		roles:  make(map[string]map[peer.ID]peerRoleInfo),
	}
}

func (pt *peerTracker) RecordSeen(shardID string, peerID peer.ID) {
	pt.mu.Lock()
	if pt.seen[shardID] == nil {
		pt.seen[shardID] = make(map[peer.ID]time.Time)
	}
	pt.seen[shardID][peerID] = time.Now()
	pt.mu.Unlock()
}

func (pt *peerTracker) RecordRole(shardID string, peerID peer.ID, role peerRole) {
	pt.mu.Lock()
	if pt.roles[shardID] == nil {
		pt.roles[shardID] = make(map[peer.ID]peerRoleInfo)
	}
	pt.roles[shardID][peerID] = peerRoleInfo{role: role, lastSeen: time.Now()}
	pt.mu.Unlock()
}

func (pt *peerTracker) RemoveRole(shardID string, peerID peer.ID) {
	pt.mu.Lock()
	if pt.roles[shardID] != nil {
		delete(pt.roles[shardID], peerID)
	}
	pt.mu.Unlock()
}

func (pt *peerTracker) CountActive(shardID string, includeSelf bool, currentShard string, activeWindow time.Duration) int {
	pt.mu.RLock()
	roles, ok := pt.roles[shardID]
	if !ok {
		pt.mu.RUnlock()
		if includeSelf && shardID == currentShard {
			return 1
		}
		return 0
	}
	cutoff := time.Now().Add(-activeWindow)
	n := 0
	for pid, info := range roles {
		if (info.role != roleActive && info.role != roleReplicator) || info.lastSeen.Before(cutoff) || pid == pt.selfID {
			continue
		}
		n++
	}
	pt.mu.RUnlock()

	if includeSelf && shardID == currentShard {
		n++
	}
	return n
}

func (pt *peerTracker) GetActiveForShard(shardID string, activeWindow time.Duration) []peer.ID {
	pt.mu.RLock()
	defer pt.mu.RUnlock()
	roles, ok := pt.roles[shardID]
	if !ok {
		return nil
	}
	cutoff := time.Now().Add(-activeWindow)
	var active []peer.ID
	for p, info := range roles {
		if (info.role == roleActive || info.role == roleReplicator) && info.lastSeen.After(cutoff) && p != pt.selfID {
			active = append(active, p)
		}
	}
	return active
}

func (pt *peerTracker) GetSeenPeers(shardID string, activeWindow time.Duration) map[peer.ID]struct{} {
	pt.mu.RLock()
	defer pt.mu.RUnlock()
	cutoff := time.Now().Add(-activeWindow)
	result := make(map[peer.ID]struct{})
	if seenMap, ok := pt.seen[shardID]; ok {
		for p, lastSeen := range seenMap {
			if lastSeen.After(cutoff) && p != pt.selfID {
				result[p] = struct{}{}
			}
		}
	}
	return result
}

func (pt *peerTracker) HasRoles(shardID string) bool {
	pt.mu.RLock()
	defer pt.mu.RUnlock()
	_, ok := pt.roles[shardID]
	return ok
}

func (pt *peerTracker) PruneStale(maxAge time.Duration) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	cutoff := time.Now().Add(-maxAge)
	for shardID, peers := range pt.seen {
		for peerID, lastSeen := range peers {
			if lastSeen.Before(cutoff) {
				delete(peers, peerID)
			}
		}
		if len(peers) == 0 {
			delete(pt.seen, shardID)
		}
	}
	for shardID, roles := range pt.roles {
		for peerID, info := range roles {
			if info.lastSeen.Before(cutoff) {
				delete(roles, peerID)
			}
		}
		if len(roles) == 0 {
			delete(pt.roles, shardID)
		}
	}
}

func parseHeartbeatRole(data []byte) peerRole {
	s := string(data)
	if !strings.HasPrefix(s, msgPrefixHeartbeat) {
		return roleActive
	}
	parts := strings.SplitN(s, ":", 4)
	if len(parts) >= 4 {
		r := peerRole(strings.ToUpper(parts[3]))
		switch r {
		case rolePassive, roleProbe, roleReplicator:
			return r
		}
	}
	return roleActive
}

func parseJoinRole(data []byte) peerRole {
	s := string(data)
	if !strings.HasPrefix(s, msgPrefixJoin) {
		return roleActive
	}
	parts := strings.SplitN(s, ":", 3)
	if len(parts) >= 3 {
		r := peerRole(strings.ToUpper(parts[2]))
		switch r {
		case rolePassive, roleReplicator:
			return r
		}
	}
	return roleActive
}

func (sm *ShardManager) getOurRole() peerRole {
	if !sm.storageMgr.CanAcceptCustodialFile() {
		return rolePassive
	}
	if !sm.IsLocalNodeIngestor() {
		return roleReplicator
	}
	return roleActive
}

// processTextProtocolForProbe updates PeerTracker for HEARTBEAT/JOIN/LEAVE/PROBE.
// Used when probing a shard to collect role info without full message handling.
func (sm *ShardManager) processTextProtocolForProbe(msg *pubsub.Message, shardID string) {
	data := msg.Data
	if len(data) == 0 {
		return
	}
	from := msg.GetFrom()
	sm.peers.RecordSeen(shardID, from)

	switch {
	case bytes.HasPrefix(data, []byte(msgPrefixHeartbeat)):
		sm.peers.RecordRole(shardID, from, parseHeartbeatRole(data))
	case bytes.HasPrefix(data, []byte(msgPrefixJoin)):
		sm.peers.RecordRole(shardID, from, parseJoinRole(data))
	case bytes.HasPrefix(data, []byte(msgPrefixLeave)):
		sm.peers.RemoveRole(shardID, from)
	case bytes.HasPrefix(data, []byte(msgPrefixProbe)):
		sm.peers.RecordRole(shardID, from, roleProbe)
	}
}
