package shard

import (
	"bytes"
	"strings"
	"sync"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

// PeerRole indicates whether a peer is actively contributing to replication.
type PeerRole string

const (
	RoleActive  PeerRole = "ACTIVE"  // Normal node, can pin new files
	RolePassive PeerRole = "PASSIVE" // At storage limit, cannot pin; not counted for replication
	RoleProbe   PeerRole = "PROBE"   // Transient viewer, not counted
)

// PeerRoleInfo holds a peer's role and last-seen time.
type PeerRoleInfo struct {
	Role     PeerRole
	LastSeen time.Time
}

// PeerTracker tracks which peers are present in each shard and their roles.
// Thread-safe with its own mutex, independent of ShardManager.mu.
type PeerTracker struct {
	mu     sync.RWMutex
	selfID peer.ID
	seen   map[string]map[peer.ID]time.Time    // shard → peer → lastSeen
	roles  map[string]map[peer.ID]PeerRoleInfo // shard → peer → role
}

func NewPeerTracker(selfID peer.ID) *PeerTracker {
	return &PeerTracker{
		selfID: selfID,
		seen:   make(map[string]map[peer.ID]time.Time),
		roles:  make(map[string]map[peer.ID]PeerRoleInfo),
	}
}

// RecordSeen marks a peer as seen in a shard.
func (pt *PeerTracker) RecordSeen(shardID string, peerID peer.ID) {
	pt.mu.Lock()
	if pt.seen[shardID] == nil {
		pt.seen[shardID] = make(map[peer.ID]time.Time)
	}
	pt.seen[shardID][peerID] = time.Now()
	pt.mu.Unlock()
}

// RecordRole records a peer's role in a shard.
func (pt *PeerTracker) RecordRole(shardID string, peerID peer.ID, role PeerRole) {
	pt.mu.Lock()
	if pt.roles[shardID] == nil {
		pt.roles[shardID] = make(map[peer.ID]PeerRoleInfo)
	}
	pt.roles[shardID][peerID] = PeerRoleInfo{Role: role, LastSeen: time.Now()}
	pt.mu.Unlock()
}

// RemoveRole removes a peer's role entry (e.g. on LEAVE).
func (pt *PeerTracker) RemoveRole(shardID string, peerID peer.ID) {
	pt.mu.Lock()
	if pt.roles[shardID] != nil {
		delete(pt.roles[shardID], peerID)
	}
	pt.mu.Unlock()
}

// CountActive returns the number of ACTIVE peers in the given shard.
// When includeSelf is true and shardID matches currentShard, adds 1 for self.
func (pt *PeerTracker) CountActive(shardID string, includeSelf bool, currentShard string, activeWindow time.Duration) int {
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
		if info.Role != RoleActive || info.LastSeen.Before(cutoff) || pid == pt.selfID {
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

// GetActiveForShard returns ACTIVE peer IDs for the given shard (excluding self).
func (pt *PeerTracker) GetActiveForShard(shardID string, activeWindow time.Duration) []peer.ID {
	pt.mu.RLock()
	defer pt.mu.RUnlock()
	roles, ok := pt.roles[shardID]
	if !ok {
		return nil
	}
	cutoff := time.Now().Add(-activeWindow)
	var active []peer.ID
	for p, info := range roles {
		if info.Role == RoleActive && info.LastSeen.After(cutoff) && p != pt.selfID {
			active = append(active, p)
		}
	}
	return active
}

// GetSeenPeers returns all peers seen in a shard within the cutoff window (excluding self).
func (pt *PeerTracker) GetSeenPeers(shardID string, activeWindow time.Duration) map[peer.ID]struct{} {
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

// HasRoles returns true if any role data exists for the given shard.
func (pt *PeerTracker) HasRoles(shardID string) bool {
	pt.mu.RLock()
	defer pt.mu.RUnlock()
	_, ok := pt.roles[shardID]
	return ok
}

// PruneStale removes peers not seen within the given duration.
func (pt *PeerTracker) PruneStale(maxAge time.Duration) {
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
			if info.LastSeen.Before(cutoff) {
				delete(roles, peerID)
			}
		}
		if len(roles) == 0 {
			delete(pt.roles, shardID)
		}
	}
}

// parseHeartbeatRole extracts role from HEARTBEAT:pid:count or HEARTBEAT:pid:count:ROLE.
func parseHeartbeatRole(data []byte) PeerRole {
	s := string(data)
	if !strings.HasPrefix(s, msgPrefixHeartbeat) {
		return RoleActive
	}
	parts := strings.SplitN(s, ":", 4)
	if len(parts) >= 4 {
		r := PeerRole(strings.ToUpper(parts[3]))
		if r == RolePassive {
			return RolePassive
		}
		if r == RoleProbe {
			return RoleProbe
		}
	}
	return RoleActive
}

// parseJoinRole extracts role from JOIN:pid or JOIN:pid:ROLE.
func parseJoinRole(data []byte) PeerRole {
	s := string(data)
	if !strings.HasPrefix(s, msgPrefixJoin) {
		return RoleActive
	}
	parts := strings.SplitN(s, ":", 3)
	if len(parts) >= 3 {
		if PeerRole(strings.ToUpper(parts[2])) == RolePassive {
			return RolePassive
		}
	}
	return RoleActive
}

// getOurRole returns ACTIVE if we can accept custodial files, PASSIVE otherwise.
func (sm *ShardManager) getOurRole() PeerRole {
	if sm.storageMgr.CanAcceptCustodialFile() {
		return RoleActive
	}
	return RolePassive
}

// processTextProtocolForProbe updates PeerTracker for HEARTBEAT/JOIN/LEAVE/PROBE.
// Used when probing a shard to collect role info without full message handling.
func (sm *ShardManager) processTextProtocolForProbe(msg *pubsub.Message, shardID string) bool {
	data := msg.Data
	if len(data) == 0 {
		return false
	}
	from := msg.GetFrom()
	sm.peers.RecordSeen(shardID, from)

	if bytes.HasPrefix(data, []byte(msgPrefixHeartbeat)) {
		sm.peers.RecordRole(shardID, from, parseHeartbeatRole(data))
		return true
	}
	if bytes.HasPrefix(data, []byte(msgPrefixJoin)) {
		sm.peers.RecordRole(shardID, from, parseJoinRole(data))
		return true
	}
	if bytes.HasPrefix(data, []byte(msgPrefixLeave)) {
		sm.peers.RemoveRole(shardID, from)
		return true
	}
	if bytes.HasPrefix(data, []byte(msgPrefixProbe)) {
		sm.peers.RecordRole(shardID, from, RoleProbe)
		return true
	}
	return false
}
