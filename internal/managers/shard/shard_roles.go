package shard

import (
	"strings"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/internal/managers/storage"
)

// PeerRole indicates whether a peer is actively contributing to replication.
const (
	RoleActive  = "ACTIVE"  // Normal node, can pin new files
	RolePassive = "PASSIVE" // At storage limit, cannot pin; not counted for replication
	RoleProbe   = "PROBE"   // Transient viewer, not counted
)

// PeerRoleInfo holds a peer's role and last-seen time.
type PeerRoleInfo struct {
	Role     string
	LastSeen time.Time
}

// parseHeartbeatRole extracts role from HEARTBEAT:pid:count or HEARTBEAT:pid:count:ROLE.
// Returns "ACTIVE" if no role (backward compat).
func parseHeartbeatRole(data []byte) string {
	s := string(data)
	if len(s) < 10 || s[:10] != "HEARTBEAT:" {
		return RoleActive
	}
	parts := strings.SplitN(s, ":", 4)
	if len(parts) >= 4 {
		r := strings.ToUpper(parts[3])
		if r == RolePassive {
			return RolePassive
		}
		if r == RoleProbe {
			return RoleProbe
		}
	}
	return RoleActive
}

// getOurRole returns ACTIVE if we can accept custodial files, PASSIVE otherwise.
func (sm *ShardManager) getOurRole() string {
	if storage.CanAcceptCustodialFile() {
		return RoleActive
	}
	return RolePassive
}

// processTextProtocolForProbe updates seenPeers and seenPeerRoles for HEARTBEAT/JOIN/LEAVE/PROBE.
// Used when probing a shard to collect role info without full message handling. Returns true if handled.
func (sm *ShardManager) processTextProtocolForProbe(msg *pubsub.Message, shardID string) bool {
	data := msg.Data
	if len(data) == 0 {
		return false
	}
	from := msg.GetFrom()
	now := time.Now()
	sm.mu.Lock()
	if sm.seenPeers[shardID] == nil {
		sm.seenPeers[shardID] = make(map[peer.ID]time.Time)
	}
	sm.seenPeers[shardID][from] = now
	sm.mu.Unlock()

	if len(data) >= 10 && string(data[:10]) == "HEARTBEAT:" {
		role := parseHeartbeatRole(data)
		sm.mu.Lock()
		if sm.seenPeerRoles[shardID] == nil {
			sm.seenPeerRoles[shardID] = make(map[peer.ID]PeerRoleInfo)
		}
		sm.seenPeerRoles[shardID][from] = PeerRoleInfo{Role: role, LastSeen: now}
		sm.mu.Unlock()
		return true
	}
	if len(data) >= 5 && string(data[:5]) == "JOIN:" {
		role := parseJoinRole(data)
		sm.mu.Lock()
		if sm.seenPeerRoles[shardID] == nil {
			sm.seenPeerRoles[shardID] = make(map[peer.ID]PeerRoleInfo)
		}
		sm.seenPeerRoles[shardID][from] = PeerRoleInfo{Role: role, LastSeen: now}
		sm.mu.Unlock()
		return true
	}
	if len(data) >= 6 && string(data[:6]) == "LEAVE:" {
		sm.mu.Lock()
		if sm.seenPeerRoles[shardID] != nil {
			delete(sm.seenPeerRoles[shardID], from)
		}
		sm.mu.Unlock()
		return true
	}
	if len(data) >= 6 && string(data[:6]) == "PROBE:" {
		sm.mu.Lock()
		if sm.seenPeerRoles[shardID] == nil {
			sm.seenPeerRoles[shardID] = make(map[peer.ID]PeerRoleInfo)
		}
		sm.seenPeerRoles[shardID][from] = PeerRoleInfo{Role: RoleProbe, LastSeen: now}
		sm.mu.Unlock()
		return true
	}
	return false
}

// parseJoinRole extracts role from JOIN:pid or JOIN:pid:ROLE.
func parseJoinRole(data []byte) string {
	s := string(data)
	if len(s) < 5 || s[:5] != "JOIN:" {
		return RoleActive
	}
	parts := strings.SplitN(s, ":", 3)
	if len(parts) >= 3 {
		r := strings.ToUpper(parts[2])
		if r == RolePassive {
			return RolePassive
		}
	}
	return RoleActive
}

// countActivePeers returns the number of ACTIVE peers in seenPeerRoles for the given shard.
// includeSelf: when true (our current shard), add 1 for ourselves as we're a full ACTIVE member.
func (sm *ShardManager) countActivePeers(shardID string, includeSelf bool, cutoff time.Duration) int {
	sm.mu.RLock()
	currentShard := sm.currentShard
	roles, ok := sm.seenPeerRoles[shardID]
	sm.mu.RUnlock()

	if !ok {
		if includeSelf && shardID == currentShard {
			return 1
		}
		return 0
	}

	cutoffTime := time.Now().Add(-cutoff)
	n := 0
	sm.mu.RLock()
	for pid, info := range roles {
		if info.Role != RoleActive {
			continue
		}
		if info.LastSeen.Before(cutoffTime) {
			continue
		}
		if pid == sm.h.ID() {
			continue
		}
		n++
	}
	sm.mu.RUnlock()

	if includeSelf && shardID == currentShard {
		n++
	}
	return n
}
