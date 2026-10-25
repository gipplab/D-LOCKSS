package protocol

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/libp2p/go-libp2p/core/peer"
)

// Role is a node's advertised shard role.
type Role string

const (
	RoleActive     Role = "ACTIVE"     // full storage member, may ingest
	RolePassive    Role = "PASSIVE"    // disk full: excluded from counts
	RoleReplicator Role = "REPLICATOR" // stores and replicates, may not ingest
	RoleProbe      Role = "PROBE"      // observer (monitor/probes): never counted
)

// ParseRole normalizes a role string; anything unknown is ACTIVE for
// backward compatibility.
func ParseRole(s string) Role {
	switch Role(strings.ToUpper(strings.TrimSpace(s))) {
	case RolePassive:
		return RolePassive
	case RoleProbe:
		return RoleProbe
	case RoleReplicator:
		return RoleReplicator
	default:
		return RoleActive
	}
}

// Counted reports whether the role counts toward shard membership.
func (r Role) Counted() bool { return r == RoleActive || r == RoleReplicator }

// TextKind identifies a plain-text shard message.
type TextKind string

const (
	TextHeartbeat TextKind = "HEARTBEAT"
	TextJoin      TextKind = "JOIN"
	TextLeave     TextKind = "LEAVE"
	TextPinned    TextKind = "PINNED"
	TextProbe     TextKind = "PROBE"
	TextSplit     TextKind = "SPLIT"
)

// TextMessage is a parsed plain-text shard message. Fields are set per kind:
//
//	HEARTBEAT: Peer, Pinned (-1 unknown), Role, Name
//	JOIN:      Peer, Role, Name
//	LEAVE:     Peer
//	PINNED:    CID
//	PROBE:     Peer
//	SPLIT:     Child0, Child1
type TextMessage struct {
	Kind   TextKind
	Peer   string
	Pinned int
	Role   Role
	Name   string
	CID    string
	Child0 string
	Child1 string
}

// ParseText parses a text shard message. It returns ok=false for CBOR or
// unknown payloads (which the caller then tries as CBOR). Legacy JSON
// messages ('{' prefix) parse as kind "" with ok=true and are ignored.
func ParseText(data []byte) (TextMessage, bool) {
	if len(data) == 0 {
		return TextMessage{}, false
	}
	if data[0] == '{' {
		return TextMessage{}, true // legacy JSON: consume and ignore
	}
	s := string(data)
	head, rest, found := strings.Cut(s, ":")
	if !found {
		return TextMessage{}, false
	}
	switch TextKind(head) {
	case TextHeartbeat:
		// HEARTBEAT:<peer>:<pinned>:<role>:<name>; trailing fields optional.
		parts := strings.SplitN(rest, ":", 4)
		m := TextMessage{Kind: TextHeartbeat, Peer: parts[0], Pinned: -1, Role: RoleActive}
		if len(parts) > 1 {
			if n, err := strconv.Atoi(parts[1]); err == nil {
				m.Pinned = n
			}
		}
		if len(parts) > 2 {
			m.Role = ParseRole(parts[2])
		}
		if len(parts) > 3 {
			m.Name = parts[3]
		}
		return m, true
	case TextJoin:
		parts := strings.SplitN(rest, ":", 3)
		m := TextMessage{Kind: TextJoin, Peer: parts[0], Pinned: -1, Role: RoleActive}
		if len(parts) > 1 {
			m.Role = ParseRole(parts[1])
		}
		if len(parts) > 2 {
			m.Name = parts[2]
		}
		return m, true
	case TextLeave:
		return TextMessage{Kind: TextLeave, Peer: rest}, true
	case TextPinned:
		return TextMessage{Kind: TextPinned, CID: rest}, true
	case TextProbe:
		return TextMessage{Kind: TextProbe, Peer: rest}, true
	case TextSplit:
		c0, c1, ok := strings.Cut(rest, ":")
		if !ok {
			return TextMessage{}, false
		}
		return TextMessage{Kind: TextSplit, Child0: c0, Child1: c1}, true
	default:
		return TextMessage{}, false
	}
}

// FormatHeartbeat builds a HEARTBEAT line.
func FormatHeartbeat(p peer.ID, pinned int, role Role, name string) string {
	return fmt.Sprintf("HEARTBEAT:%s:%d:%s:%s", p, pinned, role, name)
}

// FormatJoin builds a JOIN line.
func FormatJoin(p peer.ID, role Role, name string) string {
	return fmt.Sprintf("JOIN:%s:%s:%s", p, role, name)
}

// FormatLeave builds a LEAVE line.
func FormatLeave(p peer.ID) string { return "LEAVE:" + p.String() }

// FormatPinned builds a PINNED line.
func FormatPinned(manifestCID string) string { return "PINNED:" + manifestCID }

// FormatProbe builds a PROBE line.
func FormatProbe(p peer.ID) string { return "PROBE:" + p.String() }

// FormatSplit builds a SPLIT line.
func FormatSplit(child0, child1 string) string { return "SPLIT:" + child0 + ":" + child1 }
