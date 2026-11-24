package schema

import (
	"strconv"
	"strings"

	"github.com/libp2p/go-libp2p/core/peer"
)

// Plain-text shard announcements. The binding sits before the node name,
// which may contain ':' and so comes last:
//
//	HEARTBEAT:<helper>:<pinned>:<role>:<node>:<sig>:<name>
//	JOIN:<helper>:<role>:<node>:<sig>:<name>
//	PROBE:<helper>:<node>:<sig>
//
// A message without <node>:<sig> parses with a nil Binding and does not
// count.
const (
	HeartbeatPrefix = "HEARTBEAT:"
	JoinPrefix      = "JOIN:"
	ProbePrefix     = "PROBE:"
)

// Heartbeat is a HEARTBEAT message. Pinned is -1 if the message has none.
type Heartbeat struct {
	Helper  peer.ID
	Pinned  int
	Role    string
	Binding *Binding
	Name    string
}

// Join is a JOIN message.
type Join struct {
	Helper  peer.ID
	Role    string
	Binding *Binding
	Name    string
}

// Probe is a PROBE message.
type Probe struct {
	Helper  peer.ID
	Binding *Binding
}

func (h *Heartbeat) Marshal() []byte {
	return []byte(HeartbeatPrefix + h.Helper.String() + ":" + strconv.Itoa(h.Pinned) + ":" + h.Role + ":" + withBinding(h.Binding, h.Name))
}

func (j *Join) Marshal() []byte {
	return []byte(JoinPrefix + j.Helper.String() + ":" + j.Role + ":" + withBinding(j.Binding, j.Name))
}

func (p *Probe) Marshal() []byte {
	s := ProbePrefix + p.Helper.String()
	if p.Binding != nil {
		s += ":" + p.Binding.text()
	}
	return []byte(s)
}

func withBinding(b *Binding, name string) string {
	if b == nil {
		return name
	}
	return b.text() + ":" + name
}

// ParseHeartbeat parses a message that starts with HeartbeatPrefix.
func ParseHeartbeat(data []byte) Heartbeat {
	parts := strings.SplitN(strings.TrimPrefix(string(data), HeartbeatPrefix), ":", 4)
	h := Heartbeat{Pinned: -1}
	h.Helper, _ = peer.Decode(parts[0])
	if len(parts) > 1 {
		if n, err := strconv.Atoi(parts[1]); err == nil {
			h.Pinned = n
		}
	}
	if len(parts) > 2 {
		h.Role = parts[2]
	}
	if len(parts) > 3 {
		h.Binding, h.Name = cutBinding(parts[3])
	}
	return h
}

// ParseJoin parses a message that starts with JoinPrefix.
func ParseJoin(data []byte) Join {
	parts := strings.SplitN(strings.TrimPrefix(string(data), JoinPrefix), ":", 3)
	var j Join
	j.Helper, _ = peer.Decode(parts[0])
	if len(parts) > 1 {
		j.Role = parts[1]
	}
	if len(parts) > 2 {
		j.Binding, j.Name = cutBinding(parts[2])
	}
	return j
}

// ParseProbe parses a message that starts with ProbePrefix.
func ParseProbe(data []byte) Probe {
	parts := strings.SplitN(strings.TrimPrefix(string(data), ProbePrefix), ":", 2)
	var p Probe
	p.Helper, _ = peer.Decode(parts[0])
	if len(parts) > 1 {
		p.Binding, _ = cutBinding(parts[1])
	}
	return p
}

// AnnouncementBinding returns the binding carried by a HEARTBEAT, JOIN or
// PROBE message, or nil.
func AnnouncementBinding(data []byte) *Binding {
	s := string(data)
	switch {
	case strings.HasPrefix(s, HeartbeatPrefix):
		return ParseHeartbeat(data).Binding
	case strings.HasPrefix(s, JoinPrefix):
		return ParseJoin(data).Binding
	case strings.HasPrefix(s, ProbePrefix):
		return ParseProbe(data).Binding
	}
	return nil
}
