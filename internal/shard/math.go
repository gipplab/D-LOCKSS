// Package shard implements the dynamic sharding engine: placement math,
// membership tracking, split/merge/discovery, custodial handoff, orphan
// handling, resharding, and the replication checker.
package shard

import (
	"crypto/sha256"
	"strings"
)

// A shard ID is a string of '0'/'1' bits; the root shard is "". A node at
// depth d lives in the shard formed by the first d bits of SHA-256 of its
// peer ID string; a file belongs to the shard formed by the first d bits of
// SHA-256 of its payload CID string.

// BitPrefix returns the first depth bits (MSB first) of SHA-256(key) as a
// '0'/'1' string. depth is clamped to [0, 256].
func BitPrefix(key string, depth int) string {
	if depth <= 0 {
		return ""
	}
	if depth > 256 {
		depth = 256
	}
	sum := sha256.Sum256([]byte(key))
	var b strings.Builder
	b.Grow(depth)
	for i := 0; i < depth; i++ {
		if sum[i/8]&(0x80>>(i%8)) != 0 {
			b.WriteByte('1')
		} else {
			b.WriteByte('0')
		}
	}
	return b.String()
}

// TargetShardForPayload returns the shard a payload belongs to at the given
// depth. Depth is clamped to at least 1: even the root places files one
// level down when resolving custodial targets.
func TargetShardForPayload(payloadCID string, depth int) string {
	if depth < 1 {
		depth = 1
	}
	return BitPrefix(payloadCID, depth)
}

// Responsible reports whether a node in shardID is responsible for the key.
// The root ("") is responsible for everything.
func Responsible(shardID, key string) bool {
	return strings.HasPrefix(BitPrefix(key, len(shardID)), shardID)
}

// Parent returns the parent shard ID; the root's parent is the root.
func Parent(shardID string) string {
	if shardID == "" {
		return ""
	}
	return shardID[:len(shardID)-1]
}

// Sibling returns the shard with the last bit flipped; the root has none.
func Sibling(shardID string) string {
	if shardID == "" {
		return ""
	}
	last := byte('0')
	if shardID[len(shardID)-1] == '0' {
		last = '1'
	}
	return shardID[:len(shardID)-1] + string(last)
}

// Children returns the two child shard IDs.
func Children(shardID string) (string, string) {
	return shardID + "0", shardID + "1"
}

// Ancestors returns every proper ancestor from the parent up to and
// including the root, in that order. The root has none.
func Ancestors(shardID string) []string {
	var out []string
	for s := shardID; s != ""; {
		s = Parent(s)
		out = append(out, s)
	}
	return out
}

// IsAncestorOrSelf reports whether a is a (possibly empty) prefix of b.
func IsAncestorOrSelf(a, b string) bool { return strings.HasPrefix(b, a) }
