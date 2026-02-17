package main

import (
	"testing"
	"time"
)

func TestUpdateNodeShardLocked_CrossBranchRejected(t *testing.T) {
	m := NewMonitor("")
	peerID := "12D3KooWTestCrossBranch123"
	now := time.Now()

	tests := []struct {
		name      string
		lastShard string
		newShard  string
	}{
		{"10_to_0", "10", "0"},
		{"0_to_11", "0", "11"},
		{"0_to_10", "0", "10"},
		{"01_to_1", "01", "1"},
		{"1_to_00", "1", "00"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := &NodeState{
				PeerID:       peerID,
				CurrentShard: tt.lastShard,
				ShardHistory: []ShardHistoryEntry{{ShardID: tt.lastShard, FirstSeen: now}},
			}
			m.mu.Lock()
			m.nodes[peerID] = node
			m.mu.Unlock()

			m.mu.Lock()
			m.updateNodeShardLocked(node, tt.newShard, now)
			m.mu.Unlock()

			if node.CurrentShard != tt.lastShard {
				t.Errorf("cross-branch move %s -> %s should be rejected; CurrentShard = %s, want %s",
					tt.lastShard, tt.newShard, node.CurrentShard, tt.lastShard)
			}
			if len(node.ShardHistory) != 1 {
				t.Errorf("ShardHistory should be unchanged; len = %d, want 1", len(node.ShardHistory))
			}
		})
	}
}

func TestUpdateNodeShardLocked_ValidMovesAccepted(t *testing.T) {
	m := NewMonitor("")
	now := time.Now()

	t.Run("split_0_to_00", func(t *testing.T) {
		peerID := "12D3KooWTestSplit123"
		node := &NodeState{
			PeerID:       peerID,
			CurrentShard: "0",
			ShardHistory: []ShardHistoryEntry{{ShardID: "0", FirstSeen: now}},
		}
		m.mu.Lock()
		m.nodes[peerID] = node
		m.mu.Unlock()

		m.mu.Lock()
		m.updateNodeShardLocked(node, "00", now)
		m.mu.Unlock()

		if node.CurrentShard != "00" {
			t.Errorf("split 0 -> 00 should be accepted; CurrentShard = %s, want 00", node.CurrentShard)
		}
		if len(node.ShardHistory) != 2 {
			t.Errorf("ShardHistory should have 2 entries; len = %d", len(node.ShardHistory))
		}
	})

	t.Run("sibling_0_to_1", func(t *testing.T) {
		peerID := "12D3KooWTestSibling456"
		node := &NodeState{
			PeerID:       peerID,
			CurrentShard: "0",
			ShardHistory: []ShardHistoryEntry{{ShardID: "0", FirstSeen: now}},
		}
		m.mu.Lock()
		m.nodes[peerID] = node
		m.mu.Unlock()

		m.mu.Lock()
		m.updateNodeShardLocked(node, "1", now)
		m.mu.Unlock()

		if node.CurrentShard != "1" {
			t.Errorf("sibling move 0 -> 1 should be accepted; CurrentShard = %s, want 1", node.CurrentShard)
		}
		if len(node.ShardHistory) != 2 {
			t.Errorf("ShardHistory should have 2 entries; len = %d", len(node.ShardHistory))
		}
	})
}
