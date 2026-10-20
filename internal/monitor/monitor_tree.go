package monitor

import (
	"sort"
	"time"
)

func (m *Monitor) GetShardTree() *ShardTreeNode {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.treeDirty && m.treeCache != nil && time.Since(m.treeCacheTime) < 5*time.Second {
		return m.treeCache
	}

	// allShardIDs holds each node's shard and all its ancestors, so every leaf
	// of the tree has at least one node.
	shardCounts := make(map[string]int)
	allShardIDs := map[string]bool{"": true}
	for id, n := range m.nodes {
		if !m.isDisplayableNodeLocked(id, n) {
			continue
		}
		sid := n.EffectiveShard()
		if len(sid) > maxShardDepthForTreeDisplay {
			sid = sid[:maxShardDepthForTreeDisplay]
		}
		shardCounts[sid]++
		for ; sid != ""; sid = sid[:len(sid)-1] {
			allShardIDs[sid] = true
		}
	}
	nodeMap := make(map[string]*ShardTreeNode, len(allShardIDs))
	for id := range allShardIDs {
		nodeMap[id] = &ShardTreeNode{ShardID: id, NodeCount: shardCounts[id]}
	}
	for _, e := range m.splitEvents {
		if child, ok := nodeMap[e.ChildShard]; ok {
			t := e.Timestamp
			child.SplitTime = &t
		}
	}
	for id, node := range nodeMap {
		if id == "" {
			continue
		}
		if parent, ok := nodeMap[id[:len(id)-1]]; ok {
			parent.Children = append(parent.Children, node)
		}
	}
	root := nodeMap[""]
	sortChildren(root)
	m.treeCache = root
	m.treeCacheTime = time.Now()
	m.treeDirty = false
	return root
}

func sortChildren(node *ShardTreeNode) {
	sort.Slice(node.Children, func(i, j int) bool { return node.Children[i].ShardID < node.Children[j].ShardID })
	for _, child := range node.Children {
		sortChildren(child)
	}
}
