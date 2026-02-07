package main

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

	rawShardIDs := make(map[string]bool)
	rawShardIDs[""] = true
	for _, e := range m.splitEvents {
		rawShardIDs[e.ParentShard] = true
		rawShardIDs[e.ChildShard] = true
	}
	shardCounts := make(map[string]int)
	for _, n := range m.nodes {
		if len(n.ShardHistory) > 0 {
			sid := n.ShardHistory[len(n.ShardHistory)-1].ShardID
			shardCounts[sid]++
			rawShardIDs[sid] = true
		} else if n.CurrentShard != "" {
			sid := n.CurrentShard
			shardCounts[sid]++
			rawShardIDs[sid] = true
		}
	}
	allShardIDs := make(map[string]bool)
	for id := range rawShardIDs {
		current := id
		allShardIDs[current] = true
		for len(current) > 0 {
			current = current[:len(current)-1]
			allShardIDs[current] = true
		}
	}
	nodeMap := make(map[string]*ShardTreeNode)
	for id := range allShardIDs {
		nodeMap[id] = &ShardTreeNode{ShardID: id, Children: make([]*ShardTreeNode, 0), NodeCount: shardCounts[id]}
	}
	for _, e := range m.splitEvents {
		if child, ok := nodeMap[e.ChildShard]; ok {
			t := e.Timestamp
			child.SplitTime = &t
		}
	}
	var orderedIDs []string
	for id := range nodeMap {
		if id != "" {
			orderedIDs = append(orderedIDs, id)
		}
	}
	sort.Slice(orderedIDs, func(i, j int) bool {
		if len(orderedIDs[i]) != len(orderedIDs[j]) {
			return len(orderedIDs[i]) < len(orderedIDs[j])
		}
		return orderedIDs[i] < orderedIDs[j]
	})
	for _, id := range orderedIDs {
		parentID := id[:len(id)-1]
		parent, hasParent := nodeMap[parentID]
		if !hasParent {
			continue
		}
		child := nodeMap[id]
		exists := false
		for _, c := range parent.Children {
			if c.ShardID == id {
				exists = true
				break
			}
		}
		if !exists {
			parent.Children = append(parent.Children, child)
		}
	}
	root := nodeMap[""]
	root.Children = nil
	for i := 1; i <= 1; i++ {
		for id, node := range nodeMap {
			if id != "" && len(id) == i {
				root.Children = append(root.Children, node)
			}
		}
	}
	sort.Slice(root.Children, func(i, j int) bool { return root.Children[i].ShardID < root.Children[j].ShardID })
	nodesToRemove := make([]string, 0)
	for id, node := range nodeMap {
		if id != "" && node.NodeCount == 0 && len(node.Children) == 0 {
			nodesToRemove = append(nodesToRemove, id)
		}
	}
	for _, id := range nodesToRemove {
		parentID := id[:len(id)-1]
		if parent, ok := nodeMap[parentID]; ok {
			for i, child := range parent.Children {
				if child.ShardID == id {
					parent.Children = append(parent.Children[:i], parent.Children[i+1:]...)
					break
				}
			}
		}
		delete(nodeMap, id)
	}
	root.NodeCount = shardCounts[""]
	sortChildren(root)
	m.treeCache = root
	m.treeCacheTime = time.Now()
	m.treeDirty = false
	return root
}

func sortChildren(node *ShardTreeNode) {
	if len(node.Children) == 0 {
		return
	}
	sort.Slice(node.Children, func(i, j int) bool { return node.Children[i].ShardID < node.Children[j].ShardID })
	for _, child := range node.Children {
		sortChildren(child)
	}
}
