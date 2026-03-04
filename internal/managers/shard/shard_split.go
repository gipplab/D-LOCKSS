package shard

import (
	"fmt"
	"log/slog"
)

// announceSplit publishes SPLIT:child0:child1 on the parent topic.
func (sm *ShardManager) announceSplit(parentShard string, targetChild string) {
	sibling := getSiblingShard(targetChild)
	sm.mu.RLock()
	sub, exists := sm.shardSubs[parentShard]
	sm.mu.RUnlock()
	if !exists || sub.topic == nil {
		return
	}
	msg := []byte(fmt.Sprintf("%s%s:%s", msgPrefixSplit, targetChild, sibling))
	_ = sub.topic.Publish(sm.ctx, msg)
	slog.Info("announced split", "shard", parentShard, "child0", targetChild, "child1", sibling)
}

// publishSplitToAncestor joins the ancestor shard as observer, publishes SPLIT:child0:child1, then leaves.
func (sm *ShardManager) publishSplitToAncestor(ancestorShard string) {
	child0, child1 := childShards(ancestorShard)
	if !sm.JoinShardAsObserver(ancestorShard) {
		return
	}
	defer sm.LeaveShardAsObserver(ancestorShard)

	msg := []byte(fmt.Sprintf("%s%s:%s", msgPrefixSplit, child0, child1))
	sm.mu.RLock()
	sub, exists := sm.shardSubs[ancestorShard]
	sm.mu.RUnlock()
	if exists && sub.topic != nil {
		_ = sub.topic.Publish(sm.ctx, msg)
	} else {
		topicName := sm.shardTopicName(ancestorShard)
		_ = sm.ps.Publish(topicName, msg)
	}
	slog.Info("re-broadcast split to ancestor", "ancestor", ancestorShard, "child0", child0, "child1", child1)
}

// rebroadcastSplitToAncestors publishes SPLIT to each ancestor shard (parent, grandparent, ..., root).
func (sm *ShardManager) rebroadcastSplitToAncestors() {
	sm.mu.RLock()
	currentShard := sm.currentShard
	sm.mu.RUnlock()
	if currentShard == "" {
		return
	}
	for ancestor := currentShard[:len(currentShard)-1]; ; {
		sm.publishSplitToAncestor(ancestor)
		if ancestor == "" {
			break
		}
		ancestor = ancestor[:len(ancestor)-1]
	}
}

// splitShard moves this node to its target child. For tests; normal path uses lifecycle.checkAndSplitIfNeeded.
func (sm *ShardManager) splitShard() {
	sm.lifecycle.splitShard()
}
