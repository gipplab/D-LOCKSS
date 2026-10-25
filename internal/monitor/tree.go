package monitor

import (
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	treeCacheTTL          = 5 * time.Second
	maxShardDepthForTree  = 8 // deeper nodes display truncated to this depth
)

// TreeNode is one shard in the displayed topology.
type TreeNode struct {
	ShardID   string      `json:"shard_id"`
	SplitTime *int64      `json:"split_time,omitempty"`
	NodeCount int         `json:"node_count"`
	Children  []*TreeNode `json:"children,omitempty"`
}

type treeCache struct {
	mu    sync.Mutex
	built time.Time
	root  *TreeNode
}

// ShardTree builds (and briefly caches) the shard topology: every shard with
// nodes, truncated to the display depth, plus all ancestors.
func (s *State) ShardTree() *TreeNode {
	s.tree.mu.Lock()
	defer s.tree.mu.Unlock()
	if s.tree.root != nil && time.Since(s.tree.built) < treeCacheTTL {
		return s.tree.root
	}

	counts := map[string]int{}
	s.mu.Lock()
	for _, n := range s.nodes {
		if !displayable(n) {
			continue
		}
		sh := effectiveShard(n)
		if len(sh) > maxShardDepthForTree {
			sh = sh[:maxShardDepthForTree]
		}
		counts[sh]++
	}
	splits := map[string]time.Time{}
	for _, ev := range s.splitEvents {
		if t, ok := splits[ev.Child]; !ok || ev.At.After(t) {
			splits[ev.Child] = ev.At
		}
	}
	s.mu.Unlock()

	// Include every ancestor of a populated shard.
	include := map[string]struct{}{"": {}}
	for sh := range counts {
		for i := 0; i <= len(sh); i++ {
			include[sh[:i]] = struct{}{}
		}
	}

	nodes := map[string]*TreeNode{}
	for sh := range include {
		tn := &TreeNode{ShardID: sh, NodeCount: counts[sh]}
		if t, ok := splits[sh]; ok {
			unix := t.Unix()
			tn.SplitTime = &unix
		}
		nodes[sh] = tn
	}
	for sh, tn := range nodes {
		if sh == "" {
			continue
		}
		parent := nodes[sh[:len(sh)-1]]
		if parent == nil {
			// Ancestors are always included, but be defensive.
			continue
		}
		parent.Children = append(parent.Children, tn)
	}
	for _, tn := range nodes {
		sort.Slice(tn.Children, func(i, j int) bool { return tn.Children[i].ShardID < tn.Children[j].ShardID })
	}

	s.tree.root = nodes[""]
	s.tree.built = time.Now()
	return s.tree.root
}

// ActiveShardCount counts distinct populated shards.
func (s *State) ActiveShardCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	set := map[string]struct{}{}
	for _, n := range s.nodes {
		if displayable(n) {
			set[effectiveShard(n)] = struct{}{}
		}
	}
	return len(set)
}

// NodesInShard returns displayable nodes whose effective shard equals (or is
// truncated-to) the given shard.
func (s *State) NodesInShard(shardID string) []*NodeState {
	s.Prune()
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*NodeState
	for _, n := range s.nodes {
		if !displayable(n) {
			continue
		}
		sh := effectiveShard(n)
		display := sh
		if len(display) > maxShardDepthForTree {
			display = display[:maxShardDepthForTree]
		}
		if sh == shardID || display == shardID || (shardID != "" && strings.HasPrefix(sh, shardID) && len(sh) > maxShardDepthForTree) {
			c := *n
			out = append(out, &c)
		}
	}
	return out
}
