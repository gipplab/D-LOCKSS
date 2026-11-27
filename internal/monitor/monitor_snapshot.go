package monitor

import (
	"cmp"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	"dlockss/internal/keywords"
)

const (
	// snapshotTTL is how long one network view is served before it is
	// rebuilt; all viewers share it.
	snapshotTTL = 3 * time.Second
	// lostNodeTTL is how long a node that stopped sending messages is
	// listed as offline.
	lostNodeTTL = 24 * time.Hour
	// silentAfter marks a node whose last message is older as silent.
	// Heartbeats come every 10 s but gossip can delay them by 20-30 s.
	silentAfter = 90 * time.Second
)

// File health.
const (
	healthAtTarget = "at_target"
	healthBelow    = "below"
	healthCritical = "critical"
)

// Node status.
const (
	nodeOnline  = "online"
	nodeBehind  = "behind"
	nodeSilent  = "silent"
	nodeFull    = "full"
	nodeOffline = "offline"
)

// Indexer state of a file.
const (
	indexIndexed = "indexed"
	indexPending = "pending"
	indexSkipped = "skipped"
	indexFailed  = "failed"
)

// lostNode is a node the monitor stopped hearing from.
type lostNode struct {
	Name     string
	Shard    string
	LastSeen time.Time
}

// manifestView is one manifest of a file.
type manifestView struct {
	CID      string
	Ingester string // empty while the catalog does not know the manifest
	Holders  int    // nodes announcing it
}

// fileView is one file of the network: a payload and the manifests that
// point to it, one per ingester and user signature. While the catalog does
// not know a manifest's payload, the manifest stands for its own file.
type fileView struct {
	CID       string // payload CID, or the manifest CID while the payload is unknown
	Payload   string
	Manifests []manifestView // sorted by CID
	Copies    int
	Target    int
	Health    string
	Shard     string   // the shard that should hold the file
	Holders   []string // live nodes announcing any of its manifests
	Uploaders []string // keys whose signature verified
	Entry     *keywords.CIDKeywordEntry
	Index     string
	title     string // lower-case indexer title, for sorting
}

type nodeView struct {
	ID        string
	Name      string
	Helper    string
	Shard     string
	Role      string
	Status    string
	Held      int // files of its shard it announces
	Expected  int // files of its shard
	LastSeen  time.Time
	FirstSeen time.Time
}

type healthCounts struct {
	Total        int                          `json:"total"`
	AtTarget     int                          `json:"at_target"`
	Below        int                          `json:"below"`
	Critical     int                          `json:"critical"`
	Distribution [maxReplicationLevel + 1]int `json:"distribution"`
}

type indexCounts struct {
	Indexed int `json:"indexed"`
	Pending int `json:"pending"`
	Skipped int `json:"skipped"`
	Failed  int `json:"failed"`
}

// netSnapshot is the network view the API serves.
type netSnapshot struct {
	built     time.Time
	warmingUp bool
	target    int
	files     []*fileView // titled files first, by title, then by CID
	byCID     map[string]*fileView
	health    healthCounts
	nodes     []*nodeView // sorted by name, then ID
	nodeByID  map[string]*nodeView
	shards    int
	uploaders map[string][]*fileView // in the order of files
	index     indexCounts
}

// snapshot returns the current network view, rebuilding it if it is older
// than snapshotTTL.
func (m *Monitor) snapshot() *netSnapshot {
	m.snapMu.Lock()
	defer m.snapMu.Unlock()
	if m.snap == nil || time.Since(m.snap.built) >= snapshotTTL {
		m.snap = m.buildSnapshot(time.Now())
	}
	return m.snap
}

// fileGroup collects the manifests of one file.
type fileGroup struct {
	payload   string
	observed  string // deepest shard its manifests were announced in
	manifests []manifestView
	holders   map[string]bool
	uploaders map[string]bool
}

func (m *Monitor) buildSnapshot(now time.Time) *netSnapshot {
	// The indexer has its own lock; read it before taking m.mu.
	index := m.keywords.View()
	m.mu.RLock()
	rs := m.newReplicationSnapshotLocked()
	groups := m.groupManifestsLocked()
	nodes := m.nodeViewsLocked()
	m.mu.RUnlock()

	s := &netSnapshot{
		built:     now,
		warmingUp: now.Sub(m.started) < replicationAnnounceTTL,
		byCID:     make(map[string]*fileView),
		nodes:     nodes,
		nodeByID:  make(map[string]*nodeView),
		uploaders: make(map[string][]*fileView),
		shards:    len(rs.shardPeerCount),
	}
	held := make(map[string]int)
	expected := make(map[string]int)
	targets := make(map[int]int)
	for key, g := range groups {
		shardCounts := make(map[string]int)
		var holders []string
		for p := range g.holders {
			if shard, ok := rs.live[p]; ok {
				shardCounts[shard]++
				holders = append(holders, p)
			}
		}
		r := rs.resolveFile(g.payload, g.observed, shardCounts, len(g.holders))
		sort.Strings(holders)
		slices.SortFunc(g.manifests, func(a, b manifestView) int { return strings.Compare(a.CID, b.CID) })
		f := &fileView{
			CID:       key,
			Payload:   g.payload,
			Manifests: g.manifests,
			Copies:    r.count,
			Target:    minReplicas(r.maxRep),
			Shard:     r.targetShard,
			Holders:   holders,
			Uploaders: slices.Sorted(maps.Keys(g.uploaders)),
		}
		f.Health = fileHealth(f.Copies, f.Target)
		f.Entry, f.Index = indexState(f.Manifests, index)
		if f.Entry != nil {
			f.title = strings.ToLower(f.Entry.Title)
		}
		for _, p := range holders {
			if rs.live[p] == f.Shard {
				held[p]++
			}
		}
		expected[f.Shard]++
		targets[f.Target]++
		s.files = append(s.files, f)
	}
	sort.Slice(s.files, func(i, j int) bool { return byTitle(s.files[i], s.files[j]) })
	for _, f := range s.files {
		s.byCID[f.CID] = f
		for _, mv := range f.Manifests {
			s.byCID[mv.CID] = f
		}
		for _, u := range f.Uploaders {
			s.uploaders[u] = append(s.uploaders[u], f)
		}
		s.health.add(f)
		s.index.add(f.Index)
	}
	s.target = mostCommon(targets, replicationTarget)

	for _, n := range s.nodes {
		s.nodeByID[n.ID] = n
		if n.Status == nodeOffline {
			continue
		}
		n.Held, n.Expected = held[n.ID], expected[n.Shard]
		n.Status = nodeStatus(n, now, s.warmingUp)
	}
	return s
}

// groupManifestsLocked groups the announced manifests by payload.
func (m *Monitor) groupManifestsLocked() map[string]*fileGroup {
	groups := make(map[string]*fileGroup)
	for manifest := range m.uniqueCIDs {
		info, _ := m.catalog.get(manifest)
		if info.Legacy {
			continue
		}
		key := manifest
		if info.Payload != "" {
			key = info.Payload
		}
		g := groups[key]
		if g == nil {
			g = &fileGroup{payload: info.Payload, holders: make(map[string]bool), uploaders: make(map[string]bool)}
			groups[key] = g
		}
		peers := m.manifestReplication[manifest]
		g.manifests = append(g.manifests, manifestView{CID: manifest, Ingester: info.Ingester, Holders: len(peers)})
		for p := range peers {
			g.holders[p] = true
		}
		if observed := m.manifestShard[manifest]; len(observed) > len(g.observed) {
			g.observed = observed
		}
		if info.Uploader != "" {
			g.uploaders[info.Uploader] = true
		}
	}
	return groups
}

// nodeViewsLocked lists the nodes the monitor shows and, as offline, the
// nodes it stopped hearing from.
func (m *Monitor) nodeViewsLocked() []*nodeView {
	var out []*nodeView
	for id, node := range m.nodes {
		if !m.isDisplayableNodeLocked(id, node) {
			continue
		}
		nv := &nodeView{
			ID:        id,
			Name:      node.NodeName,
			Helper:    node.HelperID,
			Shard:     node.EffectiveShard(),
			Role:      cmp.Or(node.Role, "ACTIVE"),
			LastSeen:  node.LastSeen,
			FirstSeen: node.LastSeen,
		}
		if len(node.ShardHistory) > 0 {
			nv.FirstSeen = node.ShardHistory[0].FirstSeen
		}
		out = append(out, nv)
	}
	for id, lost := range m.lostNodes {
		if _, ok := m.nodes[id]; !ok {
			out = append(out, &nodeView{ID: id, Name: lost.Name, Shard: lost.Shard, Status: nodeOffline, LastSeen: lost.LastSeen, FirstSeen: lost.LastSeen})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := strings.ToLower(out[i].Name), strings.ToLower(out[j].Name)
		if a != b {
			return a < b
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// indexState returns the indexer's entry for a file and its index state.
// One indexed manifest is enough.
func indexState(manifests []manifestView, index keywords.IndexView) (*keywords.CIDKeywordEntry, string) {
	state := indexPending
	for _, mv := range manifests {
		if e := index.Entries[mv.CID]; e != nil {
			return e, indexIndexed
		}
		if index.Skipped[mv.CID] {
			state = indexSkipped
		} else if index.Failed[mv.CID] && state == indexPending {
			state = indexFailed
		}
	}
	return nil, state
}

// byTitle orders files with an indexer title first, by title, then by CID.
func byTitle(a, b *fileView) bool {
	if (a.title == "") != (b.title == "") {
		return a.title != ""
	}
	if a.title != b.title {
		return a.title < b.title
	}
	return a.CID < b.CID
}

// fileHealth places a file with copies of target copies. A file below its
// target with at most one copy left is critical.
func fileHealth(copies, target int) string {
	switch {
	case copies >= target:
		return healthAtTarget
	case copies <= 1:
		return healthCritical
	default:
		return healthBelow
	}
}

// nodeStatus tells how a node is doing. Right after the monitor starts,
// nodes have not re-announced all their files yet, so none is behind.
func nodeStatus(n *nodeView, now time.Time, warmingUp bool) string {
	switch {
	case now.Sub(n.LastSeen) > silentAfter:
		return nodeSilent
	case n.Role == "PASSIVE":
		return nodeFull
	case !warmingUp && isBehind(n.Held, n.Expected):
		return nodeBehind
	default:
		return nodeOnline
	}
}

// isBehind reports that a node lacks more than 1% of the files of its shard;
// a few files are always on their way after an ingest.
func isBehind(held, expected int) bool {
	return held < expected && expected-held > expected/100
}

func (h *healthCounts) add(f *fileView) {
	h.Total++
	h.Distribution[min(f.Copies, maxReplicationLevel)]++
	switch f.Health {
	case healthAtTarget:
		h.AtTarget++
	case healthBelow:
		h.Below++
	default:
		h.Critical++
	}
}

func (c *indexCounts) add(state string) {
	switch state {
	case indexIndexed:
		c.Indexed++
	case indexSkipped:
		c.Skipped++
	case indexFailed:
		c.Failed++
	default:
		c.Pending++
	}
}

func mostCommon(counts map[int]int, fallback int) int {
	best, bestCount := fallback, 0
	for v, c := range counts {
		if c > bestCount || (c == bestCount && v > best) {
			best, bestCount = v, c
		}
	}
	return best
}
