package monitor

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"dlockss/internal/keywords"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	defaultPageSize = 50
	maxPageSize     = 200
)

// RegisterRoutes wires all API endpoints and the dashboard onto mux.
func (m *Monitor) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/summary", m.handleSummary)
	mux.HandleFunc("/api/files", m.handleFiles)
	mux.HandleFunc("/api/file", m.handleFile)
	mux.HandleFunc("/api/manifest", m.handleManifest)
	mux.HandleFunc("/api/lookup", m.handleLookup)
	mux.HandleFunc("/api/collection", m.handleCollection)
	mux.HandleFunc("/api/shard-tree", m.handleShardTree)
	mux.HandleFunc("/api/payload-cids-export", m.handlePayloadCIDsExport)
	mux.HandleFunc("/api/identify", m.handleIdentify)
	mux.HandleFunc("/api/keyword-suggest", m.handleKeywordSuggest)
	mux.HandleFunc("/api/keyword-api-key", m.handleKeywordAPIKey)
	mux.HandleFunc("/ipfs/", m.handleIPFSGateway)
	mux.Handle("/static/", staticHandler())
	mux.HandleFunc("/", m.handleDashboard)
}

type nodeJSON struct {
	ID        string `json:"id"`
	Name      string `json:"name,omitempty"`
	Helper    string `json:"helper,omitempty"`
	Shard     string `json:"shard"`
	Role      string `json:"role,omitempty"`
	Status    string `json:"status"`
	Held      int    `json:"held"`
	Expected  int    `json:"expected"`
	LastSeen  int64  `json:"last_seen"`
	FirstSeen int64  `json:"first_seen"`
}

type indexJSON struct {
	indexCounts
	keywords.Stats
}

type summaryJSON struct {
	Topic     string       `json:"topic"`
	Prefix    string       `json:"prefix"`
	WarmingUp bool         `json:"warming_up"`
	Target    int          `json:"target"`
	Files     healthCounts `json:"files"`
	Nodes     []nodeJSON   `json:"nodes"`
	Shards    int          `json:"shards"`
	Index     indexJSON    `json:"index"`
}

// handleSummary serves everything the dashboard polls.
func (m *Monitor) handleSummary(w http.ResponseWriter, _ *http.Request) {
	s := m.snapshot()
	out := summaryJSON{
		Topic:     m.cfg.TopicName,
		Prefix:    m.cfg.PubsubTopicPrefix,
		WarmingUp: s.warmingUp,
		Target:    s.target,
		Files:     s.health,
		Nodes:     make([]nodeJSON, 0, len(s.nodes)),
		Shards:    s.shards,
		Index:     indexJSON{s.index, m.keywords.GetStats()},
	}
	for _, n := range s.nodes {
		out.Nodes = append(out.Nodes, nodeJSON{
			ID: n.ID, Name: n.Name, Helper: n.Helper, Shard: n.Shard, Role: n.Role, Status: n.Status,
			Held: n.Held, Expected: n.Expected, LastSeen: n.LastSeen.Unix(), FirstSeen: n.FirstSeen.Unix(),
		})
	}
	writeJSON(w, out)
}

type fileRow struct {
	CID       string `json:"cid"`
	Payload   string `json:"payload,omitempty"`
	Copies    int    `json:"copies"`
	Target    int    `json:"target"`
	Health    string `json:"health"`
	Uploaders int    `json:"uploaders"`
	Index     string `json:"index"`
	Title     string `json:"title,omitempty"`
	Field     string `json:"field,omitempty"`
	Sub       string `json:"sub,omitempty"`
}

func rowOf(f *fileView) fileRow {
	r := fileRow{
		CID: f.CID, Payload: f.Payload, Copies: f.Copies, Target: f.Target,
		Health: f.Health, Uploaders: len(f.Uploaders), Index: f.Index,
	}
	if f.Entry != nil {
		r.Title, r.Field, r.Sub = f.Entry.Title, f.Entry.BroadField, f.Entry.SubTopic
	}
	return r
}

// handleFiles lists files, a page at a time. Filters: health (at_target,
// below, critical), level (copies, 10 for 10 or more), node (files it holds),
// missing (files of its shard it lacks), key (files an uploader signed),
// q (indexer search), field and sub (indexer classification).
func (m *Monitor) handleFiles(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	s := m.snapshot()
	var matches map[string]bool
	if text := strings.TrimSpace(q.Get("q")); text != "" {
		matches = make(map[string]bool)
		for _, e := range m.keywords.Search(text) {
			matches[e.ManifestCID] = true
		}
	}
	files, err := s.filterFiles(q, matches)
	if err != nil {
		writeJSONError(w, err.Error(), http.StatusBadRequest)
		return
	}
	offset, limit := pageOf(q)
	page := []fileRow{}
	for i := offset; i < len(files) && i < offset+limit; i++ {
		page = append(page, rowOf(files[i]))
	}
	writeJSON(w, map[string]interface{}{"total": len(files), "offset": offset, "limit": limit, "files": page})
}

func pageOf(q url.Values) (offset, limit int) {
	offset, _ = strconv.Atoi(q.Get("offset"))
	limit, _ = strconv.Atoi(q.Get("limit"))
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = defaultPageSize
	}
	return offset, min(limit, maxPageSize)
}

// filterFiles returns the files matching the query: the most endangered
// first for health views, otherwise in the order of s.files. matches, if not
// nil, holds the manifests an indexer search found.
func (s *netSnapshot) filterFiles(q url.Values, matches map[string]bool) ([]*fileView, error) {
	level := -1
	if v := q.Get("level"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 || n > maxReplicationLevel {
			return nil, fmt.Errorf("level must be 0-%d", maxReplicationLevel)
		}
		level = n
	}
	health := q.Get("health")
	node, missing, key := q.Get("node"), q.Get("missing"), q.Get("key")
	field, sub := keywords.NormalizeLabel(q.Get("field")), keywords.NormalizeLabel(q.Get("sub"))
	var missingShard string
	if missing != "" {
		n, ok := s.nodeByID[missing]
		if !ok {
			return nil, fmt.Errorf("unknown node %s", missing)
		}
		missingShard = n.Shard
	}

	files := s.files
	if key != "" {
		files = s.uploaders[key]
	}
	inSearch := func(mv manifestView) bool { return matches[mv.CID] }
	var out []*fileView
	for _, f := range files {
		switch {
		case health != "" && f.Health != health,
			level >= 0 && min(f.Copies, maxReplicationLevel) != level,
			node != "" && !slices.Contains(f.Holders, node),
			missing != "" && (f.Shard != missingShard || slices.Contains(f.Holders, missing)),
			matches != nil && !slices.ContainsFunc(f.Manifests, inSearch),
			field != "" && (f.Entry == nil || keywords.NormalizeLabel(f.Entry.BroadField) != field),
			sub != "" && (f.Entry == nil || keywords.NormalizeLabel(f.Entry.SubTopic) != sub):
			continue
		}
		out = append(out, f)
	}
	if health != "" || level >= 0 || missing != "" {
		sort.SliceStable(out, func(i, j int) bool { return out[i].Copies < out[j].Copies })
	}
	return out, nil
}

type holderJSON struct {
	ID   string `json:"id"`
	Name string `json:"name,omitempty"`
}

type manifestJSON struct {
	CID          string `json:"cid"`
	Ingester     string `json:"ingester,omitempty"`
	IngesterName string `json:"ingester_name,omitempty"`
	Holders      int    `json:"holders"`
}

type fileDetail struct {
	fileRow
	Shard        string         `json:"shard"`
	Niche        string         `json:"niche,omitempty"`
	Keywords     []string       `json:"keywords,omitempty"`
	IndexedAt    int64          `json:"indexed_at,omitempty"`
	HolderList   []holderJSON   `json:"holder_list"`
	UploaderKeys []string       `json:"uploader_keys"`
	ManifestList []manifestJSON `json:"manifest_list"`
}

// handleFile serves one file, by its payload CID or one of its manifest CIDs.
func (m *Monitor) handleFile(w http.ResponseWriter, r *http.Request) {
	s := m.snapshot()
	f, ok := s.fileByCID(strings.TrimSpace(r.URL.Query().Get("cid")))
	if !ok {
		writeJSONError(w, "no such file", http.StatusNotFound)
		return
	}
	d := fileDetail{fileRow: rowOf(f), Shard: f.Shard, HolderList: []holderJSON{}, UploaderKeys: append([]string{}, f.Uploaders...), ManifestList: []manifestJSON{}}
	if f.Entry != nil {
		d.Niche, d.Keywords, d.IndexedAt = f.Entry.ResearchNiche, f.Entry.Keywords, f.Entry.IndexedAt.Unix()
	}
	for _, id := range f.Holders {
		h := holderJSON{ID: id}
		if n, ok := s.nodeByID[id]; ok {
			h.Name = n.Name
		}
		d.HolderList = append(d.HolderList, h)
	}
	for _, mv := range f.Manifests {
		mj := manifestJSON{CID: mv.CID, Ingester: mv.Ingester, Holders: mv.Holders}
		if n, ok := s.nodeByID[mj.Ingester]; ok {
			mj.IngesterName = n.Name
		}
		d.ManifestList = append(d.ManifestList, mj)
	}
	writeJSON(w, d)
}

// handleManifest serves a manifest of a known file as DAG-JSON, which the
// browser shows as JSON.
func (m *Monitor) handleManifest(w http.ResponseWriter, r *http.Request) {
	c := strings.TrimSpace(r.URL.Query().Get("cid"))
	f, ok := m.snapshot().byCID[c]
	if !ok || !slices.ContainsFunc(f.Manifests, func(mv manifestView) bool { return mv.CID == c }) {
		writeJSONError(w, "no such manifest", http.StatusNotFound)
		return
	}
	block, err := m.keywords.FetchManifestBlock(c)
	if err != nil {
		writeJSONError(w, "cannot read the manifest: "+err.Error(), http.StatusBadGateway)
		return
	}
	node, err := ipld.Decode(block, dagcbor.Decode)
	if err != nil {
		writeJSONError(w, "invalid manifest: "+err.Error(), http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Disposition", `inline; filename="`+c+`.json"`)
	if err := dagjson.Encode(node, w); err != nil {
		slog.Warn("cannot write manifest", "manifest", c, "error", err)
	}
}

// fileByCID finds a file by its payload or manifest CID, also when the CID
// is written in the other version (CIDv0 or CIDv1).
func (s *netSnapshot) fileByCID(text string) (*fileView, bool) {
	if f, ok := s.byCID[text]; ok {
		return f, true
	}
	c, err := cid.Decode(text)
	if err != nil {
		return nil, false
	}
	forms := []cid.Cid{c, cid.NewCidV1(c.Type(), c.Hash())}
	if c.Type() == cid.DagProtobuf {
		forms = append(forms, cid.NewCidV0(c.Hash()))
	}
	for _, form := range forms {
		if f, ok := s.byCID[form.String()]; ok {
			return f, true
		}
	}
	return nil, false
}

// handleLookup tells what a search term is: a file, a node, an uploader's
// key, or text for the indexer.
func (m *Monitor) handleLookup(w http.ResponseWriter, r *http.Request) {
	text := strings.TrimSpace(r.URL.Query().Get("q"))
	if text == "" {
		writeJSONError(w, "missing q parameter", http.StatusBadRequest)
		return
	}
	s := m.snapshot()
	if f, ok := s.fileByCID(text); ok {
		writeJSON(w, map[string]string{"kind": "file", "id": f.CID})
		return
	}
	if id, err := peer.Decode(text); err == nil {
		key := id.String()
		if _, ok := s.nodeByID[key]; ok {
			writeJSON(w, map[string]string{"kind": "node", "id": key})
			return
		}
		if _, ok := s.uploaders[key]; ok {
			writeJSON(w, map[string]string{"kind": "uploader", "id": key})
			return
		}
		writeJSON(w, map[string]string{"kind": "none", "message": "No node or uploader with this key."})
		return
	}
	if _, err := cid.Decode(text); err == nil {
		writeJSON(w, map[string]string{"kind": "none", "message": "No file with this CID."})
		return
	}
	writeJSON(w, map[string]string{"kind": "search", "id": text})
}

type facetJSON struct {
	Name  string      `json:"name"`
	Count int         `json:"count"`
	Subs  []facetJSON `json:"subs,omitempty"`
}

// handleCollection groups the indexed files by the indexer's broad field and
// subtopic. The labels are free text, so they are grouped case-insensitively.
func (m *Monitor) handleCollection(w http.ResponseWriter, _ *http.Request) {
	s := m.snapshot()
	fields := newFacets()
	subs := make(map[string]*facets)
	for _, f := range s.files {
		if f.Entry == nil {
			continue
		}
		field := fields.add(f.Entry.BroadField)
		if field == "" {
			continue
		}
		if subs[field] == nil {
			subs[field] = newFacets()
		}
		subs[field].add(f.Entry.SubTopic)
	}
	out := fields.list()
	for i := range out {
		out[i].Subs = subs[keywords.NormalizeLabel(out[i].Name)].list()
	}
	writeJSON(w, map[string]interface{}{"fields": out})
}

// facets counts labels case-insensitively and names each group by its most
// frequent spelling.
type facets struct {
	count     map[string]int
	spellings map[string]map[string]int
}

func newFacets() *facets {
	return &facets{count: make(map[string]int), spellings: make(map[string]map[string]int)}
}

// add counts label and returns its group, "" for an empty label.
func (f *facets) add(label string) string {
	label = strings.TrimSpace(label)
	key := keywords.NormalizeLabel(label)
	if key == "" {
		return ""
	}
	f.count[key]++
	if f.spellings[key] == nil {
		f.spellings[key] = make(map[string]int)
	}
	f.spellings[key][label]++
	return key
}

func (f *facets) list() []facetJSON {
	out := make([]facetJSON, 0, len(f.count))
	for key, n := range f.count {
		name, best := key, 0
		for spelling, c := range f.spellings[key] {
			if c > best || (c == best && spelling < name) {
				name, best = spelling, c
			}
		}
		out = append(out, facetJSON{Name: name, Count: n})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].Name < out[j].Name
	})
	return out
}

func (m *Monitor) handleShardTree(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, m.GetShardTree())
}

// handlePayloadCIDsExport serves the payload CIDs of the known files, one per
// line.
func (m *Monitor) handlePayloadCIDsExport(w http.ResponseWriter, _ *http.Request) {
	s := m.snapshot()
	var cids []string
	for _, f := range s.files {
		if f.Payload != "" {
			cids = append(cids, f.Payload)
		}
	}
	sort.Strings(cids)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="payload_cids.txt"`)
	for _, c := range cids {
		fmt.Fprintln(w, c)
	}
}

func (m *Monitor) handleIdentify(w http.ResponseWriter, r *http.Request) {
	peerIDStr := strings.TrimSpace(r.URL.Query().Get("peer"))
	if peerIDStr == "" {
		writeJSONError(w, "missing peer parameter", http.StatusBadRequest)
		return
	}
	pid, err := peer.Decode(peerIDStr)
	if err != nil {
		writeJSONError(w, "invalid peer ID", http.StatusBadRequest)
		return
	}

	// Identify dials the D-LOCKSS helper. The query may be a Kubo node ID.
	dialID := pid
	nodeID := peerIDStr
	m.mu.RLock()
	if ns, ok := m.nodes[peerIDStr]; ok {
		if helper, err := peer.Decode(ns.HelperID); err == nil {
			dialID = helper
		}
	} else if node, ok := m.helpers.Resolve(pid, nil); ok {
		nodeID = node.String()
	}
	helperID := dialID.String()
	m.mu.RUnlock()

	connectCtx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()

	pstore := m.host.Peerstore()
	if addrs := pstore.Addrs(dialID); len(addrs) > 0 {
		_ = m.host.Connect(connectCtx, peer.AddrInfo{ID: dialID, Addrs: addrs})
	}

	connected := m.host.Network().Connectedness(dialID) == network.Connected

	agentVersion, _ := pstore.Get(dialID, "AgentVersion")
	protocolVersion, _ := pstore.Get(dialID, "ProtocolVersion")
	protocols, _ := pstore.GetProtocols(dialID)

	addrs := pstore.Addrs(dialID)
	addrStrs := make([]string, 0, len(addrs))
	for _, a := range addrs {
		addrStrs = append(addrStrs, a.String())
	}

	protoStrs := make([]string, 0, len(protocols))
	for _, p := range protocols {
		protoStrs = append(protoStrs, string(p))
	}

	writeJSON(w, map[string]interface{}{
		"peer_id":          nodeID,
		"helper_id":        helperID,
		"agent_version":    fmt.Sprintf("%v", agentVersion),
		"protocol_version": fmt.Sprintf("%v", protocolVersion),
		"protocols":        protoStrs,
		"addresses":        addrStrs,
		"connected":        connected,
	})
}
