package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"

	"dlockss/internal/common"
	"dlockss/internal/keywords"
	"dlockss/internal/testutil"
)

// snapRig builds monitor state directly: nodes, the manifests they
// announce, the catalog and the indexer.
type snapRig struct {
	t   *testing.T
	m   *Monitor
	now time.Time
}

func newSnapRig(t *testing.T) *snapRig {
	t.Helper()
	m := NewMonitor(DefaultConfig())
	now := time.Now()
	m.started = now.Add(-time.Hour)
	return &snapRig{t: t, m: m, now: now}
}

// node adds a live node in shard and returns its ID.
func (r *snapRig) node(name, shard string) string {
	r.t.Helper()
	id := testutil.MustPeerID(r.t, name).String()
	r.m.nodes[id] = &nodeState{
		PeerID:       id,
		NodeName:     name,
		CurrentShard: shard,
		LastSeen:     r.now,
		ShardHistory: []ShardHistoryEntry{{ShardID: shard, FirstSeen: r.now.Add(-time.Hour)}},
	}
	r.m.setPeerShardLastSeenLocked(id, shard, r.now)
	return id
}

// file records a manifest of payload, announced in shard by holders.
func (r *snapRig) file(manifest, payload, shard string, holders ...string) {
	r.m.uniqueCIDs[manifest] = r.now
	r.m.manifestShard[manifest] = shard
	if r.m.manifestReplication[manifest] == nil {
		r.m.manifestReplication[manifest] = map[string]time.Time{}
	}
	for _, h := range holders {
		r.m.manifestReplication[manifest][h] = r.now
	}
	if payload != "" {
		r.m.catalog.put(manifest, manifestInfo{Payload: payload, Ingester: "ingester"})
	}
}

func testCID(t *testing.T, label string) cid.Cid {
	t.Helper()
	h, err := mh.Sum([]byte(label), mh.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	return cid.NewCidV1(cid.DagProtobuf, h)
}

// payloadIn returns a payload CID whose shard at depth len(shard) is shard.
func payloadIn(t *testing.T, shard string) string {
	t.Helper()
	for i := 0; i < 10000; i++ {
		p := testCID(t, fmt.Sprintf("payload-%s-%d", shard, i)).String()
		if prefix, _ := common.GetHexBinaryPrefix(common.KeyToStableHex(p), len(shard)); prefix == shard {
			return p
		}
	}
	t.Fatalf("no payload in shard %s", shard)
	return ""
}

func get(t *testing.T, h http.HandlerFunc, target string, v any) {
	t.Helper()
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, target, nil))
	if err := json.Unmarshal(rec.Body.Bytes(), v); err != nil {
		t.Fatalf("%s: %v: %s", target, err, rec.Body)
	}
}

func TestSnapshotHealth(t *testing.T) {
	r := newSnapRig(t)
	var nodes []string
	for i := 1; i <= 5; i++ {
		nodes = append(nodes, r.node(fmt.Sprintf("pi%d", i), ""))
	}
	r.file("m-full", payloadIn(t, ""), "", nodes...)
	r.file("m-four", "", "", nodes[:4]...)
	r.file("m-one", "", "", nodes[0])
	r.file("m-none", "", "")

	s := r.m.buildSnapshot(r.now)
	if s.target != 5 {
		t.Fatalf("target = %d, want 5: MinPeersPerShard capped at the 5 nodes of the shard", s.target)
	}
	h := s.health
	if h.Total != 4 || h.AtTarget != 1 || h.Below != 1 || h.Critical != 2 {
		t.Fatalf("health = %+v, want 4 files: 1 at target, 1 below, 2 critical (1 copy and none)", h)
	}
	if h.Distribution[5] != 1 || h.Distribution[4] != 1 || h.Distribution[1] != 1 || h.Distribution[0] != 1 {
		t.Fatalf("distribution = %v", h.Distribution)
	}
}

func TestSnapshotNodeStatus(t *testing.T) {
	r := newSnapRig(t)
	pi := r.node("pi", "")
	lagging := r.node("wmcloud", "")
	quiet := r.node("quiet", "")
	r.m.nodes[quiet].LastSeen = r.now.Add(-2 * silentAfter)
	for i := 0; i < 3; i++ {
		holders := []string{pi, quiet}
		if i == 0 {
			holders = append(holders, lagging)
		}
		r.file(fmt.Sprintf("m%d", i), "", "", holders...)
	}

	s := r.m.buildSnapshot(r.now)
	if n := s.nodeByID[pi]; n.Status != nodeOnline || n.Held != 3 || n.Expected != 3 {
		t.Fatalf("pi = %+v", n)
	}
	if n := s.nodeByID[lagging]; n.Status != nodeBehind || n.Held != 1 || n.Expected != 3 {
		t.Fatalf("wmcloud = %+v, want behind with 1 of 3", n)
	}
	if n := s.nodeByID[quiet]; n.Status != nodeSilent {
		t.Fatalf("quiet = %+v, want silent", n)
	}

	r.m.started = r.now
	if n := r.m.buildSnapshot(r.now).nodeByID[lagging]; n.Status != nodeOnline {
		t.Fatalf("right after the monitor starts no node is behind, got %+v", n)
	}

	r.m.mu.Lock()
	r.m.forgetNodeLocked(lagging)
	r.m.mu.Unlock()
	if n := r.m.buildSnapshot(r.now).nodeByID[lagging]; n == nil || n.Status != nodeOffline || n.Name != "wmcloud" {
		t.Fatalf("a pruned node is listed as offline, got %+v", n)
	}
	r.m.handleHeartbeat(context.Background(), testutil.MustPeerID(t, "wmcloud"), "", "", 1, "", "wmcloud")
	if n := r.m.buildSnapshot(r.now).nodeByID[lagging]; n == nil || n.Status == nodeOffline {
		t.Fatalf("a node that is heard from again is no longer offline, got %+v", n)
	}
}

func TestSnapshotGroupsManifestsByPayload(t *testing.T) {
	r := newSnapRig(t)
	a, b := r.node("a", ""), r.node("b", "")
	payload := payloadIn(t, "")
	r.file("m1", payload, "", a)
	r.file("m2", payload, "", b)
	key1 := testutil.MustPeerID(t, "alice").String()
	key2 := testutil.MustPeerID(t, "bob").String()
	r.m.catalog.put("m1", manifestInfo{Payload: payload, Ingester: a, Uploader: key1})
	r.m.catalog.put("m2", manifestInfo{Payload: payload, Ingester: b, Uploader: key2})
	r.file("legacy", "", "", a)
	r.m.catalog.put("legacy", manifestInfo{Payload: "old", Legacy: true})

	s := r.m.buildSnapshot(r.now)
	if len(s.files) != 1 {
		t.Fatalf("files = %d, want one file for both manifests and none for the legacy one", len(s.files))
	}
	f := s.files[0]
	if f.CID != payload || f.Copies != 2 || len(f.Manifests) != 2 || len(f.Uploaders) != 2 {
		t.Fatalf("file = %+v, want both holders and both uploaders of the payload", f)
	}
	if s.byCID["m1"] != f || s.byCID["m2"] != f {
		t.Fatal("each manifest CID finds its file")
	}
	if len(s.uploaders[key1]) != 1 || len(s.uploaders[key2]) != 1 {
		t.Fatalf("uploaders = %v", s.uploaders)
	}
}

func TestSnapshotTargetShardFromPayload(t *testing.T) {
	r := newSnapRig(t)
	zero := []string{r.node("z1", "0"), r.node("z2", "0")}
	one := []string{r.node("o1", "1"), r.node("o2", "1")}
	payload := payloadIn(t, "1")
	// Announced in shard 0 by a node there, held by both nodes of shard 1.
	r.file("m", payload, "0", zero[0], one[0], one[1])

	f := r.m.buildSnapshot(r.now).byCID[payload]
	if f.Shard != "1" || f.Copies != 2 || f.Target != 2 {
		t.Fatalf("file = shard %q copies %d target %d, want shard 1 with its 2 nodes", f.Shard, f.Copies, f.Target)
	}
	s := r.m.buildSnapshot(r.now)
	if n := s.nodeByID[zero[0]]; n.Expected != 0 || n.Held != 0 {
		t.Fatalf("a node of shard 0 is not expected to hold a file of shard 1: %+v", n)
	}
}

// writeIndex stores indexer entries the way the indexer saves them.
func writeIndex(t *testing.T, entries map[string]*keywords.CIDKeywordEntry) *keywords.Store {
	t.Helper()
	dir := t.TempDir()
	data, err := json.Marshal(entries)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "keyword_index.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	return keywords.NewStore(keywords.Config{DataDir: dir})
}

// apiRig is a small network: three nodes in the root shard and three files.
func apiRig(t *testing.T) (*snapRig, []string, map[string]string) {
	t.Helper()
	r := newSnapRig(t)
	nodes := []string{r.node("pi1", ""), r.node("pi2", ""), r.node("wmcloud", "")}
	p := map[string]string{"math": payloadIn(t, ""), "bio": testCID(t, "bio").String(), "raw": testCID(t, "raw").String()}
	r.file("m-math", p["math"], "", nodes...)
	r.file("m-bio", p["bio"], "", nodes[0], nodes[1])
	r.file("m-raw", p["raw"], "", nodes[0])
	key := testutil.MustPeerID(t, "alice").String()
	r.m.catalog.put("m-bio", manifestInfo{Payload: p["bio"], Ingester: nodes[2], Uploader: key})
	r.m.SetKeywords(writeIndex(t, map[string]*keywords.CIDKeywordEntry{
		"m-math": {ManifestCID: "m-math", PayloadCID: p["math"], Title: "Prime gaps", BroadField: "Mathematics", SubTopic: "Number theory", Keywords: []string{"primes"}},
		"m-bio":  {ManifestCID: "m-bio", PayloadCID: p["bio"], Title: "Cell growth", BroadField: "mathematics ", SubTopic: "Biomath", Keywords: []string{"cells"}},
	}))
	p["key"] = key
	return r, nodes, p
}

func TestFilesFiltersAndPaging(t *testing.T) {
	r, nodes, p := apiRig(t)
	type page struct {
		Total int       `json:"total"`
		Files []fileRow `json:"files"`
	}
	var got page

	get(t, r.m.handleFiles, "/api/files", &got)
	if got.Total != 3 || got.Files[0].Title != "Cell growth" || got.Files[2].Title != "" {
		t.Fatalf("all files, titled first in order: %+v", got)
	}
	get(t, r.m.handleFiles, "/api/files?health=critical", &got)
	if got.Total != 1 || got.Files[0].CID != p["raw"] {
		t.Fatalf("critical = %+v", got)
	}
	get(t, r.m.handleFiles, "/api/files?missing="+nodes[2], &got)
	if got.Total != 2 || got.Files[0].Copies != 1 {
		t.Fatalf("files wmcloud lacks, most endangered first: %+v", got)
	}
	get(t, r.m.handleFiles, "/api/files?key="+p["key"], &got)
	if got.Total != 1 || got.Files[0].CID != p["bio"] {
		t.Fatalf("files of a key = %+v", got)
	}
	get(t, r.m.handleFiles, "/api/files?field=MATHEMATICS", &got)
	if got.Total != 2 {
		t.Fatalf("field matches case-insensitively: %+v", got)
	}
	get(t, r.m.handleFiles, "/api/files?q=primes", &got)
	if got.Total != 1 || got.Files[0].Title != "Prime gaps" {
		t.Fatalf("indexer search = %+v", got)
	}
	get(t, r.m.handleFiles, "/api/files?limit=1&offset=1", &got)
	if got.Total != 3 || len(got.Files) != 1 || got.Files[0].Title != "Prime gaps" {
		t.Fatalf("second page of one = %+v", got)
	}
	rec := httptest.NewRecorder()
	r.m.handleFiles(rec, httptest.NewRequest(http.MethodGet, "/api/files?level=11", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("level 11 = %d, want 400", rec.Code)
	}
}

func TestFileView(t *testing.T) {
	r, nodes, p := apiRig(t)
	var d fileDetail
	get(t, r.m.handleFile, "/api/file?cid=m-bio", &d)
	if d.CID != p["bio"] || d.Copies != 2 || d.Target != 3 || d.Health != healthBelow || d.Title != "Cell growth" {
		t.Fatalf("detail = %+v", d)
	}
	if len(d.HolderList) != 2 || d.HolderList[0].Name == "" || len(d.UploaderKeys) != 1 {
		t.Fatalf("holders %+v uploaders %v", d.HolderList, d.UploaderKeys)
	}
	if len(d.ManifestList) != 1 || d.ManifestList[0].Ingester != nodes[2] || d.ManifestList[0].IngesterName != "wmcloud" {
		t.Fatalf("manifests = %+v", d.ManifestList)
	}
	rec := httptest.NewRecorder()
	r.m.handleFile(rec, httptest.NewRequest(http.MethodGet, "/api/file?cid=nope", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown file = %d, want 404", rec.Code)
	}
}

func TestLookup(t *testing.T) {
	r, nodes, p := apiRig(t)
	cases := map[string]string{
		p["bio"]: "file",
		cid.NewCidV0(cid.MustParse(p["bio"]).Hash()).String(): "file",
		nodes[0]:                 "node",
		p["key"]:                 "uploader",
		testCID(t, "x").String(): "none",
		testutil.MustPeerID(t, "stranger").String(): "none",
		"number theory": "search",
	}
	for q, want := range cases {
		var got map[string]string
		get(t, r.m.handleLookup, "/api/lookup?q="+strings.ReplaceAll(q, " ", "+"), &got)
		if got["kind"] != want {
			t.Errorf("lookup %q = %v, want %s", q, got, want)
		}
	}
}

func TestCollection(t *testing.T) {
	r, _, _ := apiRig(t)
	var got struct {
		Fields []facetJSON `json:"fields"`
	}
	get(t, r.m.handleCollection, "/api/collection", &got)
	if len(got.Fields) != 1 || got.Fields[0].Count != 2 || len(got.Fields[0].Subs) != 2 {
		t.Fatalf("collection = %+v, want one field for both spellings of mathematics", got)
	}
}

func TestSummary(t *testing.T) {
	r, _, _ := apiRig(t)
	var got summaryJSON
	get(t, r.m.handleSummary, "/api/summary", &got)
	if got.Files.Total != 3 || got.Target != 3 || len(got.Nodes) != 3 || got.Shards != 1 {
		t.Fatalf("summary = %+v", got)
	}
	if got.Index.Indexed != 2 || got.Index.Pending != 1 || got.Index.SettingsEnabled {
		t.Fatalf("index = %+v", got.Index)
	}
}

func TestDashboardAssets(t *testing.T) {
	m := NewMonitor(DefaultConfig())
	mux := http.NewServeMux()
	m.RegisterRoutes(mux)
	for path, want := range map[string]int{"/": 200, "/static/app.js": 200, "/static/app.css": 200, "/nope": 404} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != want {
			t.Errorf("%s = %d, want %d", path, rec.Code, want)
		}
	}
}
