package monitor

import (
	"context"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/internal/keywords"
	"dlockss/internal/protocol"
)

//go:embed static/dashboard.html
var dashboardHTML []byte

const (
	webPort              = 8080
	manifestLookupTime   = 20 * time.Second
	identifyConnectTime  = 8 * time.Second
)

// Server is the monitor's HTTP frontend.
type Server struct {
	state   *State
	net     *Net
	indexer *keywords.Indexer
	srv     *http.Server

	topicPrefix string
	topicName   string
}

// NewServer wires the routes.
func NewServer(state *State, net *Net, indexer *keywords.Indexer, topicPrefix, topicName string) *Server {
	s := &Server{state: state, net: net, indexer: indexer, topicPrefix: topicPrefix, topicName: topicName}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/nodes", s.handleNodes)
	mux.HandleFunc("GET /api/shard-tree", s.handleShardTree)
	mux.HandleFunc("GET /api/shard-nodes", s.handleShardNodes)
	mux.HandleFunc("GET /api/root-topic", s.handleRootTopic)
	mux.HandleFunc("GET /api/node-files", s.handleNodeFiles)
	mux.HandleFunc("GET /api/unique-cids", s.handleUniqueCIDs)
	mux.HandleFunc("GET /api/payload-cids-export", s.handleExport)
	mux.HandleFunc("GET /api/replication", s.handleReplication)
	mux.HandleFunc("GET /api/replication-cids", s.handleReplicationCIDs)
	mux.HandleFunc("GET /api/manifest-payload", s.handleManifestPayload)
	mux.HandleFunc("GET /api/identify", s.handleIdentify)
	mux.HandleFunc("GET /api/keyword-search", s.handleKeywordSearch)
	mux.HandleFunc("GET /api/keyword-suggest", s.handleKeywordSuggest)
	mux.HandleFunc("GET /api/recent-searches", s.handleRecentSearches)
	mux.HandleFunc("GET /api/keyword-stats", s.handleKeywordStats)
	mux.HandleFunc("POST /api/keyword-api-key", s.handleKeywordAPIKey)
	mux.HandleFunc("/ipfs/", s.handleGateway)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(dashboardHTML)
	})
	s.srv = &http.Server{
		Addr:         fmt.Sprintf(":%d", webPort),
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 20 * time.Second,
		IdleTimeout:  120 * time.Second,
	}
	return s
}

// Run serves until ctx ends, then shuts down gracefully (5s).
func (s *Server) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() { errCh <- s.srv.ListenAndServe() }()
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.srv.Shutdown(shutdownCtx)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// ---- nodes ----

type storageInfo struct {
	PinnedFiles   int `json:"pinned_files"`
	PinnedInShard int `json:"pinned_in_shard"`
	KnownFiles    int `json:"known_files"`
}

type statusResponse struct {
	PeerID        string      `json:"peer_id"`
	Version       string      `json:"version"`
	CurrentShard  string      `json:"current_shard"`
	Role          string      `json:"role"`
	PeersInShard  int         `json:"peers_in_shard"`
	Storage       storageInfo `json:"storage"`
	UptimeSeconds int64       `json:"uptime_seconds"`
}

type nodeRow struct {
	Data     statusResponse `json:"data"`
	LastSeen int64          `json:"last_seen"`
	NodeName string         `json:"node_name"`
}

func (s *Server) nodeRows(filter string) map[string]nodeRow {
	nodes := s.state.Nodes()

	// Non-PASSIVE peers per shard (min 1 for display).
	perShard := map[string]int{}
	for _, n := range nodes {
		if n.Role != protocol.RolePassive {
			perShard[effectiveShard(n)]++
		}
	}

	filter = strings.ToLower(filter)
	out := map[string]nodeRow{}
	for _, n := range nodes {
		if filter != "" {
			hay := strings.ToLower(n.PeerID.String() + " " + effectiveShard(n) + " " + n.NodeName)
			if !strings.Contains(hay, filter) {
				continue
			}
		}
		pinned := n.PinnedFiles
		if pinned < 0 {
			pinned = 0
		}
		peers := perShard[effectiveShard(n)]
		if peers < 1 {
			peers = 1
		}
		uptime := int64(0)
		if len(n.History) > 0 {
			uptime = int64(time.Since(n.History[0].At).Seconds())
		}
		out[n.PeerID.String()] = nodeRow{
			Data: statusResponse{
				PeerID:       n.PeerID.String(),
				Version:      "1.0.0",
				CurrentShard: effectiveShard(n),
				Role:         string(n.Role),
				PeersInShard: peers,
				Storage: storageInfo{
					PinnedFiles:   pinned,
					PinnedInShard: s.state.PinnedInShard(n),
					KnownFiles:    len(s.state.NodeFiles(n.PeerID)),
				},
				UptimeSeconds: uptime,
			},
			LastSeen: n.LastSeen.Unix(),
			NodeName: n.NodeName,
		}
	}
	return out
}

func (s *Server) handleNodes(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, s.nodeRows(r.URL.Query().Get("q")))
}

func (s *Server) handleShardTree(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, s.state.ShardTree())
}

func (s *Server) handleShardNodes(w http.ResponseWriter, r *http.Request) {
	shardID := r.URL.Query().Get("shard")
	nodes := s.state.NodesInShard(shardID)
	rows := map[string]nodeRow{}
	all := s.nodeRows("")
	for _, n := range nodes {
		if row, ok := all[n.PeerID.String()]; ok {
			rows[n.PeerID.String()] = row
		}
	}
	writeJSON(w, map[string]any{
		"shard_id": shardID, "shard_label": shardLabel(shardID), "nodes": rows, "count": len(rows),
	})
}

func (s *Server) handleRootTopic(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]string{
		"root_topic":   s.topicPrefix + "-" + s.topicName + "-shard-",
		"topic_prefix": s.topicPrefix,
		"topic_name":   s.topicName,
	})
}

// ---- CIDs ----

type cidRow struct {
	CID      string `json:"cid"`
	Shard    string `json:"shard"`
	Replicas int    `json:"replicas"`
}

func (s *Server) cidRow(c string) cidRow {
	return cidRow{CID: c, Shard: shardLabel(s.state.ManifestShard(c)), Replicas: s.state.ReplicaCount(c)}
}

func (s *Server) handleNodeFiles(w http.ResponseWriter, r *http.Request) {
	peerStr := r.URL.Query().Get("peer")
	if peerStr == "" {
		writeError(w, http.StatusBadRequest, "missing peer parameter")
		return
	}
	p, err := peer.Decode(peerStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid peer ID")
		return
	}
	cids := s.state.NodeFiles(p)
	sort.Strings(cids)
	rows := make([]cidRow, 0, len(cids))
	for _, c := range cids {
		rows = append(rows, s.cidRow(c))
	}
	writeJSON(w, map[string]any{"peer_id": peerStr, "cids": rows, "count": len(rows)})
}

func (s *Server) handleUniqueCIDs(w http.ResponseWriter, r *http.Request) {
	cids := s.state.UniqueCIDs()
	sort.Strings(cids)
	rows := make([]cidRow, 0, len(cids))
	for _, c := range cids {
		rows = append(rows, s.cidRow(c))
	}
	writeJSON(w, map[string]any{"cids": rows, "count": len(rows)})
}

// handleExport downloads one CID per line: the payload CID where the keyword
// index knows it, else the manifest CID.
func (s *Server) handleExport(w http.ResponseWriter, r *http.Request) {
	cids := s.state.UniqueCIDs()
	sort.Strings(cids)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="payload_cids.txt"`)
	for _, c := range cids {
		line := c
		if s.indexer != nil {
			if p, ok := s.indexer.PayloadCIDOf(c); ok {
				line = p
			}
		}
		fmt.Fprintln(w, line)
	}
}

// ---- replication ----

func (s *Server) handleReplication(w http.ResponseWriter, r *http.Request) {
	stats := s.state.ReplicationStats()
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, map[string]any{
		"replication_distribution":  stats.Distribution,
		"avg_replication_level":     stats.AvgReplication,
		"files_at_target":           stats.FilesAtTarget,
		"files_at_target_per_shard": stats.PerShard,
		"replication_note": fmt.Sprintf("target: min(%d, shard size) copies; every shard member holds every file of its shard",
			replicationTarget),
	})
}

func (s *Server) handleReplicationCIDs(w http.ResponseWriter, r *http.Request) {
	levelStr := r.URL.Query().Get("level")
	level, err := strconv.Atoi(levelStr)
	if levelStr == "" || err != nil || level < 0 || level > maxReplicationLevel {
		writeError(w, http.StatusBadRequest, "level must be 0..10")
		return
	}
	cids := s.state.CIDsAtLevel(level)
	writeJSON(w, map[string]any{"level": level, "cids": cids, "count": len(cids)})
}

// ---- manifest and identify ----

func (s *Server) handleManifestPayload(w http.ResponseWriter, r *http.Request) {
	cidStr := r.URL.Query().Get("cid")
	if cidStr == "" {
		writeError(w, http.StatusBadRequest, "missing cid parameter")
		return
	}
	if s.indexer != nil {
		if e, ok := s.indexer.Store.Get(cidStr); ok {
			writeJSON(w, map[string]any{
				"payload_cid": e.PayloadCID,
				"manifest":    map[string]any{"meta_ref": e.MetaRef, "payload": e.PayloadCID},
			})
			return
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), manifestLookupTime)
	defer cancel()
	gateway := s.indexer.Settings.Gateway()
	ro, err := keywords.FetchManifest(ctx, gateway, cidStr)
	if err != nil {
		writeError(w, http.StatusBadGateway, "manifest fetch failed: "+err.Error())
		return
	}
	writeJSON(w, map[string]any{
		"payload_cid": ro.Payload.String(),
		"manifest": map[string]any{
			"meta_ref":    ro.MetadataRef,
			"ingester_id": ro.IngestedBy.String(),
			"payload":     ro.Payload.String(),
			"size":        ro.TotalSize,
			"sig":         base64.StdEncoding.EncodeToString(ro.Signature),
		},
	})
}

func (s *Server) handleIdentify(w http.ResponseWriter, r *http.Request) {
	peerStr := r.URL.Query().Get("peer")
	p, err := peer.Decode(peerStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid peer ID")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), identifyConnectTime)
	defer cancel()
	addrs := s.net.Host.Peerstore().Addrs(p)
	_ = s.net.Host.Connect(ctx, peer.AddrInfo{ID: p, Addrs: addrs})

	ps := s.net.Host.Peerstore()
	agent, _ := ps.Get(p, "AgentVersion")
	proto, _ := ps.Get(p, "ProtocolVersion")
	protos, _ := ps.GetProtocols(p)
	protoStrs := make([]string, 0, len(protos))
	for _, pr := range protos {
		protoStrs = append(protoStrs, string(pr))
	}
	addrStrs := []string{}
	for _, a := range ps.Addrs(p) {
		addrStrs = append(addrStrs, a.String())
	}
	writeJSON(w, map[string]any{
		"peer_id":          peerStr,
		"agent_version":    str(agent),
		"protocol_version": str(proto),
		"protocols":        protoStrs,
		"addresses":        addrStrs,
		"connected":        s.net.Host.Network().Connectedness(p).String() == "Connected",
	})
}

func str(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// ---- keywords ----

func (s *Server) handleKeywordSearch(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	entries := s.indexer.Store.Search(q)
	type result struct {
		*keywords.Entry
		Shard    string `json:"shard"`
		Replicas int    `json:"replicas"`
	}
	results := make([]result, 0, len(entries))
	for _, e := range entries {
		results = append(results, result{
			Entry:    e,
			Shard:    shardLabel(s.state.ManifestShard(e.ManifestCID)),
			Replicas: s.state.ReplicaCount(e.ManifestCID),
		})
	}
	s.indexer.Store.RecordSearch(q, len(results))
	writeJSON(w, map[string]any{"results": results, "count": len(results)})
}

func (s *Server) handleKeywordSuggest(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]any{"suggestions": s.indexer.Store.Suggest(r.URL.Query().Get("q"))})
}

func (s *Server) handleRecentSearches(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]any{"searches": s.indexer.Store.RecentSearches()})
}

func (s *Server) handleKeywordStats(w http.ResponseWriter, r *http.Request) {
	indexed, skipped, failed, labels := s.indexer.Store.Counts()
	used, limit := s.indexer.DailyQuota()
	provider, _, _, model, gateway := s.indexer.Settings.Snapshot()
	total := len(s.state.UniqueCIDs())
	pending := total - indexed - skipped - failed
	if pending < 0 {
		pending = 0
	}
	writeJSON(w, map[string]any{
		"total_cids":      total,
		"indexed":         indexed,
		"failed":          failed,
		"skipped":         skipped,
		"pending":         pending,
		"unique_keywords": labels,
		"daily_remaining": limit - used,
		"daily_limit":     limit,
		"enabled":         s.indexer.Settings.HasKey(),
		"can_set_key":     !s.indexer.Settings.HasKey(),
		"provider":        string(provider),
		"model":           model,
		"gateway":         gateway,
	})
}

func (s *Server) handleKeywordAPIKey(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Password string `json:"password"`
		Provider string `json:"provider"`
		APIKey   string `json:"api_key"`
		APIBase  string `json:"api_base"`
		Gateway  string `json:"gateway"`
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil || json.Unmarshal(raw, &body) != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if body.APIKey == "" && !s.indexer.Settings.HasKey() {
		writeError(w, http.StatusBadRequest, "an API key is required")
		return
	}
	if applyErr := s.indexer.ApplySettings(body.Password, body.Provider, body.APIKey, body.APIBase, body.Gateway); applyErr != nil {
		writeError(w, applyErr.Status, applyErr.Msg)
		return
	}
	provider, _, _, model, gateway := s.indexer.Settings.Snapshot()
	writeJSON(w, map[string]any{
		"enabled": true, "can_set_key": false,
		"provider": string(provider), "model": model, "gateway": gateway,
	})
}
