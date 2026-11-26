package monitor

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

// RegisterRoutes wires all API endpoints and the dashboard onto mux.
func (m *Monitor) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/nodes", m.handleNodes)
	mux.HandleFunc("/api/shard-tree", m.handleShardTree)
	mux.HandleFunc("/api/shard-nodes", m.handleShardNodes)
	mux.HandleFunc("/api/root-topic", m.handleRootTopic)
	mux.HandleFunc("/api/node-files", m.handleNodeFiles)
	mux.HandleFunc("/api/unique-cids", m.handleUniqueCIDs)
	mux.HandleFunc("/api/payload-cids-export", m.handlePayloadCIDsExport)
	mux.HandleFunc("/api/replication", m.handleReplication)
	mux.HandleFunc("/api/replication-cids", m.handleReplicationCIDs)
	mux.HandleFunc("/api/manifest-payload", m.handleManifestPayload)
	mux.HandleFunc("/api/uploaders", m.handleUploaders)
	mux.HandleFunc("/api/uploader-files", m.handleUploaderFiles)
	mux.HandleFunc("/api/identify", m.handleIdentify)
	mux.HandleFunc("/api/keyword-search", m.handleKeywordSearch)
	mux.HandleFunc("/api/keyword-suggest", m.handleKeywordSuggest)
	mux.HandleFunc("/api/recent-searches", m.handleRecentSearches)
	mux.HandleFunc("/api/keyword-stats", m.handleKeywordStats)
	mux.HandleFunc("/api/keyword-api-key", m.handleKeywordAPIKey)
	mux.HandleFunc("/ipfs/", m.handleIPFSGateway)
	mux.HandleFunc("/", m.handleDashboard)
}

type nodeSnap struct {
	id            string
	peerID        string
	helperID      string
	nodeName      string
	currentShard  string
	role          string
	knownFiles    int
	lastSeen      int64
	shard         string
	peersInShard  int
	uptimeSeconds float64
	pinnedFiles   int
}

func (m *Monitor) snapshotNodes(filter func(id string, node *nodeState, shard string) bool) []nodeSnap {
	m.PruneStaleNodes()
	m.mu.RLock()
	defer m.mu.RUnlock()

	activeShardCounts := make(map[string]int)
	for id, node := range m.nodes {
		if m.isDisplayableNodeLocked(id, node) && node.Role != "PASSIVE" {
			activeShardCounts[node.EffectiveShard()]++
		}
	}

	var snapshot []nodeSnap
	for id, node := range m.nodes {
		if !m.isDisplayableNodeLocked(id, node) {
			continue
		}
		shard := node.EffectiveShard()
		if filter != nil && !filter(id, node, shard) {
			continue
		}
		firstSeen := node.LastSeen
		if len(node.ShardHistory) > 0 {
			firstSeen = node.ShardHistory[0].FirstSeen
		}
		role := node.Role
		if role == "" {
			role = "ACTIVE"
		}
		snapshot = append(snapshot, nodeSnap{
			id:            id,
			peerID:        node.PeerID,
			helperID:      node.HelperID,
			nodeName:      node.NodeName,
			currentShard:  node.CurrentShard,
			role:          role,
			knownFiles:    node.KnownFiles,
			lastSeen:      node.LastSeen.Unix(),
			shard:         shard,
			peersInShard:  max(activeShardCounts[shard], 1),
			uptimeSeconds: time.Since(firstSeen).Seconds(),
			pinnedFiles:   max(node.PinnedFiles, 0),
		})
	}
	return snapshot
}

func (m *Monitor) buildNodeResponse(snapshot []nodeSnap) map[string]interface{} {
	response := make(map[string]interface{})
	for _, s := range snapshot {
		status := StatusResponse{
			PeerID:       s.peerID,
			Version:      "1.0.0",
			CurrentShard: s.currentShard,
			Role:         s.role,
			PeersInShard: s.peersInShard,
			Storage: StorageStatus{
				PinnedFiles:   s.pinnedFiles,
				PinnedInShard: m.getPinnedInShardForNode(s.id, s.shard),
				KnownFiles:    s.knownFiles,
			},
			UptimeSeconds: s.uptimeSeconds,
		}
		entry := map[string]interface{}{
			"data":      status,
			"last_seen": s.lastSeen,
			"node_name": s.nodeName,
		}
		if s.helperID != "" {
			entry["helper_id"] = s.helperID
		}
		response[s.id] = entry
	}
	return response
}

func (m *Monitor) handleNodes(w http.ResponseWriter, r *http.Request) {
	query := strings.ToLower(r.URL.Query().Get("q"))
	snapshot := m.snapshotNodes(func(id string, node *nodeState, _ string) bool {
		if query == "" {
			return true
		}
		return strings.Contains(strings.ToLower(id), query) ||
			strings.Contains(strings.ToLower(node.CurrentShard), query) ||
			strings.Contains(strings.ToLower(node.NodeName), query) ||
			strings.Contains(strings.ToLower(node.HelperID), query)
	})
	writeJSON(w, m.buildNodeResponse(snapshot))
}

func (m *Monitor) handleShardTree(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, m.GetShardTree())
}

func (m *Monitor) handleShardNodes(w http.ResponseWriter, r *http.Request) {
	shardFilter := r.URL.Query().Get("shard")
	snapshot := m.snapshotNodes(func(_ string, _ *nodeState, shard string) bool {
		return shard == shardFilter
	})
	response := m.buildNodeResponse(snapshot)
	writeJSON(w, map[string]interface{}{"shard_id": shardFilter, "shard_label": shardLabel(shardFilter), "nodes": response, "count": len(response)})
}

func (m *Monitor) handleRootTopic(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, map[string]string{
		"root_topic":   m.shardTopic(""),
		"topic_prefix": m.cfg.PubsubTopicPrefix,
		"topic_name":   m.cfg.TopicName,
	})
}

func (m *Monitor) handleNodeFiles(w http.ResponseWriter, r *http.Request) {
	peerID := r.URL.Query().Get("peer")
	if peerID == "" {
		writeJSONError(w, "missing peer parameter", http.StatusBadRequest)
		return
	}
	m.mu.RLock()
	entries := m.buildCIDEntriesLocked(m.nodeFiles[peerID])
	m.mu.RUnlock()
	writeJSON(w, map[string]interface{}{"peer_id": peerID, "cids": entries, "count": len(entries)})
}

func (m *Monitor) handleUniqueCIDs(w http.ResponseWriter, _ *http.Request) {
	m.mu.RLock()
	entries := m.buildCIDEntriesLocked(m.uniqueCIDs)
	m.mu.RUnlock()
	writeJSON(w, map[string]interface{}{"cids": entries, "count": len(entries)})
}

func (m *Monitor) handlePayloadCIDsExport(w http.ResponseWriter, _ *http.Request) {
	cids := m.UniqueCIDList()
	sort.Strings(cids)

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="payload_cids.txt"`)
	for _, c := range cids {
		fmt.Fprintln(w, c)
	}
}

func (m *Monitor) handleReplication(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	dist, avg, atTarget := m.getReplicationStats()
	byShard := m.getReplicationByShard()
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	writeJSON(w, map[string]interface{}{
		"replication_distribution":  dist,
		"avg_replication_level":     avg,
		"files_at_target":           atTarget,
		"files_at_target_per_shard": byShard,
		"replication_note":          "Counts are network-wide (all shards). Nodes unpin files that no longer belong to their shard after a split.",
	})
}

func (m *Monitor) handleReplicationCIDs(w http.ResponseWriter, r *http.Request) {
	levelStr := r.URL.Query().Get("level")
	if levelStr == "" {
		writeJSONError(w, "missing level parameter", http.StatusBadRequest)
		return
	}
	level, err := strconv.Atoi(levelStr)
	if err != nil || level < 0 || level > 10 {
		writeJSONError(w, "level must be 0-10", http.StatusBadRequest)
		return
	}
	entries := m.getReplicationCIDsByLevel(level)
	writeJSON(w, map[string]interface{}{"level": level, "cids": entries, "count": len(entries)})
}

func (m *Monitor) handleManifestPayload(w http.ResponseWriter, r *http.Request) {
	manifestCID := strings.TrimSpace(r.URL.Query().Get("cid"))
	if manifestCID == "" {
		writeJSONError(w, "missing cid parameter", http.StatusBadRequest)
		return
	}
	if e, ok := m.keywords.Lookup(manifestCID); ok {
		writeJSON(w, map[string]interface{}{
			"payload_cid": e.PayloadCID,
			"manifest": map[string]interface{}{
				"meta_ref": e.MetaRef,
				"payload":  e.PayloadCID,
			},
		})
		return
	}
	ro, err := m.fetchManifest(r.Context(), manifestCID)
	if err != nil {
		writeJSONError(w, err.Error(), http.StatusBadGateway)
		return
	}
	manifest := map[string]interface{}{
		"meta_ref":    ro.MetadataRef,
		"ingester_id": ro.IngestedBy.String(),
		"payload":     ro.Payload.String(),
		"size":        ro.TotalSize,
		"sig":         base64.StdEncoding.EncodeToString(ro.Signature),
	}
	if ro.Uploader != "" {
		manifest["uploader"] = ro.Uploader.String()
		manifest["uploader_sig"] = base64.StdEncoding.EncodeToString(ro.UploaderSig)
	}
	if len(ro.Uploaders) > 0 {
		uploaders := make([]map[string]string, len(ro.Uploaders))
		for i, u := range ro.Uploaders {
			uploaders[i] = map[string]string{"key": u.Key.String(), "sig": base64.StdEncoding.EncodeToString(u.Sig)}
		}
		manifest["uploaders"] = uploaders
	}
	writeJSON(w, map[string]interface{}{"payload_cid": ro.Payload.String(), "manifest": manifest})
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

func (m *Monitor) handleDashboard(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	io.WriteString(w, dashboardHTML)
}

// RunStatusLogger periodically logs a summary of active nodes and shards.
func (m *Monitor) RunStatusLogger(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.logStatus()
		}
	}
}

func (m *Monitor) logStatus() {
	m.mu.RLock()
	shardCounts := make(map[string]int)
	totalPinned := 0
	nodeCount := 0
	for id, node := range m.nodes {
		if !m.isDisplayableNodeLocked(id, node) {
			continue
		}
		nodeCount++
		shardCounts[node.EffectiveShard()]++
		totalPinned += max(node.PinnedFiles, 0)
	}
	m.mu.RUnlock()

	shardIDs := make([]string, 0, len(shardCounts))
	for sid := range shardCounts {
		shardIDs = append(shardIDs, sid)
	}
	sort.Strings(shardIDs)
	parts := make([]string, 0, len(shardIDs))
	for _, sid := range shardIDs {
		parts = append(parts, fmt.Sprintf("%s: %d", shardLabel(sid), shardCounts[sid]))
	}
	slog.Info("status", "nodes", nodeCount, "shards", len(shardCounts), "pinned", totalPinned, "detail", strings.Join(parts, ", "))
}
