package monitor

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/pkg/schema"
)

// RegisterRoutes wires all API endpoints and the dashboard onto mux.
func (m *Monitor) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/nodes", m.handleNodes)
	mux.HandleFunc("/api/shard-tree", m.handleShardTree)
	mux.HandleFunc("/api/shard-nodes", m.handleShardNodes)
	mux.HandleFunc("/api/root-topic", m.handleRootTopic)
	mux.HandleFunc("/api/node-files", m.handleNodeFiles)
	mux.HandleFunc("/api/unique-cids", m.handleUniqueCIDs)
	mux.HandleFunc("/api/replication", m.handleReplication)
	mux.HandleFunc("/api/replication-cids", m.handleReplicationCIDs)
	mux.HandleFunc("/api/manifest-payload", m.handleManifestPayload)
	mux.HandleFunc("/api/identify", m.handleIdentify)
	mux.HandleFunc("/", m.handleDashboard)
}

type nodeSnap struct {
	id            string
	peerID        string
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

func (m *Monitor) snapshotNodes(filter func(id string, node *NodeState, shard string) bool) []nodeSnap {
	m.PruneStaleNodes()
	m.mu.RLock()
	defer m.mu.RUnlock()

	activeShardCounts := make(map[string]int)
	for id, node := range m.nodes {
		if !m.isDisplayableNodeUnlocked(id, node) {
			continue
		}
		shard := node.EffectiveShard()
		if node.Role != "PASSIVE" {
			activeShardCounts[shard]++
		}
	}

	var snapshot []nodeSnap
	for id, node := range m.nodes {
		if !m.isDisplayableNodeUnlocked(id, node) {
			continue
		}
		shard := node.EffectiveShard()
		if filter != nil && !filter(id, node, shard) {
			continue
		}
		peersInShard := activeShardCounts[shard]
		if peersInShard < 1 {
			peersInShard = 1
		}
		firstSeen := node.LastSeen
		if len(node.ShardHistory) > 0 {
			firstSeen = node.ShardHistory[0].FirstSeen
		}
		pinnedFiles := node.PinnedFiles
		if pinnedFiles < 0 {
			pinnedFiles = 0
		}
		role := node.Role
		if role == "" {
			role = "ACTIVE"
		}
		snapshot = append(snapshot, nodeSnap{
			id: id, peerID: node.PeerID, nodeName: node.NodeName, currentShard: node.CurrentShard, role: role, knownFiles: node.KnownFiles,
			lastSeen: node.LastSeen.Unix(), shard: shard, peersInShard: peersInShard, uptimeSeconds: time.Since(firstSeen).Seconds(), pinnedFiles: pinnedFiles,
		})
	}
	return snapshot
}

func (m *Monitor) buildNodeResponse(snapshot []nodeSnap) map[string]interface{} {
	response := make(map[string]interface{})
	for _, s := range snapshot {
		pinnedInShard := m.getPinnedInShardForNode(s.id, s.shard)
		status := StatusResponse{
			PeerID:        s.peerID,
			Version:       "1.0.0",
			CurrentShard:  s.currentShard,
			Role:          s.role,
			PeersInShard:  s.peersInShard,
			Storage:       StorageStatus{PinnedFiles: s.pinnedFiles, PinnedInShard: pinnedInShard, KnownFiles: s.knownFiles, KnownCIDs: []string{}},
			Replication:   ReplicationStatus{},
			UptimeSeconds: s.uptimeSeconds,
		}
		response[s.id] = map[string]interface{}{
			"data":      status,
			"last_seen": s.lastSeen,
			"node_name": s.nodeName,
		}
	}
	return response
}

func (m *Monitor) handleNodes(w http.ResponseWriter, r *http.Request) {
	query := strings.ToLower(r.URL.Query().Get("q"))
	snapshot := m.snapshotNodes(func(id string, node *NodeState, _ string) bool {
		if query == "" {
			return true
		}
		return strings.Contains(strings.ToLower(id), query) ||
			strings.Contains(strings.ToLower(node.CurrentShard), query) ||
			strings.Contains(strings.ToLower(node.NodeName), query)
	})
	response := m.buildNodeResponse(snapshot)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (m *Monitor) handleShardTree(w http.ResponseWriter, r *http.Request) {
	tree := m.GetShardTree()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tree)
}

func (m *Monitor) handleShardNodes(w http.ResponseWriter, r *http.Request) {
	shardFilter := r.URL.Query().Get("shard")
	snapshot := m.snapshotNodes(func(_ string, _ *NodeState, shard string) bool {
		return shard == shardFilter
	})
	response := m.buildNodeResponse(snapshot)
	shardLabel := shardFilter
	if shardLabel == "" {
		shardLabel = "root"
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"shard_id": shardFilter, "shard_label": shardLabel, "nodes": response, "count": len(response)})
}

func (m *Monitor) handleRootTopic(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method == http.MethodPost {
		var body struct {
			TopicPrefix string `json:"topic_prefix,omitempty"`
			TopicName   string `json:"topic_name,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeJSONError(w, `invalid JSON`, http.StatusBadRequest)
			return
		}
		if body.TopicPrefix != "" || body.TopicName == "" {
			m.SwitchTopicPrefix(r.Context(), body.TopicPrefix)
		}
		if body.TopicName != "" {
			m.SwitchTopic(r.Context(), body.TopicName)
		}
		m.writeRootTopicResponse(w)
		return
	}
	m.writeRootTopicResponse(w)
}

func (m *Monitor) writeRootTopicResponse(w http.ResponseWriter) {
	prefix := m.getTopicPrefix()
	topic := m.getTopicName()
	rootTopic := fmt.Sprintf("%s-%s-shard-", prefix, topic)
	json.NewEncoder(w).Encode(map[string]string{
		"root_topic":   rootTopic,
		"topic_prefix": prefix,
		"topic_name":   topic,
	})
}

func (m *Monitor) handleNodeFiles(w http.ResponseWriter, r *http.Request) {
	peerID := r.URL.Query().Get("peer")
	if peerID == "" {
		writeJSONError(w, "missing peer parameter", http.StatusBadRequest)
		return
	}
	m.mu.RLock()
	var entries []CIDEntry
	if files, ok := m.nodeFiles[peerID]; ok {
		entries = m.buildCIDEntriesUnlocked(files)
	} else {
		entries = []CIDEntry{}
	}
	m.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"peer_id": peerID, "cids": entries, "count": len(entries)})
}

func (m *Monitor) handleUniqueCIDs(w http.ResponseWriter, r *http.Request) {
	m.mu.RLock()
	entries := m.buildCIDEntriesUnlocked(m.uniqueCIDs)
	m.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"cids": entries, "count": len(entries)})
}

func (m *Monitor) handleReplication(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	dist, avg, atTarget := m.getReplicationStats()
	byShard := m.getReplicationByShard()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	json.NewEncoder(w).Encode(map[string]interface{}{
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
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"level": level, "cids": entries, "count": len(entries)})
}

func (m *Monitor) handleManifestPayload(w http.ResponseWriter, r *http.Request) {
	manifestCID := strings.TrimSpace(r.URL.Query().Get("cid"))
	if manifestCID == "" {
		writeJSONError(w, "missing cid parameter", http.StatusBadRequest)
		return
	}
	reqURL := "https://ipfs.io/ipfs/" + url.PathEscape(manifestCID)
	resp, err := http.Get(reqURL)
	if err != nil {
		writeJSONError(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		writeJSONError(w, "gateway: "+resp.Status, http.StatusBadGateway)
		return
	}
	block, err := io.ReadAll(resp.Body)
	if err != nil {
		writeJSONError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var ro schema.ResearchObject
	if err := ro.UnmarshalCBOR(block); err != nil {
		writeJSONError(w, "invalid manifest: "+err.Error(), http.StatusBadRequest)
		return
	}
	manifest := map[string]interface{}{
		"meta_ref":    ro.MetadataRef,
		"ingester_id": ro.IngestedBy.String(),
		"payload":     ro.Payload.String(),
		"size":        ro.TotalSize,
		"sig":         base64.StdEncoding.EncodeToString(ro.Signature),
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"payload_cid": ro.Payload.String(), "manifest": manifest})
}

func (m *Monitor) handleIdentify(w http.ResponseWriter, r *http.Request) {
	peerStr := strings.TrimSpace(r.URL.Query().Get("peer"))
	if peerStr == "" {
		writeJSONError(w, "missing peer parameter", http.StatusBadRequest)
		return
	}
	pid, err := peer.Decode(peerStr)
	if err != nil {
		writeJSONError(w, "invalid peer ID", http.StatusBadRequest)
		return
	}

	connectCtx, connectCancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer connectCancel()

	addrs := m.host.Peerstore().Addrs(pid)
	if len(addrs) > 0 {
		_ = m.host.Connect(connectCtx, peer.AddrInfo{ID: pid, Addrs: addrs})
	}

	connected := m.host.Network().Connectedness(pid) == network.Connected

	agentVersion, _ := m.host.Peerstore().Get(pid, "AgentVersion")
	protocolVersion, _ := m.host.Peerstore().Get(pid, "ProtocolVersion")
	protocols, _ := m.host.Peerstore().GetProtocols(pid)

	addrStrs := make([]string, 0, len(addrs))
	for _, a := range m.host.Peerstore().Addrs(pid) {
		addrStrs = append(addrStrs, a.String())
	}

	protoStrs := make([]string, 0, len(protocols))
	for _, p := range protocols {
		protoStrs = append(protoStrs, string(p))
	}

	region := m.resolveRegionFromAddrs(m.host.Peerstore().Addrs(pid))

	result := map[string]interface{}{
		"peer_id":          pid.String(),
		"agent_version":    fmt.Sprintf("%v", agentVersion),
		"protocol_version": fmt.Sprintf("%v", protocolVersion),
		"protocols":        protoStrs,
		"addresses":        addrStrs,
		"connected":        connected,
		"region":           region,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func (m *Monitor) handleDashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(dashboardHTML))
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
			m.mu.RLock()
			shardCounts := make(map[string]int)
			totalPinned := 0
			nodeCount := 0
			for id, node := range m.nodes {
				if !m.isDisplayableNodeUnlocked(id, node) {
					continue
				}
				nodeCount++
				shard := node.EffectiveShard()
				shardCounts[shard]++
				if node.PinnedFiles > 0 {
					totalPinned += node.PinnedFiles
				}
			}
			m.mu.RUnlock()
			shardIDs := make([]string, 0, len(shardCounts))
			for sid := range shardCounts {
				shardIDs = append(shardIDs, sid)
			}
			sort.Strings(shardIDs)
			parts := make([]string, 0, len(shardIDs))
			for _, sid := range shardIDs {
				parts = append(parts, fmt.Sprintf("%s: %d", shardLogLabel(sid), shardCounts[sid]))
			}
			slog.Info("status", "nodes", nodeCount, "shards", len(shardCounts), "pinned", totalPinned, "detail", strings.Join(parts, ", "))
		}
	}
}

// Close releases resources held by the monitor (GeoIP database, etc.).
func (m *Monitor) Close() {
	if m.geoDB != nil {
		m.geoDB.Close()
	}
}
