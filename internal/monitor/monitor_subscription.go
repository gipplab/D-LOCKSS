package monitor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"time"

	"github.com/ipfs/go-cid"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"

	"dlockss/pkg/schema"
)

// decodePeerIDWithFallback decodes a peer ID string, falling back to raw
// multihash decoding for legacy base58-encoded IDs.
func decodePeerIDWithFallback(s string) (peer.ID, error) {
	pid, err := peer.Decode(s)
	if err != nil {
		if mhBytes, mhErr := mh.FromB58String(s); mhErr == nil {
			pid, err = peer.IDFromBytes(mhBytes)
		}
	}
	return pid, err
}

// shardIDsUpToDepth returns all shard IDs in the binary tree up to the given depth.
// Depth 0: [""]. Depth 1: ["", "0", "1"]. Depth 2: ["", "0", "1", "00", "01", "10", "11"]. etc.
// This allows the monitor to bootstrap-subscribe to all potentially populated shards
// even when joining late (when nodes have already moved to deeper shards).
func shardIDsUpToDepth(depth int) []string {
	if depth < 0 {
		return nil
	}
	n := 1<<(depth+1) - 1
	out := make([]string, 0, n)
	out = append(out, "")
	for level := 1; level <= depth; level++ {
		for i := 0; i < 1<<level; i++ {
			var sb strings.Builder
			for b := level - 1; b >= 0; b-- {
				if (i>>b)&1 == 1 {
					sb.WriteByte('1')
				} else {
					sb.WriteByte('0')
				}
			}
			out = append(out, sb.String())
		}
	}
	return out
}

func (m *Monitor) ensureShardSubscription(ctx context.Context, shardID string) {
	if m.ps == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensureShardSubscriptionUnlocked(ctx, shardID)
}

func (m *Monitor) ensureShardSubscriptionUnlocked(ctx context.Context, shardID string) {
	if m.ps == nil {
		return
	}
	if len(shardID) > maxShardDepthForSubscription {
		return // Avoid subscribing to very deep shards (e.g. 16-bit IDs)
	}
	if _, exists := m.shardTopics[shardID]; exists {
		return
	}
	topicName := fmt.Sprintf("%s-%s-shard-%s", m.getTopicPrefixUnlocked(), m.getTopicNameUnlocked(), shardID)
	topic, err := m.ps.Join(topicName)
	if err != nil {
		if errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "context canceled") {
			return // Normal during shutdown; don't log
		}
		slog.Error("failed to join shard topic", "topic", topicName, "error", err)
		return
	}
	sub, err := topic.Subscribe()
	if err != nil {
		slog.Error("failed to subscribe to shard topic", "topic", topicName, "error", err)
		return
	}
	m.shardTopics[shardID] = &shardSub{topic: topic, sub: sub}
	// Publish PROBE so D-LOCKSS nodes know we're an observer (don't count us for split/replication)
	if m.host != nil {
		probeMsg := []byte("PROBE:" + m.host.ID().String())
		_ = topic.Publish(ctx, probeMsg)
	}
	go m.handleShardMessages(m.subCtx, sub, shardID, topicName)
	slog.Info("subscribed to shard topic", "shard", shardLabel(shardID))
}

// resolveIPFromPeer extracts the best public IP address for the given peer
// from the peerstore's known multiaddrs.
func (m *Monitor) resolveIPFromPeer(pid peer.ID) string {
	if m.host == nil || pid == "" {
		return ""
	}
	var ips []string
	for _, addr := range m.host.Peerstore().Addrs(pid) {
		if v, err := addr.ValueForProtocol(ma.P_IP4); err == nil {
			ips = append(ips, v)
		}
		if v, err := addr.ValueForProtocol(ma.P_IP6); err == nil {
			ips = append(ips, v)
		}
	}
	return preferPublicIP(ips)
}

// parseRole normalises a role string from a heartbeat/join message.
func parseRole(s string) string {
	switch strings.ToUpper(s) {
	case "PASSIVE":
		return "PASSIVE"
	case "PROBE":
		return "PROBE"
	case "REPLICATOR":
		return "REPLICATOR"
	default:
		return "ACTIVE"
	}
}

func (m *Monitor) handleShardMessages(ctx context.Context, sub *pubsub.Subscription, shardID string, expectedTopic string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msg, err := sub.Next(ctx)
		if err != nil {
			if ctx.Err() == nil {
				slog.Error("error reading from shard", "shard", shardID, "error", err)
			}
			return
		}
		if ctx.Err() != nil {
			return
		}
		if msg.GetTopic() != expectedTopic {
			slog.Warn("dropped cross-topic message", "expected", expectedTopic, "got", msg.GetTopic(), "shard", shardID)
			continue
		}

		senderID := msg.GetFrom()
		ip := m.resolveIPFromPeer(senderID)
		data := msg.Data

		switch {
		case hasPrefix(data, "HEARTBEAT:"):
			m.dispatchHeartbeat(ctx, data, senderID, shardID, ip)

		case hasPrefix(data, "PROBE:"):
			// Ignore probe messages from other monitors.

		case hasPrefix(data, "LEAVE:"):
			if peerStr := strings.TrimSpace(string(data[6:])); peerStr != "" {
				if leaveID, err := decodePeerIDWithFallback(peerStr); err == nil {
					m.handleLeaveShard(ctx, leaveID, shardID)
					slog.Info("shard leave", "peer", leaveID.String(), "shard", shardLabel(shardID))
				}
			}

		case hasPrefix(data, "JOIN:"):
			m.dispatchJoin(ctx, data[5:], senderID, shardID, ip)

		case hasPrefix(data, "PINNED:"):
			m.handleHeartbeatWithRole(ctx, senderID, shardID, ip, -1, "", "")
			if manifestCID, err := cid.Decode(string(data[7:])); err == nil {
				im := schema.IngestMessage{SignedEnvelope: schema.SignedEnvelope{ManifestCID: manifestCID}, ShardID: shardID}
				m.handleIngestMessage(ctx, &im, senderID, shardID, ip)
			}

		default:
			m.handleHeartbeatWithRole(ctx, senderID, shardID, ip, -1, "", "")
			var im schema.IngestMessage
			if err := im.UnmarshalCBOR(data); err == nil {
				m.dispatchIngestMessage(ctx, &im, senderID, shardID, ip)
			}
		}
	}
}

func hasPrefix(data []byte, prefix string) bool {
	return len(data) >= len(prefix) && string(data[:len(prefix)]) == prefix
}

// dispatchHeartbeat parses a HEARTBEAT:<peerID>:<pinned>:<role>:<name> message.
func (m *Monitor) dispatchHeartbeat(ctx context.Context, data []byte, senderID peer.ID, shardID, ip string) {
	if len(data) > 500 {
		return
	}
	parts := strings.SplitN(string(data), ":", 5)
	if len(parts) < 2 || parts[1] == "" {
		m.handleHeartbeatWithRole(ctx, senderID, shardID, ip, -1, "", "")
		return
	}

	authorID, err := decodePeerIDWithFallback(parts[1])
	if err != nil {
		authorID = senderID
	}
	pinnedCount := -1
	if len(parts) >= 3 {
		if n, err := fmt.Sscanf(parts[2], "%d", &pinnedCount); n == 0 || err != nil {
			pinnedCount = -1
		}
	}
	role := "ACTIVE"
	if len(parts) >= 4 {
		role = parseRole(parts[3])
	}
	var nodeName string
	if len(parts) >= 5 {
		nodeName = parts[4]
	}

	if shardID != "" {
		logLabel := authorID.String()
		if nodeName != "" {
			logLabel = nodeName + " (" + authorID.String() + ")"
		}
		slog.Info("heartbeat", "shard", shardID, "author", logLabel, "pinned", pinnedCount, "role", role)
	}
	m.handleHeartbeatWithRole(ctx, authorID, shardID, ip, pinnedCount, role, nodeName)
}

// dispatchJoin parses a JOIN:<peerID>:<role>:<name> message (data starts after "JOIN:").
func (m *Monitor) dispatchJoin(ctx context.Context, data []byte, senderID peer.ID, shardID, ip string) {
	parts := strings.SplitN(string(data), ":", 3)
	peerStr := strings.TrimSpace(parts[0])
	if peerStr == "" {
		return
	}
	joinID, err := decodePeerIDWithFallback(peerStr)
	if err != nil {
		return
	}

	role := "ACTIVE"
	if len(parts) >= 2 {
		role = parseRole(parts[1])
	}
	var nodeName string
	if len(parts) >= 3 {
		nodeName = parts[2]
	}

	if m.handleHeartbeatWithRole(ctx, joinID, shardID, ip, -1, role, nodeName) {
		logLabel := joinID.String()
		if nodeName != "" {
			logLabel = nodeName + " (" + joinID.String() + ")"
		}
		slog.Info("shard join", "peer", logLabel, "shard", shardLabel(shardID), "role", role)
	}
}

// dispatchIngestMessage handles a CBOR-encoded IngestMessage, subscribing to its
// target shard if needed.
func (m *Monitor) dispatchIngestMessage(ctx context.Context, im *schema.IngestMessage, senderID peer.ID, shardID, ip string) {
	targetShard := im.ShardID
	if m.ps != nil {
		m.mu.RLock()
		_, subscribed := m.shardTopics[targetShard]
		m.mu.RUnlock()
		if !subscribed {
			m.ensureShardSubscription(context.Background(), targetShard)
		}
	}
	authorID := im.SenderID
	if authorID == "" {
		authorID = senderID
	}
	m.handleIngestMessage(ctx, im, authorID, targetShard, ip)
	m.ensureMinPinnedForPeer(ctx, authorID.String(), 1)
	m.handleHeartbeatWithRole(ctx, authorID, targetShard, ip, -1, "", "")
}

// subscribeToActiveShards runs in the background and periodically subscribes
// to shards reported by active nodes plus their potential children (split targets).
func (m *Monitor) subscribeToActiveShards(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	m.subscribeToActiveShardsPass(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.subscribeToActiveShardsPass(ctx)
		}
	}
}

func (m *Monitor) subscribeToActiveShardsPass(ctx context.Context) {
	if m.ps == nil {
		return
	}
	targets := m.collectShardTargets()
	for shardID := range targets {
		if len(shardID) <= maxShardDepthForSubscription {
			m.ensureShardSubscription(ctx, shardID)
		}
	}
}

// collectShardTargets returns the set of shard IDs that should have
// active subscriptions: current node shards, recent history, plus their
// potential child shards (in case a split is imminent).
func (m *Monitor) collectShardTargets() map[string]bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	targets := make(map[string]bool)
	for _, node := range m.nodes {
		if node.CurrentShard != "" {
			targets[node.CurrentShard] = true
		} else {
			targets[""] = true
		}
		for _, entry := range node.ShardHistory {
			if time.Since(entry.FirstSeen) < 5*time.Minute {
				targets[entry.ShardID] = true
			}
		}
	}
	for _, event := range m.splitEvents {
		targets[event.ParentShard] = true
		targets[event.ChildShard] = true
		if event.ParentShard == "" {
			targets["0"] = true
			targets["1"] = true
		}
	}
	// Snapshot keys so we can mutate targets while iterating.
	existing := make([]string, 0, len(targets))
	for k := range targets {
		existing = append(existing, k)
	}
	for _, shardID := range existing {
		c0, c1 := shardID+"0", shardID+"1"
		if shardID == "" {
			c0, c1 = "0", "1"
		}
		targets[c0] = true
		targets[c1] = true
	}
	return targets
}

// closeAllShardSubsUnlocked tears down the current subscription generation:
// cancels the generation context (killing all goroutines immediately), then
// cancels each subscription and closes the underlying topic. A fresh
// generation context is created for subsequent subscriptions.
// Caller must hold m.mu.
func (m *Monitor) closeAllShardSubsUnlocked() {
	if m.subCancel != nil {
		m.subCancel()
	}
	for shardID, ss := range m.shardTopics {
		ss.sub.Cancel()
		_ = ss.topic.Close()
		delete(m.shardTopics, shardID)
	}
	m.subCtx, m.subCancel = context.WithCancel(m.appCtx)
}

// clearNodeStateUnlocked resets all per-network state maps so the monitor
// starts fresh after a topic switch. Caller must hold m.mu.
func (m *Monitor) clearNodeStateUnlocked() {
	m.nodes = make(map[string]*nodeState)
	m.splitEvents = m.splitEvents[:0]
	m.uniqueCIDs = make(map[string]time.Time)
	m.manifestReplication = make(map[string]map[string]time.Time)
	m.manifestShard = make(map[string]string)
	m.nodeFiles = make(map[string]map[string]time.Time)
	m.peerShardLastSeen = make(map[string]map[string]time.Time)
	m.treeCache = nil
	m.treeDirty = true
}

// resubscribeBootstrap subscribes to all bootstrap shards in the current topic.
func (m *Monitor) resubscribeBootstrap() {
	for _, shardID := range shardIDsUpToDepth(m.cfg.BootstrapShardDepth) {
		m.ensureShardSubscription(m.appCtx, shardID)
	}
}

// SwitchTopicPrefix changes the topic prefix (protocol version) and
// re-subscribes to the new network. Pass "" to reset to the config default.
func (m *Monitor) SwitchTopicPrefix(_ context.Context, newPrefix string) {
	effectivePrefix := newPrefix
	if effectivePrefix == "" {
		effectivePrefix = m.cfg.PubsubTopicPrefix
	}
	m.mu.Lock()
	m.closeAllShardSubsUnlocked()
	m.topicPrefixOverride = newPrefix
	m.clearNodeStateUnlocked()
	m.mu.Unlock()

	m.resubscribeBootstrap()
	slog.Info("switched topic prefix", "prefix", effectivePrefix, "shards", 1<<(m.cfg.BootstrapShardDepth+1)-1)
}

// SwitchTopic changes the archive topic name and re-subscribes to the new
// topic's shard tree. Pass "" to reset to the config default.
func (m *Monitor) SwitchTopic(_ context.Context, newTopic string) {
	effectiveTopic := newTopic
	if effectiveTopic == "" {
		effectiveTopic = m.cfg.TopicName
	}
	m.mu.Lock()
	m.closeAllShardSubsUnlocked()
	m.topicNameOverride = newTopic
	m.clearNodeStateUnlocked()
	m.mu.Unlock()

	m.resubscribeBootstrap()
	slog.Info("switched topic name", "topic", effectiveTopic, "shards", 1<<(m.cfg.BootstrapShardDepth+1)-1)
}

func isPrivateIP(ipStr string) bool {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return false
	}
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return true
	}
	privateIPBlocks := []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"}
	for _, cidr := range privateIPBlocks {
		_, block, _ := net.ParseCIDR(cidr)
		if block.Contains(ip) {
			return true
		}
	}
	return false
}

func preferPublicIP(ips []string) string {
	var fallback string
	for _, ip := range ips {
		if ip == "" {
			continue
		}
		if fallback == "" {
			fallback = ip
		}
		if !isPrivateIP(ip) {
			return ip
		}
	}
	return fallback
}
