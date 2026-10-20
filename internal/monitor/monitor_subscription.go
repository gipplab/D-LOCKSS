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
// Depth 0: [""]. Depth 1: ["", "0", "1"]. Depth 2: ["", "0", "1", "00", "01", "10", "11"].
// This lets the monitor bootstrap-subscribe to all potentially populated shards
// even when it joins after nodes have already moved to deeper shards.
func shardIDsUpToDepth(depth int) []string {
	if depth < 0 {
		return nil
	}
	out := make([]string, 0, 1<<(depth+1)-1)
	out = append(out, "")
	for level := 1; level <= depth; level++ {
		for i := 0; i < 1<<level; i++ {
			out = append(out, fmt.Sprintf("%0*b", level, i))
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
	m.ensureShardSubscriptionLocked(ctx, shardID)
}

func (m *Monitor) ensureShardSubscriptionLocked(ctx context.Context, shardID string) {
	if m.ps == nil || len(shardID) > maxShardDepthForSubscription {
		return
	}
	if _, exists := m.shardTopics[shardID]; exists {
		return
	}
	topicName := m.shardTopic(shardID)
	topic, err := m.ps.Join(topicName)
	if err != nil {
		if errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "context canceled") {
			return // expected during shutdown
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
	// PROBE marks the monitor as an observer so nodes leave it out of split
	// and replication counts.
	if m.host != nil {
		_ = topic.Publish(ctx, []byte("PROBE:"+m.host.ID().String()))
	}
	go m.handleShardMessages(m.appCtx, sub, shardID, topicName)
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

// parseRole normalises a role string from a heartbeat or join message.
func parseRole(s string) string {
	switch role := strings.ToUpper(s); role {
	case "PASSIVE", "PROBE", "REPLICATOR":
		return role
	default:
		return "ACTIVE"
	}
}

func (m *Monitor) handleShardMessages(ctx context.Context, sub *pubsub.Subscription, shardID string, expectedTopic string) {
	for {
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
			if peerStr := strings.TrimSpace(string(data[len("LEAVE:"):])); peerStr != "" {
				if leaveID, err := decodePeerIDWithFallback(peerStr); err == nil {
					m.handleLeaveShard(ctx, leaveID, shardID)
					slog.Info("shard leave", "peer", leaveID.String(), "shard", shardLabel(shardID))
				}
			}

		case hasPrefix(data, "JOIN:"):
			m.dispatchJoin(ctx, data[len("JOIN:"):], senderID, shardID, ip)

		case hasPrefix(data, "PINNED:"):
			m.handleHeartbeat(ctx, senderID, shardID, ip, -1, "", "")
			if manifestCID, err := cid.Decode(string(data[len("PINNED:"):])); err == nil {
				im := schema.IngestMessage{SignedEnvelope: schema.SignedEnvelope{ManifestCID: manifestCID}, ShardID: shardID}
				m.handleIngestMessage(ctx, &im, senderID, shardID, ip)
			}

		default:
			m.handleHeartbeat(ctx, senderID, shardID, ip, -1, "", "")
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
		m.handleHeartbeat(ctx, senderID, shardID, ip, -1, "", "")
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
		slog.Info("heartbeat", "shard", shardID, "author", peerLabel(authorID.String(), nodeName), "pinned", pinnedCount, "role", role)
	}
	m.handleHeartbeat(ctx, authorID, shardID, ip, pinnedCount, role, nodeName)
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

	if m.handleHeartbeat(ctx, joinID, shardID, ip, -1, role, nodeName) {
		slog.Info("shard join", "peer", peerLabel(joinID.String(), nodeName), "shard", shardLabel(shardID), "role", role)
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
	m.handleHeartbeat(ctx, authorID, targetShard, ip, -1, "", "")
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
	for shardID := range m.collectShardTargets() {
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
		targets[node.CurrentShard] = true
		for _, entry := range node.ShardHistory {
			if time.Since(entry.FirstSeen) < 5*time.Minute {
				targets[entry.ShardID] = true
			}
		}
	}
	for _, event := range m.splitEvents {
		targets[event.ParentShard] = true
		targets[event.ChildShard] = true
	}
	// Children are added from a snapshot of the keys, so only one level is added.
	existing := make([]string, 0, len(targets))
	for k := range targets {
		existing = append(existing, k)
	}
	for _, shardID := range existing {
		targets[shardID+"0"] = true
		targets[shardID+"1"] = true
	}
	return targets
}

// resubscribeBootstrap subscribes to all bootstrap shards in the current topic.
func (m *Monitor) resubscribeBootstrap() {
	for _, shardID := range shardIDsUpToDepth(m.cfg.BootstrapShardDepth) {
		m.ensureShardSubscription(m.appCtx, shardID)
	}
}

func isPrivateIP(ipStr string) bool {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return false
	}
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return true
	}
	for _, cidr := range []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"} {
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
