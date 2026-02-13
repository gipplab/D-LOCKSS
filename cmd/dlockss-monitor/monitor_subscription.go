package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/ipfs/go-cid"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"

	"dlockss/internal/config"
	"dlockss/pkg/schema"
)

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
	if len(shardID) > MaxShardDepthForSubscription {
		return // Avoid subscribing to very deep shards (e.g. 16-bit IDs)
	}
	if _, exists := m.shardTopics[shardID]; exists {
		return
	}
	topicName := fmt.Sprintf("%s-creative-commons-shard-%s", m.getTopicPrefixUnlocked(), shardID)
	topic, err := m.ps.Join(topicName)
	if err != nil {
		if errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "context canceled") {
			return // Normal during shutdown; don't log
		}
		log.Printf("[Monitor] Failed to join shard topic %s: %v", topicName, err)
		return
	}
	sub, err := topic.Subscribe()
	if err != nil {
		log.Printf("[Monitor] Failed to subscribe to shard topic %s: %v", topicName, err)
		return
	}
	m.shardTopics[shardID] = topic
	// Publish PROBE so D-LOCKSS nodes know we're an observer (don't count us for split/replication)
	if m.host != nil {
		probeMsg := []byte("PROBE:" + m.host.ID().String())
		_ = topic.Publish(ctx, probeMsg)
	}
	go m.handleShardMessages(ctx, sub, shardID)
	log.Printf("[Monitor] Subscribed to shard topic: %s", shardLogLabel(shardID))
}

func (m *Monitor) handleShardMessages(ctx context.Context, sub *pubsub.Subscription, shardID string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := sub.Next(ctx)
			if err != nil {
				if ctx.Err() == nil {
					log.Printf("[Monitor] Error reading from shard %s: %v", shardID, err)
				}
				return
			}
			ip := ""
			senderID := msg.GetFrom()
			if m.host != nil && senderID != "" {
				for _, addr := range m.host.Peerstore().Addrs(senderID) {
					if ipVal, err := addr.ValueForProtocol(ma.P_IP4); err == nil {
						ip = ipVal
						break
					}
					if ipVal, err := addr.ValueForProtocol(ma.P_IP6); err == nil {
						ip = ipVal
						break
					}
				}
			}

			if len(msg.Data) > 0 && len(msg.Data) < 500 && string(msg.Data[:min(10, len(msg.Data))]) == "HEARTBEAT:" {
				parts := strings.SplitN(string(msg.Data), ":", 4)
				role := "ACTIVE"
				if len(parts) >= 4 {
					switch strings.ToUpper(parts[3]) {
					case "PASSIVE":
						role = "PASSIVE"
					case "PROBE":
						role = "PROBE"
					}
				}
				if len(parts) >= 2 && parts[1] != "" {
					authorID, err := peer.Decode(parts[1])
					if err != nil {
						if mhBytes, mhErr := mh.FromB58String(parts[1]); mhErr == nil {
							authorID, err = peer.IDFromBytes(mhBytes)
						}
						if err != nil {
							authorID = senderID
						}
					}
					pinnedCount := -1
					if len(parts) >= 3 {
						var n int
						if _, err := fmt.Sscanf(parts[2], "%d", &n); err == nil {
							pinnedCount = n
						}
					}
					if shardID != "" {
						log.Printf("[Monitor] HEARTBEAT shard=%s author=%s pinned=%d role=%s", shardID, authorID.String(), pinnedCount, role)
					}
					m.handleHeartbeatWithRole(authorID, shardID, ip, pinnedCount, role)
				} else {
					m.handleHeartbeat(senderID, shardID, ip, -1)
				}
				continue
			}

			if len(msg.Data) >= 6 && string(msg.Data[:6]) == "PROBE:" {
				// PROBE means "I'm observing this shard", not "I'm a member". Don't update
				// node state: that would incorrectly move nodes to the probed shard and hide
				// them (Role=PROBE). Node membership is tracked via HEARTBEAT and JOIN only.
				continue
			}

			if len(msg.Data) > 6 && string(msg.Data[:6]) == "LEAVE:" {
				peerIDStr := strings.TrimSpace(string(msg.Data[6:]))
				if peerIDStr != "" {
					leaveID, err := peer.Decode(peerIDStr)
					if err != nil {
						if mhBytes, mhErr := mh.FromB58String(peerIDStr); mhErr == nil {
							leaveID, err = peer.IDFromBytes(mhBytes)
						}
					}
					if err == nil {
						m.handleLeaveShard(leaveID, shardID)
						log.Printf("[Monitor] SHARD_LEAVE peer=%s shard=%s", leaveID.String(), shardLogLabel(shardID))
					}
				}
				continue
			}

			if len(msg.Data) > 5 && string(msg.Data[:5]) == "JOIN:" {
				parts := strings.SplitN(string(msg.Data[5:]), ":", 2)
				peerIDStr := strings.TrimSpace(parts[0])
				role := "ACTIVE"
				if len(parts) >= 2 && strings.ToUpper(parts[1]) == "PASSIVE" {
					role = "PASSIVE"
				}
				if peerIDStr != "" {
					joinID, err := peer.Decode(peerIDStr)
					if err != nil {
						if mhBytes, mhErr := mh.FromB58String(peerIDStr); mhErr == nil {
							joinID, err = peer.IDFromBytes(mhBytes)
						}
					}
					if err == nil {
						if m.handleHeartbeatWithRole(joinID, shardID, ip, -1, role) {
							log.Printf("[Monitor] SHARD_JOIN peer=%s shard=%s role=%s", joinID.String(), shardLogLabel(shardID), role)
						}
					}
				}
				continue
			}

			m.handleHeartbeat(senderID, shardID, ip, -1)
			if len(msg.Data) > 7 && string(msg.Data[:7]) == "PINNED:" {
				manifestCIDStr := string(msg.Data[7:])
				if manifestCID, err := cid.Decode(manifestCIDStr); err == nil {
					im := schema.IngestMessage{ManifestCID: manifestCID, ShardID: shardID}
					m.handleIngestMessage(&im, senderID, shardID, ip)
				}
				continue
			}
			var im schema.IngestMessage
			if err := im.UnmarshalCBOR(msg.Data); err == nil {
				targetShard := im.ShardID
				if m.ps != nil {
					m.mu.RLock()
					_, alreadySubscribed := m.shardTopics[targetShard]
					m.mu.RUnlock()
					if !alreadySubscribed {
						m.ensureShardSubscription(context.Background(), targetShard)
					}
				}
				authorID := im.SenderID
				if authorID == "" {
					authorID = senderID
				}
				m.handleIngestMessage(&im, authorID, targetShard, ip)
				m.ensureMinPinnedForPeer(authorID.String(), 1)
				m.handleHeartbeat(authorID, targetShard, ip, -1)
				continue
			}
		}
	}
}

func (m *Monitor) subscribeToActiveShards(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	// Run subscription pass once immediately so we don't wait 5s for first subscription update.
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
	m.mu.RLock()
	activeShards := make(map[string]bool)
	for _, node := range m.nodes {
		if node.CurrentShard != "" {
			activeShards[node.CurrentShard] = true
		} else {
			activeShards[""] = true
		}
		for _, entry := range node.ShardHistory {
			if time.Since(entry.FirstSeen) < 5*time.Minute {
				activeShards[entry.ShardID] = true
			}
		}
	}
	potentialChildren := make(map[string]bool)
	for shardID := range activeShards {
		child0, child1 := shardID+"0", shardID+"1"
		if shardID == "" {
			child0, child1 = "0", "1"
		}
		potentialChildren[child0] = true
		potentialChildren[child1] = true
	}
	for _, event := range m.splitEvents {
		potentialChildren[event.ParentShard] = true
		potentialChildren[event.ChildShard] = true
		if event.ParentShard == "" {
			potentialChildren["0"] = true
			potentialChildren["1"] = true
		}
	}
	m.mu.RUnlock()
	for shardID := range activeShards {
		if len(shardID) <= MaxShardDepthForSubscription && m.ps != nil {
			m.ensureShardSubscription(ctx, shardID)
		}
	}
	for shardID := range potentialChildren {
		if len(shardID) <= MaxShardDepthForSubscription && m.ps != nil {
			m.ensureShardSubscription(ctx, shardID)
		}
	}
}

// SwitchTopicPrefix changes the topic prefix and re-subscribes to the new network.
// Use this to investigate different D-LOCKSS versions without restarting the monitor.
// Pass "" to reset to the config default (DLOCKSS_PUBSUB_TOPIC_PREFIX).
func (m *Monitor) SwitchTopicPrefix(ctx context.Context, newPrefix string) {
	effectivePrefix := newPrefix
	if effectivePrefix == "" {
		effectivePrefix = config.PubsubTopicPrefix
	}
	m.mu.Lock()
	// Close all current topic subscriptions
	for shardID, topic := range m.shardTopics {
		_ = topic.Close()
		delete(m.shardTopics, shardID)
	}
	m.topicPrefixOverride = newPrefix // "" clears override, effectivePrefix used for subscribe
	// Clear state from old network
	m.nodes = make(map[string]*NodeState)
	m.splitEvents = m.splitEvents[:0]
	m.uniqueCIDs = make(map[string]time.Time)
	m.manifestReplication = make(map[string]map[string]time.Time)
	m.manifestShard = make(map[string]string)
	m.nodeFiles = make(map[string]map[string]time.Time)
	m.peerShardLastSeen = make(map[string]map[string]time.Time)
	m.treeCache = nil
	m.treeDirty = true
	m.mu.Unlock()

	// Re-subscribe to bootstrap shards with new prefix
	for _, shardID := range shardIDsUpToDepth(bootstrapShardDepth) {
		m.ensureShardSubscription(ctx, shardID)
	}
	log.Printf("[Monitor] Switched to topic prefix %q, subscribed to %d shards", effectivePrefix, 1<<(bootstrapShardDepth+1)-1)
}
