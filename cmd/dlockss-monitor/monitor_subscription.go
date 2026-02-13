package main

import (
	"context"
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
	if _, exists := m.shardTopics[shardID]; exists {
		return
	}
	topicName := fmt.Sprintf("%s-creative-commons-shard-%s", config.PubsubTopicPrefix, shardID)
	topic, err := m.ps.Join(topicName)
	if err != nil {
		log.Printf("[Monitor] Failed to join shard topic %s: %v", topicName, err)
		return
	}
	sub, err := topic.Subscribe()
	if err != nil {
		log.Printf("[Monitor] Failed to subscribe to shard topic %s: %v", topicName, err)
		return
	}
	m.shardTopics[shardID] = topic
	go m.handleShardMessages(ctx, sub, shardID)
	log.Printf("[Monitor] Subscribed to shard topic: %s", shardID)
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
					pinnedCount := 0
					if len(parts) >= 3 {
						fmt.Sscanf(parts[2], "%d", &pinnedCount)
					}
					if shardID != "" {
						log.Printf("[Monitor] HEARTBEAT shard=%s author=%s pinned=%d", shardID, authorID.String(), pinnedCount)
					}
					m.handleHeartbeat(authorID, shardID, ip, -1)
					m.handleHeartbeat(authorID, shardID, ip, pinnedCount)
				} else {
					// HEARTBEAT prefix but no valid author (malformed or old format): count sender so we don't miss nodes.
					m.handleHeartbeat(senderID, shardID, ip, -1)
				}
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
				peerIDStr := strings.TrimSpace(string(msg.Data[5:]))
				if peerIDStr != "" {
					joinID, err := peer.Decode(peerIDStr)
					if err != nil {
						if mhBytes, mhErr := mh.FromB58String(peerIDStr); mhErr == nil {
							joinID, err = peer.IDFromBytes(mhBytes)
						}
					}
					if err == nil {
						m.handleHeartbeat(joinID, shardID, ip, -1)
						log.Printf("[Monitor] SHARD_JOIN peer=%s shard=%s", joinID.String(), shardLogLabel(shardID))
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
	ticker := time.NewTicker(5 * time.Second)
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
		if m.ps != nil {
			m.ensureShardSubscription(ctx, shardID)
		}
	}
	for shardID := range potentialChildren {
		if m.ps != nil {
			m.ensureShardSubscription(ctx, shardID)
		}
	}
}
