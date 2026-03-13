package shard

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// JoinShard subscribes to the shard topic (or increments refcount). Promotes observer to full member if already subscribed.
func (sm *ShardManager) JoinShard(shardID string) error {
	sm.mu.Lock()

	sub, exists := sm.shardSubs[shardID]
	if exists {
		if sub.observerOnly {
			sub.observerOnly = false
			delete(sm.observerOnlyShards, shardID)
			topic := sub.topic
			sm.mu.Unlock()
			role := sm.getOurRole()
			joinMsg := []byte(msgPrefixJoin + sm.h.ID().String() + ":" + string(role) + ":" + sm.nodeName)
			_ = topic.Publish(sm.ctx, joinMsg)
			pinnedCount := 0
			if sm.storageMgr != nil {
				pinnedCount = sm.storageMgr.GetPinnedCount()
			}
			heartbeatMsg := []byte(fmt.Sprintf("%s%s:%d:%s:%s", msgPrefixHeartbeat, sm.h.ID().String(), pinnedCount, role, sm.nodeName))
			_ = topic.Publish(sm.ctx, heartbeatMsg)
			slog.Info("promoted observer to full member", "shard", shardID)
			return nil
		}
		sub.refCount++
		sm.mu.Unlock()
		return nil
	}

	topicName := sm.shardTopicName(shardID)
	var t *pubsub.Topic
	if cached := sm.probeTopicCache[shardID]; cached != nil {
		t = cached
		delete(sm.probeTopicCache, shardID)
	} else {
		var err error
		t, err = sm.ps.Join(topicName)
		if err != nil {
			if strings.Contains(err.Error(), "topic already exists") {
				if sub, exists := sm.shardSubs[shardID]; exists {
					sub.refCount++
					sm.mu.Unlock()
					return nil
				}
			}
			sm.mu.Unlock()
			return fmt.Errorf("join shard topic %s: %w", topicName, err)
		}
	}

	psSub, err := t.Subscribe()
	if err != nil {
		sm.mu.Unlock()
		return fmt.Errorf("subscribe to shard topic %s: %w", topicName, err)
	}

	ctx, cancel := context.WithCancel(sm.ctx)
	newSub := &shardSubscription{
		topic:    t,
		sub:      psSub,
		refCount: 1,
		cancel:   cancel,
		shardID:  shardID,
	}
	sm.shardSubs[shardID] = newSub
	sm.mu.Unlock()

	slog.Info("joined shard", "shard", shardID, "topic", topicName)

	role := sm.getOurRole()
	joinMsg := []byte(msgPrefixJoin + sm.h.ID().String() + ":" + string(role) + ":" + sm.nodeName)
	_ = newSub.topic.Publish(sm.ctx, joinMsg)
	pinnedCount := 0
	if sm.storageMgr != nil {
		pinnedCount = sm.storageMgr.GetPinnedCount()
	}
	heartbeatMsg := []byte(fmt.Sprintf("%s%s:%d:%s:%s", msgPrefixHeartbeat, sm.h.ID().String(), pinnedCount, role, sm.nodeName))
	_ = newSub.topic.Publish(sm.ctx, heartbeatMsg)

	go sm.readLoop(ctx, newSub)
	return nil
}

// JoinShardAsObserver subscribes to a shard topic without publishing JOIN/HEARTBEAT (peek only, like the monitor).
// When ps.Join returns "topic already exists" (race with probeShardSilently), use Subscribe to obtain the
// existing topic's subscription.
func (sm *ShardManager) JoinShardAsObserver(shardID string) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sub, exists := sm.shardSubs[shardID]
	if exists {
		if sub.observerOnly {
			sub.refCount++
			return true
		}
		return false
	}

	topicName := sm.shardTopicName(shardID)
	var t *pubsub.Topic
	var psSub *pubsub.Subscription
	if cached := sm.probeTopicCache[shardID]; cached != nil {
		t = cached
		delete(sm.probeTopicCache, shardID)
		var err error
		psSub, err = t.Subscribe()
		if err != nil {
			slog.Error("observer join: failed to subscribe", "topic", topicName, "error", err)
			return false
		}
	} else {
		var err error
		t, err = sm.ps.Join(topicName)
		if err != nil {
			if strings.Contains(err.Error(), "topic already exists") {
				// Race: probeShardSilently has the topic but hasn't cached it yet. Subscribe uses
				// tryJoin and returns the existing topic's subscription.
				psSub, err = sm.ps.Subscribe(topicName)
				if err != nil {
					slog.Error("observer join: failed to subscribe after topic exists", "topic", topicName, "error", err)
					return false
				}
				t = nil // no topic handle; use ps.Publish for publishing
			} else {
				slog.Error("observer join: failed to join topic", "topic", topicName, "error", err)
				return false
			}
		} else {
			psSub, err = t.Subscribe()
			if err != nil {
				slog.Error("observer join: failed to subscribe", "topic", topicName, "error", err)
				return false
			}
		}
	}

	ctx, cancel := context.WithCancel(sm.ctx)
	newSub := &shardSubscription{
		topic:        t,
		sub:          psSub,
		refCount:     1,
		cancel:       cancel,
		shardID:      shardID,
		observerOnly: true,
	}
	sm.shardSubs[shardID] = newSub
	sm.observerOnlyShards[shardID] = struct{}{}

	slog.Info("joined shard as observer", "shard", shardID)
	go sm.readLoop(ctx, newSub)
	return true
}

// LeaveShardAsObserver decrements the observer reference count for a shard; if zero, unsubscribes without publishing LEAVE.
func (sm *ShardManager) LeaveShardAsObserver(shardID string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sub, exists := sm.shardSubs[shardID]
	if !exists || !sub.observerOnly {
		return
	}
	sub.refCount--
	if sub.refCount > 0 {
		return
	}
	delete(sm.observerOnlyShards, shardID)
	sub.cancel()
	sub.sub.Cancel()
	if sub.topic != nil {
		sub.topic.Close()
	}
	delete(sm.shardSubs, shardID)
	slog.Info("left shard", "shard", shardID, "observer", true)
}

// LeaveShard decrements refcount; unsubscribes and closes topic when zero.
func (sm *ShardManager) LeaveShard(shardID string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sub, exists := sm.shardSubs[shardID]
	if !exists {
		return
	}

	sub.refCount--

	if sub.refCount <= 0 {
		observerOnly := sub.observerOnly
		delete(sm.observerOnlyShards, shardID)
		originalSub := sub // save pointer to detect replacement during sleep
		if !observerOnly {
			leaveMsg := []byte(msgPrefixLeave + sm.h.ID().String())
			_ = sub.topic.Publish(sm.ctx, leaveMsg)
			sm.mu.Unlock()
			time.Sleep(150 * time.Millisecond)
			sm.mu.Lock()
			currentSub, stillExists := sm.shardSubs[shardID]
			if !stillExists {
				// Subscription was removed by someone else; clean up our original.
				originalSub.cancel()
				originalSub.sub.Cancel()
				_ = originalSub.topic.Close()
				slog.Info("left shard, cleaned up after concurrent removal", "shard", shardID)
				return
			}
			if currentSub != originalSub {
				// A new subscription replaced ours; clean up original without touching the map.
				originalSub.cancel()
				originalSub.sub.Cancel()
				_ = originalSub.topic.Close()
				slog.Info("left shard, cleaned up replaced subscription", "shard", shardID)
				return
			}
			if currentSub.refCount > 0 {
				return // re-joined during sleep
			}
		}
		sub.cancel()
		sub.sub.Cancel()
		topic := sub.topic
		delete(sm.shardSubs, shardID)
		if old := sm.probeTopicCache[shardID]; old != nil {
			_ = old.Close()
		}
		const maxProbeCache = 4
		if len(sm.probeTopicCache) >= maxProbeCache && sm.probeTopicCache[shardID] == nil {
			for k, v := range sm.probeTopicCache {
				_ = v.Close()
				delete(sm.probeTopicCache, k)
				break
			}
		}
		sm.probeTopicCache[shardID] = topic
		slog.Info("left shard", "shard", shardID, "observer", observerOnly)
	}
}

func (sm *ShardManager) readLoop(ctx context.Context, sub *shardSubscription) {
	defer sub.sub.Cancel()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msg, err := sub.sub.Next(ctx)
		if err != nil {
			return
		}

		sm.processMessage(msg, sub.shardID)
	}
}
