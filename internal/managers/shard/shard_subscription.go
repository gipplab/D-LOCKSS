package shard

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// JoinShard subscribes to the shard topic, or increments its reference count
// if already subscribed. An observer subscription is promoted to full membership.
func (sm *ShardManager) JoinShard(shardID string) error {
	sm.mu.Lock()

	if sub, exists := sm.shardSubs[shardID]; exists {
		if !sub.observerOnly {
			sub.refCount++
			sm.mu.Unlock()
			return nil
		}
		sub.observerOnly = false
		delete(sm.observerOnlyShards, shardID)
		topic := sub.topic
		sm.mu.Unlock()
		sm.announceMembership(topic)
		slog.Info("promoted observer to full member", "shard", shardID)
		return nil
	}

	topicName := sm.shardTopicName(shardID)
	t := sm.probeTopicCache[shardID]
	if t != nil {
		delete(sm.probeTopicCache, shardID)
	} else {
		var err error
		if t, err = sm.ps.Join(topicName); err != nil {
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
	sm.announceMembership(newSub.topic)
	go sm.readLoop(ctx, newSub)
	return nil
}

// JoinShardAsObserver subscribes to a shard topic without publishing JOIN or
// HEARTBEAT, the way the monitor peeks at a shard. It returns false if the
// node is already a full member of the shard or the subscription fails.
func (sm *ShardManager) JoinShardAsObserver(shardID string) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sub, exists := sm.shardSubs[shardID]; exists {
		if !sub.observerOnly {
			return false
		}
		sub.refCount++
		return true
	}

	topicName := sm.shardTopicName(shardID)
	var (
		t     *pubsub.Topic
		psSub *pubsub.Subscription
		err   error
	)
	if cached := sm.probeTopicCache[shardID]; cached != nil {
		delete(sm.probeTopicCache, shardID)
		t = cached
		psSub, err = t.Subscribe()
	} else if t, err = sm.ps.Join(topicName); err == nil {
		psSub, err = t.Subscribe()
	} else if strings.Contains(err.Error(), "topic already exists") {
		// probeShardSilently holds the topic but has not cached it yet.
		// ps.Subscribe returns a subscription on the existing topic; without a
		// topic handle, PublishToShardCBOR falls back to ps.Publish.
		if psSub, err = sm.ps.Subscribe(topicName); err != nil {
			slog.Error("observer join: failed to subscribe after topic exists", "topic", topicName, "error", err)
			return false
		}
	} else {
		slog.Error("observer join: failed to join topic", "topic", topicName, "error", err)
		return false
	}
	if err != nil {
		slog.Error("observer join: failed to subscribe", "topic", topicName, "error", err)
		return false
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

// LeaveShardAsObserver decrements the observer reference count for a shard
// and unsubscribes, without publishing LEAVE, when it reaches zero.
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
	sub.close()
	delete(sm.shardSubs, shardID)
	slog.Info("left shard", "shard", shardID, "observer", true)
}

// LeaveShard decrements the reference count and unsubscribes when it reaches
// zero. The topic handle goes to the probe cache for later probes and joins.
func (sm *ShardManager) LeaveShard(shardID string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sub, exists := sm.shardSubs[shardID]
	if !exists {
		return
	}
	sub.refCount--
	if sub.refCount > 0 {
		return
	}

	delete(sm.observerOnlyShards, shardID)
	if !sub.observerOnly {
		_ = sub.topic.Publish(sm.ctx, []byte(msgPrefixLeave+sm.h.ID().String()))
		// The lock is released while LEAVE goes out, so the subscription can
		// be removed, replaced or joined again in the meantime.
		sm.mu.Unlock()
		time.Sleep(150 * time.Millisecond)
		sm.mu.Lock()
		current, stillExists := sm.shardSubs[shardID]
		if !stillExists {
			sub.close()
			slog.Info("left shard, cleaned up after concurrent removal", "shard", shardID)
			return
		}
		if current != sub {
			sub.close()
			slog.Info("left shard, cleaned up replaced subscription", "shard", shardID)
			return
		}
		if current.refCount > 0 {
			return
		}
	}
	sub.cancel()
	sub.sub.Cancel()
	delete(sm.shardSubs, shardID)
	sm.cacheProbeTopicLocked(shardID, sub.topic)
	slog.Info("left shard", "shard", shardID, "observer", sub.observerOnly)
}

// cacheProbeTopicLocked keeps t for later probes and joins of shardID,
// evicting an arbitrary entry when the cache is full. sm.mu must be held.
func (sm *ShardManager) cacheProbeTopicLocked(shardID string, t *pubsub.Topic) {
	if old := sm.probeTopicCache[shardID]; old != nil && old != t {
		_ = old.Close()
	}
	if len(sm.probeTopicCache) >= maxProbeTopicCache && sm.probeTopicCache[shardID] == nil {
		for k, v := range sm.probeTopicCache {
			_ = v.Close()
			delete(sm.probeTopicCache, k)
			break
		}
	}
	sm.probeTopicCache[shardID] = t
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
