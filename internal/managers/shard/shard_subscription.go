package shard

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// JoinShard increments the reference count for a shard topic.
// If the topic is not currently subscribed, it joins and starts a read loop.
// If we're already in this shard as observer only (JoinShardAsObserver), we "promote" to full member: publish JOIN+HEARTBEAT and stop being observer.
func (sm *ShardManager) JoinShard(shardID string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sub, exists := sm.shardSubs[shardID]
	if exists {
		if sub.observerOnly {
			// Promote observer to full member: we're already subscribed, just announce so we count as a member.
			sub.observerOnly = false
			delete(sm.observerOnlyShards, shardID)
			sm.mu.Unlock()
			joinMsg := []byte("JOIN:" + sm.h.ID().String())
			_ = sub.topic.Publish(sm.ctx, joinMsg)
			pinnedCount := 0
			if sm.storageMgr != nil {
				pinnedCount = sm.storageMgr.GetPinnedCount()
			}
			heartbeatMsg := []byte(fmt.Sprintf("HEARTBEAT:%s:%d", sm.h.ID().String(), pinnedCount))
			_ = sub.topic.Publish(sm.ctx, heartbeatMsg)
			log.Printf("[Sharding] Promoted observer to full member in shard %s", shardID)
			sm.mu.Lock()
			return
		}
		sub.refCount++
		return
	}

	topicName := fmt.Sprintf("dlockss-creative-commons-shard-%s", shardID)
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
					return
				}
			}
			log.Printf("[Error] Failed to join shard topic %s: %v", topicName, err)
			return
		}
	}

	psSub, err := t.Subscribe()
	if err != nil {
		log.Printf("[Error] Failed to subscribe to shard topic %s: %v", topicName, err)
		return
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

	log.Printf("[Sharding] Joined shard %s (Topic: %s)", shardID, topicName)

	joinMsg := []byte("JOIN:" + sm.h.ID().String())
	_ = newSub.topic.Publish(sm.ctx, joinMsg)
	pinnedCount := 0
	if sm.storageMgr != nil {
		pinnedCount = sm.storageMgr.GetPinnedCount()
	}
	heartbeatMsg := []byte(fmt.Sprintf("HEARTBEAT:%s:%d", sm.h.ID().String(), pinnedCount))
	_ = newSub.topic.Publish(sm.ctx, heartbeatMsg)

	go sm.readLoop(ctx, newSub)
}

// JoinShardAsObserver subscribes to a shard topic without publishing JOIN/HEARTBEAT (peek only, like the monitor).
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

	topicName := fmt.Sprintf("dlockss-creative-commons-shard-%s", shardID)
	var t *pubsub.Topic
	if cached := sm.probeTopicCache[shardID]; cached != nil {
		t = cached
		delete(sm.probeTopicCache, shardID)
	} else {
		var err error
		t, err = sm.ps.Join(topicName)
		if err != nil {
			log.Printf("[Sharding] JoinShardAsObserver: failed to join topic %s: %v", topicName, err)
			return false
		}
	}

	psSub, err := t.Subscribe()
	if err != nil {
		log.Printf("[Sharding] JoinShardAsObserver: failed to subscribe to %s: %v", topicName, err)
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

	log.Printf("[Sharding] Joined shard %s as observer (peek only, no JOIN/HEARTBEAT)", shardID)
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
	sub.topic.Close()
	delete(sm.shardSubs, shardID)
	log.Printf("[Sharding] Left shard %s (observer)", shardID)
}

// LeaveShard decrements the reference count for a shard topic.
// If the count reaches zero, it unsubscribes and closes the topic.
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
		if !observerOnly {
			leaveMsg := []byte("LEAVE:" + sm.h.ID().String())
			_ = sub.topic.Publish(sm.ctx, leaveMsg)
			sm.mu.Unlock()
			time.Sleep(150 * time.Millisecond)
			sm.mu.Lock()
			sub, exists = sm.shardSubs[shardID]
			if !exists || sub.refCount > 0 {
				return
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
		if observerOnly {
			log.Printf("[Sharding] Left shard %s (observer)", shardID)
		} else {
			log.Printf("[Sharding] Left shard %s", shardID)
		}
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
