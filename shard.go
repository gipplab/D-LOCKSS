package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
)

type ShardManager struct {
	ctx context.Context
	h   host.Host
	ps  *pubsub.PubSub

	mu           sync.RWMutex
	currentShard string
	shardTopic   *pubsub.Topic
	shardSub     *pubsub.Subscription

	oldShardName    string
	oldShardTopic   *pubsub.Topic
	oldShardSub     *pubsub.Subscription
	oldShardEndTime time.Time
	inOverlap       bool

	controlTopic *pubsub.Topic
	controlSub   *pubsub.Subscription

	msgCounter int

	shardDone chan struct{}
}

func NewShardManager(ctx context.Context, h host.Host, ps *pubsub.PubSub, startShard string) *ShardManager {
	sm := &ShardManager{
		ctx:          ctx,
		h:            h,
		ps:           ps,
		currentShard: startShard,
		shardDone:    make(chan struct{}, 1),
	}
	sm.joinChannels()
	return sm
}

func (sm *ShardManager) joinChannels() {
	sm.mu.Lock()

	if sm.controlTopic == nil {
		t, err := sm.ps.Join(ControlTopicName)
		if err != nil {
			log.Printf("[Error] Failed to join control topic %s: %v", ControlTopicName, err)
			sm.mu.Unlock()
			return
		}
		sm.controlTopic = t

		sub, err := t.Subscribe()
		if err != nil {
			log.Printf("[Error] Failed to subscribe to control topic: %v", err)
			sm.mu.Unlock()
			return
		}
		sm.controlSub = sub
		log.Printf("[System] Joined Control Channel: %s", ControlTopicName)
	}

	oldShardSub := sm.shardSub
	oldShardTopic := sm.shardTopic
	oldShardName := sm.oldShardName
	oldShardTopicName := ""
	if oldShardSub != nil && oldShardName != "" {
		oldShardTopicName = fmt.Sprintf("dlockss-creative-commons-shard-%s", oldShardName)
	}

	topicName := fmt.Sprintf("dlockss-creative-commons-shard-%s", sm.currentShard)
	t, err := sm.ps.Join(topicName)
	if err != nil {
		log.Printf("[Error] Failed to subscribe to shard topic %s: %v", topicName, err)
		sm.mu.Unlock()
		return
	}
	sm.shardTopic = t

	sub, err := t.Subscribe()
	if err != nil {
		log.Printf("[Error] Failed to subscribe to shard topic: %v", err)
		sm.mu.Unlock()
		return
	}
	sm.shardSub = sub
	sm.msgCounter = 0

	if oldShardSub != nil && oldShardTopicName != topicName && oldShardTopicName != "" {
		sm.oldShardSub = oldShardSub
		sm.oldShardTopic = oldShardTopic
		sm.oldShardEndTime = time.Now().Add(ShardOverlapDuration)
		sm.inOverlap = true
		sm.mu.Unlock()
		log.Printf("[Sharding] Entering overlap state: keeping old shard %s active until %v", oldShardTopicName, sm.oldShardEndTime)
		go sm.readOldShard()
	} else {
		if oldShardSub != nil {
			oldShardSub.Cancel()
			sm.mu.Unlock()
			select {
			case <-sm.shardDone:
			case <-time.After(2 * time.Second):
				log.Printf("[Warning] Timeout waiting for old shard subscription to close")
			}
		} else {
			sm.mu.Unlock()
		}
		sm.oldShardName = ""
	}

	log.Printf("[Sharding] Active Data Shard: %s (Topic: %s)", sm.currentShard, topicName)
}

func (sm *ShardManager) Run() {
	go sm.readControl()
	go sm.readShard()
	go sm.manageOverlap()
}

func (sm *ShardManager) readControl() {
	sub := sm.controlSub
	if sub == nil {
		return
	}

	for {
		select {
		case <-sm.ctx.Done():
			return
		default:
		}

		msg, err := sub.Next(sm.ctx)
		if err != nil {
			return
		}

		if msg.GetFrom() == sm.h.ID() {
			continue
		}

		data := string(msg.Data)
		if !strings.HasPrefix(data, "DELEGATE:") {
			continue
		}

		if !checkRateLimitForMessage(msg.GetFrom(), "DELEGATE") {
			incrementMetric(&metrics.messagesDropped)
			if !canAcceptCustodialFile() {
				usage := checkDiskUsage()
				log.Printf("[Storage] Dropped DELEGATE message from %s (disk usage high: %.1f%%)", msg.GetFrom().String(), usage)
			} else {
				log.Printf("[RateLimit] Dropped DELEGATE message from %s (rate limit exceeded)", msg.GetFrom().String())
			}
			continue
		}

		parts := strings.Split(data, ":")
		if len(parts) != 3 {
			log.Printf("[Warning] Invalid DELEGATE message format: %s", data)
			continue
		}

		targetHash := parts[1]
		targetPrefix := parts[2]

		if !validateHash(targetHash) {
			log.Printf("[Warning] Invalid hash in DELEGATE message: %s", targetHash)
			continue
		}

		sm.mu.RLock()
		myShard := sm.currentShard
		sm.mu.RUnlock()

		if targetPrefix == myShard {
			incrementMetric(&metrics.messagesReceived)
			log.Printf("[Delegate] Accepted delegation for %s (I am %s)", targetHash, myShard)
			addKnownFile(targetHash)
			select {
			case replicationWorkers <- struct{}{}:
				go func(h string) {
					defer func() { <-replicationWorkers }()
					checkReplication(sm.ctx, h)
				}(targetHash)
			case <-sm.ctx.Done():
				return
			}
		}
	}
}

func (sm *ShardManager) readShard() {
	defer func() {
		select {
		case sm.shardDone <- struct{}{}:
		default:
		}
	}()

	sub := sm.shardSub
	if sub == nil {
		return
	}

	for {
		select {
		case <-sm.ctx.Done():
			return
		default:
		}

		msg, err := sub.Next(sm.ctx)
		if err != nil {
			return
		}

		if msg.GetFrom() == sm.h.ID() {
			continue
		}

		if !checkRateLimit(msg.GetFrom()) {
			incrementMetric(&metrics.messagesDropped)
			continue
		}

		sm.mu.Lock()
		sm.msgCounter++
		shouldSplit := sm.msgCounter > MaxShardLoad
		if shouldSplit {
			sm.msgCounter = 0
		}
		sm.mu.Unlock()

		if shouldSplit {
			sm.splitShard()
		}

		data := string(msg.Data)
		if strings.HasPrefix(data, "NEED:") {
			hash := strings.TrimPrefix(data, "NEED:")
			if validateHash(hash) {
				incrementMetric(&metrics.messagesReceived)
				addKnownFile(hash)
				select {
				case replicationWorkers <- struct{}{}:
					go func(h string) {
						defer func() { <-replicationWorkers }()
						checkReplication(sm.ctx, h)
					}(hash)
				case <-sm.ctx.Done():
					return
				}
			}
		} else if strings.HasPrefix(data, "NEW:") {
			hash := strings.TrimPrefix(data, "NEW:")
			if validateHash(hash) {
				incrementMetric(&metrics.messagesReceived)
				addKnownFile(hash)
				select {
				case replicationWorkers <- struct{}{}:
					go func(h string) {
						defer func() { <-replicationWorkers }()
						checkReplication(sm.ctx, h)
					}(hash)
				case <-sm.ctx.Done():
					return
				}
			}
		}
	}
}

func (sm *ShardManager) splitShard() {
	sm.mu.Lock()

	currentDepth := len(sm.currentShard)
	nextDepth := currentDepth + 1

	oldShard := sm.currentShard
	peerIDHash := getBinaryPrefix(sm.h.ID().String(), nextDepth)
	sm.currentShard = peerIDHash
	sm.oldShardName = oldShard

	incrementMetric(&metrics.shardSplits)
	log.Printf("[Sharding] Split shard to depth %d: %s -> %s (entering overlap state)", nextDepth, oldShard, sm.currentShard)

	sm.mu.Unlock()
	sm.joinChannels()
}

func (sm *ShardManager) readOldShard() {
	sm.mu.RLock()
	sub := sm.oldShardSub
	endTime := sm.oldShardEndTime
	sm.mu.RUnlock()

	if sub == nil {
		return
	}

	for {
		select {
		case <-sm.ctx.Done():
			return
		default:
		}

		if time.Now().After(endTime) {
			sm.mu.Lock()
			if sm.oldShardSub != nil {
				log.Printf("[Sharding] Overlap period ended, closing old shard subscription")
				sm.oldShardSub.Cancel()
				sm.oldShardSub = nil
				sm.oldShardTopic = nil
				sm.inOverlap = false
			}
			sm.mu.Unlock()
			return
		}

		ctx, cancel := context.WithDeadline(sm.ctx, endTime)
		msg, err := sub.Next(ctx)
		cancel()

		if err != nil {
			if err == context.DeadlineExceeded {
				continue
			}
			return
		}

		if msg.GetFrom() == sm.h.ID() {
			continue
		}

		if !checkRateLimit(msg.GetFrom()) {
			incrementMetric(&metrics.messagesDropped)
			continue
		}

		data := string(msg.Data)
		if strings.HasPrefix(data, "NEED:") {
			hash := strings.TrimPrefix(data, "NEED:")
			if validateHash(hash) {
				incrementMetric(&metrics.messagesReceived)
				log.Printf("[Sharding] Received NEED message from old shard during overlap: %s", hash[:16]+"...")
				addKnownFile(hash)
				select {
				case replicationWorkers <- struct{}{}:
					go func(h string) {
						defer func() { <-replicationWorkers }()
						checkReplication(sm.ctx, h)
					}(hash)
				case <-sm.ctx.Done():
					return
				}
			}
		} else if strings.HasPrefix(data, "NEW:") {
			hash := strings.TrimPrefix(data, "NEW:")
			if validateHash(hash) {
				incrementMetric(&metrics.messagesReceived)
				log.Printf("[Sharding] Received NEW message from old shard during overlap: %s", hash[:16]+"...")
				addKnownFile(hash)
				select {
				case replicationWorkers <- struct{}{}:
					go func(h string) {
						defer func() { <-replicationWorkers }()
						checkReplication(sm.ctx, h)
					}(hash)
				case <-sm.ctx.Done():
					return
				}
			}
		}
	}
}

func (sm *ShardManager) manageOverlap() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-sm.ctx.Done():
			return
		case <-ticker.C:
			sm.mu.RLock()
			inOverlap := sm.inOverlap
			endTime := sm.oldShardEndTime
			sm.mu.RUnlock()

			if inOverlap && time.Now().After(endTime) {
				sm.mu.Lock()
				if sm.oldShardSub != nil {
					log.Printf("[Sharding] Overlap period expired, closing old shard subscription")
					sm.oldShardSub.Cancel()
					sm.oldShardSub = nil
					sm.oldShardTopic = nil
					sm.oldShardName = ""
					sm.inOverlap = false
				}
				sm.mu.Unlock()
			}
		}
	}
}

func (sm *ShardManager) PublishToShard(msg string) {
	sm.mu.RLock()
	topic := sm.shardTopic
	inOverlap := sm.inOverlap
	oldTopic := sm.oldShardTopic
	sm.mu.RUnlock()

	if topic != nil {
		_ = topic.Publish(sm.ctx, []byte(msg))
	}

	if inOverlap && oldTopic != nil {
		_ = oldTopic.Publish(sm.ctx, []byte(msg))
		log.Printf("[Sharding] Published to both shards during overlap: %s", msg[:min(20, len(msg))])
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (sm *ShardManager) PublishToControl(msg string) {
	sm.mu.RLock()
	topic := sm.controlTopic
	sm.mu.RUnlock()

	if topic != nil {
		_ = topic.Publish(sm.ctx, []byte(msg))
	}
}

func (sm *ShardManager) AmIResponsibleFor(hash string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	prefix := getHexBinaryPrefix(hash, len(sm.currentShard))
	return prefix == sm.currentShard
}

func (sm *ShardManager) Close() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.shardSub != nil {
		sm.shardSub.Cancel()
	}
	if sm.oldShardSub != nil {
		sm.oldShardSub.Cancel()
	}
	if sm.controlSub != nil {
		sm.controlSub.Cancel()
	}
}
