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

	if sm.shardSub != nil {
		sm.shardSub.Cancel()
		sm.mu.Unlock()

		select {
		case <-sm.shardDone:
		case <-time.After(2 * time.Second):
			log.Printf("[Warning] Timeout waiting for old shard subscription to close")
		}
		sm.mu.Lock()
	}

	topicName := fmt.Sprintf("dlockss-shard-%s", sm.currentShard)
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
	sm.mu.Unlock()

	log.Printf("[Sharding] Active Data Shard: %s (Topic: %s)", sm.currentShard, topicName)
}

func (sm *ShardManager) Run() {
	go sm.readControl()
	go sm.readShard()
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

		if !checkRateLimit(msg.GetFrom()) {
			incrementMetric(&metrics.messagesDropped)
			log.Printf("[RateLimit] Dropped DELEGATE message from %s (rate limit exceeded)", msg.GetFrom().String())
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
	defer sm.mu.Unlock()

	currentDepth := len(sm.currentShard)
	nextDepth := currentDepth + 1

	peerIDHash := getBinaryPrefix(sm.h.ID().String(), nextDepth)
	sm.currentShard = peerIDHash

	incrementMetric(&metrics.shardSplits)
	log.Printf("[Sharding] Split shard to depth %d: %s", nextDepth, sm.currentShard)

	sm.joinChannels()
}

func (sm *ShardManager) PublishToShard(msg string) {
	sm.mu.RLock()
	topic := sm.shardTopic
	sm.mu.RUnlock()

	if topic != nil {
		_ = topic.Publish(sm.ctx, []byte(msg))
	}
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
	if sm.controlSub != nil {
		sm.controlSub.Cancel()
	}
}
