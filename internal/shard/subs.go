package shard

import (
	"context"
	"log/slog"
	"sync"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// leaveFlushDelay gives a final LEAVE time to reach the mesh before the
// subscription closes.
const leaveFlushDelay = 150 * time.Millisecond

// subs manages the node's shard-topic subscriptions. A shard can be held as
// a full member (announced with JOIN/HEARTBEAT, replies to PROBEs) or as an
// observer (silent, reference-counted: probes, custodial injection, split
// rebroadcast). Topic handles are cached for the life of the process because
// pubsub refuses to Join a topic twice.
type subs struct {
	ps      *pubsub.PubSub
	topicOf func(shardID string) string
	handler func(shardID string, data []byte, from string)

	mu     sync.Mutex
	topics map[string]*pubsub.Topic // shardID -> handle (evicted when idle past the cap)
	active map[string]*subState     // shardID -> subscription state
}

// maxIdleTopics caps cached topic handles: probes and custodial sends touch
// many shards over a process lifetime, and each live handle costs gossipsub
// state.
const maxIdleTopics = 64

type subState struct {
	shardID      string
	member       bool
	observerRefs int
	sub          *pubsub.Subscription
	cancel       context.CancelFunc
}

func newSubs(ps *pubsub.PubSub, topicOf func(string) string, handler func(string, []byte, string)) *subs {
	return &subs{
		ps:      ps,
		topicOf: topicOf,
		handler: handler,
		topics:  map[string]*pubsub.Topic{},
		active:  map[string]*subState{},
	}
}

// topic returns the (cached) topic handle for a shard, evicting idle handles
// over the cap. A handle with a live subscription refuses Close and is kept.
func (s *subs) topic(shardID string) (*pubsub.Topic, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t, ok := s.topics[shardID]; ok {
		return t, nil
	}
	if len(s.topics) >= maxIdleTopics {
		for id, t := range s.topics {
			if _, inUse := s.active[id]; inUse {
				continue
			}
			if err := t.Close(); err == nil {
				delete(s.topics, id)
			}
		}
	}
	t, err := s.ps.Join(s.topicOf(shardID))
	if err != nil {
		return nil, err
	}
	s.topics[shardID] = t
	return t, nil
}

// Publish sends a payload to a shard topic (joined on demand).
func (s *subs) Publish(ctx context.Context, shardID string, data []byte) error {
	t, err := s.topic(shardID)
	if err != nil {
		return err
	}
	return t.Publish(ctx, data)
}

// ensureSub starts the message pump for a shard if not yet running.
func (s *subs) ensureSub(ctx context.Context, shardID string) (*subState, error) {
	s.mu.Lock()
	if st, ok := s.active[shardID]; ok {
		s.mu.Unlock()
		return st, nil
	}
	s.mu.Unlock()

	t, err := s.topic(shardID)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.active[shardID]; ok {
		return st, nil
	}
	sub, err := t.Subscribe()
	if err != nil {
		return nil, err
	}
	pumpCtx, cancel := context.WithCancel(ctx)
	st := &subState{shardID: shardID, sub: sub, cancel: cancel}
	s.active[shardID] = st
	go s.pump(pumpCtx, st)
	return st, nil
}

func (s *subs) pump(ctx context.Context, st *subState) {
	for {
		msg, err := st.sub.Next(ctx)
		if err != nil {
			return // cancelled or subscription closed
		}
		s.handler(st.shardID, msg.Data, msg.GetFrom().String())
	}
}

// JoinMember subscribes to a shard as a full member (promoting an observer
// subscription in place). It reports whether membership is new.
func (s *subs) JoinMember(ctx context.Context, shardID string) (bool, error) {
	st, err := s.ensureSub(ctx, shardID)
	if err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if st.member {
		return false, nil
	}
	st.member = true
	return true, nil
}

// JoinObserver subscribes silently; each call must be paired with
// LeaveObserver.
func (s *subs) JoinObserver(ctx context.Context, shardID string) error {
	st, err := s.ensureSub(ctx, shardID)
	if err != nil {
		return err
	}
	s.mu.Lock()
	st.observerRefs++
	s.mu.Unlock()
	return nil
}

// LeaveMember drops full membership; the subscription closes once no
// observers hold it either.
func (s *subs) LeaveMember(shardID string) {
	s.mu.Lock()
	st, ok := s.active[shardID]
	if !ok || !st.member {
		s.mu.Unlock()
		return
	}
	st.member = false
	s.closeIfIdleLocked(st)
	s.mu.Unlock()
}

// LeaveObserver releases one observer reference.
func (s *subs) LeaveObserver(shardID string) {
	s.mu.Lock()
	st, ok := s.active[shardID]
	if !ok || st.observerRefs == 0 {
		s.mu.Unlock()
		return
	}
	st.observerRefs--
	s.closeIfIdleLocked(st)
	s.mu.Unlock()
}

func (s *subs) closeIfIdleLocked(st *subState) {
	if st.member || st.observerRefs > 0 {
		return
	}
	delete(s.active, st.shardID)
	go func() {
		time.Sleep(leaveFlushDelay)
		st.cancel()
		st.sub.Cancel()
	}()
}

// IsMember reports full membership of a shard.
func (s *subs) IsMember(shardID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.active[shardID]
	return ok && st.member
}

// IsSubscribed reports any subscription (member or observer).
func (s *subs) IsSubscribed(shardID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.active[shardID]
	return ok
}

// Close cancels every subscription.
func (s *subs) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, st := range s.active {
		st.cancel()
		st.sub.Cancel()
		delete(s.active, id)
	}
	slog.Debug("subs: closed all subscriptions")
}
