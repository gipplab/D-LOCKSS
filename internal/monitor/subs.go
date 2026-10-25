package monitor

import (
	"context"
	"log/slog"
	"sync"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/internal/protocol"
	"dlockss/internal/schema"
)

const (
	maxShardSubDepth = 10 // hard cap on subscribed shard ID length
	dynamicPassEvery = 2 * time.Second
	maxHeartbeatLen  = 500
)

// Subscriber manages the monitor's shard-topic subscriptions and feeds the
// State from their messages.
type Subscriber struct {
	net     *Net
	state   *State
	topicOf func(shardID string) string

	mu     sync.Mutex
	topics map[string]*pubsub.Topic
	active map[string]context.CancelFunc
}

// NewSubscriber wires subscriptions; State.SubscribeHint is pointed here.
func NewSubscriber(net *Net, state *State, topicOf func(string) string) *Subscriber {
	s := &Subscriber{
		net:     net,
		state:   state,
		topicOf: topicOf,
		topics:  map[string]*pubsub.Topic{},
		active:  map[string]context.CancelFunc{},
	}
	state.SubscribeHint = func(shardID string) { go s.Subscribe(context.Background(), shardID) }
	return s
}

// Bootstrap subscribes to the root shard only. Deeper shards are discovered
// dynamically: nodes rebroadcast SPLIT announcements to ancestor topics every
// minute, every SPLIT heard subscribes both children, and the dynamic pass
// follows node shards and their immediate children from there.
func (s *Subscriber) Bootstrap(ctx context.Context) {
	s.Subscribe(ctx, "")
	slog.Info("monitor subscribed to the root shard; deeper shards follow SPLIT announcements")
}

// Subscribe joins one shard topic (idempotent) and announces PROBE so nodes
// know an observer is listening.
func (s *Subscriber) Subscribe(ctx context.Context, shardID string) {
	if len(shardID) > maxShardSubDepth {
		return
	}
	s.mu.Lock()
	if _, ok := s.active[shardID]; ok {
		s.mu.Unlock()
		return
	}
	topic, ok := s.topics[s.topicOf(shardID)]
	if !ok {
		var err error
		topic, err = s.net.PubSub.Join(s.topicOf(shardID))
		if err != nil {
			s.mu.Unlock()
			slog.Debug("monitor topic join failed", "shard", shardID, "err", err)
			return
		}
		s.topics[s.topicOf(shardID)] = topic
	}
	sub, err := topic.Subscribe()
	if err != nil {
		s.mu.Unlock()
		slog.Debug("monitor subscribe failed", "shard", shardID, "err", err)
		return
	}
	pumpCtx, cancel := context.WithCancel(context.Background())
	s.active[shardID] = cancel
	s.mu.Unlock()

	go s.pump(pumpCtx, shardID, sub)
	_ = topic.Publish(ctx, []byte(protocol.FormatProbe(s.net.Host.ID())))
}

// RunDynamic re-evaluates the wanted subscription set every couple of
// seconds: every hinted shard plus its immediate children.
func (s *Subscriber) RunDynamic(ctx context.Context) {
	ticker := time.NewTicker(dynamicPassEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, sh := range s.state.RecentShardHints() {
				s.Subscribe(ctx, sh)
				s.Subscribe(ctx, sh+"0")
				s.Subscribe(ctx, sh+"1")
			}
		}
	}
}

// TopicPeers returns every peer currently listed in a subscribed topic (for
// peerstore GC retention).
func (s *Subscriber) TopicPeers() map[peer.ID]struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[peer.ID]struct{}{}
	for _, t := range s.topics {
		for _, p := range t.ListPeers() {
			out[p] = struct{}{}
		}
	}
	return out
}

func (s *Subscriber) pump(ctx context.Context, shardID string, sub *pubsub.Subscription) {
	defer sub.Cancel()
	for {
		msg, err := sub.Next(ctx)
		if err != nil {
			return
		}
		s.dispatch(shardID, msg)
	}
}

// dispatch routes one pubsub message into the State.
func (s *Subscriber) dispatch(shardID string, msg *pubsub.Message) {
	from := msg.GetFrom()
	if from == s.net.Host.ID() {
		return
	}
	data := msg.Data

	if tm, ok := protocol.ParseText(data); ok {
		switch tm.Kind {
		case protocol.TextHeartbeat:
			if len(data) > maxHeartbeatLen {
				return
			}
			author := decodeAuthor(tm.Peer, from)
			s.state.HandleHeartbeat(shardID, author, tm.Pinned, tm.Role, tm.Name, s.net.PeerIP(author))
		case protocol.TextJoin:
			author := decodeAuthor(tm.Peer, from)
			s.state.HandleHeartbeat(shardID, author, -1, tm.Role, tm.Name, s.net.PeerIP(author))
		case protocol.TextLeave:
			s.state.HandleLeave(shardID, decodeAuthor(tm.Peer, from))
		case protocol.TextPinned:
			s.state.HandlePinned(shardID, from, tm.CID)
		case protocol.TextProbe:
			// other observers: ignored
		case protocol.TextSplit:
			s.state.HandleSplit(shardID, tm.Child0, tm.Child1)
		}
		return
	}

	m, err := schema.DecodeMessage(data)
	if err != nil {
		return
	}
	if im, ok := m.(*schema.IngestMessage); ok {
		author := im.Env.SenderID
		if author == "" {
			author = from
		}
		s.state.HandleIngest(shardID, author, im.Env.ManifestCID.String(), im.ShardID)
	}
}

// decodeAuthor prefers the peer ID embedded in the message, falling back to
// the transport peer.
func decodeAuthor(embedded string, from peer.ID) peer.ID {
	if embedded == "" {
		return from
	}
	if id, err := peer.Decode(embedded); err == nil {
		return id
	}
	return from
}
