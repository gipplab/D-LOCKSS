package shard

import (
	"context"
	"log/slog"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/internal/pinset"
	"dlockss/internal/protocol"
	"dlockss/internal/schema"
)

// processMessage is the pubsub pump handler for every subscribed shard
// topic. Text messages (membership/status) are unsigned and unlimited; CBOR
// messages are rate-limited per sender and verified.
func (m *Manager) processMessage(shardID string, data []byte, fromStr string) {
	from, err := peer.Decode(fromStr)
	if err != nil || from == m.self {
		return
	}
	m.lastMessage.Store(time.Now().UnixNano())

	if tm, ok := protocol.ParseText(data); ok {
		m.handleText(shardID, tm, from)
		return
	}
	if !m.limiter.Allow(from) {
		return
	}
	// CBOR handlers do network I/O (key fetches, manifest reads, pins).
	// Running them inline would stall this topic's pump, overflow the
	// subscription buffer, and drop heartbeats — so they run in a bounded
	// pool and excess messages are shed (anti-entropy retries them later).
	select {
	case m.msgSem <- struct{}{}:
	default:
		slog.Debug("shedding CBOR message: processing slots full", "shard", shardID, "from", from)
		return
	}
	started := m.track(func() {
		defer func() { <-m.msgSem }()
		m.handleCBOR(shardID, data, from)
	})
	if !started {
		<-m.msgSem
	}
}

func (m *Manager) handleCBOR(shardID string, data []byte, from peer.ID) {
	m.tracker.touch(shardID, from)
	msg, err := schema.DecodeMessage(data)
	if err != nil {
		slog.Debug("undecodable message", "shard", shardID, "from", from, "err", err)
		return
	}
	switch v := msg.(type) {
	case *schema.IngestMessage:
		m.handleIngest(shardID, v, from)
	case *schema.ReplicationRequest:
		m.handleReplicationRequest(shardID, v, from)
	}
}

func (m *Manager) handleText(shardID string, tm protocol.TextMessage, from peer.ID) {
	switch tm.Kind {
	case protocol.TextHeartbeat, protocol.TextJoin:
		m.tracker.setRole(shardID, from, tm.Role)
	case protocol.TextLeave:
		m.tracker.remove(shardID, from)
	case protocol.TextPinned:
		m.tracker.touch(shardID, from)
		if k, err := cid.Decode(tm.CID); err == nil {
			m.store.AddKnown(k)
		}
	case protocol.TextProbe:
		m.tracker.setRole(shardID, from, protocol.RoleProbe)
		m.maybeReplyToProbe(shardID)
	case protocol.TextSplit:
		m.tracker.touch(shardID, from)
		m.recordChildren(tm.Child0, tm.Child1)
	default:
		// legacy JSON or empty: consumed and ignored
	}
}

// maybeReplyToProbe answers PROBEs on the node's own shard with one
// HEARTBEAT, so silent probes can count it. Replies are globally limited to
// one per cooldown window.
func (m *Manager) maybeReplyToProbe(shardID string) {
	if shardID != m.CurrentShard() || !m.subs.IsMember(shardID) {
		return
	}
	now := time.Now().UnixNano()
	last := m.lastProbeReply.Load()
	if now-last < int64(probeReplyCooldown) || !m.lastProbeReply.CompareAndSwap(last, now) {
		return
	}
	m.publishHeartbeat(shardID)
}

// recordChildren remembers shards named in SPLIT announcements; discovery
// only descends into shards a SPLIT has named (no phantom joins).
func (m *Manager) recordChildren(children ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, c := range children {
		if c != "" {
			m.knownChildren[c] = struct{}{}
		}
	}
}

// allowsManifest enforces the origin gate on a manifest CID: with origin
// restriction on, the manifest must be readable and its ingester trusted.
func (m *Manager) allowsManifest(k cid.Cid) bool {
	if m.trust == nil || !m.trust.RestrictsOrigins() {
		return true
	}
	ctx, cancel := context.WithTimeout(m.ctx, manifestReadTimeout)
	defer cancel()
	block, err := m.client.GetBlock(ctx, k)
	if err != nil {
		slog.Debug("origin check: unreadable manifest refused", "cid", k, "err", err)
		return false
	}
	if err := m.trust.CheckManifestOrigin(block); err != nil {
		slog.Debug("origin check: manifest refused", "cid", k, "err", err)
		return false
	}
	return true
}

// handleIngest processes an IngestMessage: authentication, then the origin
// gate; on the node's own shard, payloads it is responsible for join the
// shard pinset.
func (m *Manager) handleIngest(shardID string, msg *schema.IngestMessage, from peer.ID) {
	if m.signer == nil || m.signer.ShouldDrop(m.ctx, msg, from) {
		return
	}
	if m.trust != nil && !m.trust.AllowsOrigin(msg.Env.SenderID) {
		slog.Debug("ingest message from unauthorized ingestor", "from", msg.Env.SenderID)
		return
	}
	k := msg.Env.ManifestCID
	if !m.allowsManifest(k) {
		return
	}
	m.store.AddKnown(k)
	current := m.CurrentShard()
	if shardID != current {
		return
	}

	ctx, cancel := context.WithTimeout(m.ctx, payloadResolveTimeout)
	payloadKey := pinset.ResolvePayloadKey(ctx, m.client, k)
	cancel()
	// An unresolved payload (fallback to the manifest key) is pinned anyway;
	// a resolved one must map to this node's shard.
	if payloadKey != k.String() && !Responsible(current, payloadKey) {
		return
	}
	if _, err := m.pins.AddIfAbsent(current, k); err != nil {
		slog.Debug("ingest: pinset add failed", "cid", k, "err", err)
		return
	}
	m.pins.TriggerSync(current)
}

// handleReplicationRequest processes a repair request: when the node already
// holds the file it (re)joins the shard's pinset and syncs; otherwise it
// auto-replicates within the bounded slot pool.
func (m *Manager) handleReplicationRequest(shardID string, msg *schema.ReplicationRequest, from peer.ID) {
	if m.signer == nil || m.signer.ShouldDrop(m.ctx, msg, from) {
		return
	}
	// Only shards this node is a full member of act on repair requests:
	// observer subscriptions (probes, custodial sends) must never make the
	// node adopt a foreign shard's files.
	if !m.subs.IsMember(shardID) {
		return
	}
	k := msg.Env.ManifestCID
	if !m.allowsManifest(k) {
		return
	}

	if m.store.IsPinned(k) {
		if m.pins.Joined(shardID) {
			m.pins.TriggerSync(shardID)
		}
		return
	}
	select {
	case m.autoReplSem <- struct{}{}:
	default:
		slog.Debug("auto-replication skipped, concurrency limit reached", "cid", k)
		return
	}
	started := m.track(func() {
		defer func() { <-m.autoReplSem }()
		ctx, cancel := context.WithTimeout(m.ctx, autoReplicationTimeout)
		defer cancel()
		if err := m.client.PinManifest(ctx, k); err != nil {
			slog.Warn("auto-replication: failed to fetch/pin", "cid", k, "err", err)
			return
		}
		if _, err := m.pins.AddIfAbsent(shardID, k); err != nil {
			slog.Debug("auto-replication: pinset add failed", "cid", k, "err", err)
			return
		}
		m.pins.TriggerSync(shardID)
		slog.Info("auto-replication: fetched and pinned", "cid", k, "shard", shardID)
	})
	if !started {
		<-m.autoReplSem
	}
}
