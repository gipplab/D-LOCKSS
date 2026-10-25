package shard

import (
	"context"
	"log/slog"
	"math/rand"
	"time"

	"dlockss/internal/pinset"
	"dlockss/internal/protocol"
)

const (
	heartbeatPinnedBatch       = 20
	reprovideRepinTimeout      = 2 * time.Minute
	replicationCheckRoot       = 20 * time.Second
	replicationCooldown        = 5 * time.Minute
	replicationCooldownMaxAge  = 10 * time.Minute
	maxReplicationReqsPerCycle = 50
)

// sleep waits d or until shutdown; it reports whether to continue.
func (m *Manager) sleep(d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-m.ctx.Done():
		return false
	}
}

// runHeartbeat announces liveness and drives the per-cycle reprovide step.
func (m *Manager) runHeartbeat() {
	interval := heartbeatInterval
	for {
		m.publishHeartbeat(m.CurrentShard())
		m.reprovideStep()
		if !m.sleep(interval) {
			return
		}
	}
}

// reprovideStep re-pins and re-provides one pinned manifest per heartbeat,
// round-robin. The re-pin is idempotent and completes partial DAGs on nodes
// whose first pin was interrupted; the provides keep DHT provider records
// (TTL ~24h) fresh. A single-flight guard skips the step while the previous
// one is still running.
func (m *Manager) reprovideStep() {
	// Take the single-flight slot BEFORE consuming a round-robin key, so a
	// busy previous step never silently skips a manifest in the rotation.
	if !m.reprovideBusy.CompareAndSwap(false, true) {
		return
	}
	k, ok := m.store.NextToAnnounce()
	if !ok {
		m.reprovideBusy.Store(false)
		return
	}
	started := m.track(func() {
		defer m.reprovideBusy.Store(false)

		pctx, cancel := context.WithTimeout(m.ctx, reprovideRepinTimeout)
		err := m.client.PinManifest(pctx, k)
		cancel()
		if err != nil {
			slog.Debug("reprovide: re-pin failed", "cid", k, "err", err)
			return
		}
		ctx, cancel := context.WithTimeout(m.ctx, dhtProvideTimeout)
		m.store.Provide(ctx, k)
		cancel()

		rctx, cancel := context.WithTimeout(m.ctx, manifestReadTimeout)
		ro, err := pinset.ReadManifest(rctx, m.client, k)
		cancel()
		if err != nil || !ro.Payload.Defined() {
			return
		}
		// Pin the payload as its own root so Kubo's "pinned" reprovide
		// strategy re-announces it; on nodes whose first fetch was cut
		// short, successive steps complete the DAG incrementally.
		pinCtx, cancel := context.WithTimeout(m.ctx, reprovideRepinTimeout)
		err = m.client.PinRecursive(pinCtx, ro.Payload, "")
		cancel()
		if err != nil {
			slog.Debug("reprovide: payload pin failed", "cid", ro.Payload, "err", err)
			return
		}
		if !m.verifyPayloadSize(k, ro.Payload, ro.TotalSize) {
			return
		}
		provCtx, cancel := context.WithTimeout(m.ctx, dhtProvideTimeout)
		m.store.Provide(provCtx, ro.Payload)
		cancel()
	})
	if !started {
		m.reprovideBusy.Store(false)
	}
}

// runPeerCountChecker periodically evaluates splits and prunes stale peers.
func (m *Manager) runPeerCountChecker() {
	for m.sleep(peerCheckTick) {
		m.checkAndSplitIfNeeded()
		m.tracker.prune(pruneStalePeersMaxAge)
	}
}

// runDiscovery descends into announced child shards and merges up lonely
// ones, with jitter so shard members do not probe in lockstep.
func (m *Manager) runDiscovery() {
	for {
		interval := m.discoveryInterval()
		if !m.sleep(interval + time.Duration(rand.Int63n(int64(interval/4+1)))) {
			return
		}
		if m.skipDiscoveryTick() {
			continue
		}
		m.discoverAndMoveToDeeperShard()
		m.checkAndMergeUpIfAlone()
	}
}

func (m *Manager) discoveryInterval() time.Duration {
	if m.CurrentShard() == "" {
		return discoveryIntervalRoot
	}
	m.mu.Lock()
	hasChildren := len(m.knownChildren) > 0
	m.mu.Unlock()
	if hasChildren {
		return discoveryIntervalChild
	}
	return shardDiscoveryInterval
}

// skipDiscoveryTick suppresses the pass only in the narrow case where the
// shard is busy and overfull with no announced children (a split is about to
// handle it anyway).
func (m *Manager) skipDiscoveryTick() bool {
	current := m.CurrentShard()
	if current == "" {
		return false
	}
	m.mu.Lock()
	hasChildren := len(m.knownChildren) > 0
	m.mu.Unlock()
	if hasChildren {
		return false
	}
	lastMsg := time.Unix(0, m.lastMessage.Load())
	return time.Since(lastMsg) < time.Minute && m.countActive(current) > m.cfg.MaxPeersPerShard
}

// runSplitRebroadcast re-announces this branch's SPLIT to every ancestor so
// late joiners sitting higher up learn that the children exist.
func (m *Manager) runSplitRebroadcast() {
	interval := splitRebroadcastInterval
	jitterMax := interval / 2
	if jitterMax < time.Second {
		jitterMax = time.Second
	}
	for m.sleep(interval + time.Duration(rand.Int63n(int64(jitterMax)))) {
		current := m.CurrentShard()
		if current == "" {
			continue
		}
		for _, anc := range Ancestors(current) {
			if m.subs.IsMember(anc) {
				continue // still a full member there: no need
			}
			c0, c1 := Children(anc)
			if err := m.subs.JoinObserver(m.ctx, anc); err != nil {
				continue
			}
			m.publishText(anc, protocol.FormatSplit(c0, c1))
			m.subs.LeaveObserver(anc)
		}
	}
}

// runReplicationChecker periodically broadcasts ReplicationRequests for
// pinned manifests, bounded per cycle and per-manifest cooldown, so shard
// peers converge on the full pinset (the anti-entropy path).
func (m *Manager) runReplicationChecker() {
	for m.sleep(replicationCheckRoot) {
		interval := replicationCheckInterval
		if m.CurrentShard() == "" {
			interval = replicationCheckRoot
		}
		m.replMu.Lock()
		due := time.Since(m.lastReplPass) >= interval
		m.replMu.Unlock()
		if !due {
			continue
		}
		m.replicationPass()
	}
}

func (m *Manager) replicationPass() {
	m.replMu.Lock()
	m.lastReplPass = time.Now()
	// Prune stale cooldown entries.
	for k, t := range m.replCooldown {
		if time.Since(t) > replicationCooldownMaxAge {
			delete(m.replCooldown, k)
		}
	}
	m.replMu.Unlock()

	current := m.CurrentShard()
	sent := 0
	for _, k := range m.store.Pinned() {
		if sent >= maxReplicationReqsPerCycle {
			break
		}
		m.replMu.Lock()
		last, seen := m.replCooldown[k]
		if seen && time.Since(last) < replicationCooldown {
			m.replMu.Unlock()
			continue
		}
		m.replCooldown[k] = time.Now()
		m.replMu.Unlock()

		if err := m.sendReplicationRequest(current, k, false); err != nil {
			slog.Debug("ReplicationRequest send failed", "cid", k, "err", err)
			continue
		}
		slog.Debug("ReplicationRequest sent", "cid", k, "shard", shardLabel(current))
		sent++
	}
}

