package shard

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ipfs/go-cid"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/internal/config"
	"dlockss/internal/kubo"
	"dlockss/internal/pinset"
	"dlockss/internal/protocol"
	"dlockss/internal/store"
	"dlockss/internal/trust"
)

// Fixed engine timings (configurable values live in config.Sharding etc.).
const (
	peerCheckTick          = 30 * time.Second // base ticker; the split gate throttles further
	rootPeerCheckInterval  = 30 * time.Second
	discoveryIntervalRoot  = 10 * time.Second
	discoveryIntervalChild = 45 * time.Second // when a SPLIT announced children
	splitProbeTimeout       = 6 * time.Second
	discoveryProbeTimeout   = 12 * time.Second
	ingestChildProbeTimeout = 2 * time.Second
	probeReplyCooldown      = 5 * time.Second
	migrateDelay            = 250 * time.Millisecond
	handoffConfirmTimeout   = 30 * time.Second
	manifestReadTimeout     = 30 * time.Second
	payloadResolveTimeout   = 5 * time.Second
	heartbeatInterval       = 10 * time.Second
	dhtProvideTimeout       = time.Minute
	replicationCheckInterval = time.Minute // 20s on the root
	autoReplicationTimeout   = 5 * time.Minute
	maxAutoReplications      = 5
)

// Manager runs the dynamic sharding engine: membership, lifecycle, repair,
// custodial handoff and announcements.
type Manager struct {
	cfg      *config.Config
	self     peer.ID
	nodeName string

	signer  *protocol.Signer
	trust   *trust.Manager
	limiter *protocol.RateLimiter
	store   *store.Store
	disk    *store.DiskMonitor
	pins    *pinset.Manager
	client  *kubo.Client

	subs    *subs
	tracker *tracker

	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	closeMu sync.Mutex
	closing bool

	// bounded slot pools for work spawned from message handlers
	msgSem     chan struct{} // CBOR message processing off the pubsub pump
	payloadSem chan struct{} // payload fetches on first pin sync

	// lifecycle state, guarded by mu
	mu             sync.Mutex
	current        string
	lastShardMove  time.Time
	lastMergeUp    time.Time
	lastMoveDeeper time.Time
	lastSplitCheck time.Time
	knownChildren  map[string]struct{}
	aboveMaxStreak int
	mergeStreak    int

	lastMessage    atomic.Int64 // unix nanos of the last foreign shard message
	lastProbeReply atomic.Int64
	reprovideBusy  atomic.Bool

	// repair state
	replMu       sync.Mutex
	replCooldown map[cid.Cid]time.Time
	lastReplPass time.Time
	autoReplSem  chan struct{}

	// placement-pass state
	placeMu      sync.Mutex
	placeChecked map[cid.Cid]time.Time
	handoffs     map[cid.Cid]*handoffState
	placeBusy    atomic.Bool
}

// Deps bundles the Manager's collaborators.
type Deps struct {
	Config   *config.Config
	Self     peer.ID
	NodeName string
	PubSub   *pubsub.PubSub
	Signer   *protocol.Signer
	Trust    *trust.Manager
	Store    *store.Store
	Disk     *store.DiskMonitor
	Pins     *pinset.Manager
	Client   *kubo.Client
}

// Per-peer budget for CBOR protocol messages (frozen).
const (
	rateLimitWindow      = time.Minute
	maxMessagesPerWindow = 100
)

// New builds the Manager. Nodes always start at the root shard and descend
// through discovery.
func New(ctx context.Context, d Deps) (*Manager, error) {
	ctx, cancel := context.WithCancel(ctx)
	m := &Manager{
		cfg:          d.Config,
		self:         d.Self,
		nodeName:     d.NodeName,
		signer:       d.Signer,
		trust:        d.Trust,
		limiter:      protocol.NewRateLimiter(rateLimitWindow, maxMessagesPerWindow),
		store:        d.Store,
		disk:         d.Disk,
		pins:         d.Pins,
		client:       d.Client,
		tracker:      newTracker(),
		ctx:          ctx,
		cancel:       cancel,
		knownChildren: map[string]struct{}{},
		msgSem:       make(chan struct{}, 16),
		payloadSem:   make(chan struct{}, 8),
		replCooldown: map[cid.Cid]time.Time{},
		autoReplSem:  make(chan struct{}, maxAutoReplications),
		placeChecked: map[cid.Cid]time.Time{},
		handoffs:     map[cid.Cid]*handoffState{},
	}
	m.subs = newSubs(d.PubSub, d.Config.ShardTopic, m.processMessage)
	return m, nil
}

// Run joins the root shard and starts every background loop.
func (m *Manager) Run() error {
	if err := m.joinCurrentShard(""); err != nil {
		return err
	}
	loops := []func(){
		m.runHeartbeat,
		m.runPeerCountChecker,
		m.runDiscovery,
		m.runSplitRebroadcast,
		m.runReplicationChecker,
		m.runPlacement,
	}
	for _, loop := range loops {
		m.track(loop)
	}
	slog.Info("shard manager running", "shard", "(root)", "peer", m.self)
	return nil
}

// track runs fn on a goroutine counted by the shutdown WaitGroup. It refuses
// new work once Close started, so Add can never race Wait. Returns whether
// fn was started.
func (m *Manager) track(fn func()) bool {
	m.closeMu.Lock()
	if m.closing {
		m.closeMu.Unlock()
		return false
	}
	m.wg.Add(1)
	m.closeMu.Unlock()
	go func() {
		defer m.wg.Done()
		fn()
	}()
	return true
}

// Close stops all loops.
func (m *Manager) Close() error {
	m.closeMu.Lock()
	m.closing = true
	m.closeMu.Unlock()
	m.cancel()
	m.wg.Wait()
	m.subs.Close()
	return nil
}

// CurrentShard returns the node's shard ID.
func (m *Manager) CurrentShard() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.current
}

// AmIResponsibleFor reports whether this node's shard covers the key.
func (m *Manager) AmIResponsibleFor(key string) bool {
	return Responsible(m.CurrentShard(), key)
}

// IsLocalIngestor reports whether this node may ingest local content.
func (m *Manager) IsLocalIngestor() bool {
	return m.trust == nil || m.trust.AllowsOrigin(m.self)
}

// ourRole derives the node's advertised role from disk headroom and the
// origin gate.
func (m *Manager) ourRole() protocol.Role {
	if !m.disk.CanAcceptFiles() {
		return protocol.RolePassive
	}
	if !m.IsLocalIngestor() {
		return protocol.RoleReplicator
	}
	return protocol.RoleActive
}

// joinCurrentShard makes the node a full member of a shard: subscribes,
// announces JOIN + HEARTBEAT, and joins the shard's pinset.
func (m *Manager) joinCurrentShard(shardID string) error {
	if _, err := m.subs.JoinMember(m.ctx, shardID); err != nil {
		return err
	}
	if err := m.pins.Join(shardID); err != nil {
		return err
	}
	m.publishText(shardID, protocol.FormatJoin(m.self, m.ourRole(), m.nodeName))
	m.publishHeartbeat(shardID)
	return nil
}

// countActive counts a shard's members from the tracker, adding self when
// counting the node's own current shard.
func (m *Manager) countActive(shardID string) int {
	includeSelf := shardID == m.CurrentShard() && m.subs.IsMember(shardID)
	return m.tracker.countActive(shardID, seenPeersWindow, m.self, includeSelf)
}

// probeShard counts active peers in a shard. The node's own shard is read
// straight from the tracker (heartbeats keep it fresh); any other shard gets
// a real probe — silent join if needed, PROBE, listen for the timeout — so
// decisions never ride on stale counts from an incidental subscription.
func (m *Manager) probeShard(shardID string, timeout time.Duration) int {
	if shardID == m.CurrentShard() && m.subs.IsMember(shardID) {
		return m.countActive(shardID)
	}
	if !m.subs.IsSubscribed(shardID) {
		if err := m.subs.JoinObserver(m.ctx, shardID); err != nil {
			slog.Debug("probe: join failed", "shard", shardID, "err", err)
			return 0
		}
		defer m.subs.LeaveObserver(shardID)
	}
	m.publishText(shardID, protocol.FormatProbe(m.self))
	select {
	case <-time.After(timeout):
	case <-m.ctx.Done():
	}
	return m.tracker.countActive(shardID, seenPeersWindow, m.self, false)
}

// moveToShard transitions the node between shards: leave announcements,
// join, delayed pin migration, overlap-delayed leave, and a reshard pass.
func (m *Manager) moveToShard(from, to string, mergeUp bool) {
	m.mu.Lock()
	if m.current != from {
		m.mu.Unlock()
		return
	}
	m.current = to
	now := time.Now()
	m.lastShardMove = now
	if mergeUp {
		m.lastMergeUp = now
	} else if len(to) > len(from) {
		m.lastMoveDeeper = now
	}
	m.knownChildren = map[string]struct{}{}
	m.aboveMaxStreak = 0
	m.mergeStreak = 0
	m.lastSplitCheck = now
	m.mu.Unlock()

	// Fresh placement bookkeeping: every file deserves a prompt re-check
	// against the new shard.
	m.placeMu.Lock()
	m.placeChecked = map[cid.Cid]time.Time{}
	m.placeMu.Unlock()

	slog.Info("moving shard", "from", shardLabel(from), "to", shardLabel(to), "merge_up", mergeUp)

	if m.subs.IsMember(from) {
		m.publishText(from, protocol.FormatLeave(m.self))
	}
	if err := m.joinCurrentShard(to); err != nil {
		slog.Error("joining new shard failed", "shard", to, "err", err)
	}

	// Migrate matching pins shortly after the join so announcements flush.
	m.goAfter(migrateDelay, func() {
		dest := m.CurrentShard()
		if dest != to && !IsAncestorOrSelf(to, dest) {
			return // moved elsewhere meanwhile
		}
		m.pins.MigratePins(m.ctx, from, dest, func(payloadKey string) bool {
			return BitPrefix(payloadKey, len(dest)) == dest
		})
	})

	// Keep the old shard through the overlap window, then leave it.
	m.goAfter(shardOverlapDuration, func() {
		if m.CurrentShard() == from {
			return // moved back
		}
		if m.subs.IsMember(from) {
			m.publishText(from, protocol.FormatLeave(m.self))
			m.subs.LeaveMember(from)
		}
		m.pins.Leave(from)
	})

	// An early placement pass re-homes files the move orphaned; the regular
	// interval takes over from there.
	m.goAfter(postMoveFirstPass, func() {
		if m.CurrentShard() == to {
			m.placementPass()
		}
	})
}

// goAfter schedules fn after d, bound to the engine lifetime.
func (m *Manager) goAfter(d time.Duration, fn func()) {
	m.track(func() {
		select {
		case <-time.After(d):
			fn()
		case <-m.ctx.Done():
		}
	})
}

func shardLabel(id string) string {
	if id == "" {
		return "(root)"
	}
	return id
}
