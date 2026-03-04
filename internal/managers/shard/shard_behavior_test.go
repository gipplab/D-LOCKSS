package shard

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/internal/config"
	"dlockss/internal/managers/storage"
	"dlockss/internal/telemetry"
	"dlockss/internal/testutil"
)

// newTestShardManager creates a ShardManager for tests.
// It uses a real libp2p host and gossipsub, starting in the given shard.
func newTestShardManager(t *testing.T, ctx context.Context, startShard string) *ShardManager {
	t.Helper()
	h, err := libp2p.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })

	ps, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		t.Fatal(err)
	}

	metrics := telemetry.NewMetricsManager(config.DefaultConfig())
	dht := &testutil.MockDHTProvider{}
	cfg := config.DefaultConfig()
	storageMgr := storage.NewStorageManager(cfg, dht, metrics, nil)
	clusterMgr := &testutil.MockClusterManager{}

	sm, err := NewShardManager(ShardManagerConfig{
		Cfg:        cfg,
		Ctx:        ctx,
		Host:       h,
		PubSub:     ps,
		IPFSClient: &testutil.MockIPFSClient{},
		Storage:    storageMgr,
		Metrics:    metrics,
		Cluster:    clusterMgr,
		StartShard: startShard,
	})
	if err != nil {
		t.Fatal(err)
	}
	return sm
}

// populateFakeActivePeers injects fake ACTIVE peer entries via PeerTracker.
func populateFakeActivePeers(sm *ShardManager, shardID string, count int) []peer.ID {
	var peers []peer.ID
	for i := 0; i < count; i++ {
		pid := peer.ID(fmt.Sprintf("fake-active-peer-%d", i))
		sm.peers.RecordRole(shardID, pid, RoleActive)
		peers = append(peers, pid)
	}
	return peers
}

// --- countActivePeers tests ---

func TestCountActivePeers_OnlyCountsActive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sm := newTestShardManager(t, ctx, "0")

	shard := "0"
	sm.peers.RecordRole(shard, "peer-active-1", RoleActive)
	sm.peers.RecordRole(shard, "peer-active-2", RoleActive)
	sm.peers.RecordRole(shard, "peer-passive-1", RolePassive)
	sm.peers.RecordRole(shard, "peer-probe-1", RoleProbe)

	// includeSelf=true: should count 2 active peers + self = 3
	count := sm.peers.CountActive(shard, true, "0", sm.cfg.SeenPeersWindow)
	if count != 3 {
		t.Errorf("expected 3 (2 active + self), got %d", count)
	}

	// includeSelf=false: should count only 2 active peers
	count = sm.peers.CountActive(shard, false, "0", sm.cfg.SeenPeersWindow)
	if count != 2 {
		t.Errorf("expected 2 active peers, got %d", count)
	}
}

func TestCountActivePeers_ExcludesStaleEntries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sm := newTestShardManager(t, ctx, "0")

	shard := "0"
	now := time.Now()
	sm.peers.RecordRole(shard, "peer-fresh", RoleActive)
	// Inject a stale entry by writing directly (RecordRole always uses time.Now())
	sm.peers.mu.Lock()
	sm.peers.roles[shard]["peer-stale"] = PeerRoleInfo{Role: RoleActive, LastSeen: now.Add(-10 * time.Minute)}
	sm.peers.mu.Unlock()

	// With a 5-minute window, only the fresh peer should count
	count := sm.peers.CountActive(shard, false, "0", 5*time.Minute)
	if count != 1 {
		t.Errorf("expected 1 (only fresh peer), got %d", count)
	}

	// With a 15-minute window, both should count
	count = sm.peers.CountActive(shard, false, "0", 15*time.Minute)
	if count != 2 {
		t.Errorf("expected 2 (both within window), got %d", count)
	}
}

func TestCountActivePeers_ExcludesSelf(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sm := newTestShardManager(t, ctx, "0")

	shard := "0"
	selfID := sm.h.ID()
	sm.peers.RecordRole(shard, selfID, RoleActive)
	sm.peers.RecordRole(shard, "other-peer", RoleActive)

	// Self should not be double-counted. With includeSelf=true, self is added once
	// by the function, not counted from the map.
	count := sm.peers.CountActive(shard, true, "0", sm.cfg.SeenPeersWindow)
	if count != 2 {
		t.Errorf("expected 2 (1 other + 1 self), got %d", count)
	}

	count = sm.peers.CountActive(shard, false, "0", sm.cfg.SeenPeersWindow)
	if count != 1 {
		t.Errorf("expected 1 (only other peer), got %d", count)
	}
}

// --- Merge behavior tests ---

func TestMergeRefusal_HealthyShardEmptySibling(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sm := newTestShardManager(t, ctx, "00")

	sm.cfg.ProbeTimeoutMerge = 100 * time.Millisecond
	sm.cfg.MergeUpCooldown = 50 * time.Millisecond
	sm.cfg.SiblingEmptyMergeAfter = 50 * time.Millisecond

	populateFakeActivePeers(sm, "00", sm.cfg.MinPeersPerShard+2)

	// Set lastMoveToDeeperShard far enough in the past to pass both cooldown and siblingEmptyMergeAfter
	sm.mu.Lock()
	sm.lastMoveToDeeperShard = time.Now().Add(-1 * time.Minute)
	sm.mu.Unlock()

	sm.lifecycle.checkAndMergeUpIfAlone()

	sm.mu.RLock()
	current := sm.currentShard
	sm.mu.RUnlock()
	if current != "00" {
		t.Errorf("expected to stay in shard 00 (healthy, empty sibling is just skew), but moved to %s", current)
	}
}

func TestMergeAllowed_UnderstaffedShardEmptySibling(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sm := newTestShardManager(t, ctx, "00")

	sm.cfg.ProbeTimeoutMerge = 100 * time.Millisecond
	sm.cfg.MergeUpCooldown = 50 * time.Millisecond
	sm.cfg.SiblingEmptyMergeAfter = 50 * time.Millisecond

	if sm.cfg.MinPeersPerShard > 2 {
		populateFakeActivePeers(sm, "00", sm.cfg.MinPeersPerShard-2)
	}

	// Set lastMoveToDeeperShard far enough in the past
	sm.mu.Lock()
	sm.lastMoveToDeeperShard = time.Now().Add(-1 * time.Minute)
	sm.mu.Unlock()

	sm.lifecycle.checkAndMergeUpIfAlone()

	sm.mu.RLock()
	current := sm.currentShard
	sm.mu.RUnlock()
	if current != "0" {
		t.Errorf("expected to merge up to shard 0 (understaffed + empty sibling), but stayed in %s", current)
	}
}

func TestMergeRefusal_CooldownPreventsEarlyMerge(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sm := newTestShardManager(t, ctx, "00")

	sm.cfg.ProbeTimeoutMerge = 100 * time.Millisecond
	sm.cfg.MergeUpCooldown = 10 * time.Minute

	// Set lastMoveToDeeperShard to very recently (within cooldown)
	sm.mu.Lock()
	sm.lastMoveToDeeperShard = time.Now().Add(-1 * time.Second)
	sm.mu.Unlock()

	sm.lifecycle.checkAndMergeUpIfAlone()

	sm.mu.RLock()
	current := sm.currentShard
	sm.mu.RUnlock()
	if current != "00" {
		t.Errorf("expected to stay in shard 00 (cooldown active), but moved to %s", current)
	}
}

// --- moveToShard LEAVE test ---

func TestMoveToShard_PublishesLeave(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create two connected hosts so the second can receive messages from the first
	h1, err := libp2p.New()
	if err != nil {
		t.Fatal(err)
	}
	defer h1.Close()

	h2, err := libp2p.New()
	if err != nil {
		t.Fatal(err)
	}
	defer h2.Close()

	// Connect h2 to h1
	h2.Peerstore().AddAddrs(h1.ID(), h1.Addrs(), time.Hour)
	if err := h2.Connect(ctx, peer.AddrInfo{ID: h1.ID(), Addrs: h1.Addrs()}); err != nil {
		t.Fatal(err)
	}

	ps1, err := pubsub.NewGossipSub(ctx, h1)
	if err != nil {
		t.Fatal(err)
	}
	ps2, err := pubsub.NewGossipSub(ctx, h2)
	if err != nil {
		t.Fatal(err)
	}

	// Set up ShardManager on h1 starting in shard "0"
	metrics := telemetry.NewMetricsManager(config.DefaultConfig())
	dht := &testutil.MockDHTProvider{}
	cfg1 := config.DefaultConfig()
	storageMgr := storage.NewStorageManager(cfg1, dht, metrics, nil)
	clusterMgr := &testutil.MockClusterManager{}
	sm, err := NewShardManager(ShardManagerConfig{
		Cfg:        cfg1,
		Ctx:        ctx,
		Host:       h1,
		PubSub:     ps1,
		IPFSClient: &testutil.MockIPFSClient{},
		Storage:    storageMgr,
		Metrics:    metrics,
		Cluster:    clusterMgr,
		StartShard: "0",
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = sm

	// h2 subscribes to shard "0" topic to catch the LEAVE
	topicName := fmt.Sprintf("%s-creative-commons-shard-%s", cfg1.PubsubTopicPrefix, "0")
	topic2, err := ps2.Join(topicName)
	if err != nil {
		t.Fatal(err)
	}
	sub2, err := topic2.Subscribe()
	if err != nil {
		t.Fatal(err)
	}

	// Give gossipsub meshes time to form
	time.Sleep(500 * time.Millisecond)

	// Trigger moveToShard: "0" -> "1"
	sm.moveToShard("0", "1", false)

	// Read messages on h2 and look for LEAVE from h1
	deadline := time.After(3 * time.Second)
	foundLeave := false
	expectedPrefix := "LEAVE:" + h1.ID().String()
	for !foundLeave {
		select {
		case <-deadline:
			t.Error("timed out waiting for LEAVE message from moveToShard")
			return
		default:
		}
		msgCtx, msgCancel := context.WithTimeout(ctx, 200*time.Millisecond)
		msg, err := sub2.Next(msgCtx)
		msgCancel()
		if err != nil {
			continue
		}
		if msg.GetFrom() == h2.ID() {
			continue
		}
		if strings.HasPrefix(string(msg.Data), expectedPrefix) {
			foundLeave = true
		}
	}
	if !foundLeave {
		t.Error("did not receive LEAVE message after moveToShard")
	}
}

// --- PROBE response test ---

func TestProcessMessage_ProbeTriggersHeartbeat(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h1, err := libp2p.New()
	if err != nil {
		t.Fatal(err)
	}
	defer h1.Close()

	h2, err := libp2p.New()
	if err != nil {
		t.Fatal(err)
	}
	defer h2.Close()

	h2.Peerstore().AddAddrs(h1.ID(), h1.Addrs(), time.Hour)
	if err := h2.Connect(ctx, peer.AddrInfo{ID: h1.ID(), Addrs: h1.Addrs()}); err != nil {
		t.Fatal(err)
	}

	ps1, err := pubsub.NewGossipSub(ctx, h1)
	if err != nil {
		t.Fatal(err)
	}
	ps2, err := pubsub.NewGossipSub(ctx, h2)
	if err != nil {
		t.Fatal(err)
	}

	// Set up ShardManager on h1 in shard "0"
	metrics := telemetry.NewMetricsManager(config.DefaultConfig())
	dht := &testutil.MockDHTProvider{}
	cfg2 := config.DefaultConfig()
	storageMgr := storage.NewStorageManager(cfg2, dht, metrics, nil)
	clusterMgr := &testutil.MockClusterManager{}
	sm, err := NewShardManager(ShardManagerConfig{
		Cfg:        cfg2,
		Ctx:        ctx,
		Host:       h1,
		PubSub:     ps1,
		IPFSClient: &testutil.MockIPFSClient{},
		Storage:    storageMgr,
		Metrics:    metrics,
		Cluster:    clusterMgr,
		StartShard: "0",
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = sm

	// h2 subscribes to shard "0"
	topicName := fmt.Sprintf("%s-creative-commons-shard-%s", cfg2.PubsubTopicPrefix, "0")
	topic2, err := ps2.Join(topicName)
	if err != nil {
		t.Fatal(err)
	}
	sub2, err := topic2.Subscribe()
	if err != nil {
		t.Fatal(err)
	}

	// Wait for mesh formation
	time.Sleep(500 * time.Millisecond)

	// Drain any initial JOIN/HEARTBEAT messages from sm joining
	drainCtx, drainCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	for {
		_, err := sub2.Next(drainCtx)
		if err != nil {
			break
		}
	}
	drainCancel()

	// h2 publishes a PROBE message
	probeMsg := []byte("PROBE:" + h2.ID().String())
	if err := topic2.Publish(ctx, probeMsg); err != nil {
		t.Fatal(err)
	}

	// Wait for HEARTBEAT response from h1
	deadline := time.After(3 * time.Second)
	foundHeartbeat := false
	expectedPrefix := "HEARTBEAT:" + h1.ID().String()
	for !foundHeartbeat {
		select {
		case <-deadline:
			t.Error("timed out waiting for HEARTBEAT response to PROBE")
			return
		default:
		}
		msgCtx, msgCancel := context.WithTimeout(ctx, 200*time.Millisecond)
		msg, err := sub2.Next(msgCtx)
		msgCancel()
		if err != nil {
			continue
		}
		if msg.GetFrom() == h2.ID() {
			continue
		}
		if strings.HasPrefix(string(msg.Data), expectedPrefix) {
			foundHeartbeat = true
		}
	}
	if !foundHeartbeat {
		t.Error("did not receive HEARTBEAT response to PROBE from ShardManager")
	}
}
