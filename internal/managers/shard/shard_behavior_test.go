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

	metrics := telemetry.NewMetricsManager()
	dht := &MockDHTProvider{}
	storageMgr := storage.NewStorageManager(dht, metrics)
	clusterMgr := &MockClusterManager{}

	sm := NewShardManager(ctx, h, ps, &MockIPFSClient{}, storageMgr, metrics, nil, nil, clusterMgr, startShard)
	return sm
}

// populateFakeActivePeers injects fake ACTIVE peer entries into seenPeerRoles.
func populateFakeActivePeers(sm *ShardManager, shardID string, count int) []peer.ID {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.seenPeerRoles[shardID] == nil {
		sm.seenPeerRoles[shardID] = make(map[peer.ID]PeerRoleInfo)
	}
	var peers []peer.ID
	for i := 0; i < count; i++ {
		pid := peer.ID(fmt.Sprintf("fake-active-peer-%d", i))
		sm.seenPeerRoles[shardID][pid] = PeerRoleInfo{Role: RoleActive, LastSeen: time.Now()}
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
	now := time.Now()
	sm.mu.Lock()
	sm.seenPeerRoles[shard] = map[peer.ID]PeerRoleInfo{
		"peer-active-1":  {Role: RoleActive, LastSeen: now},
		"peer-active-2":  {Role: RoleActive, LastSeen: now},
		"peer-passive-1": {Role: RolePassive, LastSeen: now},
		"peer-probe-1":   {Role: RoleProbe, LastSeen: now},
	}
	sm.mu.Unlock()

	// includeSelf=true: should count 2 active peers + self = 3
	count := sm.countActivePeers(shard, true, config.SeenPeersWindow)
	if count != 3 {
		t.Errorf("expected 3 (2 active + self), got %d", count)
	}

	// includeSelf=false: should count only 2 active peers
	count = sm.countActivePeers(shard, false, config.SeenPeersWindow)
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
	sm.mu.Lock()
	sm.seenPeerRoles[shard] = map[peer.ID]PeerRoleInfo{
		"peer-fresh": {Role: RoleActive, LastSeen: now},
		"peer-stale": {Role: RoleActive, LastSeen: now.Add(-10 * time.Minute)},
	}
	sm.mu.Unlock()

	// With a 5-minute window, only the fresh peer should count
	count := sm.countActivePeers(shard, false, 5*time.Minute)
	if count != 1 {
		t.Errorf("expected 1 (only fresh peer), got %d", count)
	}

	// With a 15-minute window, both should count
	count = sm.countActivePeers(shard, false, 15*time.Minute)
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
	now := time.Now()
	sm.mu.Lock()
	sm.seenPeerRoles[shard] = map[peer.ID]PeerRoleInfo{
		selfID:       {Role: RoleActive, LastSeen: now},
		"other-peer": {Role: RoleActive, LastSeen: now},
	}
	sm.mu.Unlock()

	// Self should not be double-counted. With includeSelf=true, self is added once
	// by the function, not counted from the map.
	count := sm.countActivePeers(shard, true, config.SeenPeersWindow)
	if count != 2 {
		t.Errorf("expected 2 (1 other + 1 self), got %d", count)
	}

	count = sm.countActivePeers(shard, false, config.SeenPeersWindow)
	if count != 1 {
		t.Errorf("expected 1 (only other peer), got %d", count)
	}
}

// --- Merge behavior tests ---

func TestMergeRefusal_HealthyShardEmptySibling(t *testing.T) {
	// Override config for fast probe timeouts so the test doesn't block
	origProbeTimeout := config.ProbeTimeoutMerge
	origMergeCooldown := config.MergeUpCooldown
	origSiblingEmpty := config.SiblingEmptyMergeAfter
	config.ProbeTimeoutMerge = 100 * time.Millisecond
	config.MergeUpCooldown = 50 * time.Millisecond
	config.SiblingEmptyMergeAfter = 50 * time.Millisecond
	defer func() {
		config.ProbeTimeoutMerge = origProbeTimeout
		config.MergeUpCooldown = origMergeCooldown
		config.SiblingEmptyMergeAfter = origSiblingEmpty
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sm := newTestShardManager(t, ctx, "00")

	// Populate enough ACTIVE peers to be "healthy" (>= MinPeersPerShard)
	populateFakeActivePeers(sm, "00", config.MinPeersPerShard+2)

	// Set lastMoveToDeeperShard far enough in the past to pass both cooldown and siblingEmptyMergeAfter
	sm.mu.Lock()
	sm.lastMoveToDeeperShard = time.Now().Add(-1 * time.Minute)
	sm.mu.Unlock()

	sm.checkAndMergeUpIfAlone()

	sm.mu.RLock()
	current := sm.currentShard
	sm.mu.RUnlock()
	if current != "00" {
		t.Errorf("expected to stay in shard 00 (healthy, empty sibling is just skew), but moved to %s", current)
	}
}

func TestMergeAllowed_UnderstaffedShardEmptySibling(t *testing.T) {
	origProbeTimeout := config.ProbeTimeoutMerge
	origMergeCooldown := config.MergeUpCooldown
	origSiblingEmpty := config.SiblingEmptyMergeAfter
	config.ProbeTimeoutMerge = 100 * time.Millisecond
	config.MergeUpCooldown = 50 * time.Millisecond
	config.SiblingEmptyMergeAfter = 50 * time.Millisecond
	defer func() {
		config.ProbeTimeoutMerge = origProbeTimeout
		config.MergeUpCooldown = origMergeCooldown
		config.SiblingEmptyMergeAfter = origSiblingEmpty
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sm := newTestShardManager(t, ctx, "00")

	// Populate fewer ACTIVE peers than MinPeersPerShard (understaffed)
	if config.MinPeersPerShard > 2 {
		populateFakeActivePeers(sm, "00", config.MinPeersPerShard-2)
	}

	// Set lastMoveToDeeperShard far enough in the past
	sm.mu.Lock()
	sm.lastMoveToDeeperShard = time.Now().Add(-1 * time.Minute)
	sm.mu.Unlock()

	sm.checkAndMergeUpIfAlone()

	sm.mu.RLock()
	current := sm.currentShard
	sm.mu.RUnlock()
	if current != "0" {
		t.Errorf("expected to merge up to shard 0 (understaffed + empty sibling), but stayed in %s", current)
	}
}

func TestMergeRefusal_CooldownPreventsEarlyMerge(t *testing.T) {
	origProbeTimeout := config.ProbeTimeoutMerge
	origMergeCooldown := config.MergeUpCooldown
	config.ProbeTimeoutMerge = 100 * time.Millisecond
	config.MergeUpCooldown = 10 * time.Minute // long cooldown
	defer func() {
		config.ProbeTimeoutMerge = origProbeTimeout
		config.MergeUpCooldown = origMergeCooldown
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sm := newTestShardManager(t, ctx, "00")

	// Set lastMoveToDeeperShard to very recently (within cooldown)
	sm.mu.Lock()
	sm.lastMoveToDeeperShard = time.Now().Add(-1 * time.Second)
	sm.mu.Unlock()

	sm.checkAndMergeUpIfAlone()

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
	metrics := telemetry.NewMetricsManager()
	dht := &MockDHTProvider{}
	storageMgr := storage.NewStorageManager(dht, metrics)
	clusterMgr := &MockClusterManager{}
	sm := NewShardManager(ctx, h1, ps1, &MockIPFSClient{}, storageMgr, metrics, nil, nil, clusterMgr, "0")
	_ = sm

	// h2 subscribes to shard "0" topic to catch the LEAVE
	topicName := fmt.Sprintf("%s-creative-commons-shard-%s", config.PubsubTopicPrefix, "0")
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
	metrics := telemetry.NewMetricsManager()
	dht := &MockDHTProvider{}
	storageMgr := storage.NewStorageManager(dht, metrics)
	clusterMgr := &MockClusterManager{}
	sm := NewShardManager(ctx, h1, ps1, &MockIPFSClient{}, storageMgr, metrics, nil, nil, clusterMgr, "0")
	_ = sm

	// h2 subscribes to shard "0"
	topicName := fmt.Sprintf("%s-creative-commons-shard-%s", config.PubsubTopicPrefix, "0")
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
