package shard

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/internal/common"
	"dlockss/internal/config"
	"dlockss/internal/managers/storage"
	"dlockss/internal/testutil"
)

// newTestShardManager creates a ShardManager on a real libp2p host and
// gossipsub, starting in the given shard.
func newTestShardManager(t *testing.T, ctx context.Context, startShard string) *ShardManager {
	t.Helper()
	h := newTestHost(t)
	return newTestShardManagerOn(t, ctx, h, newTestGossipSub(t, ctx, h), startShard)
}

func newTestShardManagerOn(t *testing.T, ctx context.Context, h host.Host, ps *pubsub.PubSub, startShard string) *ShardManager {
	t.Helper()
	cfg := config.DefaultConfig()
	sm, err := NewShardManager(ShardManagerConfig{
		Cfg:        cfg,
		Ctx:        ctx,
		Host:       h,
		PubSub:     ps,
		IPFSClient: &testutil.MockIPFSClient{},
		Storage:    storage.NewStorageManager(cfg, &testutil.MockDHTProvider{}, nil),
		Cluster:    &testutil.MockClusterManager{},
		StartShard: startShard,
	})
	if err != nil {
		t.Fatal(err)
	}
	return sm
}

func newTestHost(t *testing.T) host.Host {
	t.Helper()
	h, err := libp2p.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	return h
}

func newTestGossipSub(t *testing.T, ctx context.Context, h host.Host) *pubsub.PubSub {
	t.Helper()
	ps, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		t.Fatal(err)
	}
	return ps
}

// populateFakeActivePeers injects fake ACTIVE peer entries via PeerTracker.
func populateFakeActivePeers(sm *ShardManager, shardID string, count int) []peer.ID {
	var peers []peer.ID
	for i := 0; i < count; i++ {
		pid := peer.ID(fmt.Sprintf("fake-active-peer-%d", i))
		sm.peers.RecordRole(shardID, pid, roleActive)
		peers = append(peers, pid)
	}
	return peers
}

func TestCountActivePeers_OnlyCountsActive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sm := newTestShardManager(t, ctx, "0")

	shard := "0"
	sm.peers.RecordRole(shard, "peer-active-1", roleActive)
	sm.peers.RecordRole(shard, "peer-active-2", roleActive)
	sm.peers.RecordRole(shard, "peer-passive-1", rolePassive)
	sm.peers.RecordRole(shard, "peer-probe-1", roleProbe)

	count := sm.peers.CountActive(shard, true, "0", sm.cfg.Sharding.SeenPeersWindow)
	if count != 3 {
		t.Errorf("expected 3 (2 active + self), got %d", count)
	}

	count = sm.peers.CountActive(shard, false, "0", sm.cfg.Sharding.SeenPeersWindow)
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
	sm.peers.RecordRole(shard, "peer-fresh", roleActive)
	sm.peers.mu.Lock()
	sm.peers.roles[shard]["peer-stale"] = peerRoleInfo{role: roleActive, lastSeen: now.Add(-10 * time.Minute)}
	sm.peers.mu.Unlock()

	count := sm.peers.CountActive(shard, false, "0", 5*time.Minute)
	if count != 1 {
		t.Errorf("expected 1 (only fresh peer), got %d", count)
	}

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
	sm.peers.RecordRole(shard, selfID, roleActive)
	sm.peers.RecordRole(shard, "other-peer", roleActive)

	// With includeSelf, self is added once by CountActive, never counted from the map.
	count := sm.peers.CountActive(shard, true, "0", sm.cfg.Sharding.SeenPeersWindow)
	if count != 2 {
		t.Errorf("expected 2 (1 other + 1 self), got %d", count)
	}

	count = sm.peers.CountActive(shard, false, "0", sm.cfg.Sharding.SeenPeersWindow)
	if count != 1 {
		t.Errorf("expected 1 (only other peer), got %d", count)
	}
}

func TestMergeRefusal_HealthyShardEmptySibling(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sm := newTestShardManager(t, ctx, "00")

	sm.cfg.Sharding.ProbeTimeoutMerge = 100 * time.Millisecond
	sm.cfg.Sharding.MergeUpCooldown = 50 * time.Millisecond
	sm.cfg.Sharding.SiblingEmptyMergeAfter = 50 * time.Millisecond

	populateFakeActivePeers(sm, "00", sm.cfg.Sharding.MinPeersPerShard+2)

	// Past both MergeUpCooldown and SiblingEmptyMergeAfter.
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

	sm.cfg.Sharding.ProbeTimeoutMerge = 100 * time.Millisecond
	sm.cfg.Sharding.MergeUpCooldown = 50 * time.Millisecond
	sm.cfg.Sharding.SiblingEmptyMergeAfter = 50 * time.Millisecond

	if sm.cfg.Sharding.MinPeersPerShard > 2 {
		populateFakeActivePeers(sm, "00", sm.cfg.Sharding.MinPeersPerShard-2)
	}

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

	sm.cfg.Sharding.ProbeTimeoutMerge = 100 * time.Millisecond
	sm.cfg.Sharding.MergeUpCooldown = 10 * time.Minute

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

// peersHashingTo returns n fake peer IDs whose shard prefix is prefix.
func peersHashingTo(prefix string, n int) []peer.ID {
	var out []peer.ID
	for i := 0; len(out) < n; i++ {
		pid := peer.ID(fmt.Sprintf("hash-peer-%s-%d", prefix, i))
		if common.GetBinaryPrefix(pid.String(), len(prefix)) == prefix {
			out = append(out, pid)
		}
	}
	return out
}

func recordActive(sm *ShardManager, shardID string, peers []peer.ID) {
	for _, p := range peers {
		sm.peers.RecordRole(shardID, p, roleActive)
	}
}

func currentShardOf(sm *ShardManager) string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.currentShard
}

func TestChildCounts(t *testing.T) {
	self := peer.ID("self-peer")
	peers := append(peersHashingTo("00", 3), peersHashingTo("01", 2)...)
	zero, one := childCounts("0", self, peers)
	if common.GetBinaryPrefix(self.String(), 2) == "00" {
		zero--
	} else {
		one--
	}
	if zero != 3 || one != 2 {
		t.Errorf("expected 3/2 peers per child excluding self, got %d/%d", zero, one)
	}
}

func prepareMerge(t *testing.T, sm *ShardManager) {
	t.Helper()
	sm.cfg.Sharding.ProbeTimeoutMerge = 100 * time.Millisecond
	sm.cfg.Sharding.MergeUpCooldown = 50 * time.Millisecond
	sm.mu.Lock()
	sm.lastMoveToDeeperShard = time.Now().Add(-1 * time.Minute)
	sm.mu.Unlock()
}

func TestMergeUp_SiblingBelowMinPeers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sm := newTestShardManager(t, ctx, "00")
	prepareMerge(t, sm)
	minPeers := sm.cfg.Sharding.MinPeersPerShard

	// The pair total is above MinPeersAcrossSiblings, so only the min-peers
	// rule keeps the sibling from holding 2 copies indefinitely.
	populateFakeActivePeers(sm, "00", minPeers+2)
	recordActive(sm, "01", peersHashingTo("01", 2))

	sm.lifecycle.checkAndMergeUpIfAlone()
	if got := currentShardOf(sm); got != "00" {
		t.Fatalf("expected to wait for a 2nd check, moved to %s", got)
	}
	sm.lifecycle.checkAndMergeUpIfAlone()
	if got := currentShardOf(sm); got != "0" {
		t.Errorf("expected merge up to 0 because sibling is below min peers, got %s", got)
	}
}

func TestMergeRefusal_SplitInProgress(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sm := newTestShardManager(t, ctx, "00")
	prepareMerge(t, sm)
	minPeers := sm.cfg.Sharding.MinPeersPerShard

	// The parent still has members that have not descended yet, so the
	// small sibling is filling up, not depleted.
	populateFakeActivePeers(sm, "00", minPeers+2)
	recordActive(sm, "01", peersHashingTo("01", 2))
	recordActive(sm, "0", peersHashingTo("01", 3))

	sm.lifecycle.checkAndMergeUpIfAlone()
	sm.lifecycle.checkAndMergeUpIfAlone()
	if got := currentShardOf(sm); got != "00" {
		t.Errorf("expected to stay in 00 while the parent still has members, moved to %s", got)
	}
}

// A depleted shard 1 merged into the root, next to the subtree 00/01. The
// root now holds 2 members that cannot descend, so the subtree merges up.
func TestMergeUp_StrandedAncestor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sm := newTestShardManager(t, ctx, "00")
	prepareMerge(t, sm)
	sm.cfg.Sharding.MinPeersPerShard = 3
	sm.cfg.Sharding.SiblingEmptyMergeAfter = 50 * time.Millisecond

	populateFakeActivePeers(sm, "00", 4)
	recordActive(sm, "01", peersHashingTo("01", 3))
	recordActive(sm, "", peersHashingTo("1", 2))

	sm.lifecycle.checkAndMergeUpIfAlone()
	if got := currentShardOf(sm); got != "00" {
		t.Fatalf("expected to wait SiblingEmptyMergeAfter first, moved to %s", got)
	}
	time.Sleep(60 * time.Millisecond)
	sm.lifecycle.checkAndMergeUpIfAlone()
	if got := currentShardOf(sm); got != "0" {
		t.Errorf("expected merge up to 0 toward the stranded root, got %s", got)
	}
}

func TestMergeRefusal_AncestorAtMinPeers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sm := newTestShardManager(t, ctx, "00")
	prepareMerge(t, sm)
	sm.cfg.Sharding.MinPeersPerShard = 3
	sm.cfg.Sharding.SiblingEmptyMergeAfter = 50 * time.Millisecond

	populateFakeActivePeers(sm, "00", 4)
	recordActive(sm, "01", peersHashingTo("01", 3))
	recordActive(sm, "", peersHashingTo("1", 3))

	sm.lifecycle.checkAndMergeUpIfAlone()
	time.Sleep(60 * time.Millisecond)
	sm.lifecycle.checkAndMergeUpIfAlone()
	if got := currentShardOf(sm); got != "00" {
		t.Errorf("expected to stay in 00: the root holds min peers, moved to %s", got)
	}
}

func TestMergeUp_SelfBelowMinPeers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sm := newTestShardManager(t, ctx, "00")
	prepareMerge(t, sm)
	minPeers := sm.cfg.Sharding.MinPeersPerShard

	populateFakeActivePeers(sm, "00", 1)
	recordActive(sm, "01", peersHashingTo("01", minPeers+3))

	sm.lifecycle.checkAndMergeUpIfAlone()
	sm.lifecycle.checkAndMergeUpIfAlone()
	if got := currentShardOf(sm); got != "0" {
		t.Errorf("expected merge up to 0 because this shard is below min peers, got %s", got)
	}
}

func TestMergeRefusal_PairAtMinPeers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sm := newTestShardManager(t, ctx, "00")
	prepareMerge(t, sm)
	minPeers := sm.cfg.Sharding.MinPeersPerShard

	populateFakeActivePeers(sm, "00", minPeers-1)
	recordActive(sm, "01", peersHashingTo("01", minPeers))

	sm.lifecycle.checkAndMergeUpIfAlone()
	sm.lifecycle.checkAndMergeUpIfAlone()
	if got := currentShardOf(sm); got != "00" {
		t.Errorf("expected to stay in 00 with both shards at min peers, moved to %s", got)
	}
}

// discoveryAtRoot sets up a root node that knows both children, with five
// peers in its own child and the given sibling and root membership.
func discoveryAtRoot(t *testing.T, siblingPeers, rootPeers []peer.ID) *ShardManager {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sm := newTestShardManager(t, ctx, "")
	sm.cfg.Sharding.MinPeersPerShard = 3
	own := common.GetBinaryPrefix(sm.h.ID().String(), 1)
	other := getSiblingShard(own)
	recordActive(sm, own, peersHashingTo(own, 5))
	recordActive(sm, other, siblingPeers)
	recordActive(sm, "", rootPeers)
	sm.lifecycle.mu.Lock()
	sm.lifecycle.knownChildShards[own] = time.Now()
	sm.lifecycle.knownChildShards[other] = time.Now()
	sm.lifecycle.mu.Unlock()
	return sm
}

func TestDiscoveryRefusal_MergedPeersCountedOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("probes two shards with the discovery timeout")
	}
	// Two peers just merged up from the sibling: they are root members and
	// still look active in the sibling until SeenPeersWindow passes.
	sm := discoveryAtRoot(t, nil, nil)
	sib := getSiblingShard(common.GetBinaryPrefix(sm.h.ID().String(), 1))
	merged := peersHashingTo(sib, 2)
	recordActive(sm, sib, merged)
	recordActive(sm, "", merged)

	sm.lifecycle.discoverAndMoveToDeeperShard()
	if got := currentShardOf(sm); got != "" {
		t.Errorf("expected to stay at root: the sibling would get only 2 distinct peers, moved to %s", got)
	}
}

func TestDiscoveryAllowed_SiblingAtMinPeers(t *testing.T) {
	if testing.Short() {
		t.Skip("probes two shards with the discovery timeout")
	}
	sm := discoveryAtRoot(t, nil, nil)
	own := common.GetBinaryPrefix(sm.h.ID().String(), 1)
	sib := getSiblingShard(own)
	recordActive(sm, sib, peersHashingTo(sib, 3))
	sm.cfg.Sharding.MinPeersAcrossSiblings = 2

	sm.lifecycle.discoverAndMoveToDeeperShard()
	if got := currentShardOf(sm); got != own {
		t.Errorf("expected to descend into %s with 3 distinct sibling peers, stayed in %q", own, got)
	}
}

// payloadWithPrefix returns a payload key whose shard prefix at len(prefix) is prefix.
func payloadWithPrefix(prefix string) string {
	for i := 0; ; i++ {
		p := fmt.Sprintf("payload-%d", i)
		if s, _ := common.TargetShardForPayload(p, len(prefix)); s == prefix {
			return p
		}
	}
}

func TestDiscovery_CompletesHalfFinishedSplit(t *testing.T) {
	if testing.Short() {
		t.Skip("probes two shards with the discovery timeout")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sm := newTestShardManager(t, ctx, "")
	sm.cfg.Sharding.MinPeersPerShard = 3
	own := common.GetBinaryPrefix(sm.h.ID().String(), 1)
	other := getSiblingShard(own)
	// The other half already split off; two more root members hash into the
	// node's half, so with the node itself its child would hold 3.
	recordActive(sm, other, peersHashingTo(other, 4))
	recordActive(sm, "", peersHashingTo(own, 2))
	sm.lifecycle.mu.Lock()
	sm.lifecycle.knownChildShards[other] = time.Now()
	sm.lifecycle.mu.Unlock()

	sm.lifecycle.discoverAndMoveToDeeperShard()
	if got := currentShardOf(sm); got != own {
		t.Errorf("expected to create child %s, stayed in %q", own, got)
	}
}

func TestDiscovery_HalfFinishedSplitTooSmall(t *testing.T) {
	if testing.Short() {
		t.Skip("probes two shards with the discovery timeout")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sm := newTestShardManager(t, ctx, "")
	sm.cfg.Sharding.MinPeersPerShard = 3
	own := common.GetBinaryPrefix(sm.h.ID().String(), 1)
	other := getSiblingShard(own)
	recordActive(sm, other, peersHashingTo(other, 4))
	recordActive(sm, "", peersHashingTo(own, 1))
	sm.lifecycle.mu.Lock()
	sm.lifecycle.knownChildShards[other] = time.Now()
	sm.lifecycle.mu.Unlock()

	sm.lifecycle.discoverAndMoveToDeeperShard()
	if got := currentShardOf(sm); got != "" {
		t.Errorf("expected to stay at root with only 2 members for our child, moved to %s", got)
	}
}

func TestResolveCustodial_ShallowerTree(t *testing.T) {
	if testing.Short() {
		t.Skip("probes shards with the resolve timeout")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sm := newTestShardManager(t, ctx, "00")
	// The payload's range is still held by unsplit shard 1, although the
	// ingesting node sits at depth 2.
	recordActive(sm, "1", peersHashingTo("1", 3))

	if got := sm.ResolveTargetShardForCustodial("10", payloadWithPrefix("10")); got != "1" {
		t.Errorf("expected target 1 (nearest populated ancestor), got %q", got)
	}
}

func TestResolveCustodial_DeeperTree(t *testing.T) {
	if testing.Short() {
		t.Skip("probes shards with the resolve timeout")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sm := newTestShardManager(t, ctx, "1")
	// Shard 0 has split; its members now sit in 00 and 01.
	recordActive(sm, "00", peersHashingTo("00", 3))
	recordActive(sm, "01", peersHashingTo("01", 3))

	if got := sm.ResolveTargetShardForCustodial("0", payloadWithPrefix("00")); got != "00" {
		t.Errorf("expected target 00 (deepest populated shard on the path), got %q", got)
	}
}

func TestSplitRefusal_ChildWouldBeBelowMinPeers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sm := newTestShardManager(t, ctx, "0")
	sm.cfg.Sharding.MaxPeersPerShard = 4
	sm.cfg.Sharding.MinPeersPerShard = 2
	sm.cfg.Sharding.ShardPeerCheckInterval = time.Nanosecond

	// Six members, all but possibly self hashing into child 00: half of the
	// shard size would suffice, but child 01 would get at most one peer.
	recordActive(sm, "0", peersHashingTo("00", 5))

	sm.lifecycle.checkAndSplitIfNeeded()
	sm.lifecycle.checkAndSplitIfNeeded()
	if got := currentShardOf(sm); got != "0" {
		t.Errorf("expected no split with an unbalanced membership, moved to %s", got)
	}
}

// shardObserver is a second host subscribed to the topic of shard "0".
type shardObserver struct {
	id    peer.ID
	topic *pubsub.Topic
	sub   *pubsub.Subscription
}

// newObservedShardManager starts a ShardManager in shard "0" and connects a
// second host that subscribes to that shard's topic.
func newObservedShardManager(t *testing.T, ctx context.Context) (*ShardManager, *shardObserver) {
	t.Helper()
	h1 := newTestHost(t)
	h2 := newTestHost(t)
	h2.Peerstore().AddAddrs(h1.ID(), h1.Addrs(), time.Hour)
	if err := h2.Connect(ctx, peer.AddrInfo{ID: h1.ID(), Addrs: h1.Addrs()}); err != nil {
		t.Fatal(err)
	}
	ps1 := newTestGossipSub(t, ctx, h1)
	ps2 := newTestGossipSub(t, ctx, h2)

	sm := newTestShardManagerOn(t, ctx, h1, ps1, "0")

	topicName := fmt.Sprintf("%s-%s-shard-%s", sm.cfg.PubsubTopicPrefix, sm.cfg.TopicName, "0")
	topic, err := ps2.Join(topicName)
	if err != nil {
		t.Fatal(err)
	}
	sub, err := topic.Subscribe()
	if err != nil {
		t.Fatal(err)
	}

	// Let the gossipsub mesh form.
	time.Sleep(500 * time.Millisecond)
	return sm, &shardObserver{id: h2.ID(), topic: topic, sub: sub}
}

// await reports whether a message from another peer starting with prefix
// arrives within timeout.
func (o *shardObserver) await(ctx context.Context, prefix string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		msgCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		msg, err := o.sub.Next(msgCtx)
		cancel()
		if err != nil {
			continue
		}
		if msg.GetFrom() != o.id && strings.HasPrefix(string(msg.Data), prefix) {
			return true
		}
	}
	return false
}

func TestMoveToShard_PublishesLeave(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sm, observer := newObservedShardManager(t, ctx)

	sm.moveToShard("0", "1", false)

	if !observer.await(ctx, "LEAVE:"+sm.h.ID().String(), 3*time.Second) {
		t.Error("timed out waiting for LEAVE message from moveToShard")
	}
}

func TestProcessMessage_ProbeTriggersHeartbeat(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sm, observer := newObservedShardManager(t, ctx)

	// Drain the JOIN and HEARTBEAT that sm published when joining.
	drainCtx, drainCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	for {
		if _, err := observer.sub.Next(drainCtx); err != nil {
			break
		}
	}
	drainCancel()

	if err := observer.topic.Publish(ctx, []byte("PROBE:"+observer.id.String())); err != nil {
		t.Fatal(err)
	}

	if !observer.await(ctx, "HEARTBEAT:"+sm.h.ID().String(), 3*time.Second) {
		t.Error("timed out waiting for HEARTBEAT response to PROBE")
	}
}
