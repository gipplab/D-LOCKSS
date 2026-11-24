package shard

import (
	"context"
	"testing"

	"github.com/ipfs/go-cid"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/internal/common"
	"dlockss/internal/config"
	"dlockss/internal/managers/storage"
	"dlockss/internal/testutil"
	"dlockss/internal/trust"
	"dlockss/pkg/schema"
)

// newMembershipSM returns a ShardManager in shard "0" whose node ID differs
// from its helper ID and whose membership follows tm.
func newMembershipSM(t *testing.T, ctx context.Context, tm *trust.TrustManager, dht *providerDHT) *ShardManager {
	t.Helper()
	h := newTestHost(t)
	cfg := config.DefaultConfig()
	var d common.DHTProvider = &testutil.MockDHTProvider{}
	if dht != nil {
		d = dht
	}
	sm, err := NewShardManager(ShardManagerConfig{
		Cfg:        cfg,
		Ctx:        ctx,
		Host:       h,
		NodeID:     testNodeID(t, h),
		Nodes:      trust.NewNodes(tm),
		PubSub:     newTestGossipSub(t, ctx, h),
		IPFSClient: &testutil.MockIPFSClient{},
		Storage:    storage.NewStorageManager(cfg, d, nil),
		Cluster:    &testutil.MockClusterManager{},
		StartShard: "0",
	})
	if err != nil {
		t.Fatal(err)
	}
	return sm
}

// heartbeatFrom returns a HEARTBEAT that helper sends with binding b.
func heartbeatFrom(helper peer.ID, b *schema.Binding) *pubsub.Message {
	hb := schema.Heartbeat{Helper: helper, Pinned: 1, Role: string(roleActive), Binding: b, Name: "n"}
	return &pubsub.Message{Message: &pb.Message{From: []byte(helper), Data: hb.Marshal()}}
}

func activeMembers(sm *ShardManager, shard string) map[peer.ID]bool {
	sm.peers.mu.RLock()
	defer sm.peers.mu.RUnlock()
	out := make(map[peer.ID]bool)
	for p, info := range sm.peers.roles[shard] {
		if info.role == roleActive {
			out[p] = true
		}
	}
	return out
}

func TestMembership_BoundHelperCountsAsItsNode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sm := newMembershipSM(t, ctx, trust.NewTrustManager("open"), nil)
	node, helper := testutil.NewKey(t), testutil.NewKey(t)

	sm.processMessage(heartbeatFrom(helper.ID, node.Bind(t, helper.ID)), "0")

	got := activeMembers(sm, "0")
	if !got[node.ID] || got[helper.ID] || len(got) != 1 {
		t.Fatalf("members = %v, want only node %v", got, node.ID)
	}
	if _, seen := sm.peers.GetSeenPeers("0", sm.cfg.Sharding.SeenPeersWindow)[node.ID]; !seen {
		t.Fatal("node should be recorded as seen")
	}
}

func TestMembership_AllowlistMode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	listed, unlisted, legacy := testutil.NewKey(t), testutil.NewKey(t), testutil.NewKey(t)
	tm := trust.NewTrustManager("allowlist")
	tm.AddOrigins([]string{listed.ID.String(), legacy.ID.String()})
	sm := newMembershipSM(t, ctx, tm, nil)

	stranger := testutil.NewKey(t)
	sm.processMessage(heartbeatFrom(stranger.ID, nil), "0")
	if n := len(activeMembers(sm, "0")); n != 0 {
		t.Fatalf("a host without binding off the allowlist must not count, got %d members", n)
	}

	h1 := testutil.NewKey(t)
	sm.processMessage(heartbeatFrom(h1.ID, unlisted.Bind(t, h1.ID)), "0")
	if n := len(activeMembers(sm, "0")); n != 0 {
		t.Fatalf("a node off the allowlist must not count in allowlist mode, got %d members", n)
	}

	// A host without binding does not count even if its ID is listed.
	sm.processMessage(heartbeatFrom(legacy.ID, nil), "0")
	if n := len(activeMembers(sm, "0")); n != 0 {
		t.Fatalf("an unbound sender on the allowlist must not count, got %d members", n)
	}

	h2 := testutil.NewKey(t)
	sm.processMessage(heartbeatFrom(h2.ID, listed.Bind(t, h2.ID)), "0")
	got := activeMembers(sm, "0")
	if !got[listed.ID] || len(got) != 1 {
		t.Fatalf("members = %v, want only the listed node", got)
	}
}

func TestMembership_OpenModeIgnoresUnbound(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sm := newMembershipSM(t, ctx, trust.NewTrustManager("open"), nil)
	host := testutil.NewKey(t)
	sm.processMessage(heartbeatFrom(host.ID, nil), "0")
	if n := len(activeMembers(sm, "0")); n != 0 {
		t.Fatalf("a host without binding must not count in open mode, got %d members", n)
	}
}

func TestMembership_OpenModeCountsBoundNodeOffAllowlist(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// In open mode the origin list restricts data, not membership.
	tm := trust.NewTrustManager("open")
	tm.AddOrigins([]string{testutil.NewKey(t).ID.String()})
	sm := newMembershipSM(t, ctx, tm, nil)
	node, helper := testutil.NewKey(t), testutil.NewKey(t)

	sm.processMessage(heartbeatFrom(helper.ID, node.Bind(t, helper.ID)), "0")
	if !activeMembers(sm, "0")[node.ID] {
		t.Fatal("a bound node off the allowlist counts in open mode")
	}
}

func TestMembership_CopiedBindingNotCounted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sm := newMembershipSM(t, ctx, trust.NewTrustManager("open"), nil)
	node, helper, thief := testutil.NewKey(t), testutil.NewKey(t), testutil.NewKey(t)

	sm.processMessage(heartbeatFrom(thief.ID, node.Bind(t, helper.ID)), "0")
	if got := activeMembers(sm, "0"); len(got) != 0 {
		t.Fatalf("a binding sent by another helper must not count, members = %v", got)
	}
}

func TestMembership_HelperOfOwnNodeNotCounted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sm := newMembershipSM(t, ctx, trust.NewTrustManager("open"), nil)
	sm.processMessage(heartbeatFrom(sm.h.ID(), nil), "0")
	if n := sm.peers.CountActive("0", false, "0", sm.cfg.Sharding.SeenPeersWindow); n != 0 {
		t.Fatalf("own messages must not count, got %d", n)
	}
}

func TestOutgoingAnnouncementsCarryBinding(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sm := newMembershipSM(t, ctx, trust.NewTrustManager("open"), nil)
	b := &schema.Binding{Node: sm.nodeID, Sig: []byte("sig")}
	sm.binding = b

	hb := schema.ParseHeartbeat(sm.heartbeatMessage(3, roleActive))
	if hb.Helper != sm.h.ID() || hb.Binding == nil || hb.Binding.Node != sm.nodeID {
		t.Fatalf("heartbeat = %+v", hb)
	}
	p := schema.ParseProbe(sm.probeMessage())
	if p.Helper != sm.h.ID() || p.Binding == nil || p.Binding.Node != sm.nodeID {
		t.Fatalf("probe = %+v", p)
	}
}

// providerDHT reports a fixed set of providers for every key.
type providerDHT struct {
	testutil.MockDHTProvider
	providers []peer.ID
}

func (d *providerDHT) FindProvidersAsync(ctx context.Context, _ cid.Cid, _ int) <-chan peer.AddrInfo {
	ch := make(chan peer.AddrInfo, len(d.providers))
	for _, p := range d.providers {
		ch <- peer.AddrInfo{ID: p}
	}
	close(ch)
	return ch
}

// DHT provider records name Kubo nodes, so a handoff counts providers other
// than this node by its node ID, not by its helper ID.
func TestHandoffConfirmed_ExcludesOwnNodeID(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dht := &providerDHT{}
	sm := newMembershipSM(t, ctx, trust.NewTrustManager("open"), dht)
	sm.cfg.Sharding.MinPeersPerShard = 2
	key := "bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am"

	dht.providers = []peer.ID{sm.nodeID, testutil.NewKey(t).ID}
	if sm.handoffConfirmed(key) {
		t.Fatal("this node's own provider record must not count toward the handoff")
	}
	dht.providers = append(dht.providers, testutil.NewKey(t).ID)
	if !sm.handoffConfirmed(key) {
		t.Fatal("two other providers should confirm the handoff")
	}
}

// Announced roles count: a PASSIVE (full) node is not an active member, a
// REPLICATOR is, and JOIN cannot announce PROBE.
func TestMembership_AnnouncedRoles(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sm := newMembershipSM(t, ctx, trust.NewTrustManager("open"), nil)
	send := func(helper peer.ID, data []byte) {
		sm.processMessage(&pubsub.Message{Message: &pb.Message{From: []byte(helper), Data: data}}, "0")
	}
	announce := func(role string, join bool) peer.ID {
		node, helper := testutil.NewKey(t), testutil.NewKey(t)
		b := node.Bind(t, helper.ID)
		if join {
			send(helper.ID, (&schema.Join{Helper: helper.ID, Role: role, Binding: b}).Marshal())
		} else {
			send(helper.ID, (&schema.Heartbeat{Helper: helper.ID, Pinned: 1, Role: role, Binding: b}).Marshal())
		}
		return node.ID
	}
	passive := announce("PASSIVE", false)
	replicator := announce("REPLICATOR", false)
	joinedAsProbe := announce("PROBE", true)

	roles := sm.peers.roles["0"]
	if roles[passive].role != rolePassive || roles[replicator].role != roleReplicator || roles[joinedAsProbe].role != roleActive {
		t.Fatalf("roles: passive=%s replicator=%s join PROBE=%s", roles[passive].role, roles[replicator].role, roles[joinedAsProbe].role)
	}
	if n := sm.peers.CountActive("0", false, "0", sm.cfg.Sharding.SeenPeersWindow); n != 2 {
		t.Fatalf("active members = %d, want 2 (replicator and joined node)", n)
	}
}
