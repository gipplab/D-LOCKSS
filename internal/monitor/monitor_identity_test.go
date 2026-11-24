package monitor

import (
	"context"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/internal/testutil"
	"dlockss/pkg/schema"
)

func heartbeat(helper peer.ID, b *schema.Binding, name string) []byte {
	return (&schema.Heartbeat{Helper: helper, Pinned: 2, Role: "ACTIVE", Binding: b, Name: name}).Marshal()
}

func TestMonitorHeartbeat_BoundHelperShownAsNode(t *testing.T) {
	m := NewMonitor(DefaultConfig())
	node, helper := testutil.NewKey(t), testutil.NewKey(t)

	m.dispatchHeartbeat(context.Background(), heartbeat(helper.ID, node.Bind(t, helper.ID), "pi"), helper.ID, "0", "")

	ns, ok := m.nodes[node.ID.String()]
	if !ok {
		t.Fatalf("node %s not listed; nodes = %v", node.ID, m.nodes)
	}
	if ns.HelperID != helper.ID.String() || ns.NodeName != "pi" {
		t.Fatalf("node state = %+v", ns)
	}
	if _, listed := m.nodes[helper.ID.String()]; listed {
		t.Fatal("the helper must not be listed as a node of its own")
	}
	if got, ok := m.helpers.Resolve(helper.ID, nil); !ok || got != node.ID {
		t.Fatalf("helper resolves to %v, %v; want %v", got, ok, node.ID)
	}
}

// A binding counts only for the helper it names, so another helper cannot
// show up as someone else's node by copying a heartbeat.
func TestMonitorHeartbeat_CopiedBindingIgnored(t *testing.T) {
	m := NewMonitor(DefaultConfig())
	node, helper, thief := testutil.NewKey(t), testutil.NewKey(t), testutil.NewKey(t)

	m.dispatchHeartbeat(context.Background(), heartbeat(helper.ID, node.Bind(t, helper.ID), "pi"), thief.ID, "0", "")

	if len(m.nodes) != 0 {
		t.Fatalf("a copied binding must not list any node, nodes = %v", m.nodes)
	}
}

func TestMonitorHeartbeat_UnboundSenderIgnored(t *testing.T) {
	m := NewMonitor(DefaultConfig())
	host := testutil.NewKey(t)

	m.dispatchHeartbeat(context.Background(), heartbeat(host.ID, nil, "old"), host.ID, "0", "")

	if len(m.nodes) != 0 {
		t.Fatalf("a heartbeat without binding must not list a node, nodes = %v", m.nodes)
	}
}

// Messages without binding (PINNED, LEAVE) count only once the helper's
// binding is known, and then for its node.
func TestMonitorResolve_UnboundAfterBinding(t *testing.T) {
	m := NewMonitor(DefaultConfig())
	node, helper := testutil.NewKey(t), testutil.NewKey(t)

	if _, ok := m.helpers.Resolve(helper.ID, nil); ok {
		t.Fatal("an unknown helper without binding must not resolve")
	}
	m.dispatchHeartbeat(context.Background(), heartbeat(helper.ID, node.Bind(t, helper.ID), "pi"), helper.ID, "0", "")
	if got, ok := m.helpers.Resolve(helper.ID, nil); !ok || got != node.ID {
		t.Fatalf("helper resolves to %v, %v; want %v", got, ok, node.ID)
	}
}

func TestMonitorPrune_ForgetsHelperMapping(t *testing.T) {
	m := NewMonitor(DefaultConfig())
	node, helper := testutil.NewKey(t), testutil.NewKey(t)
	m.dispatchHeartbeat(context.Background(), heartbeat(helper.ID, node.Bind(t, helper.ID), ""), helper.ID, "0", "")

	m.mu.Lock()
	m.forgetNodeLocked(node.ID.String())
	m.mu.Unlock()
	if _, ok := m.helpers.Resolve(helper.ID, nil); ok {
		t.Fatal("helper binding should go with its node")
	}
}

// A LEAVE counts for the node of the helper that sent it, so one helper
// cannot take another node out of its shard.
func TestMonitorLeave_CountsForSender(t *testing.T) {
	m := NewMonitor(DefaultConfig())
	ctx := context.Background()
	victim, victimHelper := testutil.NewKey(t), testutil.NewKey(t)
	other, otherHelper := testutil.NewKey(t), testutil.NewKey(t)
	m.dispatchHeartbeat(ctx, heartbeat(victimHelper.ID, victim.Bind(t, victimHelper.ID), ""), victimHelper.ID, "0", "")
	m.dispatchHeartbeat(ctx, heartbeat(otherHelper.ID, other.Bind(t, otherHelper.ID), ""), otherHelper.ID, "0", "")

	m.dispatchLeave(ctx, otherHelper.ID, "0")
	if got := m.nodes[victim.ID.String()].CurrentShard; got != "0" {
		t.Fatalf("victim's shard = %q, want it to stay in \"0\"", got)
	}
	if got := m.nodes[other.ID.String()].CurrentShard; got != "" {
		t.Fatalf("sender's shard = %q, want it to have left", got)
	}
}
