package trust

import (
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/internal/testutil"
)

func TestNodes_Resolve_Binding(t *testing.T) {
	n := NewNodes(NewTrustManager("open"))
	node, helper := testutil.NewKey(t), testutil.NewKey(t)

	// A sender without a binding does not count, even in open mode.
	if got, ok := n.Resolve(helper.ID, nil); ok {
		t.Fatalf("unbound sender: got %v ok=%v", got, ok)
	}

	b := node.Bind(t, helper.ID)
	got, ok := n.Resolve(helper.ID, b)
	if !ok || got != node.ID {
		t.Fatalf("bound: got %v ok=%v want %v", got, ok, node.ID)
	}
	got, ok = n.Resolve(helper.ID, nil)
	if !ok || got != node.ID {
		t.Fatalf("cached: got %v ok=%v", got, ok)
	}

	if _, ok := n.Resolve(testutil.NewKey(t).ID, b); ok {
		t.Fatal("binding copied to another helper must fail")
	}
}

func TestNodes_Allowlist(t *testing.T) {
	tm := NewTrustManager("allowlist")
	node, other := testutil.NewKey(t), testutil.NewKey(t)
	helper, helper2 := testutil.NewKey(t), testutil.NewKey(t)
	tm.trustedPeers.Add(node.ID)

	n := NewNodes(tm)
	if _, ok := n.Resolve(helper.ID, node.Bind(t, helper.ID)); !ok {
		t.Fatal("helper of allowlisted node should pass")
	}
	if _, ok := n.Resolve(helper2.ID, other.Bind(t, helper2.ID)); ok {
		t.Fatal("helper of non-allowlisted node should fail")
	}
	if _, ok := n.Resolve(testutil.NewKey(t).ID, nil); ok {
		t.Fatal("unbound sender not on allowlist should fail")
	}
	if _, ok := n.Resolve(node.ID, nil); ok {
		t.Fatal("unbound sender must fail even if its ID is on the allowlist")
	}
}

func TestNodes_OnBind(t *testing.T) {
	n := NewNodes(NewTrustManager("open"))
	node, helper := testutil.NewKey(t), testutil.NewKey(t)
	var sawHelper, sawNode peer.ID
	n.OnBind(func(h, nd peer.ID) { sawHelper, sawNode = h, nd })
	if _, ok := n.Resolve(helper.ID, node.Bind(t, helper.ID)); !ok {
		t.Fatal("resolve")
	}
	if sawHelper != helper.ID || sawNode != node.ID {
		t.Fatalf("OnBind: helper=%v node=%v", sawHelper, sawNode)
	}
}
