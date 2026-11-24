package trust

import (
	"sync"

	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/pkg/schema"
)

// Nodes maps D-LOCKSS helpers to the Kubo nodes they act for, using the
// bindings the helpers present, and decides which nodes' messages count.
type Nodes struct {
	tm *TrustManager

	mu     sync.RWMutex
	bound  map[peer.ID]peer.ID
	onBind func(helper, node peer.ID)
}

func NewNodes(tm *TrustManager) *Nodes {
	return &Nodes{tm: tm, bound: make(map[peer.ID]peer.ID)}
}

// OnBind sets fn to run when a helper's binding to an accepted node first
// verifies.
func (n *Nodes) OnBind(fn func(helper, node peer.ID)) {
	n.mu.Lock()
	n.onBind = fn
	n.mu.Unlock()
}

// Resolve returns the node that helper acts for. A valid binding b is
// remembered, so later messages without one (LEAVE, PINNED) resolve too. ok
// is false without a valid binding, or if the node is not on the allowlist in
// allowlist mode.
func (n *Nodes) Resolve(helper peer.ID, b *schema.Binding) (node peer.ID, ok bool) {
	n.mu.RLock()
	known, isBound := n.bound[helper]
	n.mu.RUnlock()

	if b == nil {
		if !isBound {
			return "", false
		}
		return known, n.tm.acceptsNode(known)
	}
	if !isBound || known != b.Node {
		if b.Verify(helper) != nil {
			return "", false
		}
		n.mu.Lock()
		n.bound[helper] = b.Node
		onBind := n.onBind
		n.mu.Unlock()
		if onBind != nil && n.tm.acceptsNode(b.Node) {
			onBind(helper, b.Node)
		}
	}
	return b.Node, n.tm.acceptsNode(b.Node)
}

// Forget drops the binding remembered for helper.
func (n *Nodes) Forget(helper peer.ID) {
	n.mu.Lock()
	delete(n.bound, helper)
	n.mu.Unlock()
}
