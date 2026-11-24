package schema

import (
	"encoding/base64"
	"errors"
	"strings"

	"github.com/libp2p/go-libp2p/core/peer"
)

// Binding is a Kubo node's signature, made with 'ipfs key sign' over
// BindingMessage(helper), naming the D-LOCKSS helper that acts for it.
type Binding struct {
	Node peer.ID
	Sig  []byte
}

// KuboSignPrefix is what 'ipfs key sign' signs in front of the message.
const KuboSignPrefix = "libp2p-key signed message:"

// ErrBindingSignature reports a binding that does not name the helper.
var ErrBindingSignature = errors.New("binding signature does not match")

// BindingMessage returns the text a node signs for its helper.
func BindingMessage(helper peer.ID) []byte {
	return []byte("dlockss-helper v1 " + helper.String())
}

// Verify checks that b binds helper to b.Node.
func (b *Binding) Verify(helper peer.ID) error {
	if !verifyKuboSignature(b.Node, BindingMessage(helper), b.Sig) {
		return ErrBindingSignature
	}
	return nil
}

// text returns b as "<node ID>:<signature>" for plain-text messages.
func (b *Binding) text() string {
	return b.Node.String() + ":" + base64.RawURLEncoding.EncodeToString(b.Sig)
}

// verifyKuboSignature checks a signature that 'ipfs key sign' made with key
// over msg.
func verifyKuboSignature(key peer.ID, msg, sig []byte) bool {
	return verifyRawSignature(key, append([]byte(KuboSignPrefix), msg...), sig)
}

// verifyRawSignature checks a signature made with key directly over msg.
func verifyRawSignature(key peer.ID, msg, sig []byte) bool {
	pub, err := key.ExtractPublicKey()
	if err != nil {
		return false
	}
	ok, err := pub.Verify(msg, sig)
	return err == nil && ok
}

// cutBinding splits "<node ID>:<signature>[:<rest>]", as written by text, off
// the front of s. b is nil and rest is s if s does not start with a binding.
func cutBinding(s string) (b *Binding, rest string) {
	parts := strings.SplitN(s, ":", 3)
	if len(parts) < 2 {
		return nil, s
	}
	node, err := peer.Decode(parts[0])
	if err != nil {
		return nil, s
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || len(sig) == 0 {
		return nil, s
	}
	if len(parts) == 3 {
		rest = parts[2]
	}
	return &Binding{Node: node, Sig: sig}, rest
}
