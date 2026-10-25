// Package protocol implements message authentication for the D-LOCKSS wire
// protocol: signing and verification of CBOR messages, nonce-based replay
// protection, per-peer rate limiting, and the plain-text shard messages.
package protocol

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/internal/schema"
	"dlockss/internal/trust"
)

// Verification parameters (frozen). D-LOCKSS identities are Ed25519, whose
// peer IDs embed the public key, so no key fetching or peer dialing is ever
// needed to verify a signature.
const (
	signatureMaxAge = 10 * time.Minute
	futureSkew      = 30 * time.Second
	nonceSize       = 16
	minNonceSize    = 8
	maxNonceSize    = 64
)

// Signer signs outgoing protocol messages and verifies incoming ones.
// Verification is strict: any failure drops the message.
type Signer struct {
	self   peer.ID
	key    crypto.PrivKey
	trust  *trust.Manager
	nonces *nonceStore
}

// NewSigner builds a Signer. key may be nil (outgoing messages are then
// unsigned and will be dropped by strict peers — degraded setups only).
func NewSigner(self peer.ID, key crypto.PrivKey, tm *trust.Manager) *Signer {
	return &Signer{
		self:   self,
		key:    key,
		trust:  tm,
		nonces: newNonceStore(signatureMaxAge),
	}
}

// Sign fills the envelope (sender, timestamp, nonce) and signs the message.
func (s *Signer) Sign(msg schema.Message) error {
	env := msg.Envelope()
	env.SenderID = s.self
	env.Timestamp = time.Now().Unix()
	env.Nonce = make([]byte, nonceSize)
	if _, err := rand.Read(env.Nonce); err != nil {
		return fmt.Errorf("generating nonce: %w", err)
	}
	env.Sig = nil
	if s.key == nil {
		slog.Warn("no private key: sending unsigned protocol message")
		return nil
	}
	unsigned, err := msg.MarshalCBORForSigning()
	if err != nil {
		return err
	}
	env.Sig, err = s.key.Sign(unsigned)
	return err
}

// ShouldDrop verifies an incoming message and reports whether to drop it.
// receivedFrom is the authenticated pubsub originator.
func (s *Signer) ShouldDrop(_ context.Context, msg schema.Message, receivedFrom peer.ID) bool {
	if s.trust == nil {
		return true
	}
	env := msg.Envelope()
	if err := s.trust.AuthorizeSender(env.SenderID, receivedFrom); err != nil {
		slog.Debug("dropping message: sender not authorized", "err", err)
		return true
	}
	if err := s.verify(msg); err != nil {
		slog.Debug("dropping message: verification failed", "sender", env.SenderID, "err", err)
		return true
	}
	return false
}

// verify checks fields, timestamp window, the Ed25519 signature (key
// extracted from the sender's peer ID), and finally replay. The nonce is
// recorded only after the signature verifies, so attackers cannot poison the
// replay store.
func (s *Signer) verify(msg schema.Message) error {
	env := msg.Envelope()
	if env.Timestamp == 0 {
		return fmt.Errorf("missing timestamp")
	}
	if n := len(env.Nonce); n < minNonceSize || n > maxNonceSize {
		return fmt.Errorf("nonce length %d outside [%d, %d]", n, minNonceSize, maxNonceSize)
	}
	if len(env.Sig) == 0 {
		return fmt.Errorf("missing signature")
	}
	now := time.Now().Unix()
	if env.Timestamp > now+int64(futureSkew.Seconds()) {
		return fmt.Errorf("timestamp %d too far in the future", env.Timestamp)
	}
	if now-env.Timestamp > int64(signatureMaxAge.Seconds()) {
		return fmt.Errorf("message too old (%ds)", now-env.Timestamp)
	}

	pub, err := env.SenderID.ExtractPublicKey()
	if err != nil || pub == nil {
		return fmt.Errorf("sender key not extractable (non-Ed25519 identity?): %v", err)
	}
	unsigned, err := msg.MarshalCBORForSigning()
	if err != nil || len(unsigned) == 0 {
		return fmt.Errorf("re-encoding for verification: %v", err)
	}
	ok, err := pub.Verify(unsigned, env.Sig)
	if err != nil || !ok {
		return fmt.Errorf("invalid signature")
	}
	if s.nonces.seenBefore(env.SenderID, env.Nonce) {
		return fmt.Errorf("nonce replay")
	}
	return nil
}

// SignManifest signs a ResearchObject in place. A nil key leaves it unsigned
// with a warning (the manifest is still stored).
func (s *Signer) SignManifest(ro *schema.ResearchObject) error {
	if s.key == nil {
		slog.Warn("no private key: storing unsigned manifest")
		return nil
	}
	unsigned, err := ro.MarshalCBORForSigning()
	if err != nil {
		return err
	}
	ro.Signature, err = s.key.Sign(unsigned)
	return err
}
