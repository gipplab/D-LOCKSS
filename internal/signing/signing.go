package signing

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"dlockss/internal/common"
	"dlockss/internal/config"
	"dlockss/internal/trust"
	"dlockss/pkg/schema"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

// errReplay is returned when a message nonce was already seen (replay attack).
// Callers must always drop on replay, regardless of SignatureMode.
var errReplay = errors.New("replay detected")

// maxNonceSize caps nonce allocation to prevent DoS via huge config values.
// 64 bytes is more than enough for replay entropy; verification requires effectiveMinNonceSize minimum.
const maxNonceSize = 64

// minNonceSizeFloor is the minimum nonce length we ever produce or accept.
// Prevents panic from make([]byte, n) with n <= 0 and ensures non-empty replay entropy.
const minNonceSizeFloor = 1

// effectiveMinNonceSize returns the minimum nonce length we accept in verification.
// Signing must use this same floor so we never produce nonces our verifier would reject.
func effectiveMinNonceSize() int {
	n := config.MinNonceSize
	if n < minNonceSizeFloor {
		n = minNonceSizeFloor
	}
	if n > maxNonceSize {
		return maxNonceSize
	}
	return n
}

// EffectiveNonceSizeForSigning returns the nonce length to use when signing (clamped to [effectiveMinNonceSize, maxNonceSize]).
// Callers that generate nonces for protocol messages (e.g. fallback signers) must use this so verifiers accept the nonce.
func EffectiveNonceSizeForSigning() int {
	n := config.NonceSize
	minSize := effectiveMinNonceSize()
	if n < minSize {
		n = minSize
	}
	if n > maxNonceSize {
		n = maxNonceSize
	}
	return n
}

// effectiveSignatureMaxAge returns the duration to use for message age checks.
// If config.SignatureMaxAge is zero or negative (misconfiguration), returns a safe default
// so we don't reject all messages; otherwise returns config.SignatureMaxAge.
func effectiveSignatureMaxAge() time.Duration {
	if config.SignatureMaxAge > 0 {
		return config.SignatureMaxAge
	}
	return 10 * time.Minute
}

// maxFutureSkewCap limits how far in the future we accept timestamps (prevents misconfiguration).
const maxFutureSkewCap = 5 * time.Minute

// effectiveFutureSkewTolerance returns the duration used for future timestamp tolerance.
// Caps config.FutureSkewTolerance so a bad config cannot accept messages far in the future.
func effectiveFutureSkewTolerance() time.Duration {
	d := config.FutureSkewTolerance
	if d <= 0 {
		return 30 * time.Second // default
	}
	if d > maxFutureSkewCap {
		return maxFutureSkewCap
	}
	return d
}

type Signer struct {
	h          host.Host
	privKey    crypto.PrivKey
	peerID     peer.ID
	nonceStore *common.NonceStore
	trustMgr   *trust.TrustManager
	dht        common.DHTProvider
}

func NewSigner(h host.Host, privKey crypto.PrivKey, peerID peer.ID, nonceStore *common.NonceStore, trustMgr *trust.TrustManager, dht common.DHTProvider) *Signer {
	return &Signer{
		h:          h,
		privKey:    privKey,
		peerID:     peerID,
		nonceStore: nonceStore,
		trustMgr:   trustMgr,
		dht:        dht,
	}
}

// Mode checks: "off" | "warn" | "strict". Unknown/typo values are treated as "strict" (fail closed).
func (s *Signer) shouldEnforceSignatures() bool {
	return config.SignatureMode == "strict" ||
		(config.SignatureMode != "off" && config.SignatureMode != "warn")
}
func (s *Signer) shouldWarnOnBadSignatures() bool { return config.SignatureMode == "warn" }
func (s *Signer) signaturesDisabled() bool        { return config.SignatureMode == "off" }

// signMessageEnvelope sets envelope fields (SenderID, Timestamp, Nonce) and computes signature.
// This helper reduces duplication in signProtocolMessage.
func (s *Signer) signMessageEnvelope(marshalForSigning func() ([]byte, error), setSig func([]byte)) error {
	if s.privKey == nil {
		return fmt.Errorf("missing self private key")
	}

	// Marshal for signing (signature should be nil/empty at this point)
	b, err := marshalForSigning()
	if err != nil {
		return err
	}

	// Sign the marshaled bytes
	sig, err := s.privKey.Sign(b)
	if err != nil {
		return err
	}

	// Set the signature
	setSig(sig)
	return nil
}

// SignProtocolMessage sets envelope fields (SenderID, Timestamp, Nonce), signs the message, and sets Sig.
// Caller must not mutate the message after signing; the signed payload is fixed at sign time.
func (s *Signer) SignProtocolMessage(msg interface{}) error {
	if msg == nil {
		return fmt.Errorf("message is nil")
	}
	// Single source of truth: verifier accepts nonces in [effectiveMinNonceSize(), maxNonceSize].
	nonce, err := common.NewNonce(EffectiveNonceSizeForSigning())
	if err != nil {
		return err
	}
	ts := time.Now().Unix()

	switch m := msg.(type) {
	case *schema.IngestMessage:
		m.SenderID = s.peerID
		m.Timestamp = ts
		m.Nonce = nonce
		m.Sig = nil
		return s.signMessageEnvelope(
			func() ([]byte, error) { return m.MarshalCBORForSigning() },
			func(sig []byte) { m.Sig = sig },
		)
	case *schema.ReplicationRequest:
		m.SenderID = s.peerID
		m.Timestamp = ts
		m.Nonce = nonce
		m.Sig = nil
		return s.signMessageEnvelope(
			func() ([]byte, error) { return m.MarshalCBORForSigning() },
			func(sig []byte) { m.Sig = sig },
		)
	case *schema.UnreplicateRequest:
		m.SenderID = s.peerID
		m.Timestamp = ts
		m.Nonce = nonce
		m.Sig = nil
		return s.signMessageEnvelope(
			func() ([]byte, error) { return m.MarshalCBORForSigning() },
			func(sig []byte) { m.Sig = sig },
		)
	default:
		return fmt.Errorf("unsupported message type for signing: %T", msg)
	}
}

func (s *Signer) verifySignedMessage(receivedFrom peer.ID, sender peer.ID, ts int64, nonce []byte, sig []byte, unsigned []byte) error {
	if s.signaturesDisabled() {
		return nil
	}
	if s.h == nil {
		return fmt.Errorf("signer host is nil")
	}
	if sender == "" {
		return fmt.Errorf("missing sender id")
	}
	// Bind sender to transport: when we know who sent the message, require envelope sender to match (prevents impersonation).
	if receivedFrom != "" && sender != receivedFrom {
		return fmt.Errorf("sender mismatch: sender=%s received_from=%s", sender.String(), receivedFrom.String())
	}
	// In strict mode, require transport binding so we never accept without knowing the actual sender.
	if s.shouldEnforceSignatures() && receivedFrom == "" {
		return fmt.Errorf("missing received_from for strict verification")
	}
	if ts == 0 {
		return fmt.Errorf("missing timestamp")
	}
	maxAge := effectiveSignatureMaxAge()
	now := time.Now()
	msgTime := time.Unix(ts, 0)
	if msgTime.After(now.Add(effectiveFutureSkewTolerance())) {
		return fmt.Errorf("timestamp too far in future: %v", msgTime)
	}
	if now.Sub(msgTime) > maxAge {
		return fmt.Errorf("message too old: age=%v", now.Sub(msgTime))
	}
	if len(nonce) < effectiveMinNonceSize() {
		return fmt.Errorf("nonce too short")
	}
	if len(nonce) > maxNonceSize {
		return fmt.Errorf("nonce too long")
	}
	if len(sig) == 0 {
		return fmt.Errorf("missing signature")
	}
	if len(unsigned) == 0 {
		return fmt.Errorf("empty message for verification")
	}

	pk := s.h.Peerstore().PubKey(sender)
	if pk == nil {
		// Peers should already be in peerstore from PubSub discovery
		// Only try to fetch if we're not connected and have no addresses
		if s.h.Network().Connectedness(sender) != network.Connected {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			// Get addresses from peerstore (should be populated by PubSub)
			addrs := s.h.Peerstore().Addrs(sender)

			// DHT lookup should be very rare - only if peer is truly not found via PubSub
			if len(addrs) == 0 {
				// This should rarely happen - peers are discovered via PubSub topics
				log.Printf("[Sig] Warning: Peer %s not in peerstore (should be discovered via PubSub). Trying DHT as last resort...", sender.String())

				if s.dht != nil {
					addrInfo, err := s.dht.FindPeer(ctx, sender)
					if err == nil {
						s.h.Peerstore().AddAddrs(addrInfo.ID, addrInfo.Addrs, 10*time.Minute)
						addrs = addrInfo.Addrs
						log.Printf("[Sig] Found peer %s via DHT (unusual - should be via PubSub)", sender.String())
					} else {
						log.Printf("[Sig] DHT lookup failed for %s: %v", sender.String(), err)
					}
				}
			}

			if len(addrs) > 0 {
				if err := s.h.Connect(ctx, peer.AddrInfo{ID: sender, Addrs: addrs}); err != nil {
					// Connection failed, but continue - connection attempt may have populated peerstore
					log.Printf("[Sig] Failed to connect to %s to fetch public key: %v", sender.String(), err)
				}
			}
		}

		// Try again after potential connection
		pk = s.h.Peerstore().PubKey(sender)
		if pk == nil {
			return fmt.Errorf("missing public key for sender %s (peer should be discovered via PubSub)", sender.String())
		}
		// Re-check age and future skew: message may have crossed validity window during DHT/connect
		now = time.Now()
		if msgTime.After(now.Add(effectiveFutureSkewTolerance())) {
			return fmt.Errorf("timestamp too far in future after key fetch: %v", msgTime)
		}
		if now.Sub(msgTime) > maxAge {
			return fmt.Errorf("message too old after key fetch: age=%v", now.Sub(msgTime))
		}
	}

	ok, err := pk.Verify(unsigned, sig)
	if err != nil {
		return fmt.Errorf("signature verify error: %w", err)
	}
	if !ok {
		return fmt.Errorf("invalid signature")
	}

	// Only record nonce after signature is valid (prevents trivial nonce-poisoning).
	if s.nonceStore == nil {
		return fmt.Errorf("nonce store missing: cannot perform replay check")
	}
	// Use a snapshot of the nonce for replay check so we are not affected by caller mutating the message buffer.
	nonceSnapshot := make([]byte, len(nonce))
	copy(nonceSnapshot, nonce)
	if s.nonceStore.SeenBefore(sender, nonceSnapshot) {
		return errReplay
	}
	return nil
}

// VerifySignedObject verifies a stored object signature (no replay protection).
// Useful for verifying ResearchObject manifests (which are content-addressed and immutable).
// Enforces max age so objects signed with rotated or compromised keys are rejected.
// Does not perform DHT lookup for missing keys; caller is expected to have the sender in peerstore (e.g. from a prior verified protocol message).
func (s *Signer) VerifySignedObject(sender peer.ID, ts int64, sig []byte, unsigned []byte) error {
	if s.signaturesDisabled() {
		return nil
	}
	if s.h == nil {
		return fmt.Errorf("signer host is nil")
	}
	if sender == "" {
		return fmt.Errorf("missing sender id")
	}
	if ts == 0 {
		return fmt.Errorf("missing timestamp")
	}
	now := time.Now()
	msgTime := time.Unix(ts, 0)
	if msgTime.After(now.Add(effectiveFutureSkewTolerance())) {
		return fmt.Errorf("timestamp too far in future: %v", msgTime)
	}
	if now.Sub(msgTime) > effectiveSignatureMaxAge() {
		return fmt.Errorf("object signature too old: age=%v", now.Sub(msgTime))
	}
	if len(sig) == 0 {
		return fmt.Errorf("missing signature")
	}
	if len(unsigned) == 0 {
		return fmt.Errorf("empty message for verification")
	}

	pk := s.h.Peerstore().PubKey(sender)
	if pk == nil {
		return fmt.Errorf("missing public key for sender %s", sender.String())
	}
	ok, err := pk.Verify(unsigned, sig)
	if err != nil {
		return fmt.Errorf("signature verify error: %w", err)
	}
	if !ok {
		return fmt.Errorf("invalid signature")
	}
	return nil
}

func (s *Signer) handleSignatureError(logContext string, err error) bool {
	// Returns true if caller should drop message
	if err == nil {
		return false
	}
	// Replay must always cause drop regardless of SignatureMode.
	if errors.Is(err, errReplay) {
		log.Printf("[Sig] Dropped %s: %v", logContext, err)
		return true
	}
	if s.shouldEnforceSignatures() {
		log.Printf("[Sig] Dropped %s: %v", logContext, err)
		return true
	}
	if s.shouldWarnOnBadSignatures() {
		log.Printf("[Sig] Warning: %s: %v", logContext, err)
	}
	return false
}

// VerifyAndAuthorizeMessage performs authorization and signature verification for a protocol message.
// Returns true if the caller should drop the message (auth failed, bad signature, or replay); false to accept.
func (s *Signer) VerifyAndAuthorizeMessage(receivedFrom peer.ID, senderID peer.ID, timestamp int64, nonce []byte, sig []byte, marshalForSigning func() ([]byte, error), logContext string) bool {
	// Step 1: Authorize sender (always required; we drop untrusted senders regardless of signature mode).
	if s.trustMgr == nil {
		log.Printf("[Trust] Dropped %s: trust manager missing", logContext)
		return true
	}
	if err := s.trustMgr.AuthorizeIncomingSender(receivedFrom, senderID); err != nil {
		log.Printf("[Trust] Dropped %s: %v", logContext, err)
		return true
	}

	// When signatures are off, accept after auth without marshaling or verifying (avoids unnecessary work).
	if s.signaturesDisabled() {
		return false
	}

	if marshalForSigning == nil {
		log.Printf("[Sig] Dropped %s: marshal function missing", logContext)
		return true
	}

	// Step 2: Marshal for signing
	unsigned, err := marshalForSigning()
	if err != nil {
		// Never accept when we cannot verify: marshal failure means we cannot check the signature.
		log.Printf("[Sig] Dropped %s (marshal for signing): %v", logContext, err)
		return true
	}

	// Step 3: Verify signature (and replay)
	if s.handleSignatureError(logContext, s.verifySignedMessage(receivedFrom, senderID, timestamp, nonce, sig, unsigned)) {
		return true
	}

	return false
}
