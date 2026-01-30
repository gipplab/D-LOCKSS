package signing

import (
	"context"
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

func (s *Signer) shouldEnforceSignatures() bool   { return config.SignatureMode == "strict" }
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

func (s *Signer) SignProtocolMessage(msg interface{}) error {
	nonce, err := common.NewNonce(config.NonceSize)
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
	if sender == "" {
		return fmt.Errorf("missing sender id")
	}
	if receivedFrom != "" && sender != receivedFrom {
		return fmt.Errorf("sender mismatch: sender=%s received_from=%s", sender.String(), receivedFrom.String())
	}
	if ts == 0 {
		return fmt.Errorf("missing timestamp")
	}
	now := time.Now()
	msgTime := time.Unix(ts, 0)
	if msgTime.After(now.Add(config.FutureSkewTolerance)) { // small future skew tolerance
		return fmt.Errorf("timestamp too far in future: %v", msgTime)
	}
	if now.Sub(msgTime) > config.SignatureMaxAge {
		return fmt.Errorf("message too old: age=%v", now.Sub(msgTime))
	}
	if len(nonce) < config.MinNonceSize {
		return fmt.Errorf("nonce too short")
	}
	if len(sig) == 0 {
		return fmt.Errorf("missing signature")
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
				log.Printf("[Sig] Warning: Peer %s not in peerstore (should be discovered via PubSub). Trying DHT as last resort...", sender.String()[:12])

				if s.dht != nil {
					addrInfo, err := s.dht.FindPeer(ctx, sender)
					if err == nil {
						s.h.Peerstore().AddAddrs(addrInfo.ID, addrInfo.Addrs, 10*time.Minute)
						addrs = addrInfo.Addrs
						log.Printf("[Sig] Found peer %s via DHT (unusual - should be via PubSub)", sender.String()[:12])
					} else {
						log.Printf("[Sig] DHT lookup failed for %s: %v", sender.String()[:12], err)
					}
				}
			}

			if len(addrs) > 0 {
				if err := s.h.Connect(ctx, peer.AddrInfo{ID: sender, Addrs: addrs}); err != nil {
					// Connection failed, but continue - connection attempt may have populated peerstore
					log.Printf("[Sig] Failed to connect to %s to fetch public key: %v", sender.String()[:12], err)
				}
			}
		}

		// Try again after potential connection
		pk = s.h.Peerstore().PubKey(sender)
		if pk == nil {
			return fmt.Errorf("missing public key for sender %s (peer should be discovered via PubSub)", sender.String()[:12])
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
	if s.nonceStore.SeenBefore(sender, nonce) {
		return fmt.Errorf("replay detected")
	}
	return nil
}

// verifySignedObject verifies a stored object signature (no replay protection).
// Useful for verifying ResearchObject manifests (which are content-addressed and immutable).
func (s *Signer) VerifySignedObject(sender peer.ID, ts int64, sig []byte, unsigned []byte) error {
	if s.signaturesDisabled() {
		return nil
	}
	if sender == "" {
		return fmt.Errorf("missing sender id")
	}
	if ts == 0 {
		return fmt.Errorf("missing timestamp")
	}
	now := time.Now()
	msgTime := time.Unix(ts, 0)
	if msgTime.After(now.Add(config.FutureSkewTolerance)) {
		return fmt.Errorf("timestamp too far in future: %v", msgTime)
	}
	if len(sig) == 0 {
		return fmt.Errorf("missing signature")
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

func (s *Signer) handleSignatureError(context string, err error) bool {
	// Returns true if caller should drop message
	if err == nil {
		return false
	}
	if s.shouldEnforceSignatures() {
		log.Printf("[Sig] Dropped %s: %v", context, err)
		return true
	}
	if s.shouldWarnOnBadSignatures() {
		log.Printf("[Sig] Warning: %s: %v", context, err)
	}
	return false
}

// VerifyAndAuthorizeMessage performs authorization and signature verification for a protocol message.
func (s *Signer) VerifyAndAuthorizeMessage(receivedFrom peer.ID, senderID peer.ID, timestamp int64, nonce []byte, sig []byte, marshalForSigning func() ([]byte, error), context string) bool {
	// Step 1: Authorize sender
	if err := s.trustMgr.AuthorizeIncomingSender(receivedFrom, senderID); err != nil {
		// metrics.messagesDropped is now tracked by caller or we need metrics injection
		// For now, logging. Caller should handle metrics.
		// incrementMetric(&metrics.messagesDropped)
		log.Printf("[Trust] Dropped %s: %v", context, err)
		return true
	}

	// Step 2: Marshal for signing
	unsigned, err := marshalForSigning()
	if err != nil {
		if s.handleSignatureError(context+" (marshal for signing)", err) {
			// incrementMetric(&metrics.messagesDropped)
			return true
		}
		return false
	}

	// Step 3: Verify signature
	if s.handleSignatureError(context, s.verifySignedMessage(receivedFrom, senderID, timestamp, nonce, sig, unsigned)) {
		// incrementMetric(&metrics.messagesDropped)
		return true
	}

	return false
}
