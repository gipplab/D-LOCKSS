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

var errReplay = errors.New("replay detected")

const maxNonceSize = 64
const minNonceSizeFloor = 1

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

func effectiveSignatureMaxAge() time.Duration {
	if config.SignatureMaxAge > 0 {
		return config.SignatureMaxAge
	}
	return 10 * time.Minute
}

const maxFutureSkewCap = 5 * time.Minute

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

func (s *Signer) shouldEnforceSignatures() bool {
	return config.SignatureMode == "strict" ||
		(config.SignatureMode != "off" && config.SignatureMode != "warn")
}
func (s *Signer) shouldWarnOnBadSignatures() bool { return config.SignatureMode == "warn" }
func (s *Signer) signaturesDisabled() bool        { return config.SignatureMode == "off" }

func (s *Signer) signMessageEnvelope(marshalForSigning func() ([]byte, error), setSig func([]byte)) error {
	if s.privKey == nil {
		return fmt.Errorf("missing self private key")
	}

	b, err := marshalForSigning()
	if err != nil {
		return err
	}
	sig, err := s.privKey.Sign(b)
	if err != nil {
		return err
	}
	setSig(sig)
	return nil
}

func (s *Signer) SignProtocolMessage(msg interface{}) error {
	if msg == nil {
		return fmt.Errorf("message is nil")
	}
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
	if receivedFrom != "" && sender != receivedFrom {
		return fmt.Errorf("sender mismatch: sender=%s received_from=%s", sender.String(), receivedFrom.String())
	}
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
		if s.h.Network().Connectedness(sender) != network.Connected {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			addrs := s.h.Peerstore().Addrs(sender)
			if len(addrs) == 0 && s.dht != nil {
				addrInfo, err := s.dht.FindPeer(ctx, sender)
				if err == nil {
					s.h.Peerstore().AddAddrs(addrInfo.ID, addrInfo.Addrs, 10*time.Minute)
					addrs = addrInfo.Addrs
				}
			}
			if len(addrs) > 0 {
				_ = s.h.Connect(ctx, peer.AddrInfo{ID: sender, Addrs: addrs})
			}
		}
		pk = s.h.Peerstore().PubKey(sender)
		if pk == nil {
			return fmt.Errorf("missing public key for sender %s", sender.String())
		}
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

	if s.nonceStore == nil {
		return fmt.Errorf("nonce store missing")
	}
	nonceSnapshot := make([]byte, len(nonce))
	copy(nonceSnapshot, nonce)
	if s.nonceStore.SeenBefore(sender, nonceSnapshot) {
		return errReplay
	}
	return nil
}

// VerifySignedObject verifies a stored object signature (no replay). No DHT lookup.
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
	if err == nil {
		return false
	}
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

// ShouldDropMessage returns true if the message should be dropped (auth/signature failed).
func (s *Signer) ShouldDropMessage(receivedFrom peer.ID, senderID peer.ID, timestamp int64, nonce []byte, sig []byte, marshalForSigning func() ([]byte, error), logContext string) bool {
	if s.trustMgr == nil {
		log.Printf("[Trust] Dropped %s: trust manager missing", logContext)
		return true
	}
	if err := s.trustMgr.AuthorizeIncomingSender(receivedFrom, senderID); err != nil {
		log.Printf("[Trust] Dropped %s: %v", logContext, err)
		return true
	}

	if s.signaturesDisabled() {
		return false
	}
	if marshalForSigning == nil {
		log.Printf("[Sig] Dropped %s: marshal function missing", logContext)
		return true
	}
	unsigned, err := marshalForSigning()
	if err != nil {
		log.Printf("[Sig] Dropped %s (marshal): %v", logContext, err)
		return true
	}
	if s.handleSignatureError(logContext, s.verifySignedMessage(receivedFrom, senderID, timestamp, nonce, sig, unsigned)) {
		return true
	}

	return false
}

// VerifyAndAuthorizeMessage returns true to drop (auth/signature failed), false to accept.
// Deprecated: use ShouldDropMessage for clearer semantics.
func (s *Signer) VerifyAndAuthorizeMessage(receivedFrom peer.ID, senderID peer.ID, timestamp int64, nonce []byte, sig []byte, marshalForSigning func() ([]byte, error), logContext string) bool {
	return s.ShouldDropMessage(receivedFrom, senderID, timestamp, nonce, sig, marshalForSigning, logContext)
}
