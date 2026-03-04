package signing

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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

func (s *Signer) effectiveMinNonceSize() int {
	n := s.cfg.MinNonceSize
	if n < minNonceSizeFloor {
		n = minNonceSizeFloor
	}
	if n > maxNonceSize {
		return maxNonceSize
	}
	return n
}

func (s *Signer) effectiveNonceSizeForSigning() int {
	n := s.cfg.NonceSize
	minSize := s.effectiveMinNonceSize()
	if n < minSize {
		n = minSize
	}
	if n > maxNonceSize {
		n = maxNonceSize
	}
	return n
}

func (s *Signer) effectiveSignatureMaxAge() time.Duration {
	if s.cfg.SignatureMaxAge > 0 {
		return s.cfg.SignatureMaxAge
	}
	return 10 * time.Minute
}

const maxFutureSkewCap = 5 * time.Minute

func (s *Signer) effectiveFutureSkewTolerance() time.Duration {
	d := s.cfg.FutureSkewTolerance
	if d <= 0 {
		return 30 * time.Second
	}
	if d > maxFutureSkewCap {
		return maxFutureSkewCap
	}
	return d
}

func (s *Signer) effectiveNonceTTL() time.Duration {
	ttl := s.cfg.SignatureMaxAge
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	return ttl
}

type Signer struct {
	cfg      *config.Config
	h        host.Host
	privKey  crypto.PrivKey
	peerID   peer.ID
	nonces   *nonceStore
	trustMgr *trust.TrustManager
	dht      common.DHTProvider
}

// SignerConfig holds all dependencies for a Signer.
type SignerConfig struct {
	Cfg      *config.Config
	Host     host.Host
	PrivKey  crypto.PrivKey
	PeerID   peer.ID
	TrustMgr *trust.TrustManager
	DHT      common.DHTProvider
}

func NewSigner(cfg SignerConfig) *Signer {
	return &Signer{
		cfg:      cfg.Cfg,
		h:        cfg.Host,
		privKey:  cfg.PrivKey,
		peerID:   cfg.PeerID,
		nonces:   newNonceStore(),
		trustMgr: cfg.TrustMgr,
		dht:      cfg.DHT,
	}
}

func (s *Signer) shouldEnforceSignatures() bool {
	return s.cfg.SignatureMode == "strict" ||
		(s.cfg.SignatureMode != "off" && s.cfg.SignatureMode != "warn")
}
func (s *Signer) shouldWarnOnBadSignatures() bool { return s.cfg.SignatureMode == "warn" }
func (s *Signer) signaturesDisabled() bool        { return s.cfg.SignatureMode == "off" }

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

func (s *Signer) SignProtocolMessage(msg schema.Signable) error {
	if msg == nil {
		return fmt.Errorf("message is nil")
	}
	nonce, err := common.NewNonce(s.effectiveNonceSizeForSigning())
	if err != nil {
		return err
	}
	env := msg.GetEnvelope()
	env.SenderID = s.peerID
	env.Timestamp = time.Now().Unix()
	env.Nonce = nonce
	env.Sig = nil
	return s.signMessageEnvelope(
		msg.MarshalCBORForSigning,
		func(sig []byte) { env.Sig = sig },
	)
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
	maxAge := s.effectiveSignatureMaxAge()
	now := time.Now()
	msgTime := time.Unix(ts, 0)
	if msgTime.After(now.Add(s.effectiveFutureSkewTolerance())) {
		return fmt.Errorf("timestamp too far in future: %v", msgTime)
	}
	if now.Sub(msgTime) > maxAge {
		return fmt.Errorf("message too old: age=%v", now.Sub(msgTime))
	}
	if len(nonce) < s.effectiveMinNonceSize() {
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
		if msgTime.After(now.Add(s.effectiveFutureSkewTolerance())) {
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

	if s.nonces == nil {
		return fmt.Errorf("nonce store missing")
	}
	nonceSnapshot := make([]byte, len(nonce))
	copy(nonceSnapshot, nonce)
	if s.nonces.seenBefore(sender, nonceSnapshot, s.effectiveNonceTTL()) {
		return errReplay
	}
	return nil
}

func (s *Signer) handleSignatureError(logContext string, err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, errReplay) {
		slog.Warn("message dropped", "context", logContext, "error", err)
		return true
	}
	if s.shouldEnforceSignatures() {
		slog.Warn("message dropped", "context", logContext, "error", err)
		return true
	}
	if s.shouldWarnOnBadSignatures() {
		slog.Warn("signature verification failed", "context", logContext, "error", err)
	}
	return false
}

// ShouldDropMessage returns true if the message should be dropped (auth/signature failed).
func (s *Signer) ShouldDropMessage(receivedFrom peer.ID, senderID peer.ID, timestamp int64, nonce []byte, sig []byte, marshalForSigning func() ([]byte, error), logContext string) bool {
	if s.trustMgr == nil {
		slog.Warn("message dropped: trust manager missing", "context", logContext)
		return true
	}
	if err := s.trustMgr.AuthorizeIncomingSender(receivedFrom, senderID); err != nil {
		slog.Warn("message dropped", "context", logContext, "error", err)
		return true
	}

	if s.signaturesDisabled() {
		return false
	}
	if marshalForSigning == nil {
		slog.Warn("message dropped: marshal function missing", "context", logContext)
		return true
	}
	unsigned, err := marshalForSigning()
	if err != nil {
		slog.Warn("message dropped: marshal failed", "context", logContext, "error", err)
		return true
	}
	if s.handleSignatureError(logContext, s.verifySignedMessage(receivedFrom, senderID, timestamp, nonce, sig, unsigned)) {
		return true
	}

	return false
}
