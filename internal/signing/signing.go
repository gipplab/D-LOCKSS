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

const (
	maxNonceSize      = 64
	minNonceSizeFloor = 1

	defaultSignatureMaxAge     = 10 * time.Minute
	defaultFutureSkewTolerance = 30 * time.Second
	maxFutureSkewCap           = 5 * time.Minute
)

// Signer signs outgoing protocol messages and verifies incoming ones
// according to the configured signature mode.
type Signer struct {
	cfg     *config.Config
	h       host.Host
	privKey crypto.PrivKey
	peerID  peer.ID
	binding *schema.Binding
	nonces  *nonceStore
	nodes   *trust.Nodes
	dht     common.DHTProvider
}

// SignerConfig holds all dependencies for a Signer.
type SignerConfig struct {
	Cfg     *config.Config
	Host    host.Host
	PrivKey crypto.PrivKey
	PeerID  peer.ID
	// Binding is this helper's binding, sent with every message.
	Binding *schema.Binding
	Nodes   *trust.Nodes
	DHT     common.DHTProvider
}

func NewSigner(cfg SignerConfig) *Signer {
	return &Signer{
		cfg:     cfg.Cfg,
		h:       cfg.Host,
		privKey: cfg.PrivKey,
		peerID:  cfg.PeerID,
		binding: cfg.Binding,
		nonces:  newNonceStore(),
		nodes:   cfg.Nodes,
		dht:     cfg.DHT,
	}
}

func (s *Signer) effectiveMinNonceSize() int {
	return min(max(s.cfg.Security.MinNonceSize, minNonceSizeFloor), maxNonceSize)
}

func (s *Signer) effectiveNonceSizeForSigning() int {
	return min(max(s.cfg.Security.NonceSize, s.effectiveMinNonceSize()), maxNonceSize)
}

func (s *Signer) effectiveSignatureMaxAge() time.Duration {
	if s.cfg.Security.SignatureMaxAge > 0 {
		return s.cfg.Security.SignatureMaxAge
	}
	return defaultSignatureMaxAge
}

func (s *Signer) effectiveFutureSkewTolerance() time.Duration {
	d := s.cfg.Security.FutureSkewTolerance
	if d <= 0 {
		return defaultFutureSkewTolerance
	}
	return min(d, maxFutureSkewCap)
}

// effectiveNonceTTL matches the signature max age: a nonce evicted earlier
// could be replayed while its message is still fresh enough to be accepted.
func (s *Signer) effectiveNonceTTL() time.Duration {
	return s.effectiveSignatureMaxAge()
}

// shouldEnforceSignatures is true for "strict" and for any unrecognised mode,
// so a misconfigured mode fails closed.
func (s *Signer) shouldEnforceSignatures() bool {
	mode := s.cfg.Security.SignatureMode
	return mode != "off" && mode != "warn"
}
func (s *Signer) shouldWarnOnBadSignatures() bool { return s.cfg.Security.SignatureMode == "warn" }
func (s *Signer) signaturesDisabled() bool        { return s.cfg.Security.SignatureMode == "off" }

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

// SignProtocolMessage stamps msg's envelope with this node's sender ID, the
// current time and a fresh nonce, then signs it.
func (s *Signer) SignProtocolMessage(msg schema.Signable) error {
	if msg == nil {
		return fmt.Errorf("message is nil")
	}
	nonce, err := newNonce(s.effectiveNonceSizeForSigning())
	if err != nil {
		return err
	}
	env := msg.GetEnvelope()
	env.SenderID = s.peerID
	if s.binding != nil {
		env.NodeID, env.NodeSig = s.binding.Node, s.binding.Sig
	}
	env.Timestamp = time.Now().Unix()
	env.Nonce = nonce
	env.Sig = nil
	return s.signMessageEnvelope(
		msg.MarshalCBORForSigning,
		func(sig []byte) { env.Sig = sig },
	)
}

func (s *Signer) verifySignedMessage(receivedFrom, sender peer.ID, ts int64, nonce, sig, unsigned []byte) error {
	if s.signaturesDisabled() {
		return nil
	}
	if err := s.validateMessageFields(receivedFrom, sender, ts, nonce, sig, unsigned); err != nil {
		return err
	}

	pk := s.h.Peerstore().PubKey(sender)
	if pk == nil {
		pk = s.fetchPublicKey(sender)
		if pk == nil {
			return fmt.Errorf("missing public key for sender %s", sender.String())
		}
	}

	// Read the clock after the key lookup, which may block on the network.
	if err := s.checkTimestamp(time.Unix(ts, 0), time.Now(), s.effectiveSignatureMaxAge()); err != nil {
		return err
	}
	if err := verifySignatureBytes(pk, unsigned, sig); err != nil {
		return err
	}

	if s.nonces == nil {
		return fmt.Errorf("nonce store missing")
	}
	if s.nonces.seenBefore(sender, nonce, s.effectiveNonceTTL()) {
		return errReplay
	}
	return nil
}

func (s *Signer) validateMessageFields(receivedFrom, sender peer.ID, ts int64, nonce, sig, unsigned []byte) error {
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
	return nil
}

func (s *Signer) checkTimestamp(msgTime, now time.Time, maxAge time.Duration) error {
	if msgTime.After(now.Add(s.effectiveFutureSkewTolerance())) {
		return fmt.Errorf("timestamp too far in future: %v", msgTime)
	}
	if now.Sub(msgTime) > maxAge {
		return fmt.Errorf("message too old: age=%v", now.Sub(msgTime))
	}
	return nil
}

// fetchPublicKey connects to an unconnected sender, looking up its addresses
// in the DHT if needed, so the identify protocol can fill in its public key.
func (s *Signer) fetchPublicKey(sender peer.ID) crypto.PubKey {
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
	return s.h.Peerstore().PubKey(sender)
}

func verifySignatureBytes(pk crypto.PubKey, payload, sig []byte) error {
	ok, err := pk.Verify(payload, sig)
	if err != nil {
		return fmt.Errorf("signature verify error: %w", err)
	}
	if !ok {
		return fmt.Errorf("invalid signature")
	}
	return nil
}

// handleSignatureError logs a verification failure and reports whether the
// message must be dropped. Replays are dropped in every mode.
func (s *Signer) handleSignatureError(logContext string, err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, errReplay) || s.shouldEnforceSignatures() {
		slog.Warn("message dropped", "context", logContext, "error", err)
		return true
	}
	if s.shouldWarnOnBadSignatures() {
		slog.Warn("signature verification failed", "context", logContext, "error", err)
	}
	return false
}

// Authenticate verifies msg, received from receivedFrom, and returns the node
// its sender acts for. ok is false if msg must be dropped.
func (s *Signer) Authenticate(receivedFrom peer.ID, msg schema.Signable, logContext string) (node peer.ID, ok bool) {
	if s.nodes == nil {
		slog.Warn("message dropped: node resolver missing", "context", logContext)
		return "", false
	}
	env := msg.GetEnvelope()
	if env.SenderID == "" {
		slog.Warn("message dropped: missing sender_id", "context", logContext)
		return "", false
	}
	if receivedFrom != "" && env.SenderID != receivedFrom {
		slog.Warn("message dropped: sender_id mismatch", "context", logContext, "sender_id", env.SenderID, "received_from", receivedFrom)
		return "", false
	}
	// The signature covers the binding. In strict mode a bad signature drops
	// msg before its binding is remembered.
	if !s.signaturesDisabled() {
		unsigned, err := msg.MarshalCBORForSigning()
		if err != nil {
			slog.Warn("message dropped: marshal failed", "context", logContext, "error", err)
			return "", false
		}
		if s.handleSignatureError(logContext, s.verifySignedMessage(receivedFrom, env.SenderID, env.Timestamp, env.Nonce, env.Sig, unsigned)) {
			return "", false
		}
	}
	node, ok = s.nodes.Resolve(env.SenderID, env.Binding())
	if !ok {
		slog.Warn("message dropped: sender's node not accepted", "context", logContext, "sender_id", env.SenderID)
		return "", false
	}
	return node, true
}
