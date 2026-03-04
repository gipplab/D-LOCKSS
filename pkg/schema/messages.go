package schema

import (
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/libp2p/go-libp2p/core/peer"
)

// MessageType represents the type of a protocol message.
type MessageType uint8

const (
	MessageTypeIngest MessageType = iota + 1
	MessageTypeReplicationRequest
	MessageTypeUnreplicateRequest
)

// SignedEnvelope holds the fields common to every signed protocol message:
// type, manifest CID, sender, timestamp, nonce, and signature.
type SignedEnvelope struct {
	Type        MessageType
	ManifestCID cid.Cid
	SenderID    peer.ID
	Timestamp   int64
	Nonce       []byte
	Sig         []byte
}

// prefixFields returns the CBOR key-value pairs that appear before per-message fields.
func (e *SignedEnvelope) prefixFields() []cborKV {
	return []cborKV{
		{"type", int64(e.Type)},
		{"manifest_cid", e.ManifestCID.String()},
	}
}

// suffixFields returns the CBOR key-value pairs that appear after per-message fields.
func (e *SignedEnvelope) suffixFields(includeSig bool) []cborKV {
	fields := []cborKV{
		{"sender_id", e.SenderID.String()},
		{"ts", e.Timestamp},
		{"nonce", e.Nonce},
	}
	if includeSig {
		fields = append(fields, cborKV{"sig", e.Sig})
	}
	return fields
}

// unmarshalEnvelope reads the common fields from a decoded CBOR map node.
func (e *SignedEnvelope) unmarshalEnvelope(node datamodel.Node) error {
	typeInt, err := readInt(node, "type")
	if err != nil {
		return err
	}
	e.Type = MessageType(typeInt)

	e.ManifestCID, err = readCID(node, "manifest_cid")
	if err != nil {
		return err
	}
	e.SenderID, err = readPeerID(node, "sender_id")
	if err != nil {
		return err
	}
	e.Timestamp, err = readInt(node, "ts")
	if err != nil {
		return err
	}
	e.Nonce, err = readBytes(node, "nonce")
	if err != nil {
		return err
	}
	e.Sig, err = readBytes(node, "sig")
	if err != nil {
		return err
	}
	return nil
}

// Signable is implemented by all signed protocol messages.
// It provides uniform access to the envelope for signing and verification.
type Signable interface {
	GetEnvelope() *SignedEnvelope
	MarshalCBORForSigning() ([]byte, error)
}

// IngestMessage announces a new ResearchObject for ingestion.
type IngestMessage struct {
	SignedEnvelope
	ShardID  string `cbor:"shard_id"`  // Target shard prefix
	HintSize uint64 `cbor:"hint_size"` // Total size in bytes
}

func (m *IngestMessage) GetEnvelope() *SignedEnvelope { return &m.SignedEnvelope }

// ReplicationRequest asks peers to replicate a ResearchObject.
type ReplicationRequest struct {
	SignedEnvelope
	Priority uint8 `cbor:"priority"` // 0=Low, 1=High
	Deadline int64 `cbor:"deadline"` // Unix timestamp deadline (0 = no deadline)
}

func (m *ReplicationRequest) GetEnvelope() *SignedEnvelope { return &m.SignedEnvelope }

// UnreplicateRequest asks peers to drop over-replicated files.
// Peers use deterministic selection (hash of ManifestCID + PeerID) to decide
// whether to drop, ensuring distributed consensus without coordination.
type UnreplicateRequest struct {
	SignedEnvelope
	ExcessCount  int `cbor:"excess_count"`  // How many replicas to drop
	CurrentCount int `cbor:"current_count"` // Current replication count
}

func (m *UnreplicateRequest) GetEnvelope() *SignedEnvelope { return &m.SignedEnvelope }

// marshalFields builds a CBOR map from envelope prefix + message-specific + envelope suffix fields.
func marshalFields(env *SignedEnvelope, specific []cborKV, includeSig bool) ([]byte, error) {
	fields := env.prefixFields()
	fields = append(fields, specific...)
	fields = append(fields, env.suffixFields(includeSig)...)
	return marshalCBORMap(fields)
}

// --- IngestMessage CBOR ---

func (m *IngestMessage) MarshalCBOR() ([]byte, error) {
	return marshalFields(&m.SignedEnvelope, m.specificFields(), true)
}

func (m *IngestMessage) MarshalCBORForSigning() ([]byte, error) {
	return marshalFields(&m.SignedEnvelope, m.specificFields(), false)
}

func (m *IngestMessage) specificFields() []cborKV {
	return []cborKV{
		{"shard_id", m.ShardID},
		{"hint_size", int64(m.HintSize)},
	}
}

func (m *IngestMessage) UnmarshalCBOR(data []byte) error {
	node, err := decodeCBORMap(data)
	if err != nil {
		return err
	}
	if err := m.unmarshalEnvelope(node); err != nil {
		return err
	}
	m.ShardID, err = readString(node, "shard_id")
	if err != nil {
		return err
	}
	sizeInt, err := readInt(node, "hint_size")
	if err != nil {
		return err
	}
	m.HintSize = uint64(sizeInt)
	return nil
}

// --- ReplicationRequest CBOR ---

func (m *ReplicationRequest) MarshalCBOR() ([]byte, error) {
	return marshalFields(&m.SignedEnvelope, m.specificFields(), true)
}

func (m *ReplicationRequest) MarshalCBORForSigning() ([]byte, error) {
	return marshalFields(&m.SignedEnvelope, m.specificFields(), false)
}

func (m *ReplicationRequest) specificFields() []cborKV {
	return []cborKV{
		{"priority", int64(m.Priority)},
		{"deadline", m.Deadline},
	}
}

func (m *ReplicationRequest) UnmarshalCBOR(data []byte) error {
	node, err := decodeCBORMap(data)
	if err != nil {
		return err
	}
	if err := m.unmarshalEnvelope(node); err != nil {
		return err
	}
	priorityInt, err := readInt(node, "priority")
	if err != nil {
		return err
	}
	m.Priority = uint8(priorityInt)
	m.Deadline, err = readInt(node, "deadline")
	if err != nil {
		return err
	}
	return nil
}
