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
)

// SignedEnvelope holds the fields common to every signed protocol message:
// type, manifest CID, sender, timestamp, nonce, and signature. SenderID is the
// D-LOCKSS helper; NodeID and NodeSig are its Binding.
type SignedEnvelope struct {
	Type        MessageType
	ManifestCID cid.Cid
	SenderID    peer.ID
	NodeID      peer.ID
	NodeSig     []byte
	Timestamp   int64
	Nonce       []byte
	Sig         []byte
}

// Binding returns the sender's binding, or nil if the message has none.
func (e *SignedEnvelope) Binding() *Binding {
	if e.NodeID == "" {
		return nil
	}
	return &Binding{Node: e.NodeID, Sig: e.NodeSig}
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
	fields := []cborKV{{"sender_id", e.SenderID.String()}}
	if e.NodeID != "" {
		fields = append(fields, cborKV{"node_id", e.NodeID.String()}, cborKV{"node_sig", e.NodeSig})
	}
	fields = append(fields,
		cborKV{"ts", e.Timestamp},
		cborKV{"nonce", e.Nonce},
	)
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
	if _, err := node.LookupByString("node_id"); err == nil {
		if e.NodeID, err = readPeerID(node, "node_id"); err != nil {
			return err
		}
		if e.NodeSig, err = readBytes(node, "node_sig"); err != nil {
			return err
		}
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
	return err
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
	ShardID string `cbor:"shard_id"` // Target shard prefix
}

func (m *IngestMessage) GetEnvelope() *SignedEnvelope { return &m.SignedEnvelope }

// ReplicationRequest asks peers to replicate a ResearchObject.
type ReplicationRequest struct {
	SignedEnvelope
}

func (m *ReplicationRequest) GetEnvelope() *SignedEnvelope { return &m.SignedEnvelope }

// marshalFields builds a CBOR map from envelope prefix + message-specific + envelope suffix fields.
func marshalFields(env *SignedEnvelope, specific []cborKV, includeSig bool) ([]byte, error) {
	fields := env.prefixFields()
	fields = append(fields, specific...)
	fields = append(fields, env.suffixFields(includeSig)...)
	return marshalCBORMap(fields)
}

func (m *IngestMessage) MarshalCBOR() ([]byte, error) {
	return marshalFields(&m.SignedEnvelope, m.specificFields(), true)
}

func (m *IngestMessage) MarshalCBORForSigning() ([]byte, error) {
	return marshalFields(&m.SignedEnvelope, m.specificFields(), false)
}

func (m *IngestMessage) specificFields() []cborKV {
	return []cborKV{
		{"shard_id", m.ShardID},
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
	return err
}

func (m *ReplicationRequest) MarshalCBOR() ([]byte, error) {
	return marshalFields(&m.SignedEnvelope, m.specificFields(), true)
}

func (m *ReplicationRequest) MarshalCBORForSigning() ([]byte, error) {
	return marshalFields(&m.SignedEnvelope, m.specificFields(), false)
}

func (m *ReplicationRequest) specificFields() []cborKV {
	return nil
}

func (m *ReplicationRequest) UnmarshalCBOR(data []byte) error {
	node, err := decodeCBORMap(data)
	if err != nil {
		return err
	}
	return m.unmarshalEnvelope(node)
}
