package schema

import (
	"bytes"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/libp2p/go-libp2p/core/peer"
)

// MessageType represents the type of a protocol message
type MessageType uint8

const (
	MessageTypeIngest MessageType = iota + 1
	MessageTypeReplicationRequest
	MessageTypeDelegate
	MessageTypeUnreplicateRequest
)

// IngestMessage replaces the old "NEW:<hash>" string format.
// It contains metadata about a new ResearchObject being announced.
type IngestMessage struct {
	Type       MessageType `cbor:"type"`        // Always MessageTypeIngest
	ManifestCID cid.Cid   `cbor:"manifest_cid"` // The CID of the ResearchObject (root of graph)
	ShardID    string     `cbor:"shard_id"`     // Target shard prefix
	HintSize   uint64     `cbor:"hint_size"`    // Total size in bytes (for immediate disk-usage checks)
	SenderID   peer.ID    `cbor:"sender_id"`    // PeerID of the sender
	Timestamp  int64      `cbor:"ts"`           // Unix timestamp
	Nonce      []byte     `cbor:"nonce"`        // Random nonce for replay protection
	Sig        []byte     `cbor:"sig"`          // Signature over canonical CBOR (excluding sig)
}

// ReplicationRequest replaces the old "NEED:<hash>" string format.
// It requests replication of a ResearchObject.
type ReplicationRequest struct {
	Type       MessageType `cbor:"type"`        // Always MessageTypeReplicationRequest
	ManifestCID cid.Cid   `cbor:"manifest_cid"` // The CID of the ResearchObject to replicate
	Priority   uint8       `cbor:"priority"`    // 0=Low, 1=High
	Deadline   int64       `cbor:"deadline"`     // Unix timestamp deadline (0 = no deadline)
	SenderID   peer.ID     `cbor:"sender_id"`    // PeerID of the sender
	Timestamp  int64       `cbor:"ts"`           // Unix timestamp
	Nonce      []byte      `cbor:"nonce"`        // Random nonce for replay protection
	Sig        []byte      `cbor:"sig"`          // Signature over canonical CBOR (excluding sig)
}

// DelegateMessage replaces the old "DELEGATE:<hash>:<prefix>" string format.
// It delegates responsibility for a ResearchObject to a specific shard.
type DelegateMessage struct {
	Type       MessageType `cbor:"type"`        // Always MessageTypeDelegate
	ManifestCID cid.Cid    `cbor:"manifest_cid"` // The CID of the ResearchObject
	TargetShard string     `cbor:"target_shard"`  // Target shard prefix
	SenderID   peer.ID     `cbor:"sender_id"`     // PeerID of the sender
	Timestamp  int64       `cbor:"ts"`            // Unix timestamp
	Nonce      []byte      `cbor:"nonce"`         // Random nonce for replay protection
	Sig        []byte      `cbor:"sig"`           // Signature over canonical CBOR (excluding sig)
}

// UnreplicateRequest requests peers to drop over-replicated files.
// Peers use deterministic selection (hash of ManifestCID + PeerID) to decide
// if they should drop the file, ensuring distributed consensus without coordination.
type UnreplicateRequest struct {
	Type        MessageType `cbor:"type"`         // Always MessageTypeUnreplicateRequest
	ManifestCID cid.Cid     `cbor:"manifest_cid"` // The CID of the ResearchObject to drop
	ExcessCount int         `cbor:"excess_count"` // How many replicas need to be dropped (count - MaxReplication)
	CurrentCount int        `cbor:"current_count"` // Current replication count (for verification)
	SenderID    peer.ID     `cbor:"sender_id"`    // PeerID of the sender
	Timestamp   int64       `cbor:"ts"`          // Unix timestamp
	Nonce       []byte      `cbor:"nonce"`        // Random nonce for replay protection
	Sig         []byte      `cbor:"sig"`          // Signature over canonical CBOR (excluding sig)
}

// MarshalCBOR serializes an IngestMessage to CBOR format.
func (m *IngestMessage) MarshalCBOR() ([]byte, error) {
	return m.marshalCBOR(true)
}

// MarshalCBORForSigning serializes the message without the signature field.
func (m *IngestMessage) MarshalCBORForSigning() ([]byte, error) {
	return m.marshalCBOR(false)
}

func (m *IngestMessage) marshalCBOR(includeSig bool) ([]byte, error) {
	nb := basicnode.Prototype.Map.NewBuilder()
	fieldCount := int64(8)
	if includeSig {
		fieldCount = 9
	}
	ma, err := nb.BeginMap(fieldCount)
	if err != nil {
		return nil, fmt.Errorf("failed to begin map: %w", err)
	}

	if err := ma.AssembleKey().AssignString("type"); err != nil {
		return nil, err
	}
	if err := ma.AssembleValue().AssignInt(int64(m.Type)); err != nil {
		return nil, err
	}

	if err := ma.AssembleKey().AssignString("manifest_cid"); err != nil {
		return nil, err
	}
	if err := ma.AssembleValue().AssignString(m.ManifestCID.String()); err != nil {
		return nil, err
	}

	if err := ma.AssembleKey().AssignString("shard_id"); err != nil {
		return nil, err
	}
	if err := ma.AssembleValue().AssignString(m.ShardID); err != nil {
		return nil, err
	}

	if err := ma.AssembleKey().AssignString("hint_size"); err != nil {
		return nil, err
	}
	if err := ma.AssembleValue().AssignInt(int64(m.HintSize)); err != nil {
		return nil, err
	}

	if err := ma.AssembleKey().AssignString("sender_id"); err != nil {
		return nil, err
	}
	if err := ma.AssembleValue().AssignString(m.SenderID.String()); err != nil {
		return nil, err
	}

	if err := ma.AssembleKey().AssignString("ts"); err != nil {
		return nil, err
	}
	if err := ma.AssembleValue().AssignInt(m.Timestamp); err != nil {
		return nil, err
	}

	if err := ma.AssembleKey().AssignString("nonce"); err != nil {
		return nil, err
	}
	if err := ma.AssembleValue().AssignBytes(m.Nonce); err != nil {
		return nil, err
	}

	if includeSig {
		if err := ma.AssembleKey().AssignString("sig"); err != nil {
			return nil, err
		}
		if err := ma.AssembleValue().AssignBytes(m.Sig); err != nil {
			return nil, err
		}
	}

	if err := ma.Finish(); err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if err := dagcbor.Encode(nb.Build(), &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// UnmarshalCBOR deserializes an IngestMessage from CBOR format.
func (m *IngestMessage) UnmarshalCBOR(data []byte) error {
	nb := basicnode.Prototype.Map.NewBuilder()
	if err := dagcbor.Decode(nb, bytes.NewReader(data)); err != nil {
		return err
	}
	node := nb.Build()

	typeNode, err := node.LookupByString("type")
	if err != nil {
		return err
	}
	typeInt, err := typeNode.AsInt()
	if err != nil {
		return err
	}
	m.Type = MessageType(typeInt)

	manifestNode, err := node.LookupByString("manifest_cid")
	if err != nil {
		return err
	}
	manifestStr, err := manifestNode.AsString()
	if err != nil {
		return err
	}
	m.ManifestCID, err = cid.Decode(manifestStr)
	if err != nil {
		return err
	}

	shardNode, err := node.LookupByString("shard_id")
	if err != nil {
		return err
	}
	m.ShardID, err = shardNode.AsString()
	if err != nil {
		return err
	}

	sizeNode, err := node.LookupByString("hint_size")
	if err != nil {
		return err
	}
	sizeInt, err := sizeNode.AsInt()
	if err != nil {
		return err
	}
	m.HintSize = uint64(sizeInt)

	senderNode, err := node.LookupByString("sender_id")
	if err != nil {
		return err
	}
	senderStr, err := senderNode.AsString()
	if err != nil {
		return err
	}
	m.SenderID, err = peer.Decode(senderStr)
	if err != nil {
		return err
	}

	tsNode, err := node.LookupByString("ts")
	if err != nil {
		return err
	}
	m.Timestamp, err = tsNode.AsInt()
	if err != nil {
		return err
	}

	nonceNode, err := node.LookupByString("nonce")
	if err != nil {
		return err
	}
	m.Nonce, err = nonceNode.AsBytes()
	if err != nil {
		return err
	}

	sigNode, err := node.LookupByString("sig")
	if err != nil {
		return err
	}
	m.Sig, err = sigNode.AsBytes()
	if err != nil {
		return err
	}

	return nil
}

// MarshalCBOR serializes a ReplicationRequest to CBOR format.
func (m *ReplicationRequest) MarshalCBOR() ([]byte, error) {
	return m.marshalCBOR(true)
}

// MarshalCBORForSigning serializes the message without the signature field.
func (m *ReplicationRequest) MarshalCBORForSigning() ([]byte, error) {
	return m.marshalCBOR(false)
}

func (m *ReplicationRequest) marshalCBOR(includeSig bool) ([]byte, error) {
	nb := basicnode.Prototype.Map.NewBuilder()
	fieldCount := int64(7)
	if includeSig {
		fieldCount = 8
	}
	ma, err := nb.BeginMap(fieldCount)
	if err != nil {
		return nil, err
	}

	if err := ma.AssembleKey().AssignString("type"); err != nil {
		return nil, err
	}
	if err := ma.AssembleValue().AssignInt(int64(m.Type)); err != nil {
		return nil, err
	}

	if err := ma.AssembleKey().AssignString("manifest_cid"); err != nil {
		return nil, err
	}
	if err := ma.AssembleValue().AssignString(m.ManifestCID.String()); err != nil {
		return nil, err
	}

	if err := ma.AssembleKey().AssignString("priority"); err != nil {
		return nil, err
	}
	if err := ma.AssembleValue().AssignInt(int64(m.Priority)); err != nil {
		return nil, err
	}

	if err := ma.AssembleKey().AssignString("deadline"); err != nil {
		return nil, err
	}
	if err := ma.AssembleValue().AssignInt(m.Deadline); err != nil {
		return nil, err
	}

	if err := ma.AssembleKey().AssignString("sender_id"); err != nil {
		return nil, err
	}
	if err := ma.AssembleValue().AssignString(m.SenderID.String()); err != nil {
		return nil, err
	}

	if err := ma.AssembleKey().AssignString("ts"); err != nil {
		return nil, err
	}
	if err := ma.AssembleValue().AssignInt(m.Timestamp); err != nil {
		return nil, err
	}

	if err := ma.AssembleKey().AssignString("nonce"); err != nil {
		return nil, err
	}
	if err := ma.AssembleValue().AssignBytes(m.Nonce); err != nil {
		return nil, err
	}

	if includeSig {
		if err := ma.AssembleKey().AssignString("sig"); err != nil {
			return nil, err
		}
		if err := ma.AssembleValue().AssignBytes(m.Sig); err != nil {
			return nil, err
		}
	}

	if err := ma.Finish(); err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if err := dagcbor.Encode(nb.Build(), &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// UnmarshalCBOR deserializes a ReplicationRequest from CBOR format.
func (m *ReplicationRequest) UnmarshalCBOR(data []byte) error {
	nb := basicnode.Prototype.Map.NewBuilder()
	if err := dagcbor.Decode(nb, bytes.NewReader(data)); err != nil {
		return err
	}
	node := nb.Build()

	typeNode, err := node.LookupByString("type")
	if err != nil {
		return err
	}
	typeInt, err := typeNode.AsInt()
	if err != nil {
		return err
	}
	m.Type = MessageType(typeInt)

	manifestNode, err := node.LookupByString("manifest_cid")
	if err != nil {
		return err
	}
	manifestStr, err := manifestNode.AsString()
	if err != nil {
		return err
	}
	m.ManifestCID, err = cid.Decode(manifestStr)
	if err != nil {
		return err
	}

	priorityNode, err := node.LookupByString("priority")
	if err != nil {
		return err
	}
	priorityInt, err := priorityNode.AsInt()
	if err != nil {
		return err
	}
	m.Priority = uint8(priorityInt)

	deadlineNode, err := node.LookupByString("deadline")
	if err != nil {
		return err
	}
	m.Deadline, err = deadlineNode.AsInt()
	if err != nil {
		return err
	}

	senderNode, err := node.LookupByString("sender_id")
	if err != nil {
		return err
	}
	senderStr, err := senderNode.AsString()
	if err != nil {
		return err
	}
	m.SenderID, err = peer.Decode(senderStr)
	if err != nil {
		return err
	}

	tsNode, err := node.LookupByString("ts")
	if err != nil {
		return err
	}
	m.Timestamp, err = tsNode.AsInt()
	if err != nil {
		return err
	}

	nonceNode, err := node.LookupByString("nonce")
	if err != nil {
		return err
	}
	m.Nonce, err = nonceNode.AsBytes()
	if err != nil {
		return err
	}

	sigNode, err := node.LookupByString("sig")
	if err != nil {
		return err
	}
	m.Sig, err = sigNode.AsBytes()
	if err != nil {
		return err
	}

	return nil
}

// MarshalCBOR serializes a DelegateMessage to CBOR format.
func (m *DelegateMessage) MarshalCBOR() ([]byte, error) {
	return m.marshalCBOR(true)
}

// MarshalCBORForSigning serializes the message without the signature field.
func (m *DelegateMessage) MarshalCBORForSigning() ([]byte, error) {
	return m.marshalCBOR(false)
}

func (m *DelegateMessage) marshalCBOR(includeSig bool) ([]byte, error) {
	nb := basicnode.Prototype.Map.NewBuilder()
	fieldCount := int64(6)
	if includeSig {
		fieldCount = 7
	}
	ma, err := nb.BeginMap(fieldCount)
	if err != nil {
		return nil, err
	}

	if err := ma.AssembleKey().AssignString("type"); err != nil {
		return nil, err
	}
	if err := ma.AssembleValue().AssignInt(int64(m.Type)); err != nil {
		return nil, err
	}

	if err := ma.AssembleKey().AssignString("manifest_cid"); err != nil {
		return nil, err
	}
	if err := ma.AssembleValue().AssignString(m.ManifestCID.String()); err != nil {
		return nil, err
	}

	if err := ma.AssembleKey().AssignString("target_shard"); err != nil {
		return nil, err
	}
	if err := ma.AssembleValue().AssignString(m.TargetShard); err != nil {
		return nil, err
	}

	if err := ma.AssembleKey().AssignString("sender_id"); err != nil {
		return nil, err
	}
	if err := ma.AssembleValue().AssignString(m.SenderID.String()); err != nil {
		return nil, err
	}

	if err := ma.AssembleKey().AssignString("ts"); err != nil {
		return nil, err
	}
	if err := ma.AssembleValue().AssignInt(m.Timestamp); err != nil {
		return nil, err
	}

	if err := ma.AssembleKey().AssignString("nonce"); err != nil {
		return nil, err
	}
	if err := ma.AssembleValue().AssignBytes(m.Nonce); err != nil {
		return nil, err
	}

	if includeSig {
		if err := ma.AssembleKey().AssignString("sig"); err != nil {
			return nil, err
		}
		if err := ma.AssembleValue().AssignBytes(m.Sig); err != nil {
			return nil, err
		}
	}

	if err := ma.Finish(); err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if err := dagcbor.Encode(nb.Build(), &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// UnmarshalCBOR deserializes a DelegateMessage from CBOR format.
func (m *DelegateMessage) UnmarshalCBOR(data []byte) error {
	nb := basicnode.Prototype.Map.NewBuilder()
	if err := dagcbor.Decode(nb, bytes.NewReader(data)); err != nil {
		return err
	}
	node := nb.Build()

	typeNode, err := node.LookupByString("type")
	if err != nil {
		return err
	}
	typeInt, err := typeNode.AsInt()
	if err != nil {
		return err
	}
	m.Type = MessageType(typeInt)

	manifestNode, err := node.LookupByString("manifest_cid")
	if err != nil {
		return err
	}
	manifestStr, err := manifestNode.AsString()
	if err != nil {
		return err
	}
	m.ManifestCID, err = cid.Decode(manifestStr)
	if err != nil {
		return err
	}

	shardNode, err := node.LookupByString("target_shard")
	if err != nil {
		return err
	}
	m.TargetShard, err = shardNode.AsString()
	if err != nil {
		return err
	}

	senderNode, err := node.LookupByString("sender_id")
	if err != nil {
		return err
	}
	senderStr, err := senderNode.AsString()
	if err != nil {
		return err
	}
	m.SenderID, err = peer.Decode(senderStr)
	if err != nil {
		return err
	}

	tsNode, err := node.LookupByString("ts")
	if err != nil {
		return err
	}
	m.Timestamp, err = tsNode.AsInt()
	if err != nil {
		return err
	}

	nonceNode, err := node.LookupByString("nonce")
	if err != nil {
		return err
	}
	m.Nonce, err = nonceNode.AsBytes()
	if err != nil {
		return err
	}

	sigNode, err := node.LookupByString("sig")
	if err != nil {
		return err
	}
	m.Sig, err = sigNode.AsBytes()
	if err != nil {
		return err
	}

	return nil
}
