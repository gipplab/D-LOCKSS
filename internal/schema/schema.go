// Package schema defines the wire formats of the D-LOCKSS protocol: the
// ResearchObject manifest and the signed CBOR protocol messages. Encodings
// are byte-compatible with protocol version dlockss-v0.0.3: ordered DAG-CBOR
// maps with CIDs and peer IDs as strings. Manifest CIDs are deterministic
// (no timestamp field), so re-ingesting identical content dedupes.
package schema

import (
	"bytes"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/libp2p/go-libp2p/core/peer"
)

// MessageType identifies a signed protocol message.
type MessageType uint8

const (
	MessageTypeIngest MessageType = iota + 1
	MessageTypeReplicationRequest
)

// ---- ordered CBOR map encoding ----

type field struct {
	key string
	val any // int64, string, or []byte
}

func encodeMap(fields []field) ([]byte, error) {
	nb := basicnode.Prototype.Map.NewBuilder()
	ma, err := nb.BeginMap(int64(len(fields)))
	if err != nil {
		return nil, err
	}
	for _, f := range fields {
		if err := ma.AssembleKey().AssignString(f.key); err != nil {
			return nil, err
		}
		switch v := f.val.(type) {
		case int64:
			err = ma.AssembleValue().AssignInt(v)
		case string:
			err = ma.AssembleValue().AssignString(v)
		case []byte:
			err = ma.AssembleValue().AssignBytes(v)
		default:
			return nil, fmt.Errorf("unsupported CBOR value type %T for key %q", f.val, f.key)
		}
		if err != nil {
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

func decodeMap(data []byte) (datamodel.Node, error) {
	nb := basicnode.Prototype.Map.NewBuilder()
	if err := dagcbor.Decode(nb, bytes.NewReader(data)); err != nil {
		return nil, err
	}
	return nb.Build(), nil
}

func getInt(n datamodel.Node, key string) (int64, error) {
	v, err := n.LookupByString(key)
	if err != nil {
		return 0, fmt.Errorf("field %q: %w", key, err)
	}
	return v.AsInt()
}

func getString(n datamodel.Node, key string) (string, error) {
	v, err := n.LookupByString(key)
	if err != nil {
		return "", fmt.Errorf("field %q: %w", key, err)
	}
	return v.AsString()
}

func getBytes(n datamodel.Node, key string) ([]byte, error) {
	v, err := n.LookupByString(key)
	if err != nil {
		return nil, fmt.Errorf("field %q: %w", key, err)
	}
	return v.AsBytes()
}

func getCID(n datamodel.Node, key string) (cid.Cid, error) {
	s, err := getString(n, key)
	if err != nil {
		return cid.Undef, err
	}
	return cid.Decode(s)
}

func getPeerID(n datamodel.Node, key string) (peer.ID, error) {
	s, err := getString(n, key)
	if err != nil {
		return "", err
	}
	return peer.Decode(s)
}

// ---- ResearchObject ----

// ResearchObject is the unit of preservation: a signed, content-addressed
// manifest linking metadata to the payload DAG. It carries no timestamp, so
// identical content from the same ingester always yields the same CID.
type ResearchObject struct {
	MetadataRef string  // DOI/URL/file reference
	IngestedBy  peer.ID // node that ingested the content
	Signature   []byte  // over the for-signing encoding
	Payload     cid.Cid // root of the raw UnixFS DAG
	TotalSize   uint64  // payload size in bytes
}

func (ro *ResearchObject) encode(includeSig bool) ([]byte, error) {
	fields := []field{
		{"meta_ref", ro.MetadataRef},
		{"ingester_id", ro.IngestedBy.String()},
	}
	if includeSig {
		fields = append(fields, field{"sig", ro.Signature})
	}
	fields = append(fields,
		field{"payload", ro.Payload.String()},
		field{"size", int64(ro.TotalSize)},
	)
	return encodeMap(fields)
}

// MarshalCBOR returns the full manifest encoding.
func (ro *ResearchObject) MarshalCBOR() ([]byte, error) { return ro.encode(true) }

// MarshalCBORForSigning returns the canonical bytes signed by the ingester.
func (ro *ResearchObject) MarshalCBORForSigning() ([]byte, error) { return ro.encode(false) }

// UnmarshalCBOR decodes a manifest block.
func (ro *ResearchObject) UnmarshalCBOR(data []byte) error {
	n, err := decodeMap(data)
	if err != nil {
		return fmt.Errorf("decode manifest: %w", err)
	}
	if ro.MetadataRef, err = getString(n, "meta_ref"); err != nil {
		return err
	}
	if ro.IngestedBy, err = getPeerID(n, "ingester_id"); err != nil {
		return err
	}
	if ro.Signature, err = getBytes(n, "sig"); err != nil {
		return err
	}
	if ro.Payload, err = getCID(n, "payload"); err != nil {
		return err
	}
	size, err := getInt(n, "size")
	if err != nil {
		return err
	}
	ro.TotalSize = uint64(size)
	return nil
}

// ---- signed protocol messages ----

// Envelope holds the fields shared by every signed protocol message.
type Envelope struct {
	Type        MessageType
	ManifestCID cid.Cid
	SenderID    peer.ID
	Timestamp   int64 // unix seconds
	Nonce       []byte
	Sig         []byte
}

// Message is any signed protocol message.
type Message interface {
	Envelope() *Envelope
	MarshalCBOR() ([]byte, error)
	MarshalCBORForSigning() ([]byte, error)
}

func encodeMessage(env *Envelope, specific []field, includeSig bool) ([]byte, error) {
	fields := []field{
		{"type", int64(env.Type)},
		{"manifest_cid", env.ManifestCID.String()},
	}
	fields = append(fields, specific...)
	fields = append(fields,
		field{"sender_id", env.SenderID.String()},
		field{"ts", env.Timestamp},
		field{"nonce", env.Nonce},
	)
	if includeSig {
		fields = append(fields, field{"sig", env.Sig})
	}
	return encodeMap(fields)
}

func (e *Envelope) decodeFrom(n datamodel.Node) error {
	t, err := getInt(n, "type")
	if err != nil {
		return err
	}
	e.Type = MessageType(t)
	if e.ManifestCID, err = getCID(n, "manifest_cid"); err != nil {
		return err
	}
	if e.SenderID, err = getPeerID(n, "sender_id"); err != nil {
		return err
	}
	if e.Timestamp, err = getInt(n, "ts"); err != nil {
		return err
	}
	if e.Nonce, err = getBytes(n, "nonce"); err != nil {
		return err
	}
	e.Sig, err = getBytes(n, "sig")
	return err
}

// IngestMessage announces a new ResearchObject to a shard.
type IngestMessage struct {
	Env     Envelope
	ShardID string // target shard prefix, e.g. "01"
}

func (m *IngestMessage) Envelope() *Envelope { return &m.Env }

func (m *IngestMessage) specific() []field { return []field{{"shard_id", m.ShardID}} }

func (m *IngestMessage) MarshalCBOR() ([]byte, error) {
	return encodeMessage(&m.Env, m.specific(), true)
}

func (m *IngestMessage) MarshalCBORForSigning() ([]byte, error) {
	return encodeMessage(&m.Env, m.specific(), false)
}

func (m *IngestMessage) UnmarshalCBOR(data []byte) error {
	n, err := decodeMap(data)
	if err != nil {
		return err
	}
	if err := m.Env.decodeFrom(n); err != nil {
		return err
	}
	m.ShardID, err = getString(n, "shard_id")
	return err
}

// ReplicationRequest asks shard peers to hold a ResearchObject.
type ReplicationRequest struct {
	Env Envelope
}

func (m *ReplicationRequest) Envelope() *Envelope { return &m.Env }

func (m *ReplicationRequest) MarshalCBOR() ([]byte, error) {
	return encodeMessage(&m.Env, nil, true)
}

func (m *ReplicationRequest) MarshalCBORForSigning() ([]byte, error) {
	return encodeMessage(&m.Env, nil, false)
}

func (m *ReplicationRequest) UnmarshalCBOR(data []byte) error {
	n, err := decodeMap(data)
	if err != nil {
		return err
	}
	return m.Env.decodeFrom(n)
}

// DecodeMessage decodes a CBOR protocol message by its type field.
func DecodeMessage(data []byte) (Message, error) {
	n, err := decodeMap(data)
	if err != nil {
		return nil, err
	}
	t, err := getInt(n, "type")
	if err != nil {
		return nil, err
	}
	switch MessageType(t) {
	case MessageTypeIngest:
		m := &IngestMessage{}
		return m, m.UnmarshalCBOR(data)
	case MessageTypeReplicationRequest:
		m := &ReplicationRequest{}
		return m, m.UnmarshalCBOR(data)
	default:
		return nil, fmt.Errorf("unknown message type %d", t)
	}
}
