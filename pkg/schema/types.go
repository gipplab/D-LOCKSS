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

// ResearchObject is the unit of storage: metadata, payload link, and validation hints.
// The manifest is content-addressed: same content + same ingester = same ManifestCID.
// No timestamp is included to ensure deterministic CIDs for deduplication.
type ResearchObject struct {
	MetadataRef string  `cbor:"meta_ref"`    // DOI or URL to external metadata
	IngestedBy  peer.ID `cbor:"ingester_id"` // PeerID of the node that ingested this
	Signature   []byte  `cbor:"sig"`         // Cryptographic signature

	Payload cid.Cid `cbor:"payload"` // Link to raw UnixFS DAG (file data)

	TotalSize uint64 `cbor:"size"` // Total size in bytes

	// HasLegacyTimestamp is true if the manifest was created with the old format
	// that included a "ts" field. These manifests produce non-deterministic CIDs
	// (same content → different ManifestCID on each ingest) and should be ignored.
	// Not serialized — set during UnmarshalCBOR only.
	HasLegacyTimestamp bool `cbor:"-"`
}

// MarshalCBOR serializes the ResearchObject to CBOR format.
func (ro *ResearchObject) MarshalCBOR() ([]byte, error) {
	return ro.marshalCBOR(true)
}

// MarshalCBORForSigning returns CBOR without the signature (canonical bytes for signing).
func (ro *ResearchObject) MarshalCBORForSigning() ([]byte, error) {
	return ro.marshalCBOR(false)
}

func (ro *ResearchObject) marshalCBOR(includeSig bool) ([]byte, error) {
	nb := basicnode.Prototype.Map.NewBuilder()
	fieldCount := int64(4)
	if includeSig {
		fieldCount = 5
	}
	ma, err := nb.BeginMap(fieldCount)
	if err != nil {
		return nil, fmt.Errorf("failed to begin map: %w", err)
	}

	// MetadataRef
	metaNode := basicnode.NewString(ro.MetadataRef)
	if err := ma.AssembleKey().AssignString("meta_ref"); err != nil {
		return nil, fmt.Errorf("failed to assign meta_ref key: %w", err)
	}
	if err := ma.AssembleValue().AssignNode(metaNode); err != nil {
		return nil, fmt.Errorf("failed to assign meta_ref value: %w", err)
	}

	// IngestedBy (PeerID as string)
	ingesterStr := ro.IngestedBy.String()
	ingesterNode := basicnode.NewString(ingesterStr)
	if err := ma.AssembleKey().AssignString("ingester_id"); err != nil {
		return nil, fmt.Errorf("failed to assign ingester_id key: %w", err)
	}
	if err := ma.AssembleValue().AssignNode(ingesterNode); err != nil {
		return nil, fmt.Errorf("failed to assign ingester_id value: %w", err)
	}

	if includeSig {
		// Signature (bytes)
		sigNode := basicnode.NewBytes(ro.Signature)
		if err := ma.AssembleKey().AssignString("sig"); err != nil {
			return nil, fmt.Errorf("failed to assign sig key: %w", err)
		}
		if err := ma.AssembleValue().AssignNode(sigNode); err != nil {
			return nil, fmt.Errorf("failed to assign sig value: %w", err)
		}
	}

	// Payload (CID as string)
	payloadStr := ro.Payload.String()
	payloadNode := basicnode.NewString(payloadStr)
	if err := ma.AssembleKey().AssignString("payload"); err != nil {
		return nil, fmt.Errorf("failed to assign payload key: %w", err)
	}
	if err := ma.AssembleValue().AssignNode(payloadNode); err != nil {
		return nil, fmt.Errorf("failed to assign payload value: %w", err)
	}

	// TotalSize
	sizeNode := basicnode.NewInt(int64(ro.TotalSize))
	if err := ma.AssembleKey().AssignString("size"); err != nil {
		return nil, fmt.Errorf("failed to assign size key: %w", err)
	}
	if err := ma.AssembleValue().AssignNode(sizeNode); err != nil {
		return nil, fmt.Errorf("failed to assign size value: %w", err)
	}

	if err := ma.Finish(); err != nil {
		return nil, fmt.Errorf("failed to finish map: %w", err)
	}

	node := nb.Build()

	// Encode to CBOR
	var buf bytes.Buffer
	if err := dagcbor.Encode(node, &buf); err != nil {
		return nil, fmt.Errorf("failed to encode CBOR: %w", err)
	}

	return buf.Bytes(), nil
}

// UnmarshalCBOR deserializes a ResearchObject from CBOR format.
func (ro *ResearchObject) UnmarshalCBOR(data []byte) error {
	// Decode CBOR to IPLD node
	nb := basicnode.Prototype.Map.NewBuilder()
	if err := dagcbor.Decode(nb, bytes.NewReader(data)); err != nil {
		return fmt.Errorf("failed to decode CBOR: %w", err)
	}
	node := nb.Build()

	// Extract fields - node should be a map
	if node.Kind() != datamodel.Kind_Map {
		return fmt.Errorf("expected map node, got %v", node.Kind())
	}

	// MetadataRef
	metaNode, err := node.LookupByString("meta_ref")
	if err != nil {
		return fmt.Errorf("failed to get meta_ref: %w", err)
	}
	ro.MetadataRef, err = metaNode.AsString()
	if err != nil {
		return fmt.Errorf("failed to get meta_ref string: %w", err)
	}

	// IngestedBy
	ingesterNode, err := node.LookupByString("ingester_id")
	if err != nil {
		return fmt.Errorf("failed to get ingester_id: %w", err)
	}
	ingesterStr, err := ingesterNode.AsString()
	if err != nil {
		return fmt.Errorf("failed to get ingester_id string: %w", err)
	}
	ro.IngestedBy, err = peer.Decode(ingesterStr)
	if err != nil {
		return fmt.Errorf("failed to decode peer ID: %w", err)
	}

	// Signature
	sigNode, err := node.LookupByString("sig")
	if err != nil {
		return fmt.Errorf("failed to get sig: %w", err)
	}
	ro.Signature, err = sigNode.AsBytes()
	if err != nil {
		return fmt.Errorf("failed to get sig bytes: %w", err)
	}

	// Timestamp (optional, for backward compatibility with old manifests)
	// Not stored in the struct, but we flag its presence so callers can
	// identify and skip legacy manifests with non-deterministic CIDs.
	if _, tsErr := node.LookupByString("ts"); tsErr == nil {
		ro.HasLegacyTimestamp = true
	}

	// Payload
	payloadNode, err := node.LookupByString("payload")
	if err != nil {
		return fmt.Errorf("failed to get payload: %w", err)
	}
	payloadStr, err := payloadNode.AsString()
	if err != nil {
		return fmt.Errorf("failed to get payload string: %w", err)
	}
	ro.Payload, err = cid.Decode(payloadStr)
	if err != nil {
		return fmt.Errorf("failed to decode payload CID: %w", err)
	}

	// TotalSize
	sizeNode, err := node.LookupByString("size")
	if err != nil {
		return fmt.Errorf("failed to get size: %w", err)
	}
	sizeInt, err := sizeNode.AsInt()
	if err != nil {
		return fmt.Errorf("failed to get size int: %w", err)
	}
	ro.TotalSize = uint64(sizeInt)

	return nil
}

// NewResearchObject creates a new ResearchObject with the given parameters.
// The manifest is deterministic: same (metadataRef, ingesterID, payloadCID, totalSize) → same ManifestCID.
func NewResearchObject(metadataRef string, ingesterID peer.ID, payloadCID cid.Cid, totalSize uint64) *ResearchObject {
	return &ResearchObject{
		MetadataRef: metadataRef,
		IngestedBy:  ingesterID,
		Payload:     payloadCID,
		TotalSize:   totalSize,
	}
}
