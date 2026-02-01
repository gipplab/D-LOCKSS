package schema

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
)

// ResearchObject is the fundamental unit of storage in the D-LOCKSS network.
// It wraps content in a structured format with metadata references and validation hints.
// Pinning a ResearchObject (ManifestCID) recursively pins the underlying PayloadCID.
type ResearchObject struct {
	// --- Metadata (Context) ---
	MetadataRef string  `cbor:"meta_ref"`    // DOI or URL to external metadata
	IngestedBy  peer.ID `cbor:"ingester_id"` // PeerID of the node that ingested this
	Signature   []byte  `cbor:"sig"`         // Cryptographic signature
	Timestamp   int64   `cbor:"ts"`          // Unix timestamp of ingestion

	// --- The Content (Redundancy Target) ---
	// Pinning 'ResearchObject' recursively pins this CID.
	Payload cid.Cid `cbor:"payload"` // Link to the raw UnixFS DAG containing the actual file data

	// --- Validation Hints ---
	TotalSize uint64 `cbor:"size"` // Total size in bytes (used for rate limiting and liar detection)
}

// ManifestCID returns the CID of this ResearchObject when serialized.
// This is the "root" CID that should be pinned recursively.
func (ro *ResearchObject) ManifestCID() (cid.Cid, error) {
	// Serialize to CBOR
	data, err := ro.MarshalCBOR()
	if err != nil {
		return cid.Cid{}, err
	}

	// Create CID from CBOR bytes
	hash := sha256.Sum256(data)
	mh, err := multihash.Sum(hash[:], multihash.SHA2_256, -1)
	if err != nil {
		return cid.Cid{}, err
	}

	return cid.NewCidV1(cid.DagCBOR, mh), nil
}

// MarshalCBOR serializes the ResearchObject to CBOR format.
func (ro *ResearchObject) MarshalCBOR() ([]byte, error) {
	return ro.marshalCBOR(true)
}

// MarshalCBORForSigning serializes the ResearchObject to CBOR without the signature field.
// This is the canonical byte representation that should be signed and verified.
func (ro *ResearchObject) MarshalCBORForSigning() ([]byte, error) {
	return ro.marshalCBOR(false)
}

func (ro *ResearchObject) marshalCBOR(includeSig bool) ([]byte, error) {
	nb := basicnode.Prototype.Map.NewBuilder()
	fieldCount := int64(5)
	if includeSig {
		fieldCount = 6
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

	// Timestamp
	tsNode := basicnode.NewInt(ro.Timestamp)
	if err := ma.AssembleKey().AssignString("ts"); err != nil {
		return nil, fmt.Errorf("failed to assign ts key: %w", err)
	}
	if err := ma.AssembleValue().AssignNode(tsNode); err != nil {
		return nil, fmt.Errorf("failed to assign ts value: %w", err)
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

	// Timestamp
	tsNode, err := node.LookupByString("ts")
	if err != nil {
		return fmt.Errorf("failed to get ts: %w", err)
	}
	tsInt, err := tsNode.AsInt()
	if err != nil {
		return fmt.Errorf("failed to get ts int: %w", err)
	}
	ro.Timestamp = tsInt

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
func NewResearchObject(metadataRef string, ingesterID peer.ID, payloadCID cid.Cid, totalSize uint64) *ResearchObject {
	return &ResearchObject{
		MetadataRef: metadataRef,
		IngestedBy:  ingesterID,
		Timestamp:   time.Now().Unix(),
		Payload:     payloadCID,
		TotalSize:   totalSize,
	}
}
