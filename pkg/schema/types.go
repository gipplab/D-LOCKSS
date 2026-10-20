package schema

import (
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

// ResearchObject is the unit of storage: metadata, payload link, and validation hints.
// The manifest is content-addressed: same content + same ingester = same ManifestCID.
// No timestamp is included to ensure deterministic CIDs for deduplication.
type ResearchObject struct {
	MetadataRef string  `cbor:"meta_ref"`    // DOI or URL to external metadata
	IngestedBy  peer.ID `cbor:"ingester_id"` // PeerID of the node that ingested this
	Signature   []byte  `cbor:"sig"`

	Payload cid.Cid `cbor:"payload"` // Link to raw UnixFS DAG (file data)

	TotalSize uint64 `cbor:"size"` // Total size in bytes

	// HasLegacyTimestamp reports that the manifest carries a "ts" field.
	// Such manifests have non-deterministic CIDs (the same content yields a
	// different ManifestCID on each ingest) and should be ignored. It is set
	// by UnmarshalCBOR and never serialized.
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
	fields := []cborKV{
		{"meta_ref", ro.MetadataRef},
		{"ingester_id", ro.IngestedBy.String()},
	}
	if includeSig {
		fields = append(fields, cborKV{"sig", ro.Signature})
	}
	fields = append(fields,
		cborKV{"payload", ro.Payload.String()},
		cborKV{"size", int64(ro.TotalSize)},
	)
	b, err := marshalCBORMap(fields)
	if err != nil {
		return nil, fmt.Errorf("failed to encode research object: %w", err)
	}
	return b, nil
}

// UnmarshalCBOR deserializes a ResearchObject from CBOR format.
func (ro *ResearchObject) UnmarshalCBOR(data []byte) error {
	node, err := decodeCBORMap(data)
	if err != nil {
		return fmt.Errorf("failed to decode CBOR: %w", err)
	}
	if ro.MetadataRef, err = readString(node, "meta_ref"); err != nil {
		return fmt.Errorf("failed to read meta_ref: %w", err)
	}
	if ro.IngestedBy, err = readPeerID(node, "ingester_id"); err != nil {
		return fmt.Errorf("failed to read ingester_id: %w", err)
	}
	if ro.Signature, err = readBytes(node, "sig"); err != nil {
		return fmt.Errorf("failed to read sig: %w", err)
	}
	if _, err := node.LookupByString("ts"); err == nil {
		ro.HasLegacyTimestamp = true
	}
	if ro.Payload, err = readCID(node, "payload"); err != nil {
		return fmt.Errorf("failed to read payload: %w", err)
	}
	size, err := readInt(node, "size")
	if err != nil {
		return fmt.Errorf("failed to read size: %w", err)
	}
	ro.TotalSize = uint64(size)
	return nil
}

// NewResearchObject creates a new ResearchObject with the given parameters.
// The manifest is deterministic: the same (metadataRef, ingesterID,
// payloadCID, totalSize) always yields the same ManifestCID.
func NewResearchObject(metadataRef string, ingesterID peer.ID, payloadCID cid.Cid, totalSize uint64) *ResearchObject {
	return &ResearchObject{
		MetadataRef: metadataRef,
		IngestedBy:  ingesterID,
		Payload:     payloadCID,
		TotalSize:   totalSize,
	}
}
