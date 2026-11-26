package schema

import (
	"errors"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/libp2p/go-libp2p/core/peer"
)

// ResearchObject is the unit of storage: metadata, payload link, and validation hints.
// The manifest is content-addressed: same content + same ingester = same ManifestCID.
// No timestamp is included to ensure deterministic CIDs for deduplication.
type ResearchObject struct {
	MetadataRef string  `cbor:"meta_ref"`    // DOI or URL to external metadata
	IngestedBy  peer.ID `cbor:"ingester_id"` // Node (Kubo) ID of the node that ingested this
	Signature   []byte  `cbor:"sig"`

	Payload cid.Cid `cbor:"payload"` // Link to raw UnixFS DAG (file data)

	TotalSize uint64 `cbor:"size"` // Total size in bytes

	// Uploaders are the users who signed the payload, see VerifyUpload:
	// the uploader of /upload, or the signatures the wiki holds for a
	// pinned file. Sorted by key; only serialized when set, so other
	// manifests keep their CIDs.
	Uploaders []UploaderSignature `cbor:"uploaders"`

	// Uploader and UploaderSig are the single uploader of manifests made
	// before Uploaders existed. They are read and re-encoded, so such
	// manifests keep their signatures, but no longer written.
	// AllUploaders includes them.
	Uploader    peer.ID `cbor:"uploader"`
	UploaderSig []byte  `cbor:"uploader_sig"`

	// HasLegacyTimestamp reports that the manifest carries a "ts" field.
	// Such manifests have non-deterministic CIDs (the same content yields a
	// different ManifestCID on each ingest) and should be ignored. It is set
	// by UnmarshalCBOR and never serialized.
	HasLegacyTimestamp bool `cbor:"-"`
}

// UploaderSignature is a user's key and its signature of the payload.
type UploaderSignature struct {
	Key peer.ID
	Sig []byte
}

// AllUploaders returns the uploaders of the manifest, old format included.
func (ro *ResearchObject) AllUploaders() []UploaderSignature {
	if ro.Uploader == "" {
		return ro.Uploaders
	}
	return append([]UploaderSignature{{ro.Uploader, ro.UploaderSig}}, ro.Uploaders...)
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
	if ro.Uploader != "" {
		fields = append(fields,
			cborKV{"uploader", ro.Uploader.String()},
			cborKV{"uploader_sig", ro.UploaderSig},
		)
	}
	if len(ro.Uploaders) > 0 {
		list := make([][]cborKV, len(ro.Uploaders))
		for i, u := range ro.Uploaders {
			list[i] = []cborKV{{"key", u.Key.String()}, {"sig", u.Sig}}
		}
		fields = append(fields, cborKV{"uploaders", list})
	}
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
	if _, err := node.LookupByString("uploader"); err == nil {
		if ro.Uploader, err = readPeerID(node, "uploader"); err != nil {
			return fmt.Errorf("failed to read uploader: %w", err)
		}
		if ro.UploaderSig, err = readBytes(node, "uploader_sig"); err != nil {
			return fmt.Errorf("failed to read uploader_sig: %w", err)
		}
	}
	if ro.Uploaders, err = readUploaders(node); err != nil {
		return fmt.Errorf("failed to read uploaders: %w", err)
	}
	return nil
}

func readUploaders(node datamodel.Node) ([]UploaderSignature, error) {
	list, err := node.LookupByString("uploaders")
	if err != nil {
		return nil, nil
	}
	var out []UploaderSignature
	it := list.ListIterator()
	if it == nil {
		return nil, errors.New("not a list")
	}
	for !it.Done() {
		_, entry, err := it.Next()
		if err != nil {
			return nil, err
		}
		var u UploaderSignature
		if u.Key, err = readPeerID(entry, "key"); err != nil {
			return nil, err
		}
		if u.Sig, err = readBytes(entry, "sig"); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, nil
}

// VerifyUpload checks that sig is uploader's signature of payload, made with
// 'ipfs key sign' over the message "dlockss-upload <payload CID>".
func VerifyUpload(uploader peer.ID, payload cid.Cid, sig []byte) error {
	if !verifyKuboSignature(uploader, []byte("dlockss-upload "+payload.String()), sig) {
		return ErrUploadSignature
	}
	return nil
}

// VerifySignature checks that Signature is the ingesting node's signature
// over MarshalCBORForSigning, made with 'ipfs key sign' or, in older
// manifests, directly with the node key.
func (ro *ResearchObject) VerifySignature() error {
	unsigned, err := ro.MarshalCBORForSigning()
	if err != nil {
		return err
	}
	if !verifyKuboSignature(ro.IngestedBy, unsigned, ro.Signature) && !verifyRawSignature(ro.IngestedBy, unsigned, ro.Signature) {
		return ErrManifestSignature
	}
	return nil
}

// ErrManifestSignature reports a manifest not signed by its ingester.
var ErrManifestSignature = errors.New("manifest signature does not match its ingester")

// ErrUploadSignature reports a signature that does not match the upload.
var ErrUploadSignature = errors.New("signature does not match \"dlockss-upload <payload CID>\"")

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
