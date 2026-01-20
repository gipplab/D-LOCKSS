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
// It wraps content in a structured format with metadata, citations, and validation hints.
// Pinning a ResearchObject (ManifestCID) recursively pins the underlying PayloadCID.
type ResearchObject struct {
	// --- Metadata (Context) ---
	Title      string   `cbor:"title"`       // Document title
	Authors    []string `cbor:"authors"`     // List of author names
	IngestedBy peer.ID  `cbor:"ingester_id"` // PeerID of the node that ingested this
	Signature  []byte   `cbor:"sig"`         // Cryptographic signature (future: verify ownership)
	Timestamp  int64    `cbor:"ts"`          // Unix timestamp of ingestion

	// --- The Content (Redundancy Target) ---
	// Pinning 'ResearchObject' recursively pins this CID.
	Payload cid.Cid `cbor:"payload"` // Link to the raw UnixFS DAG containing the actual file data

	// --- The Graph (Citations) ---
	// Immutable links to other ResearchObjects within the network
	References []cid.Cid `cbor:"refs"` // CIDs of referenced ResearchObjects (citation graph)

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
	fieldCount := int64(6)
	if includeSig {
		fieldCount = 7
	}
	ma, err := nb.BeginMap(fieldCount)
	if err != nil {
		return nil, fmt.Errorf("failed to begin map: %w", err)
	}

	// Title
	titleNode := basicnode.NewString(ro.Title)
	if err := ma.AssembleKey().AssignString("title"); err != nil {
		return nil, fmt.Errorf("failed to assign title key: %w", err)
	}
	if err := ma.AssembleValue().AssignNode(titleNode); err != nil {
		return nil, fmt.Errorf("failed to assign title value: %w", err)
	}

	// Authors (array)
	authorsBuilder := basicnode.Prototype.List.NewBuilder()
	la, err := authorsBuilder.BeginList(int64(len(ro.Authors)))
	if err != nil {
		return nil, fmt.Errorf("failed to begin authors list: %w", err)
	}
	for _, author := range ro.Authors {
		if err := la.AssembleValue().AssignString(author); err != nil {
			return nil, fmt.Errorf("failed to assign author: %w", err)
		}
	}
	if err := la.Finish(); err != nil {
		return nil, fmt.Errorf("failed to finish authors list: %w", err)
	}
	authorsNode := authorsBuilder.Build()
	if err := ma.AssembleKey().AssignString("authors"); err != nil {
		return nil, fmt.Errorf("failed to assign authors key: %w", err)
	}
	if err := ma.AssembleValue().AssignNode(authorsNode); err != nil {
		return nil, fmt.Errorf("failed to assign authors value: %w", err)
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

	// References (array of CIDs)
	refsBuilder := basicnode.Prototype.List.NewBuilder()
	refsList, err := refsBuilder.BeginList(int64(len(ro.References)))
	if err != nil {
		return nil, fmt.Errorf("failed to begin references list: %w", err)
	}
	for _, ref := range ro.References {
		if err := refsList.AssembleValue().AssignString(ref.String()); err != nil {
			return nil, fmt.Errorf("failed to assign reference: %w", err)
		}
	}
	if err := refsList.Finish(); err != nil {
		return nil, fmt.Errorf("failed to finish references list: %w", err)
	}
	refsNode := refsBuilder.Build()
	if err := ma.AssembleKey().AssignString("refs"); err != nil {
		return nil, fmt.Errorf("failed to assign refs key: %w", err)
	}
	if err := ma.AssembleValue().AssignNode(refsNode); err != nil {
		return nil, fmt.Errorf("failed to assign refs value: %w", err)
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

	// Title
	titleNode, err := node.LookupByString("title")
	if err != nil {
		return fmt.Errorf("failed to get title: %w", err)
	}
	ro.Title, err = titleNode.AsString()
	if err != nil {
		return fmt.Errorf("failed to get title string: %w", err)
	}

	// Authors
	authorsNode, err := node.LookupByString("authors")
	if err != nil {
		return fmt.Errorf("failed to get authors: %w", err)
	}
	if authorsNode.Kind() != datamodel.Kind_List {
		return fmt.Errorf("authors is not a list")
	}
	// Iterate using Length and LookupByIndex
	length := authorsNode.Length()
	ro.Authors = make([]string, 0, length)
	for i := int64(0); i < length; i++ {
		authorNode, err := authorsNode.LookupByIndex(i)
		if err != nil {
			return fmt.Errorf("failed to lookup author at index %d: %w", i, err)
		}
		author, err := authorNode.AsString()
		if err != nil {
			return fmt.Errorf("failed to get author string: %w", err)
		}
		ro.Authors = append(ro.Authors, author)
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

	// References
	refsNode, err := node.LookupByString("refs")
	if err != nil {
		return fmt.Errorf("failed to get refs: %w", err)
	}
	if refsNode.Kind() != datamodel.Kind_List {
		return fmt.Errorf("refs is not a list")
	}
	// Iterate using Length and LookupByIndex
	refsLength := refsNode.Length()
	ro.References = make([]cid.Cid, 0, refsLength)
	for i := int64(0); i < refsLength; i++ {
		refNode, err := refsNode.LookupByIndex(i)
		if err != nil {
			return fmt.Errorf("failed to lookup ref at index %d: %w", i, err)
		}
		refStr, err := refNode.AsString()
		if err != nil {
			return fmt.Errorf("failed to get ref string: %w", err)
		}
		refCID, err := cid.Decode(refStr)
		if err != nil {
			return fmt.Errorf("failed to decode ref CID: %w", err)
		}
		ro.References = append(ro.References, refCID)
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
func NewResearchObject(title string, authors []string, ingesterID peer.ID, payloadCID cid.Cid, references []cid.Cid, totalSize uint64) *ResearchObject {
	return &ResearchObject{
		Title:      title,
		Authors:    authors,
		IngestedBy: ingesterID,
		Timestamp:  time.Now().Unix(),
		Payload:    payloadCID,
		References: references,
		TotalSize:  totalSize,
	}
}
