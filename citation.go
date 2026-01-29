package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ipfs/go-cid"

	"dlockss/pkg/schema"
)

type CitationJSON struct {
	ManifestCID string `json:"manifest_cid"`
	PayloadCID  string `json:"payload_cid"`
	MetadataRef string `json:"metadata_ref"`
	IngestedBy  string `json:"ingested_by"`
	Timestamp   int64  `json:"timestamp"`
	TotalSize   uint64 `json:"total_size"`
}

func ResolveCitation(ctx context.Context, manifestCIDStr string) (*CitationJSON, *schema.ResearchObject, error) {
	if ipfsClient == nil {
		return nil, nil, fmt.Errorf("ipfs client not initialized")
	}

	manifestCIDStr = strings.TrimSpace(manifestCIDStr)
	manifestCID, err := cid.Decode(manifestCIDStr)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid ManifestCID: %w", err)
	}

	b, err := ipfsClient.GetBlock(ctx, manifestCID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to fetch manifest block: %w", err)
	}

	var ro schema.ResearchObject
	if err := ro.UnmarshalCBOR(b); err != nil {
		return nil, nil, fmt.Errorf("failed to decode ResearchObject: %w", err)
	}

	out := &CitationJSON{
		ManifestCID: manifestCID.String(),
		PayloadCID:  ro.Payload.String(),
		MetadataRef: ro.MetadataRef,
		IngestedBy:  ro.IngestedBy.String(),
		Timestamp:   ro.Timestamp,
		TotalSize:   ro.TotalSize,
	}

	return out, &ro, nil
}

func FormatCitationJSON(c *CitationJSON) (string, error) {
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func FormatBibTeX(c *CitationJSON) string {
	year := time.Unix(c.Timestamp, 0).UTC().Year()
	key := fmt.Sprintf("dlockss:%s", c.ManifestCID[:min(8, len(c.ManifestCID))])
	
	// Use MetadataRef as the primary identifier/title since we don't have a title
	title := c.MetadataRef
	if title == "" {
		title = "D-LOCKSS Preserved Object"
	}

	// Minimal BibTeX entry: treat as @misc with howpublished pointing to payload CID.
	return fmt.Sprintf(
		"@misc{%s,\n  title={%s},\n  year={%d},\n  howpublished={ipfs:%s},\n  note={manifest:%s, metadata:%s}\n}\n",
		key,
		escapeBibTeX(title),
		year,
		c.PayloadCID,
		c.ManifestCID,
		c.MetadataRef,
	)
}

func escapeBibTeX(s string) string {
	// very small escape set; enough to avoid breaking braces
	s = strings.ReplaceAll(s, "{", "\\{")
	s = strings.ReplaceAll(s, "}", "\\}")
	return s
}

// RefTreeNode and ResolveReferenceTree are removed as references are no longer part of the schema.
