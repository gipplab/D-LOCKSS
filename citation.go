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
	ManifestCID string   `json:"manifest_cid"`
	PayloadCID  string   `json:"payload_cid"`
	Title       string   `json:"title"`
	Authors     []string `json:"authors"`
	IngestedBy  string   `json:"ingested_by"`
	Timestamp   int64    `json:"timestamp"`
	References  []string `json:"references"`
	TotalSize   uint64   `json:"total_size"`
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

	refs := make([]string, 0, len(ro.References))
	for _, r := range ro.References {
		refs = append(refs, r.String())
	}

	out := &CitationJSON{
		ManifestCID: manifestCID.String(),
		PayloadCID:  ro.Payload.String(),
		Title:       ro.Title,
		Authors:     ro.Authors,
		IngestedBy:  ro.IngestedBy.String(),
		Timestamp:   ro.Timestamp,
		References:  refs,
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
	authors := strings.Join(c.Authors, " and ")
	if authors == "" {
		authors = "Unknown"
	}
	title := c.Title
	if title == "" {
		title = "Untitled"
	}

	// Minimal BibTeX entry: treat as @misc with howpublished pointing to payload CID.
	return fmt.Sprintf(
		"@misc{%s,\n  title={%s},\n  author={%s},\n  year={%d},\n  howpublished={ipfs:%s},\n  note={manifest:%s}\n}\n",
		key,
		escapeBibTeX(title),
		escapeBibTeX(authors),
		year,
		c.PayloadCID,
		c.ManifestCID,
	)
}

func escapeBibTeX(s string) string {
	// very small escape set; enough to avoid breaking braces
	s = strings.ReplaceAll(s, "{", "\\{")
	s = strings.ReplaceAll(s, "}", "\\}")
	return s
}

type RefTreeNode struct {
	ManifestCID string        `json:"manifest_cid"`
	Title       string        `json:"title"`
	Children    []*RefTreeNode `json:"children,omitempty"`
}

// ResolveReferenceTree loads the ResearchObject graph starting at rootManifestCIDStr up to maxDepth.
// maxDepth=0 returns only the root node.
func ResolveReferenceTree(ctx context.Context, rootManifestCIDStr string, maxDepth int) (*RefTreeNode, [][2]string, error) {
	if maxDepth < 0 {
		maxDepth = 0
	}

	rootCitation, ro, err := ResolveCitation(ctx, rootManifestCIDStr)
	if err != nil {
		return nil, nil, err
	}

	root := &RefTreeNode{
		ManifestCID: rootCitation.ManifestCID,
		Title:       rootCitation.Title,
	}

	visited := map[string]bool{root.ManifestCID: true}
	edges := make([][2]string, 0)

	var walk func(parent *RefTreeNode, parentRO *schema.ResearchObject, depth int) error
	walk = func(parent *RefTreeNode, parentRO *schema.ResearchObject, depth int) error {
		if depth >= maxDepth {
			return nil
		}
		for _, refCID := range parentRO.References {
			refStr := refCID.String()
			edges = append(edges, [2]string{parent.ManifestCID, refStr})

			child := &RefTreeNode{ManifestCID: refStr}
			parent.Children = append(parent.Children, child)

			if visited[refStr] {
				// Cycle or already-seen node; don't expand further.
				continue
			}
			visited[refStr] = true

			c, refRO, err := ResolveCitation(ctx, refStr)
			if err != nil {
				// Keep node but mark title as unresolved.
				child.Title = "(unresolved)"
				continue
			}
			child.Title = c.Title
			if err := walk(child, refRO, depth+1); err != nil {
				return err
			}
		}
		return nil
	}

	if err := walk(root, ro, 0); err != nil {
		return nil, nil, err
	}

	return root, edges, nil
}

func FormatRefTree(root *RefTreeNode) string {
	var sb strings.Builder
	var rec func(n *RefTreeNode, indent int)
	rec = func(n *RefTreeNode, indent int) {
		prefix := strings.Repeat("  ", indent)
		title := n.Title
		if title == "" {
			title = "(untitled)"
		}
		sb.WriteString(fmt.Sprintf("%s- %s  [%s]\n", prefix, title, n.ManifestCID))
		for _, ch := range n.Children {
			rec(ch, indent+1)
		}
	}
	rec(root, 0)
	return sb.String()
}

