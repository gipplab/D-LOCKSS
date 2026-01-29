package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log"
	"strings"

	"dlockss/pkg/ipfs"
	"dlockss/pkg/schema"

	"github.com/ipfs/go-cid"
)

// validateHash validates that a string is a 64-character hex string.
// Note: ManifestCID strings don't need this validation.
func validateHash(hash string) bool {
	if len(hash) != 64 {
		return false
	}
	_, err := hex.DecodeString(hash)
	return err == nil
}

// getBinaryPrefix computes a binary prefix from a string by hashing it.
// Used for shard routing based on peer IDs or other identifiers.
func getBinaryPrefix(s string, depth int) string {
	h := sha256.Sum256([]byte(s))
	return bytesToBinaryString(h[:], depth)
}

// getHexBinaryPrefix extracts a binary prefix from a hex string.
// For ManifestCID strings, use keyToStableHex first to get a stable hex representation.
func getHexBinaryPrefix(hexStr string, depth int) string {
	b, err := hex.DecodeString(hexStr)
	if err != nil {
		logError("Utils", "decode hex string", hexStr, err)
		return ""
	}
	return bytesToBinaryString(b, depth)
}

// keyToCID tries to interpret key as a CID string.
func keyToCID(key string) (cid.Cid, error) {
	return cid.Decode(key)
}

// keyToStableHex returns a stable 64-char hex string for any key (CID strings, hashes, etc.).
// Used to keep shard routing stable even when the key isn't a raw SHA-256 hex string.
// This is essential as keys are ManifestCID strings, not raw hashes.
func keyToStableHex(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

func bytesToBinaryString(b []byte, length int) string {
	var sb strings.Builder
	for _, byteVal := range b {
		for i := 7; i >= 0; i-- {
			if length <= 0 {
				return sb.String()
			}
			if (byteVal>>i)&1 == 1 {
				sb.WriteRune('1')
			} else {
				sb.WriteRune('0')
			}
			length--
		}
	}
	return sb.String()
}

// getPayloadCIDForShardAssignment extracts PayloadCID from a ResearchObject given its ManifestCID.
// This is used for stable shard assignment - PayloadCID is content-based and stable,
// while ManifestCID includes timestamp/metadata and changes on every ingestion.
// Returns the PayloadCID string, or the ManifestCID string as fallback if extraction fails.
func getPayloadCIDForShardAssignment(ctx context.Context, client ipfs.IPFSClient, manifestCIDStr string) string {
	if client == nil {
		// Fallback to ManifestCID if IPFS client not available
		return manifestCIDStr
	}

	manifestCID, err := cid.Decode(manifestCIDStr)
	if err != nil {
		// Not a valid CID, return as-is
		return manifestCIDStr
	}

	// Fetch ResearchObject block
	manifestBytes, err := client.GetBlock(ctx, manifestCID)
	if err != nil {
		// If we can't fetch, fallback to ManifestCID (shouldn't happen in normal operation)
		log.Printf("[Shard] Warning: Could not fetch ResearchObject for %s, using ManifestCID for shard assignment: %v", manifestCIDStr[:min(16, len(manifestCIDStr))]+"...", err)
		return manifestCIDStr
	}

	// Decode ResearchObject
	var ro schema.ResearchObject
	if err := ro.UnmarshalCBOR(manifestBytes); err != nil {
		log.Printf("[Shard] Warning: Could not decode ResearchObject for %s, using ManifestCID for shard assignment: %v", manifestCIDStr[:min(16, len(manifestCIDStr))]+"...", err)
		return manifestCIDStr
	}

	// Return PayloadCID string for stable shard assignment
	return ro.Payload.String()
}

// truncateCID truncates a CID string to a maximum length, appending "..." if truncated.
// This is used for consistent logging format throughout the codebase.
func truncateCID(cidStr string, maxLen int) string {
	if len(cidStr) <= maxLen {
		return cidStr
	}
	return cidStr[:maxLen] + "..."
}

// logError logs an error with consistent formatting including component, operation, and identifier.
// This standardizes error logging across the codebase.
func logError(component, operation, identifier string, err error) {
	log.Printf("[Error] %s: Failed to %s %s: %v",
		component, operation, truncateCID(identifier, 16), err)
}

// logErrorWithContext logs an error with additional context information.
// Use this when you need to include extra details beyond the standard format.
func logErrorWithContext(component, operation, identifier string, context string, err error) {
	log.Printf("[Error] %s: Failed to %s %s (%s): %v",
		component, operation, truncateCID(identifier, 16), context, err)
}

// logWarning logs a warning with consistent formatting.
func logWarning(component, message, identifier string) {
	log.Printf("[Warning] %s: %s %s", component, message, truncateCID(identifier, 16))
}

// logWarningWithContext logs a warning with additional context.
func logWarningWithContext(component, message, identifier, context string) {
	log.Printf("[Warning] %s: %s %s (%s)", component, message, truncateCID(identifier, 16), context)
}
