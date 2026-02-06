package common

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

// ValidateHash validates that a string is a 64-character hex string.
// Note: ManifestCID strings don't need this validation.
func ValidateHash(hash string) bool {
	if len(hash) != 64 {
		return false
	}
	_, err := hex.DecodeString(hash)
	return err == nil
}

// GetBinaryPrefix computes a binary prefix from a string by hashing it.
// Used for shard routing based on peer IDs or other identifiers.
func GetBinaryPrefix(s string, depth int) string {
	h := sha256.Sum256([]byte(s))
	return bytesToBinaryString(h[:], depth)
}

// GetHexBinaryPrefix extracts a binary prefix from a hex string.
// For ManifestCID strings, use KeyToStableHex first to get a stable hex representation.
func GetHexBinaryPrefix(hexStr string, depth int) string {
	b, err := hex.DecodeString(hexStr)
	if err != nil {
		LogError("Utils", "decode hex string", hexStr, err)
		return ""
	}
	return bytesToBinaryString(b, depth)
}

// KeyToCID tries to interpret key as a CID string.
func KeyToCID(key string) (cid.Cid, error) {
	return cid.Decode(key)
}

// KeyToStableHex returns a stable 64-char hex string for any key (CID strings, hashes, etc.).
// Used to keep shard routing stable even when the key isn't a raw SHA-256 hex string.
// This is essential as keys are ManifestCID strings, not raw hashes.
func KeyToStableHex(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// TargetShardForPayload returns the shard (cluster) that should hold a file, given its payload CID
// and the current tree depth. Invariant: each file lives in exactly one cluster; this is the canonical
// way to derive that cluster from content. depth 1 => "0" or "1"; depth 2 => "00","01","10","11"; etc.
func TargetShardForPayload(payloadCIDStr string, depth int) string {
	if depth < 1 {
		depth = 1
	}
	return GetHexBinaryPrefix(KeyToStableHex(payloadCIDStr), depth)
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

// GetPayloadCIDForShardAssignment extracts PayloadCID from a ResearchObject given its ManifestCID.
// This is used for stable shard assignment - PayloadCID is content-based and stable,
// while ManifestCID includes timestamp/metadata and changes on every ingestion.
// Returns the PayloadCID string, or the ManifestCID string as fallback if extraction fails.
func GetPayloadCIDForShardAssignment(ctx context.Context, client ipfs.IPFSClient, manifestCIDStr string) string {
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
		log.Printf("[Shard] Warning: Could not fetch ResearchObject for %s, using ManifestCID for shard assignment: %v", manifestCIDStr, err)
		return manifestCIDStr
	}

	// Decode ResearchObject
	var ro schema.ResearchObject
	if err := ro.UnmarshalCBOR(manifestBytes); err != nil {
		log.Printf("[Shard] Warning: Could not decode ResearchObject for %s, using ManifestCID for shard assignment: %v", manifestCIDStr, err)
		return manifestCIDStr
	}

	// Return PayloadCID string for stable shard assignment
	return ro.Payload.String()
}

// TruncateCID returns the CID string unchanged (no truncation).
// Kept for API compatibility; callers get full CID/identifier in logs.
func TruncateCID(cidStr string, maxLen int) string {
	return cidStr
}

// LogError logs an error with consistent formatting including component, operation, and identifier.
// This standardizes error logging across the codebase.
func LogError(component, operation, identifier string, err error) {
	log.Printf("[Error] %s: Failed to %s %s: %v",
		component, operation, identifier, err)
}

// LogErrorWithContext logs an error with additional context information.
// Use this when you need to include extra details beyond the standard format.
func LogErrorWithContext(component, operation, identifier string, context string, err error) {
	log.Printf("[Error] %s: Failed to %s %s (%s): %v",
		component, operation, identifier, context, err)
}

// LogWarning logs a warning with consistent formatting.
func LogWarning(component, message, identifier string) {
	log.Printf("[Warning] %s: %s %s", component, message, identifier)
}

// LogWarningWithContext logs a warning with additional context.
func LogWarningWithContext(component, message, identifier, context string) {
	log.Printf("[Warning] %s: %s %s (%s)", component, message, identifier, context)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
