package common

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"dlockss/pkg/ipfs"
	"dlockss/pkg/schema"

	"github.com/ipfs/go-cid"
)

func GetBinaryPrefix(s string, depth int) string {
	h := sha256.Sum256([]byte(s))
	return bytesToBinaryString(h[:], depth)
}

func GetHexBinaryPrefix(hexStr string, depth int) (string, error) {
	b, err := hex.DecodeString(hexStr)
	if err != nil {
		return "", fmt.Errorf("decode hex %q: %w", hexStr, err)
	}
	return bytesToBinaryString(b, depth), nil
}

func KeyToStableHex(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

func TargetShardForPayload(payloadCIDStr string, depth int) (string, error) {
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

// GetPayloadCIDForShardAssignment resolves the payload CID from a manifest.
// Returns (payloadCID, nil) on success, or (manifestCIDStr, error) as a
// fallback when the payload cannot be resolved.
func GetPayloadCIDForShardAssignment(ctx context.Context, client ipfs.IPFSClient, manifestCIDStr string) (string, error) {
	if client == nil {
		return manifestCIDStr, fmt.Errorf("IPFS client is nil")
	}
	manifestCID, err := cid.Decode(manifestCIDStr)
	if err != nil {
		return manifestCIDStr, fmt.Errorf("decode manifest CID %s: %w", manifestCIDStr, err)
	}
	manifestBytes, err := client.GetBlock(ctx, manifestCID)
	if err != nil {
		return manifestCIDStr, fmt.Errorf("getblock %s: %w", manifestCIDStr, err)
	}
	var ro schema.ResearchObject
	if err := ro.UnmarshalCBOR(manifestBytes); err != nil {
		return manifestCIDStr, fmt.Errorf("unmarshal cbor %s: %w", manifestCIDStr, err)
	}
	return ro.Payload.String(), nil
}

// IsLegacyManifest returns true if the manifest at the given CID contains a
// legacy "ts" (timestamp) field. Legacy manifests produce non-deterministic
// CIDs and should be ignored by the network.
// Returns false if the block cannot be fetched or decoded (assume non-legacy).
func IsLegacyManifest(ctx context.Context, client ipfs.IPFSClient, manifestCIDStr string) bool {
	if client == nil {
		return false
	}
	manifestCID, err := cid.Decode(manifestCIDStr)
	if err != nil {
		return false
	}
	manifestBytes, err := client.GetBlock(ctx, manifestCID)
	if err != nil {
		return false
	}
	var ro schema.ResearchObject
	if err := ro.UnmarshalCBOR(manifestBytes); err != nil {
		return false
	}
	return ro.HasLegacyTimestamp
}
