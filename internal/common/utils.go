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

func ValidateHash(hash string) bool {
	if len(hash) != 64 {
		return false
	}
	_, err := hex.DecodeString(hash)
	return err == nil
}

func GetBinaryPrefix(s string, depth int) string {
	h := sha256.Sum256([]byte(s))
	return bytesToBinaryString(h[:], depth)
}

func GetHexBinaryPrefix(hexStr string, depth int) string {
	b, err := hex.DecodeString(hexStr)
	if err != nil {
		LogError("Utils", "decode hex string", hexStr, err)
		return ""
	}
	return bytesToBinaryString(b, depth)
}

func KeyToCID(key string) (cid.Cid, error) {
	return cid.Decode(key)
}

func KeyToStableHex(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

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

func GetPayloadCIDForShardAssignment(ctx context.Context, client ipfs.IPFSClient, manifestCIDStr string) string {
	if client == nil {
		return manifestCIDStr
	}
	manifestCID, err := cid.Decode(manifestCIDStr)
	if err != nil {
		return manifestCIDStr
	}
	manifestBytes, err := client.GetBlock(ctx, manifestCID)
	if err != nil {
		log.Printf("[Shard] GetBlock %s: %v", manifestCIDStr, err)
		return manifestCIDStr
	}
	var ro schema.ResearchObject
	if err := ro.UnmarshalCBOR(manifestBytes); err != nil {
		log.Printf("[Shard] UnmarshalCBOR %s: %v", manifestCIDStr, err)
		return manifestCIDStr
	}
	return ro.Payload.String()
}

func LogError(component, operation, identifier string, err error) {
	log.Printf("[Error] %s: Failed to %s %s: %v", component, operation, identifier, err)
}

func LogErrorWithContext(component, operation, identifier string, context string, err error) {
	log.Printf("[Error] %s: Failed to %s %s (%s): %v", component, operation, identifier, context, err)
}

func LogWarning(component, message, identifier string) {
	log.Printf("[Warning] %s: %s %s", component, message, identifier)
}

func LogWarningWithContext(component, message, identifier, context string) {
	log.Printf("[Warning] %s: %s %s (%s)", component, message, identifier, context)
}
