package clusters

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

const (
	// ClusterSecretSalt is the salt used to derive shard-specific secrets.
	// Changing this invalidates all cluster secrets (effectively a network reset).
	ClusterSecretSalt = "dlockss-v1-cluster-secret"
)

// GenerateClusterSecret derives a deterministic 32-byte secret for a given shard ID.
// This secret is used by IPFS Cluster's libp2p protector to secure the private swarm.
func GenerateClusterSecret(shardID string) ([]byte, error) {
	// Use HMAC-SHA256 with the fixed salt and the shard ID
	h := hmac.New(sha256.New, []byte(ClusterSecretSalt))
	h.Write([]byte(shardID))
	sum := h.Sum(nil)

	// IPFS Cluster requires a 32-byte secret. SHA256 output is 32 bytes.
	// We return the raw bytes.
	if len(sum) != 32 {
		return nil, fmt.Errorf("generated secret has invalid length: %d", len(sum))
	}
	return sum, nil
}

// GenerateClusterSecretHex returns the hex-encoded string of the secret,
// often used for configuration or logging.
func GenerateClusterSecretHex(shardID string) (string, error) {
	secret, err := GenerateClusterSecret(shardID)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(secret), nil
}
