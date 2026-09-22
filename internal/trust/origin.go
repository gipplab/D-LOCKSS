package trust

import (
	"fmt"

	"dlockss/pkg/schema"

	"github.com/libp2p/go-libp2p/core/peer"
)

// CheckManifestOrigin returns nil if this node may store the research object
// in block. When the gate is nil or unrestricted, every origin is allowed.
// Restriction is fail-closed: undecodable manifests and missing ingesters
// are refused.
func CheckManifestOrigin(block []byte, gate OriginGate) (peer.ID, error) {
	if gate == nil || !gate.RestrictsOrigins() {
		return "", nil
	}
	if len(block) == 0 {
		return "", fmt.Errorf("empty manifest")
	}
	var ro schema.ResearchObject
	if err := ro.UnmarshalCBOR(block); err != nil {
		return "", fmt.Errorf("manifest decode: %w", err)
	}
	if ro.IngestedBy == "" {
		return "", fmt.Errorf("manifest missing ingester")
	}
	if !gate.AllowsOrigin(ro.IngestedBy) {
		return ro.IngestedBy, fmt.Errorf("untrusted origin %s", ro.IngestedBy)
	}
	return ro.IngestedBy, nil
}
