package ipfs

import ipfsapi "github.com/ipfs/go-ipfs-api"

// unixfs-v1-2025 (IPIP-0499): CIDv1, sha2-256, 1 MiB raw leaves, balanced, width 1024.
const (
	unixfsV1Chunker      = "size-1048576"
	unixfsV1Hash         = "sha2-256"
	unixfsV1CidVersion   = 1
	unixfsV1MaxFileLinks = 1024
)

func unixfsV1AddOpts() []ipfsapi.AddOpts {
	return []ipfsapi.AddOpts{
		ipfsapi.Pin(true),
		ipfsapi.CidVersion(unixfsV1CidVersion),
		ipfsapi.RawLeaves(true),
		ipfsapi.Hash(unixfsV1Hash),
		addOption("chunker", unixfsV1Chunker),
		addOption("trickle", false),
		addOption("max-file-links", unixfsV1MaxFileLinks),
	}
}

func addOption(key string, value interface{}) ipfsapi.AddOpts {
	return func(rb *ipfsapi.RequestBuilder) error {
		rb.Option(key, value)
		return nil
	}
}
