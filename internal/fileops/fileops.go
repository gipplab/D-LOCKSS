package fileops

import (
	"context"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/internal/badbits"
	"dlockss/internal/config"
	"dlockss/pkg/ipfs"
	"dlockss/pkg/schema"
)

// ShardIdentity provides the node's identity and shard membership queries.
type ShardIdentity interface {
	NodeID() peer.ID
	GetShardInfo() string
	AnnouncePinned(manifestCID string)
	AmIResponsibleFor(key string) bool
	IsLocalNodeIngestor() bool
}

// ShardPublisher handles ingest publishing and cluster pinning within the node's own shard.
type ShardPublisher interface {
	PinToCluster(ctx context.Context, c cid.Cid) error
	PublishIngestMessageToCurrentAndChildIfSplit(data []byte, currentShard, payloadCIDStr string)
}

// CustodialInjector handles cross-shard file injection for files the node is not responsible for.
type CustodialInjector interface {
	ResolveTargetShardForCustodial(nominalTargetShard, payloadCIDStr string) string
	JoinShardAsObserver(shardID string) bool
	LeaveShardAsObserver(shardID string)
	EnsureClusterForShard(ctx context.Context, shardID string) error
	PinToShard(ctx context.Context, shardID string, c cid.Cid) error
	PublishToShardCBOR(data []byte, shardID string)
}

// ShardCoordinator composes all shard-management capabilities needed by file processing.
type ShardCoordinator interface {
	ShardIdentity
	ShardPublisher
	CustodialInjector
}

// StorageTracker abstracts the storage operations needed by file processing.
type StorageTracker interface {
	PinFile(manifestCIDStr string) bool
	AddKnownFile(key string)
	ProvideFile(ctx context.Context, key string)
}

// MessageSigner abstracts protocol message signing.
type MessageSigner interface {
	SignProtocolMessage(msg schema.Signable) error
}

// NodeSigner signs data with the Kubo node's key ('ipfs key sign').
type NodeSigner interface {
	SignWithNodeKey(ctx context.Context, data []byte) ([]byte, error)
}

// SignatureSource returns the users' signatures of a payload, one per key,
// see schema.VerifyUpload (trust.WikiSignatures).
type SignatureSource interface {
	Signatures(ctx context.Context, payload cid.Cid) ([]schema.UploaderSignature, error)
}

// FileProcessor ingests the files that reach Kubo: uploads through the
// node's API and pins that users create.
type FileProcessor struct {
	cfg        *config.Config
	ipfsClient ipfs.IPFSClient
	badBits    *badbits.Filter
	shardMgr   ShardCoordinator
	storageMgr StorageTracker
	nodeSigner NodeSigner
	signatures SignatureSource
	signer     MessageSigner
	ctx        context.Context
	cancel     context.CancelFunc

	manifests manifestIndex
}

// FileProcessorConfig holds all dependencies for a FileProcessor.
type FileProcessorConfig struct {
	Cfg        *config.Config
	IPFSClient ipfs.IPFSClient
	Shard      ShardCoordinator
	Storage    StorageTracker
	NodeSigner NodeSigner
	Signer     MessageSigner
	BadBits    *badbits.Filter
	// Signatures, if not nil (DLOCKSS_WIKI_API is set), makes pin ingest
	// accept only files that a user signed, with one manifest per signature.
	Signatures SignatureSource
}

// NewFileProcessor creates a new FileProcessor with dependencies.
func NewFileProcessor(cfg FileProcessorConfig) *FileProcessor {
	ctx, cancel := context.WithCancel(context.Background())
	return &FileProcessor{
		cfg:        cfg.Cfg,
		ipfsClient: cfg.IPFSClient,
		badBits:    cfg.BadBits,
		shardMgr:   cfg.Shard,
		storageMgr: cfg.Storage,
		nodeSigner: cfg.NodeSigner,
		signatures: cfg.Signatures,
		signer:     cfg.Signer,
		ctx:        ctx,
		cancel:     cancel,
	}
}

// Stop cancels the work the processor started in the background, such as
// DHT provides.
func (fp *FileProcessor) Stop() {
	fp.cancel()
}
