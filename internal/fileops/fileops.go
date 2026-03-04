package fileops

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/internal/badbits"
	"dlockss/internal/common"
	"dlockss/internal/config"
	"dlockss/pkg/ipfs"
	"dlockss/pkg/schema"
)

const maxRetryQueueSize = 1000
const retryDrainInterval = 10 * time.Second

const recentIngestTTL = 30 * time.Second

// ShardIdentity provides the node's identity and shard membership queries.
type ShardIdentity interface {
	PeerID() peer.ID
	GetShardInfo() (string, int)
	AnnouncePinned(manifestCID string)
	AmIResponsibleFor(key string) bool
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

// FileProcessor handles file ingestion and processing.
type FileProcessor struct {
	cfg        *config.Config
	ipfsClient ipfs.IPFSClient
	badBits    *badbits.Filter
	shardMgr   ShardCoordinator
	storageMgr StorageTracker
	privKey    crypto.PrivKey
	jobQueue   chan string
	signer     MessageSigner
	ctx        context.Context
	cancel     context.CancelFunc

	retryQueue []string
	retryMu    sync.Mutex

	recentIngestMu sync.Mutex
	recentIngests  map[string]time.Time

	// File event deduplication (watcher)
	deduperMu   sync.Mutex
	deduperInfo map[string]fileEventInfo

	// Stability tracking: files waiting for size to settle before ingest
	stabilityMu    sync.Mutex
	stabilityPath  map[string]int64
	stabilityTimer map[string]*time.Timer
}

// FileProcessorConfig holds all dependencies for a FileProcessor.
type FileProcessorConfig struct {
	Cfg        *config.Config
	IPFSClient ipfs.IPFSClient
	Shard      ShardCoordinator
	Storage    StorageTracker
	PrivKey    crypto.PrivKey
	Signer     MessageSigner
	BadBits    *badbits.Filter
}

// NewFileProcessor creates a new FileProcessor with dependencies.
func NewFileProcessor(cfg FileProcessorConfig) *FileProcessor {
	ctx, cancel := context.WithCancel(context.Background())
	fp := &FileProcessor{
		cfg:            cfg.Cfg,
		ipfsClient:     cfg.IPFSClient,
		badBits:        cfg.BadBits,
		shardMgr:       cfg.Shard,
		storageMgr:     cfg.Storage,
		privKey:        cfg.PrivKey,
		signer:         cfg.Signer,
		jobQueue:       make(chan string, cfg.Cfg.MaxConcurrentFileProcessing*100),
		ctx:            ctx,
		cancel:         cancel,
		recentIngests:  make(map[string]time.Time),
		deduperInfo:    make(map[string]fileEventInfo),
		stabilityPath:  make(map[string]int64),
		stabilityTimer: make(map[string]*time.Timer),
	}
	fp.startWorkers()
	go fp.retryLoop()
	return fp
}

func (fp *FileProcessor) startWorkers() {
	for i := 0; i < fp.cfg.MaxConcurrentFileProcessing; i++ {
		go fp.workerLoop()
	}
}

func (fp *FileProcessor) workerLoop() {
	for {
		select {
		case <-fp.ctx.Done():
			return
		case path := <-fp.jobQueue:
			fp.processNewFile(path)
		}
	}
}

// Stop stops the file processor workers.
func (fp *FileProcessor) Stop() {
	fp.cancel()
}

// retryLoop periodically drains the retry queue and re-attempts enqueue.
func (fp *FileProcessor) retryLoop() {
	ticker := time.NewTicker(retryDrainInterval)
	defer ticker.Stop()
	for {
		select {
		case <-fp.ctx.Done():
			return
		case <-ticker.C:
			fp.drainRetryQueue()
		}
	}
}

func (fp *FileProcessor) drainRetryQueue() {
	fp.retryMu.Lock()
	toRetry := fp.retryQueue
	fp.retryQueue = nil
	fp.retryMu.Unlock()

	var stillPending []string
	for _, path := range toRetry {
		if fp.TryEnqueue(path) {
			// success
		} else {
			stillPending = append(stillPending, path)
		}
	}
	if len(stillPending) > 0 {
		fp.retryMu.Lock()
		space := maxRetryQueueSize - len(fp.retryQueue)
		add := len(stillPending)
		if add > space {
			add = space
			slog.Warn("retry queue full, dropping paths", "dropped", len(stillPending)-space)
		}
		fp.retryQueue = append(fp.retryQueue, stillPending[:add]...)
		fp.retryMu.Unlock()
	}
}

// ScanExistingFiles walks the data directory and processes any existing files.
func (fp *FileProcessor) ScanExistingFiles() {
	var fileCount int
	err := filepath.Walk(fp.cfg.FileWatchFolder, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			slog.Warn("error accessing path", "path", path, "error", err)
			return nil
		}
		if info.IsDir() {
			return nil
		}
		fp.EnqueueOrRetry(path)
		fileCount++
		return nil
	})
	if err != nil {
		slog.Error("error scanning existing files", "error", err)
		return
	}
	slog.Info("found existing files", "count", fileCount)
}

// TryEnqueue attempts to add a file to the processing queue.
func (fp *FileProcessor) TryEnqueue(path string) bool {
	select {
	case fp.jobQueue <- path:
		return true
	default:
		return false
	}
}

// EnqueueOrRetry tries to enqueue; on backpressure, adds to retry queue for later.
func (fp *FileProcessor) EnqueueOrRetry(path string) bool {
	if fp.TryEnqueue(path) {
		return true
	}
	fp.retryMu.Lock()
	if len(fp.retryQueue) < maxRetryQueueSize {
		fp.retryQueue = append(fp.retryQueue, path)
		fp.retryMu.Unlock()
		slog.Warn("queue full, queued for retry", "path", path)
		return true
	}
	fp.retryMu.Unlock()
	slog.Warn("queue and retry full, dropping file", "path", path)
	return false
}

// SignProtocolMessage signs a message with the node's private key.
func (fp *FileProcessor) SignProtocolMessage(msg schema.Signable) error {
	if fp.signer != nil {
		return fp.signer.SignProtocolMessage(msg)
	}
	if msg == nil {
		return fmt.Errorf("message is nil")
	}
	nonceSize := fp.cfg.NonceSize
	if nonceSize < 1 {
		nonceSize = 16
	}
	nonce, err := common.NewNonce(nonceSize)
	if err != nil {
		return err
	}
	env := msg.GetEnvelope()
	env.SenderID = fp.shardMgr.PeerID()
	env.Timestamp = time.Now().Unix()
	env.Nonce = nonce
	env.Sig = nil

	unsigned, err := msg.MarshalCBORForSigning()
	if err != nil {
		return err
	}
	sig, err := fp.privKey.Sign(unsigned)
	if err != nil {
		return err
	}
	env.Sig = sig
	return nil
}
