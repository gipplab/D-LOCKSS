package fileops

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"

	"dlockss/internal/common"
	"dlockss/internal/config"
	"dlockss/internal/managers/shard"
	"dlockss/internal/managers/storage"
	"dlockss/internal/signing"
	"dlockss/pkg/ipfs"
	"dlockss/pkg/schema"
)

const maxRetryQueueSize = 1000
const retryDrainInterval = 10 * time.Second

// FileProcessor handles file ingestion and processing.
type FileProcessor struct {
	ipfsClient ipfs.IPFSClient
	shardMgr   *shard.ShardManager
	storageMgr *storage.StorageManager
	privKey    crypto.PrivKey
	semaphore  chan struct{}
	jobQueue   chan string
	signer     *signing.Signer
	ctx        context.Context
	cancel     context.CancelFunc

	retryQueue []string
	retryMu    sync.Mutex
}

// NewFileProcessor creates a new FileProcessor with dependencies.
func NewFileProcessor(
	client ipfs.IPFSClient,
	sm *shard.ShardManager,
	stm *storage.StorageManager,
	key crypto.PrivKey,
	signer *signing.Signer,
) *FileProcessor {
	ctx, cancel := context.WithCancel(context.Background())
	fp := &FileProcessor{
		ipfsClient: client,
		shardMgr:   sm,
		storageMgr: stm,
		privKey:    key,
		signer:     signer,
		semaphore:  make(chan struct{}, config.MaxConcurrentFileProcessing),
		jobQueue:   make(chan string, config.MaxConcurrentFileProcessing*100),
		ctx:        ctx,
		cancel:     cancel,
	}
	fp.startWorkers()
	go fp.retryLoop()
	return fp
}

func (fp *FileProcessor) startWorkers() {
	for i := 0; i < config.MaxConcurrentFileProcessing; i++ {
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
			log.Printf("[FileOps] Retry queue full, dropping %d paths", len(stillPending)-space)
		}
		fp.retryQueue = append(fp.retryQueue, stillPending[:add]...)
		fp.retryMu.Unlock()
	}
}

// ScanExistingFiles walks the data directory and processes any existing files.
func (fp *FileProcessor) ScanExistingFiles() {
	var fileCount int
	err := filepath.Walk(config.FileWatchFolder, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Printf("[Warning] Error accessing %s: %v", path, err)
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
		log.Printf("[Error] Error scanning existing files: %v", err)
		return
	}
	log.Printf("[System] Found %d existing files (recursive scan).", fileCount)
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
		log.Printf("[FileOps] Queue full, queued for retry: %s", path)
		return true
	}
	fp.retryMu.Unlock()
	log.Printf("[FileOps] Queue and retry full, dropping file: %s", path)
	return false
}

// SignProtocolMessage signs a message with the node's private key.
func (fp *FileProcessor) SignProtocolMessage(msg interface{}) error {
	if fp.signer != nil {
		return fp.signer.SignProtocolMessage(msg)
	}
	if msg == nil {
		return fmt.Errorf("message is nil")
	}
	nonceSize := signing.EffectiveNonceSizeForSigning()
	nonce, err := common.NewNonce(nonceSize)
	if err != nil {
		return err
	}

	ts := time.Now().Unix()

	switch m := msg.(type) {
	case *schema.IngestMessage:
		m.SenderID = fp.shardMgr.GetHost().ID()
		m.Timestamp = ts
		m.Nonce = nonce
		m.Sig = nil

		unsigned, err := m.MarshalCBORForSigning()
		if err != nil {
			return err
		}

		sig, err := fp.privKey.Sign(unsigned)
		if err != nil {
			return err
		}

		m.Sig = sig
		return nil
	case *schema.ReplicationRequest:
		m.SenderID = fp.shardMgr.GetHost().ID()
		m.Timestamp = ts
		m.Nonce = nonce
		m.Sig = nil

		unsigned, err := m.MarshalCBORForSigning()
		if err != nil {
			return err
		}

		sig, err := fp.privKey.Sign(unsigned)
		if err != nil {
			return err
		}

		m.Sig = sig
		return nil
	case *schema.UnreplicateRequest:
		m.SenderID = fp.shardMgr.GetHost().ID()
		m.Timestamp = ts
		m.Nonce = nonce
		m.Sig = nil

		unsigned, err := m.MarshalCBORForSigning()
		if err != nil {
			return err
		}

		sig, err := fp.privKey.Sign(unsigned)
		if err != nil {
			return err
		}

		m.Sig = sig
		return nil
	default:
		return fmt.Errorf("unsupported message type for signing: %T", msg)
	}
}
