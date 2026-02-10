package fileops

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
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
		fp.TryEnqueue(path)
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
		log.Printf("[FileOps] Queue full, dropping file: %s", path)
		return false
	}
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
