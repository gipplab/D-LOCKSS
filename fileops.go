package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

func scanExistingFiles() {
	files, err := os.ReadDir(FileWatchFolder)
	if err != nil {
		log.Printf("Error scanning existing files: %v", err)
		return
	}

	log.Printf("[System] Found %d existing files.", len(files))
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		fullPath := filepath.Join(FileWatchFolder, file.Name())
		processNewFile(fullPath)
	}
}

func watchFolder(ctx context.Context) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Printf("[Error] Failed to create file watcher: %v", err)
		return
	}
	defer watcher.Close()

	if err := watcher.Add(FileWatchFolder); err != nil {
		log.Printf("[Error] Failed to add watch folder %s: %v", FileWatchFolder, err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}
			if event.Op&fsnotify.Create == fsnotify.Create {
				time.Sleep(100 * time.Millisecond)
				processNewFile(event.Name)
			}
		}
	}
}

func processNewFile(path string) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		log.Printf("[Error] Invalid path: %v", err)
		return
	}

	absWatch, err := filepath.Abs(FileWatchFolder)
	if err != nil {
		log.Printf("[Error] Invalid watch folder: %v", err)
		return
	}

	if !strings.HasPrefix(absPath, absWatch) {
		log.Printf("[Security] Rejected path outside watch folder: %s", path)
		return
	}

	hash, err := calculateFileHash(path)
	if err != nil {
		log.Printf("[Error] Failed to calculate hash for %s: %v", path, err)
		return
	}

	if !validateHash(hash) {
		log.Printf("[Error] Invalid hash format: %s", hash)
		return
	}

	pinFile(hash)
	provideCtx, provideCancel := context.WithTimeout(context.Background(), 30*time.Second)
	go func() {
		defer provideCancel()
		provideFile(provideCtx, hash)
	}()

	addKnownFile(hash)

	if shardMgr.AmIResponsibleFor(hash) {
		log.Printf("[Core] I am responsible for %s. Announcing to Shard.", hash)
		shardMgr.PublishToShard("NEW:" + hash)
	} else {
		targetPrefix := getHexBinaryPrefix(hash, len(shardMgr.currentShard))
		log.Printf("[Core] Custodial Mode: %s belongs to shard %s. Delegating but holding...", hash, targetPrefix)
		msg := fmt.Sprintf("DELEGATE:%s:%s", hash, targetPrefix)
		shardMgr.PublishToControl(msg)
	}
}

func calculateFileHash(filePath string) (string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer f.Close()
	h := sha256.New()
	_, err = io.Copy(h, f)
	if err != nil {
		return "", fmt.Errorf("failed to read file %s: %w", filePath, err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func hashToCid(hash string) (cid.Cid, error) {
	b, err := hex.DecodeString(hash)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("invalid hash format: %w", err)
	}
	mh, err := multihash.Sum(b, 0x12, 32)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("failed to create multihash: %w", err)
	}
	return cid.NewCidV1(cid.Raw, mh), nil
}
