package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	"github.com/multiformats/go-multihash"
)

// --- Configuration ---
const (
	ControlTopicName    = "dlockss-control"
	DiscoveryServiceTag = "dlockss-v2-prod" // Changed tag to avoid finding dev nodes
	FileWatchFolder     = "./data"          // Production usually avoids "my-pdfs"
	MinReplication      = 5
	MaxReplication      = 10

	// 15 Minutes is standard for DHT provider refresh cycles.
	CheckInterval = 15 * time.Minute

	// If more messages came in in the last 60s --> new shards
	MaxShardLoad = 2000
)

// --- Globals ---
var (
	// State for what we are physically storing
	pinnedFiles = struct {
		sync.RWMutex
		hashes map[string]bool
	}{hashes: make(map[string]bool)}

	// FIX: State for what we are tracking/monitoring
	knownFiles = struct {
		sync.RWMutex
		hashes map[string]bool
	}{hashes: make(map[string]bool)}

	globalDHT *dht.IpfsDHT
	shardMgr  *ShardManager
)

// --- Main ---
func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Host Setup
	h, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0", "/ip6/::/tcp/0"),
		libp2p.Routing(func(h host.Host) (routing.PeerRouting, error) {
			var err error
			globalDHT, err = dht.New(ctx, h, dht.Mode(dht.ModeServer))
			return globalDHT, err
		}),
	)
	if err != nil {
		panic(err)
	}

	fmt.Printf("--- Node ID: %s ---\n", h.ID().String())
	fmt.Printf("--- Addresses: %v ---\n", h.Addrs())

	// 2. MDNS
	mdnsSvc := mdns.NewMdnsService(h, DiscoveryServiceTag, &discoveryNotifee{h: h, ctx: ctx})
	if err := mdnsSvc.Start(); err != nil {
		panic(err)
	}

	// 3. DHT Bootstrap
	if err = globalDHT.Bootstrap(ctx); err != nil {
		panic(err)
	}

	// 4. GossipSub
	ps, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		panic(err)
	}

	// 5. Initialize Shard Manager
	initialShard := getBinaryPrefix(h.ID().String(), 1)
	shardMgr = NewShardManager(ctx, h, ps, initialShard)

	// 6. Start Message Handlers
	go shardMgr.Run()
	go inputLoop(shardMgr)

	// 7. File System Setup
	if err := os.Mkdir(FileWatchFolder, 0755); err != nil && !os.IsExist(err) {
		log.Printf("Error creating folder: %v", err)
	}

	log.Println("Scanning for existing files...")
	scanExistingFiles()

	go watchFolder(ctx)

	// 8. Periodic Maintenance
	go runReplicationChecker(ctx)

	// 9. Shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\nShutting down...")
}

// ######################################################################
// Shard Manager
// ######################################################################

type ShardManager struct {
	ctx context.Context
	h   host.Host
	ps  *pubsub.PubSub

	mu           sync.RWMutex
	currentShard string
	shardTopic   *pubsub.Topic
	shardSub     *pubsub.Subscription

	controlTopic *pubsub.Topic
	controlSub   *pubsub.Subscription

	msgCounter int
}

func NewShardManager(ctx context.Context, h host.Host, ps *pubsub.PubSub, startShard string) *ShardManager {
	sm := &ShardManager{
		ctx:          ctx,
		h:            h,
		ps:           ps,
		currentShard: startShard,
	}
	sm.joinChannels()
	return sm
}

func (sm *ShardManager) joinChannels() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// 1. Join Control Channel
	if sm.controlTopic == nil {
		t, _ := sm.ps.Join(ControlTopicName)
		s, _ := t.Subscribe()
		sm.controlTopic = t
		sm.controlSub = s
		log.Printf("[System] Joined Control Channel: %s", ControlTopicName)
	}

	// 2. Join Data Shard
	topicName := fmt.Sprintf("dlockss-shard-%s", sm.currentShard)
	t, err := sm.ps.Join(topicName)
	if err != nil {
		log.Printf("Failed to join shard %s: %v", topicName, err)
		return
	}
	sub, _ := t.Subscribe()

	if sm.shardTopic != nil {
		sm.shardSub.Cancel()
		sm.shardTopic.Close()
	}

	sm.shardTopic = t
	sm.shardSub = sub
	sm.msgCounter = 0
	log.Printf("[Sharding] Active Data Shard: %s (Topic: %s)", sm.currentShard, topicName)
}

func (sm *ShardManager) Run() {
	go sm.readControl()
	go sm.readShard()
}

func (sm *ShardManager) readControl() {
	for {
		msg, err := sm.controlSub.Next(sm.ctx)
		if err != nil {
			return
		}
		if msg.GetFrom() == sm.h.ID() {
			continue
		}

		data := string(msg.Data)
		if strings.HasPrefix(data, "DELEGATE:") {
			parts := strings.Split(data, ":")
			if len(parts) < 3 {
				continue
			}
			targetHash := parts[1]
			targetShard := parts[2]

			sm.mu.RLock()
			myShard := sm.currentShard
			sm.mu.RUnlock()

			if strings.HasPrefix(targetShard, myShard) || strings.HasPrefix(myShard, targetShard) {
				log.Printf("[Delegate] Accepted delegation for %s (I am %s)", targetHash, myShard)
				// FIX: Track this file so we monitor it
				addKnownFile(targetHash)
				go checkReplication(sm.ctx, targetHash)
			}
		}
	}
}

func (sm *ShardManager) readShard() {
	for {
		sm.mu.RLock()
		sub := sm.shardSub
		sm.mu.RUnlock()

		if sub == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		msg, err := sub.Next(sm.ctx)
		if err != nil {
			return
		}

		sm.mu.Lock()
		sm.msgCounter++
		shouldSplit := false
		if sm.msgCounter > MaxShardLoad {
			sm.msgCounter = 0
			shouldSplit = true
		}
		sm.mu.Unlock()

		if shouldSplit {
			sm.splitShard()
		}

		if msg.GetFrom() == sm.h.ID() {
			continue
		}

		data := string(msg.Data)
		if strings.HasPrefix(data, "NEED:") {
			hash := strings.TrimPrefix(data, "NEED:")
			// FIX: Track announced files
			addKnownFile(hash)
			go checkReplication(sm.ctx, hash)
		} else if strings.HasPrefix(data, "NEW:") {
			hash := strings.TrimPrefix(data, "NEW:")
			// FIX: Track announced files
			addKnownFile(hash)
			log.Printf("[Network] New content announced in my shard: %s", hash)
			go checkReplication(sm.ctx, hash)
		}
	}
}

func (sm *ShardManager) splitShard() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	nextBit := getBinaryPrefix(sm.h.ID().String(), len(sm.currentShard)+1)

	if nextBit == sm.currentShard {
		return
	}

	oldShard := sm.currentShard
	sm.currentShard = nextBit

	log.Printf("!!! SHARD OVERLOAD (%s) !!! Splitting from %s -> %s", oldShard, oldShard, sm.currentShard)
	go sm.joinChannels()
}

func (sm *ShardManager) PublishToShard(msg string) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	if sm.shardTopic != nil {
		sm.shardTopic.Publish(sm.ctx, []byte(msg))
	}
}

func (sm *ShardManager) PublishToControl(msg string) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	if sm.controlTopic != nil {
		sm.controlTopic.Publish(sm.ctx, []byte(msg))
	}
}

func (sm *ShardManager) AmIResponsibleFor(hash string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	hashPrefix := getHexBinaryPrefix(hash, len(sm.currentShard))
	return hashPrefix == sm.currentShard
}

// ######################################################################
// Logic & Utils
// ######################################################################

func getBinaryPrefix(s string, depth int) string {
	h := sha256.Sum256([]byte(s))
	return bytesToBinaryString(h[:], depth)
}

func getHexBinaryPrefix(hexStr string, depth int) string {
	b, _ := hex.DecodeString(hexStr)
	return bytesToBinaryString(b, depth)
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

// ######################################################################
// File Handling
// ######################################################################

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
	watcher, _ := fsnotify.NewWatcher()
	defer watcher.Close()
	watcher.Add(FileWatchFolder)

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
	hash, err := calculateFileHash(path)
	if err != nil {
		return
	}

	pinFile(hash)
	go provideFile(context.Background(), hash)

	// FIX: Ensure we track this file
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

func checkReplication(ctx context.Context, hash string) {
	responsible := shardMgr.AmIResponsibleFor(hash)
	pinned := isPinned(hash)

	// FIX: If we are not responsible AND not pinning, we don't need to track this anymore.
	if !responsible && !pinned {
		removeKnownFile(hash)
		return
	}

	c, _ := hashToCid(hash)
	provs := globalDHT.FindProvidersAsync(ctx, c, 0)
	count := 0
	for range provs {
		count++
	}

	// CASE 1: Replication is too low
	if count < MinReplication {
		if responsible && !pinned {
			log.Printf("[Replication] Low Redundancy (%d/%d). RE-PINNING %s", count, MinReplication, hash)
			pinFile(hash)
			provideFile(ctx, hash)
		}
		// If custodial, keep holding.

		// Ask for help
		shardMgr.PublishToShard("NEED:" + hash)
	}

	// CASE 2: Garbage Collection
	// Sub-case A: I am responsible, but it's over-replicated
	if responsible && count > MaxReplication && pinned {
		log.Printf("[Replication] High Redundancy (%d/%d). Unpinning %s (Monitoring only)", count, MaxReplication, hash)
		unpinFile(hash) // Remove from storage, but KEPT in knownFiles for tracking
	}

	// Sub-case B: I am NOT responsible (Custodial Mode), and it is Safe to Handoff
	if !responsible && count >= MinReplication && pinned {
		log.Printf("[Replication] Handoff Complete (%d/%d copies found). Unpinning custodial file %s", count, MinReplication, hash)
		unpinFile(hash)
	}
}

func runReplicationChecker(ctx context.Context) {
	ticker := time.NewTicker(CheckInterval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// FIX: Iterate over KnownFiles (Tracking List) instead of PinnedFiles (Storage List)
			knownFiles.RLock()
			for hash := range knownFiles.hashes {
				go checkReplication(ctx, hash)
			}
			knownFiles.RUnlock()
		}
	}
}

// ######################################################################
// Helpers
// ######################################################################

func pinFile(hash string) {
	pinnedFiles.Lock()
	defer pinnedFiles.Unlock()
	if !pinnedFiles.hashes[hash] {
		pinnedFiles.hashes[hash] = true
	}
}

func unpinFile(hash string) {
	pinnedFiles.Lock()
	defer pinnedFiles.Unlock()
	if pinnedFiles.hashes[hash] {
		delete(pinnedFiles.hashes, hash)
	}
}

func isPinned(hash string) bool {
	pinnedFiles.RLock()
	defer pinnedFiles.RUnlock()
	return pinnedFiles.hashes[hash]
}

// FIX: Helper to track files we care about
func addKnownFile(hash string) {
	knownFiles.Lock()
	defer knownFiles.Unlock()
	knownFiles.hashes[hash] = true
}

// FIX: Helper to stop tracking files
func removeKnownFile(hash string) {
	knownFiles.Lock()
	defer knownFiles.Unlock()
	delete(knownFiles.hashes, hash)
}

func provideFile(ctx context.Context, hash string) {
	c, _ := hashToCid(hash)
	_ = globalDHT.Provide(ctx, c, true)
}

func calculateFileHash(filePath string) (string, error) {
	f, _ := os.Open(filePath)
	defer f.Close()
	h := sha256.New()
	io.Copy(h, f)
	return hex.EncodeToString(h.Sum(nil)), nil
}

func hashToCid(hash string) (cid.Cid, error) {
	b, _ := hex.DecodeString(hash)
	mh, _ := multihash.Sum(b, 0x12, 32)
	return cid.NewCidV1(cid.Raw, mh), nil
}

type discoveryNotifee struct {
	h   host.Host
	ctx context.Context
}

func (n *discoveryNotifee) HandlePeerFound(pi peer.AddrInfo) {
	if n.h.Network().Connectedness(pi.ID) != network.Connected {
		n.h.Connect(n.ctx, pi)
	}
}

func inputLoop(sm *ShardManager) {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		sm.PublishToShard(line)
	}
}
