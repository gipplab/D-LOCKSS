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
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	"github.com/multiformats/go-multihash"
)

// --- Configuration ---
const TopicName = "dlockss-topic"
const discoveryServiceTag = "dlockss-example"
const fileWatchFolder = "./my-pdfs" // The folder to watch
const minReplication = 5
const maxReplication = 10
const checkInterval = 1 * time.Minute // How often to check replication

// --- Globals ---
var pinnedFiles = struct {
	sync.RWMutex
	hashes map[string]bool
}{
	hashes: make(map[string]bool),
}

var knownFiles = struct {
	sync.RWMutex
	hashes map[string]bool
}{
	hashes: make(map[string]bool),
}

var globalDHT *dht.IpfsDHT

func main() {
	// Create a context that gracefully handles termination signals
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- 1. Create a libp2p Host (MODIFIED) ---
	host, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0", "/ip6/::/tcp/0"),
		libp2p.Routing(func(h host.Host) (routing.PeerRouting, error) {
			var err error
			globalDHT, err = dht.New(ctx, h, dht.Mode(dht.ModeServer))
			return globalDHT, err
		}),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create libp2p host: %w", err))
	}

	// Print this node's multiaddresses
	fmt.Println("--- Your Node's Addresses ---")
	for _, addr := range host.Addrs() {
		fmt.Printf("%s/p2p/%s\n", addr, host.ID().String())
	}
	fmt.Println("-------------------------------")
	fmt.Println("Waiting for peers... (Run another instance of this app on your network)")

	// --- 2. Set up mDNS Discovery ---
	notifee := &discoveryNotifee{h: host, ctx: ctx}
	mdnsService := mdns.NewMdnsService(host, discoveryServiceTag, notifee)
	if err := mdnsService.Start(); err != nil {
		panic(fmt.Errorf("failed to start mDNS service: %w", err))
	}

	// --- 2.5. Bootstrap the DHT ---
	fmt.Println("Bootstrapping DHT...")
	if err = globalDHT.Bootstrap(ctx); err != nil {
		panic(fmt.Errorf("failed to bootstrap DHT: %w", err))
	}

	// --- 3. Set up GossipSub ---
	ps, err := pubsub.NewGossipSub(ctx, host)
	if err != nil {
		panic(fmt.Errorf("failed to create pubsub service: %w", err))
	}

	// --- 4. Join the Topic ---
	topic, err := ps.Join(TopicName)
	if err != nil {
		panic(fmt.Errorf("failed to join topic: %w", err))
	}

	// --- 5. Subscribe to the Topic ---
	sub, err := topic.Subscribe()
	if err != nil {
		panic(fmt.Errorf("failed to subscribe to topic: %w", err))
	}

	// --- 6. Handle Incoming and Outgoing Messages ---
	go readMessages(ctx, sub, host.ID(), topic)
	go publishMessages(ctx, topic)

	// --- 6.5. Start the File Watcher ---
	log.Println("Starting file watcher on:", fileWatchFolder)
	// Ensure the folder exists
	_ = os.Mkdir(fileWatchFolder, 0755)

	// --- NEW: Scan for existing files before starting watcher ---
	log.Println("Scanning for existing files in", fileWatchFolder, "...")
	scanExistingFiles(ctx, topic)
	// -----------------------------------------------------------

	go watchFolder(ctx, topic)

	// --- 6.6. Start the Replication Checker ---
	go startReplicationChecker(ctx, topic)

	// --- 7. Wait for Shutdown Signal ---
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\nShutting down...")

	if err := host.Close(); err != nil {
		fmt.Println("Error closing host:", err)
	}
	fmt.Println("Done.")
}

// ######################################################################
// discoveryNotifee (mDNS)
// ######################################################################

type discoveryNotifee struct {
	h   host.Host
	ctx context.Context
}

func (n *discoveryNotifee) HandlePeerFound(pi peer.AddrInfo) {
	fmt.Printf("\rDiscovered new peer: %s\n> ", pi.ID.String())
	if err := n.h.Connect(n.ctx, pi); err != nil {
		fmt.Printf("\rError connecting to peer %s: %s\n> ", pi.ID.String(), err)
	} else {
		fmt.Printf("\rConnected to: %s\n> ", pi.ID.String())
	}
}

// ######################################################################
// PubSub (Chat & Announcements)
// ######################################################################

func readMessages(ctx context.Context, sub *pubsub.Subscription, selfID peer.ID, topic *pubsub.Topic) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := sub.Next(ctx)
			if err != nil {
				if ctx.Err() == context.Canceled {
					return
				}
				fmt.Fprintln(os.Stderr, "Error reading from subscription:", err)
				return
			}

			if msg.GetFrom() == selfID {
				continue
			}

			data := string(msg.Data)

			if len(data) > 4 && data[:4] == "NEW:" {
				hash := data[4:]
				fmt.Printf("\r[Network]: New file announced: %s\n> ", hash)
				addKnownFile(hash)
				go checkReplication(ctx, hash, topic)

			} else if len(data) > 5 && data[:5] == "NEED:" {
				hash := data[5:]
				addKnownFile(hash)

				if !isPinned(hash) {
					fmt.Printf("\r[Network]: Received NEED for unpinned file, checking: %s\n> ", hash)
					go checkReplication(ctx, hash, topic)
				}

			} else {
				fmt.Printf("\r[%s]: %s\n> ", msg.GetFrom().ShortString(), data)
			}
		}
	}
}

func publishMessages(ctx context.Context, topic *pubsub.Topic) {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("> ")

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			fmt.Print("> ")
			continue
		}

		if (len(line) > 4 && line[:4] == "NEW:") || (len(line) > 5 && line[:5] == "NEED:") {
			fmt.Println("Cannot send messages starting with 'NEW:' or 'NEED:'")
			fmt.Print("> ")
			continue
		}

		if err := topic.Publish(ctx, []byte(line)); err != nil {
			fmt.Fprintln(os.Stderr, "Error publishing message:", err)
		}
		fmt.Print("> ")
	}

	if err := scanner.Err(); err != nil {
		if ctx.Err() != context.Canceled {
			fmt.Fprintln(os.Stderr, "Error reading from stdin:", err)
		}
	}
}

// ######################################################################
// Hashing and CID Helpers
// ######################################################################

func calculateFileHash(filePath string) (string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func hashToCid(hash string) (cid.Cid, error) {
	hashBytes, err := hex.DecodeString(hash)
	if err != nil {
		return cid.Undef, err
	}
	mh, err := multihash.Sum(hashBytes, 0x12, 32)
	if err != nil {
		return cid.Undef, err
	}
	return cid.NewCidV1(cid.Raw, mh), nil
}

// --- FUNCTION REMOVED ---
// func cidToHash(c cid.Cid) (string, error) { ... }
// (This was the unused function)

// ######################################################################
// File Watching and Replication Logic
// ######################################################################

func getKnownFiles() []string {
	knownFiles.RLock()
	defer knownFiles.RUnlock()
	keys := make([]string, 0, len(knownFiles.hashes))
	for k := range knownFiles.hashes {
		keys = append(keys, k)
	}
	return keys
}

func addKnownFile(hash string) {
	knownFiles.Lock()
	defer knownFiles.Unlock()
	knownFiles.hashes[hash] = true
}

func startReplicationChecker(ctx context.Context, topic *pubsub.Topic) {
	log.Println("Replication checker started, will run every", checkInterval)
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Stopping replication checker...")
			return
		case <-ticker.C:
			log.Println("--- Running periodic replication check ---")
			filesToCheck := getKnownFiles()
			if len(filesToCheck) == 0 {
				log.Println("No known files to check.")
				continue
			}

			log.Printf("Checking replication for %d files...", len(filesToCheck))
			for _, hash := range filesToCheck {
				go checkReplication(ctx, hash, topic)
			}
		}
	}
}

// scanExistingFiles processes all files already in the folder at startup
func scanExistingFiles(ctx context.Context, topic *pubsub.Topic) {
	entries, err := os.ReadDir(fileWatchFolder)
	if err != nil {
		log.Printf("Error scanning folder: %s", err)
		return
	}

	found := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		fullPath := filepath.Join(fileWatchFolder, entry.Name())
		log.Printf("Processing existing file: %s", fullPath)

		hash, err := calculateFileHash(fullPath)
		if err != nil {
			log.Printf("Error hashing existing file %s: %s", fullPath, err)
			continue
		}

		// Treat it just like a newly created file
		pinFile(hash)
		addKnownFile(hash)
		go provideFile(ctx, hash)
		announceFile(ctx, topic, hash) // Announce "NEW"
		found++
	}
	log.Printf("Finished processing %d existing files.", found)
}

func watchFolder(ctx context.Context, topic *pubsub.Topic) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal("Failed to create file watcher:", err)
	}
	defer watcher.Close()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if event.Op&fsnotify.Create == fsnotify.Create {
					log.Println("New file detected:", event.Name)
					time.Sleep(100 * time.Millisecond)

					hash, err := calculateFileHash(event.Name)
					if err != nil {
						log.Println("Error hashing file:", err)
						continue
					}
					pinFile(hash)
					addKnownFile(hash)
					go provideFile(ctx, hash)
					announceFile(ctx, topic, hash) // Announce "NEW"
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Println("File watcher error:", err)
			}
		}
	}()

	err = watcher.Add(fileWatchFolder)
	if err != nil {
		log.Fatal("Failed to add folder to watcher:", err)
	}
	<-ctx.Done()
}

func pinFile(hash string) {
	pinnedFiles.Lock()
	defer pinnedFiles.Unlock()

	if !pinnedFiles.hashes[hash] {
		log.Println("[PINNING]", hash)
		pinnedFiles.hashes[hash] = true
	}
}

func unpinFile(hash string) {
	pinnedFiles.Lock()
	defer pinnedFiles.Unlock()

	if pinnedFiles.hashes[hash] {
		log.Println("[UNPINNING]", hash)
		delete(pinnedFiles.hashes, hash)
	}
}

func isPinned(hash string) bool {
	pinnedFiles.RLock()
	defer pinnedFiles.RUnlock()
	return pinnedFiles.hashes[hash]
}

func provideFile(ctx context.Context, hash string) {
	c, err := hashToCid(hash)
	if err != nil {
		log.Println("Error converting hash to CID:", err)
		return
	}

	log.Println("Telling DHT we are a provider for:", hash)
	if err := globalDHT.Provide(ctx, c, true); err != nil {
		log.Println("Error providing to DHT:", err)
	}
}

// announceFile sends the initial "NEW" message
func announceFile(ctx context.Context, topic *pubsub.Topic, hash string) {
	msg := fmt.Sprintf("NEW:%s", hash)
	if err := topic.Publish(ctx, []byte(msg)); err != nil {
		log.Println("Error publishing file announcement:", err)
	}
}

// checkReplication is the core logic
func checkReplication(ctx context.Context, hash string, topic *pubsub.Topic) {
	c, err := hashToCid(hash)
	if err != nil {
		log.Println("Error converting hash to CID:", err)
		return
	}

	checkCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	log.Println("Checking replication for:", hash)
	providers := globalDHT.FindProvidersAsync(checkCtx, c, 0)

	count := 0
	// FIXED: Simplified the range expression
	for range providers {
		count++
	}

	if checkCtx.Err() == context.DeadlineExceeded {
		log.Println("Timeout checking replication for:", hash)
		return
	}

	log.Printf("Found %d providers for %s", count, hash)

	amIPinning := isPinned(hash)

	if count < minReplication {
		log.Println("Replication low! Announcing NEED for", hash)
		msg := fmt.Sprintf("NEED:%s", hash)
		go func() {
			if err := topic.Publish(context.Background(), []byte(msg)); err != nil {
				log.Println("Error publishing NEED announcement:", err)
			}
		}()

		if !amIPinning {
			log.Println("Must pin", hash)

			// --- CRITICAL TODO ---
			log.Println("TODO: Fetch file from peer")
			// Example: go fetchFile(ctx, hash)
			// ---------------------

			pinFile(hash)
			go provideFile(ctx, hash)
		}

	} else if count > maxReplication && amIPinning {
		log.Println("Replication high! Unpinning", hash)
		unpinFile(hash)
	}
}
