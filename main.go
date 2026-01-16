package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0", "/ip6/::/tcp/0"),
		libp2p.Routing(func(h host.Host) (routing.PeerRouting, error) {
			var err error
			globalDHT, err = dht.New(ctx, h, dht.Mode(dht.ModeServer))
			return globalDHT, err
		}),
	)
	if err != nil {
		log.Fatalf("[Fatal] Failed to initialize libp2p host: %v", err)
	}

	fmt.Printf("--- Node ID: %s ---\n", h.ID().String())
	fmt.Printf("--- Addresses: %v ---\n", h.Addrs())

	mdnsSvc := mdns.NewMdnsService(h, DiscoveryServiceTag, &discoveryNotifee{h: h, ctx: ctx})
	if err := mdnsSvc.Start(); err != nil {
		log.Fatalf("[Fatal] Failed to start mDNS service: %v", err)
	}

	if err = globalDHT.Bootstrap(ctx); err != nil {
		log.Fatalf("[Fatal] Failed to bootstrap DHT: %v", err)
	}

	ps, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		log.Fatalf("[Fatal] Failed to initialize GossipSub: %v", err)
	}

	replicationWorkers = make(chan struct{}, MaxConcurrentReplicationChecks)

	logConfiguration()

	initialShard := getBinaryPrefix(h.ID().String(), 1)
	shardMgr = NewShardManager(ctx, h, ps, initialShard)

	go shardMgr.Run()
	go inputLoop(shardMgr)

	if err := os.Mkdir(FileWatchFolder, 0755); err != nil && !os.IsExist(err) {
		log.Printf("Error creating folder: %v", err)
	}

	log.Println("Scanning for existing files...")
	scanExistingFiles()

	go watchFolder(ctx)

	go runReplicationChecker(ctx)
	go runRateLimiterCleanup(ctx)
	go runMetricsReporter(ctx)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\nShutting down...")

	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if shardMgr != nil {
		shardMgr.Close()
	}
	if globalDHT != nil {
		globalDHT.Close()
	}
	if h != nil {
		if err := h.Close(); err != nil {
			log.Printf("[Error] Error closing host: %v", err)
		}
	}

	<-shutdownCtx.Done()
	log.Println("[Warning] Shutdown timeout exceeded, forcing exit")

	log.Println("[System] Shutdown complete")
}
