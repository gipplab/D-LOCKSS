// Command dlockss runs a D-LOCKSS preservation node: it ingests local and
// Kubo-pinned content, wraps it in signed manifests, and keeps it replicated
// across its shard of the network.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"

	"dlockss/internal/api"
	"dlockss/internal/badbits"
	"dlockss/internal/config"
	"dlockss/internal/identity"
	"dlockss/internal/ingest"
	"dlockss/internal/kubo"
	"dlockss/internal/p2p"
	"dlockss/internal/pinset"
	"dlockss/internal/protocol"
	"dlockss/internal/shard"
	"dlockss/internal/store"
	"dlockss/internal/trust"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg := config.LoadFromEnv()
	level := slog.LevelInfo
	if cfg.VerboseLogging {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))
	cfg.Validate()
	cfg.Log()

	if err := cfg.ValidatePathSafety(); err != nil {
		slog.Error("unsafe path layout", "err", err)
		os.Exit(1)
	}

	bb, err := badbits.Load(cfg.BadBitsPath)
	if err != nil {
		slog.Warn("could not load BadBits denylist; content blocking is off", "path", cfg.BadBitsPath, "err", err)
		bb = nil
	} else if bb.Len() > 0 {
		slog.Info("BadBits denylist loaded", "entries", bb.Len())
	}

	nodeName := identity.ResolveNodeName(cfg)

	client, err := kubo.New(ctx, cfg.IPFSNodeAddress)
	if err != nil {
		slog.Error("IPFS is not available; start the Kubo daemon first (ipfs daemon)", "err", err)
		os.Exit(1)
	}

	key, err := identity.LoadKey(cfg)
	if err != nil {
		slog.Error("could not resolve node identity", "err", err)
		os.Exit(1)
	}

	node, err := p2p.NewNode(ctx, cfg, key)
	if err != nil {
		slog.Error("could not start libp2p", "err", err)
		os.Exit(1)
	}
	defer node.Close()
	self := node.Host.ID()
	slog.Info("node started", "peer_id", self, "name", nodeName, "private_network", cfg.PrivateNetwork)

	ps, err := pubsub.NewGossipSub(ctx, node.Host)
	if err != nil {
		slog.Error("could not start gossipsub", "err", err)
		os.Exit(1)
	}

	if cfg.PrivateNetwork {
		slog.Info("private network mode: skipping public DHT bootstrap and AutoRelay")
	} else {
		p2p.ConnectBootstrapPeers(ctx, node.Host)
	}
	p2p.StartDiscovery(ctx, node.Host, node.DHT, cfg.DiscoveryServiceTag)

	trustMgr := trust.NewManager(cfg.TrustMode)
	if err := trustMgr.LoadStore(cfg.TrustStorePath); err != nil && !os.IsNotExist(err) {
		slog.Error("could not load trust store", "path", cfg.TrustStorePath, "err", err)
		os.Exit(1)
	}
	trustMgr.AddOrigins(cfg.IngestAllowlist)
	if trustMgr.RestrictsOrigins() {
		slog.Info("origin restriction active", "trusted_peers", trustMgr.Len(), "mode", trustMgr.Mode())
	} else if trustMgr.Mode() == trust.ModeAllowlist {
		slog.Info("allowlist mode with an empty trust list: accepting all origins")
	}

	st := store.New(client, bb)
	disk := store.NewDiskMonitor(cfg.FileWatchFolder, cfg.DiskUsageHighWaterMark)
	signer := protocol.NewSigner(self, key, trustMgr)

	pins, err := pinset.NewManager(ctx, cfg.ClusterStorePath, client, bb, trustMgr)
	if err != nil {
		slog.Error("could not open the pinset store", "err", err)
		os.Exit(1)
	}

	shardMgr, err := shard.New(ctx, shard.Deps{
		Config:   cfg,
		Self:     self,
		NodeName: nodeName,
		PubSub:   ps,
		Signer:   signer,
		Trust:    trustMgr,
		Store:    st,
		Disk:     disk,
		Pins:     pins,
		Client:   client,
	})
	if err != nil {
		slog.Error("could not build the shard manager", "err", err)
		os.Exit(1)
	}

	pins.OnSynced = shardMgr.OnPinSynced
	pins.OnRemoved = st.Unpin

	apiServer := api.New(cfg.APIPort)
	apiServer.Start()

	if err := shardMgr.Run(); err != nil {
		slog.Error("could not start the shard manager", "err", err)
		os.Exit(1)
	}

	// The first rescan only records candidates and the second (30 s later)
	// ingests, so the shard manager has settled before announcements start.
	proc := ingest.NewProcessor(cfg, client, st, bb, signer, shardMgr, self)
	go proc.WatchFolder(ctx)
	if cfg.PinIngestEnabled {
		go proc.WatchPins(ctx)
	}

	<-ctx.Done()
	slog.Info("shutting down")
	if err := shardMgr.Close(); err != nil {
		slog.Warn("shard manager shutdown", "err", err)
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := apiServer.Shutdown(shutdownCtx); err != nil {
		slog.Warn("api shutdown", "err", err)
	}
}
