package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

const TelemetryProtocol = protocol.ID("/dlockss/telemetry/1.0.0")

type TelemetryClient struct {
	host      host.Host
	monitorID peer.ID
	dht       DHTProvider
}

func NewTelemetryClient(h host.Host, dht DHTProvider, monitorIDStr string) *TelemetryClient {
	if monitorIDStr == "" {
		return nil
	}

	pid, err := peer.Decode(monitorIDStr)
	if err != nil {
		log.Printf("[Telemetry] Invalid monitor PeerID: %v", err)
		return nil
	}

	return &TelemetryClient{
		host:      h,
		monitorID: pid,
		dht:       dht,
	}
}

func (tc *TelemetryClient) Start(ctx context.Context) {
	log.Printf("[Telemetry] Starting telemetry client. Monitor: %s", tc.monitorID)
	go tc.runLoop(ctx)
}

func (tc *TelemetryClient) runLoop(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute) // Report every minute
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tc.pushTelemetry(ctx)
		}
	}
}

func (tc *TelemetryClient) pushTelemetry(ctx context.Context) {
	// 1. Gather Data (Reuse StatusResponse from api.go)
	metrics.RLock()
	pinned := metrics.pinnedFilesCount
	known := metrics.knownFilesCount
	startTime := metrics.startTime
	metrics.RUnlock()

	shardID, peers := getShardInfo()

	activeWorkers := 0
	queueDepth := 0
	if replicationMgr != nil {
		activeWorkers = replicationMgr.checkingFiles.Size()
	}

	status := StatusResponse{
		PeerID:       selfPeerID.String(),
		Version:      "1.0.0",
		CurrentShard: shardID,
		PeersInShard: peers,
		Storage: StorageStatus{
			PinnedFiles: pinned,
			KnownFiles:  known,
		},
		Replication: ReplicationStatus{
			QueueDepth:    queueDepth,
			ActiveWorkers: activeWorkers,
		},
		UptimeSeconds: time.Since(startTime).Seconds(),
	}

	// 2. Marshal
	data, err := json.Marshal(status)
	if err != nil {
		log.Printf("[Telemetry] Marshal error: %v", err)
		return
	}

	// 3. Connect and Send
	streamCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Ensure we have address for monitor
	if len(tc.host.Peerstore().Addrs(tc.monitorID)) == 0 {
		log.Printf("[Telemetry] Monitor %s not found in peerstore. Searching DHT...", tc.monitorID)
		info, err := tc.dht.FindPeer(streamCtx, tc.monitorID)
		if err != nil {
			log.Printf("[Telemetry] Monitor lookup failed: %v", err)
			return
		}
		log.Printf("[Telemetry] Found monitor at %v", info.Addrs)
		tc.host.Peerstore().AddAddrs(info.ID, info.Addrs, 10*time.Minute)
	}

	stream, err := tc.host.NewStream(streamCtx, tc.monitorID, TelemetryProtocol)
	if err != nil {
		log.Printf("[Telemetry] Connection to monitor failed: %v", err)
		return
	}
	defer stream.Close()

	// Use GZIP compression
	gw := gzip.NewWriter(stream)
	if _, err := gw.Write(data); err != nil {
		log.Printf("[Telemetry] Write error: %v", err)
		return
	}
	if err := gw.Close(); err != nil {
		log.Printf("[Telemetry] Gzip close error: %v", err)
		return
	}
	log.Printf("[Telemetry] Successfully pushed status to monitor")
}
