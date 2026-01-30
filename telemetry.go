package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"log"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

const (
	TelemetryProtocol     = protocol.ID("/dlockss/telemetry/1.0.0")
	MonitorDiscoveryTopic = "dlockss-monitor"
)

type TelemetryClient struct {
	host      host.Host
	monitorID peer.ID
	dht       DHTProvider
	ps        *pubsub.PubSub
	monitorTopic *pubsub.Topic
}

func NewTelemetryClient(h host.Host, ps *pubsub.PubSub, dht DHTProvider, monitorIDStr string) *TelemetryClient {
	if monitorIDStr == "" {
		return nil
	}

	pid, err := peer.Decode(monitorIDStr)
	if err != nil {
		log.Printf("[Telemetry] Invalid monitor PeerID: %v", err)
		return nil
	}

	tc := &TelemetryClient{
		host:      h,
		monitorID: pid,
		dht:       dht,
		ps:        ps,
	}

	// Join monitor discovery topic to discover monitor via PubSub
	if ps != nil {
		topic, err := ps.Join(MonitorDiscoveryTopic)
		if err != nil {
			log.Printf("[Telemetry] Failed to join monitor discovery topic: %v", err)
		} else {
			tc.monitorTopic = topic
			log.Printf("[Telemetry] Joined monitor discovery topic: %s", MonitorDiscoveryTopic)
		}
	}

	return tc
}

func (tc *TelemetryClient) Start(ctx context.Context) {
	log.Printf("[Telemetry] Starting telemetry client. Monitor: %s", tc.monitorID)
	go tc.runLoop(ctx)
}

func (tc *TelemetryClient) runLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second) // Report every 30 seconds
	defer ticker.Stop()

	// Try to discover monitor immediately on startup
	tc.pushTelemetry(ctx)

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

	knownCIDs := []string(nil)
	if storageMgr != nil {
		all := storageMgr.knownFiles.All()
		knownCIDs = make([]string, 0, len(all))
		for cid := range all {
			knownCIDs = append(knownCIDs, cid)
		}
	}

	// Get replication metrics
	metrics.RLock()
	avgReplication := metrics.avgReplicationLevel
	filesAtTarget := metrics.filesAtTargetReplication
	replicationDist := metrics.replicationDistribution
	metrics.RUnlock()

	status := StatusResponse{
		PeerID:       selfPeerID.String(), // Now uses IPFS peer ID when available
		Version:      "1.0.0",
		CurrentShard: shardID,
		PeersInShard: peers,
		Storage: StorageStatus{
			PinnedFiles: pinned,
			KnownFiles:  known,
			KnownCIDs:   knownCIDs,
		},
		Replication: ReplicationStatus{
			QueueDepth:            queueDepth,
			ActiveWorkers:         activeWorkers,
			AvgReplicationLevel:   avgReplication,
			FilesAtTarget:        filesAtTarget,
			ReplicationDistribution: replicationDist,
		},
		UptimeSeconds: time.Since(startTime).Seconds(),
	}
	
	// Log telemetry data for debugging
	log.Printf("[Telemetry] Sending status: pinned=%d, known=%d, shard=%s, peers=%d", 
		pinned, known, shardID, peers)

	// 2. Marshal
	data, err := json.Marshal(status)
	if err != nil {
		log.Printf("[Telemetry] Marshal error: %v", err)
		return
	}

	// 3. Connect and Send
	streamCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Check if we're already connected to monitor (best case - no discovery needed)
	if tc.host.Network().Connectedness(tc.monitorID) == network.Connected {
		// Already connected, proceed to send telemetry
	} else if len(tc.host.Peerstore().Addrs(tc.monitorID)) == 0 {
		// No addresses in peerstore - need to discover monitor
		// First, try to discover monitor via PubSub topic
		foundViaPubSub := false
		if tc.monitorTopic != nil {
			// GossipSub's ListPeers() returns peers in the mesh (connected peers)
			// But we should also check if GossipSub has discovered the peer and added it to peerstore
			// Try multiple times with increasing wait to allow mesh to form
			for attempt := 0; attempt < 3; attempt++ {
				if attempt > 0 {
					select {
					case <-time.After(time.Duration(attempt) * 2 * time.Second):
					case <-streamCtx.Done():
						return
					}
				}
				
				// Check if monitor is in the topic's peer list (mesh peers)
				// ListPeers() returns peers that are connected and in the mesh
				topicPeers := tc.monitorTopic.ListPeers()
				for _, peerID := range topicPeers {
					if peerID == tc.monitorID {
						// Monitor is in mesh - we should be connected
						if tc.host.Network().Connectedness(tc.monitorID) == network.Connected {
							log.Printf("[Telemetry] Monitor discovered via PubSub topic (connected in mesh)")
							foundViaPubSub = true
							break
						}
					}
				}
				
				if foundViaPubSub {
					break
				}
				
				// Also check peerstore - GossipSub or mDNS might have discovered peer
				// In this case, we can try to connect using addresses from peerstore
				addrs := tc.host.Peerstore().Addrs(tc.monitorID)
				if len(addrs) > 0 {
					// Peer discovered (via mDNS or GossipSub) but not connected - try to connect
					log.Printf("[Telemetry] Monitor found in peerstore (%d addresses), attempting connection", len(addrs))
					connectCtx, connectCancel := context.WithTimeout(streamCtx, 5*time.Second)
					if err := tc.host.Connect(connectCtx, peer.AddrInfo{ID: tc.monitorID, Addrs: addrs}); err == nil {
						log.Printf("[Telemetry] Monitor connected successfully")
						foundViaPubSub = true
						connectCancel()
						break
					} else {
						log.Printf("[Telemetry] Connection attempt failed: %v", err)
					}
					connectCancel()
				}
			}
		}
		
		// Only use DHT as last resort if PubSub discovery failed
		if !foundViaPubSub && tc.dht != nil {
			log.Printf("[Telemetry] Monitor not found via PubSub, falling back to DHT lookup (should be rare)")
			
			lookupCtx, lookupCancel := context.WithTimeout(ctx, 5*time.Second)
			info, err := tc.dht.FindPeer(lookupCtx, tc.monitorID)
			lookupCancel()
			
			if err != nil {
				log.Printf("[Telemetry] DHT lookup failed: %v", err)
				log.Printf("[Telemetry] Monitor will be discovered via PubSub when it joins the topic")
				return
			}
			
			log.Printf("[Telemetry] Found monitor via DHT at %v", info.Addrs)
			tc.host.Peerstore().AddAddrs(info.ID, info.Addrs, 10*time.Minute)
			
			// Try to connect after DHT discovery
			connectCtx, connectCancel := context.WithTimeout(streamCtx, 5*time.Second)
			if err := tc.host.Connect(connectCtx, info); err != nil {
				log.Printf("[Telemetry] Failed to connect to monitor after DHT discovery: %v", err)
				connectCancel()
				return
			}
			connectCancel()
			log.Printf("[Telemetry] Connected to monitor via DHT")
		} else if !foundViaPubSub {
			// Keep trying - mDNS or PubSub might discover the monitor soon
			// Don't return immediately, wait a bit more for discovery
			log.Printf("[Telemetry] Monitor not yet discovered via PubSub/mDNS. Will retry on next interval...")
			// Give mDNS a bit more time (it's usually fast on localhost)
			select {
			case <-time.After(5 * time.Second):
				// Check peerstore again after brief wait
				addrs := tc.host.Peerstore().Addrs(tc.monitorID)
				if len(addrs) > 0 {
					log.Printf("[Telemetry] Monitor discovered in peerstore after wait, attempting connection")
					connectCtx, connectCancel := context.WithTimeout(streamCtx, 5*time.Second)
					if err := tc.host.Connect(connectCtx, peer.AddrInfo{ID: tc.monitorID, Addrs: addrs}); err == nil {
						log.Printf("[Telemetry] Monitor connected after discovery wait")
						connectCancel()
						// Continue to send telemetry
					} else {
						log.Printf("[Telemetry] Connection failed after discovery wait: %v", err)
						connectCancel()
						return
					}
				} else {
					return
				}
			case <-streamCtx.Done():
				return
			}
		}
	} else {
		// We have addresses but might not be connected - try to connect
		if tc.host.Network().Connectedness(tc.monitorID) != network.Connected {
			addrs := tc.host.Peerstore().Addrs(tc.monitorID)
			if len(addrs) > 0 {
				log.Printf("[Telemetry] Monitor addresses in peerstore, attempting connection")
				connectCtx, connectCancel := context.WithTimeout(streamCtx, 5*time.Second)
				if err := tc.host.Connect(connectCtx, peer.AddrInfo{ID: tc.monitorID, Addrs: addrs}); err != nil {
					log.Printf("[Telemetry] Failed to connect to monitor: %v", err)
					connectCancel()
					return
				}
				connectCancel()
			}
		}
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
