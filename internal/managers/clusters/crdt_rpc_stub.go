// Package clusters: embedded RPC stubs so ipfs-cluster CRDT consensus can run
// without a full cluster daemon. The CRDT uses gorpc to call PinTracker and
// PeerMonitor; no-op stubs, client set on consensus.
package clusters

import (
	"context"
	"time"

	"github.com/ipfs-cluster/ipfs-cluster/api"
	"github.com/ipfs-cluster/ipfs-cluster/version"
	rpc "github.com/libp2p/go-libp2p-gorpc"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

const rpcStreamBufferSize = 1024

// embeddedPinTrackerStub implements PinTracker RPC used by CRDT PutHook/DeleteHook.
// When onTrack is set, Track/Untrack trigger an immediate PinTracker sync for this shard
// so pin changes propagate without waiting for the 10s ticker.
type embeddedPinTrackerStub struct {
	shardID string
	onTrack func(shardID string)
}

func (s embeddedPinTrackerStub) Track(ctx context.Context, in api.Pin, out *struct{}) error {
	if s.onTrack != nil {
		s.onTrack(s.shardID)
	}
	return nil
}

func (s embeddedPinTrackerStub) Untrack(ctx context.Context, in api.Pin, out *struct{}) error {
	if s.onTrack != nil {
		s.onTrack(s.shardID)
	}
	return nil
}

// embeddedPeerMonitorStub implements PeerMonitor RPC used by CRDT Peers().
// When getPeers is set, returns one metric per shard peer (from pubsub mesh) so Peers() and
// allocations use real shard membership; otherwise returns only our host.
type embeddedPeerMonitorStub struct {
	host     host.Host
	shardID  string
	getPeers func(shardID string) []peer.ID
}

func (s embeddedPeerMonitorStub) LatestMetrics(ctx context.Context, name string, out *[]api.Metric) error {
	peers := []peer.ID{s.host.ID()}
	if s.getPeers != nil {
		mesh := s.getPeers(s.shardID)
		seen := make(map[peer.ID]bool)
		for _, p := range mesh {
			seen[p] = true
		}
		if !seen[s.host.ID()] {
			peers = append([]peer.ID{s.host.ID()}, mesh...)
		} else {
			peers = mesh
		}
	}
	metrics := make([]api.Metric, 0, len(peers))
	for _, p := range peers {
		m := api.Metric{
			Name:  name,
			Peer:  p,
			Value: "0",
			Valid: true,
		}
		m.SetTTL(2 * time.Minute)
		metrics = append(metrics, m)
	}
	*out = metrics
	return nil
}

func (embeddedPeerMonitorStub) MetricNames(ctx context.Context, in struct{}, out *[]string) error {
	*out = []string{"ping"}
	return nil
}

// newEmbeddedRPCClient creates a gorpc Server with stub handlers and a Client
// that uses it for local calls. The CRDT expects PinTracker and PeerMonitor RPCs.
// getPeers(shardID) returns shard peers for PeerMonitor; when nil, Peers() returns only self.
// onTrack(shardID) is called on Track/Untrack so PinTracker can sync immediately; when nil, no trigger.
func newEmbeddedRPCClient(h host.Host, shardID string, getPeers func(shardID string) []peer.ID, onTrack func(shardID string)) *rpc.Client {
	srv := rpc.NewServer(h, version.RPCProtocol, rpc.WithStreamBufferSize(rpcStreamBufferSize))
	_ = srv.RegisterName("PinTracker", &embeddedPinTrackerStub{shardID: shardID, onTrack: onTrack})
	_ = srv.RegisterName("PeerMonitor", &embeddedPeerMonitorStub{host: h, shardID: shardID, getPeers: getPeers})
	return rpc.NewClientWithServer(h, version.RPCProtocol, srv, rpc.WithMultiStreamBufferSize(rpcStreamBufferSize))
}

// setConsensusRPCClient wires the gorpc client to CRDT. getPeers and onTrack are optional.
func setConsensusRPCClient(consensus interface{ SetClient(*rpc.Client) }, h host.Host, shardID string, getPeers func(string) []peer.ID, onTrack func(string)) {
	client := newEmbeddedRPCClient(h, shardID, getPeers, onTrack)
	consensus.SetClient(client)
}
