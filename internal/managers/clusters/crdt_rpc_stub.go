package clusters

import (
	"context"
	"slices"
	"time"

	"github.com/ipfs-cluster/ipfs-cluster/api"
	"github.com/ipfs-cluster/ipfs-cluster/version"
	rpc "github.com/libp2p/go-libp2p-gorpc"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

const rpcStreamBufferSize = 1024

// embeddedPinTrackerStub serves the PinTracker RPCs that the CRDT calls from
// its PutHook/DeleteHook. A non-nil onTrack makes pin changes trigger an
// immediate sync instead of waiting for the tracker's ticker.
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

// embeddedPeerMonitorStub serves the PeerMonitor RPCs behind CRDT Peers().
// It reports one metric per shard peer from getPeers, always including this
// host; without getPeers it reports only this host.
type embeddedPeerMonitorStub struct {
	host     host.Host
	shardID  string
	getPeers func(shardID string) []peer.ID
}

func (s embeddedPeerMonitorStub) LatestMetrics(ctx context.Context, name string, out *[]api.Metric) error {
	self := s.host.ID()
	peers := []peer.ID{self}
	if s.getPeers != nil {
		mesh := s.getPeers(s.shardID)
		if slices.Contains(mesh, self) {
			peers = mesh
		} else {
			peers = append(peers, mesh...)
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

// newEmbeddedRPCClient returns a gorpc client served in-process by the stubs
// above, letting CRDT consensus run without a cluster daemon. getPeers and
// onTrack may be nil.
func newEmbeddedRPCClient(h host.Host, shardID string, getPeers func(shardID string) []peer.ID, onTrack func(shardID string)) *rpc.Client {
	srv := rpc.NewServer(h, version.RPCProtocol, rpc.WithStreamBufferSize(rpcStreamBufferSize))
	_ = srv.RegisterName("PinTracker", &embeddedPinTrackerStub{shardID: shardID, onTrack: onTrack})
	_ = srv.RegisterName("PeerMonitor", &embeddedPeerMonitorStub{host: h, shardID: shardID, getPeers: getPeers})
	return rpc.NewClientWithServer(h, version.RPCProtocol, srv, rpc.WithMultiStreamBufferSize(rpcStreamBufferSize))
}
