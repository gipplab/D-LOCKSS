package shard

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"

	"dlockss/internal/common"
	"dlockss/internal/testutil"
	"dlockss/pkg/schema"
)

// manifestIPFS serves one manifest block and records unpins.
type manifestIPFS struct {
	testutil.MockIPFSClient
	manifest cid.Cid
	block    []byte
	mu       sync.Mutex
	unpinned []string
}

func (m *manifestIPFS) GetBlock(_ context.Context, c cid.Cid) ([]byte, error) {
	if c.Equals(m.manifest) {
		return m.block, nil
	}
	return nil, fmt.Errorf("not found")
}

func (m *manifestIPFS) UnpinRecursive(_ context.Context, c cid.Cid) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.unpinned = append(m.unpinned, c.String())
	return nil
}

func testCID(t *testing.T, s string) cid.Cid {
	t.Helper()
	h, err := mh.Sum([]byte(s), mh.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	return cid.NewCidV1(cid.Raw, h)
}

// payloadInShard returns a payload CID whose depth-1 shard is shard.
func payloadInShard(t *testing.T, shard string) cid.Cid {
	t.Helper()
	for i := 0; ; i++ {
		c := testCID(t, fmt.Sprintf("payload-%d", i))
		if s, _ := common.TargetShardForPayload(c.String(), 1); s == shard {
			return c
		}
	}
}

func custodialSetup(t *testing.T, payloadShard string, providers int) (*ShardManager, *recordingStorage, *manifestIPFS, string) {
	t.Helper()
	storage := newRecordingStorage()
	storage.providers = providers
	sm := buildTestSM(context.Background(), storage, &recordingCluster{})
	sm.signer = nil
	sm.currentShard = "0"

	payload := payloadInShard(t, payloadShard)
	ro := schema.ResearchObject{MetadataRef: "file://x", IngestedBy: testutil.MustPeerID(t, "ingester"), Payload: payload, TotalSize: 1}
	block, err := ro.MarshalCBOR()
	if err != nil {
		t.Fatal(err)
	}
	manifest := testCID(t, "manifest")
	ipfs := &manifestIPFS{manifest: manifest, block: block}
	sm.ipfsClient = ipfs

	key := manifest.String()
	storage.AddKnownFile(key)
	storage.PinFile(key)
	storage.pinTimes[key] = time.Now().Add(-time.Hour)
	return sm, storage, ipfs, key
}

func TestCustodialRetry_ReleasesConfirmedCopy(t *testing.T) {
	sm, storage, ipfs, key := custodialSetup(t, "1", 10)
	if p, err := common.GetPayloadCIDForShardAssignment(context.Background(), sm.ipfsClient, key); err != nil || p == key {
		t.Fatalf("test manifest does not resolve to a payload: %q, %v", p, err)
	}

	sm.RunCustodialRetryPass()

	if storage.IsPinned(key) {
		t.Error("confirmed custodial copy should be released")
	}
	if len(ipfs.unpinned) != 2 {
		t.Errorf("expected manifest and payload unpinned, got %v", ipfs.unpinned)
	}
}

func TestCustodialRetry_SkipsOwnShard(t *testing.T) {
	sm, storage, ipfs, key := custodialSetup(t, "0", 10)
	if p, err := common.GetPayloadCIDForShardAssignment(context.Background(), sm.ipfsClient, key); err != nil || p == key {
		t.Fatalf("test manifest does not resolve to a payload: %q, %v", p, err)
	}

	sm.RunCustodialRetryPass()

	if !storage.IsPinned(key) || len(ipfs.unpinned) != 0 {
		t.Error("a file of the node's own shard must not be released")
	}
	if pinned := sm.clusterMgr.(*recordingCluster).pinned; len(pinned) != 0 {
		t.Errorf("a well-replicated own-shard file needs no re-announce, got %v", pinned)
	}
}

func TestCustodialRetry_ReannouncesUnderReplicatedOwnShardFile(t *testing.T) {
	sm, storage, ipfs, key := custodialSetup(t, "0", 0)

	sm.RunCustodialRetryPass()

	if !storage.IsPinned(key) || len(ipfs.unpinned) != 0 {
		t.Error("an under-replicated own-shard file must stay pinned")
	}
	pinned := sm.clusterMgr.(*recordingCluster).pinned
	if len(pinned) != 1 || pinned[0] != key {
		t.Errorf("expected the file re-added to the own shard's CRDT, got %v", pinned)
	}
}
