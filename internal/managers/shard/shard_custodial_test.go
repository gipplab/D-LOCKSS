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

// manifestIPFS serves manifest blocks and records unpins.
type manifestIPFS struct {
	testutil.MockIPFSClient
	blocks   map[string][]byte
	mu       sync.Mutex
	unpinned []string
}

func (m *manifestIPFS) GetBlock(_ context.Context, c cid.Cid) ([]byte, error) {
	if b, ok := m.blocks[c.String()]; ok {
		return b, nil
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

// custodialSetup pins one manifest, pinned an hour ago, whose payload belongs
// to payloadShard, on a node in shard 0.
func custodialSetup(t *testing.T, payloadShard string, providers int) (*ShardManager, *recordingStorage, *manifestIPFS, string) {
	t.Helper()
	sm, storage, ipfs := newCustodialRig(t, providers)
	payload, _ := nextPayloadInShard(t, payloadShard, 0)
	key := pinManifest(t, storage, ipfs, payload, "manifest", time.Now().Add(-time.Hour))
	if p, err := common.GetPayloadCIDForShardAssignment(context.Background(), sm.ipfsClient, key); err != nil || p == key {
		t.Fatalf("test manifest does not resolve to a payload: %q, %v", p, err)
	}
	return sm, storage, ipfs, key
}

func TestCustodialRetry_ReleasesConfirmedCopy(t *testing.T) {
	sm, storage, ipfs, key := custodialSetup(t, "1", 10)

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

	sm.RunCustodialRetryPass()

	if !storage.IsPinned(key) || len(ipfs.unpinned) != 0 {
		t.Error("a file of the node's own shard must not be released")
	}
	if pinned := sm.clusterMgr.(*recordingCluster).pinned; len(pinned) != 0 {
		t.Errorf("a well-replicated own-shard file needs no re-announce, got %v", pinned)
	}
}

func TestOrderCustodialWork_OldestFirstWithinBudget(t *testing.T) {
	base := time.Now()
	items := []custodialItem{
		{key: "newest", pinTime: base.Add(-1 * time.Hour), kind: custodialReannounce},
		{key: "oldest", pinTime: base.Add(-3 * time.Hour), kind: custodialReannounce},
		{key: "middle", pinTime: base.Add(-2 * time.Hour), kind: custodialReinject},
		{key: "release", pinTime: base.Add(-30 * time.Minute), kind: custodialRelease},
	}
	got := orderCustodialWork(items, 2)
	want := []string{"oldest", "middle", "release"}
	if len(got) != len(want) {
		t.Fatalf("got %d items, want %d (%v)", len(got), len(want), keysOf(got))
	}
	for i, key := range want {
		if got[i].key != key {
			t.Fatalf("position %d: got %s, want %s (all %v)", i, got[i].key, key, keysOf(got))
		}
	}
}

func TestOrderCustodialWork_ZeroBudgetStillReleases(t *testing.T) {
	items := []custodialItem{
		{key: "retry", pinTime: time.Now().Add(-time.Hour), kind: custodialReinject},
		{key: "release", pinTime: time.Now(), kind: custodialRelease},
	}
	got := orderCustodialWork(items, 0)
	if len(got) != 1 || got[0].key != "release" || got[0].kind != custodialRelease {
		t.Fatalf("a confirmed copy must be released with no retry budget, got %+v", got)
	}
}

func TestOrderCustodialWork_UnknownPinTimeIsOldest(t *testing.T) {
	items := []custodialItem{
		{key: "dated", pinTime: time.Now().Add(-time.Hour), kind: custodialReannounce},
		{key: "unknown", kind: custodialReannounce},
	}
	got := orderCustodialWork(items, 1)
	if len(got) != 1 || got[0].key != "unknown" {
		t.Fatalf("a file with no pin time must be retried before dated ones, got %+v", got)
	}
}

func TestCustodialRetry_ReannouncesOldestWithinRateLimit(t *testing.T) {
	sm, storage, ipfs := newCustodialRig(t, 0)
	sm.cfg.MaxMessagesPerWindow = 2
	now := time.Now()
	start := 0
	keys := make([]string, 4)
	for i := 0; i < 4; i++ {
		var payload cid.Cid
		payload, start = nextPayloadInShard(t, "0", start)
		// i == 0 is the oldest.
		keys[i] = pinManifest(t, storage, ipfs, payload, fmt.Sprintf("own-%d", i), now.Add(-time.Duration(4-i)*time.Hour))
	}
	payload, _ := nextPayloadInShard(t, "0", start)
	young := pinManifest(t, storage, ipfs, payload, "young", now.Add(-30*time.Second))

	sm.RunCustodialRetryPass()

	pinned := sm.clusterMgr.(*recordingCluster).pinned
	if len(pinned) != 2 || pinned[0] != keys[0] || pinned[1] != keys[1] {
		t.Fatalf("budget 2 should reannounce the two oldest, got %v want %v", pinned, keys[:2])
	}
	for _, key := range append(keys, young) {
		if !storage.IsPinned(key) {
			t.Errorf("file %s was released", key)
		}
	}
}

func TestCustodialRetry_ReleasesConfirmedWhenBudgetIsZero(t *testing.T) {
	sm, storage, ipfs := newCustodialRig(t, 0)
	sm.cfg.MaxMessagesPerWindow = 0
	now := time.Now().Add(-time.Hour)

	ownPayload, start := nextPayloadInShard(t, "0", 0)
	own := pinManifest(t, storage, ipfs, ownPayload, "own", now)

	foreignPayload, _ := nextPayloadInShard(t, "1", start)
	foreign := pinManifest(t, storage, ipfs, foreignPayload, "foreign", now.Add(time.Minute))
	storage.providersByKey = map[string]int{foreign: 10}

	sm.RunCustodialRetryPass()

	if storage.IsPinned(foreign) {
		t.Error("confirmed custodial copy should be released even when the retry budget is zero")
	}
	if !storage.IsPinned(own) {
		t.Error("own-shard file must stay pinned when the budget admits no reannounce")
	}
	if pinned := sm.clusterMgr.(*recordingCluster).pinned; len(pinned) != 0 {
		t.Errorf("zero budget must not reannounce, got %v", pinned)
	}
}

func TestCustodialRetry_KeepsUnconfirmedCustody(t *testing.T) {
	sm, storage, ipfs := newCustodialRig(t, 0)
	payload, _ := nextPayloadInShard(t, "1", 0)
	key := pinManifest(t, storage, ipfs, payload, "foreign", time.Now().Add(-time.Hour))

	sm.RunCustodialRetryPass()

	if !storage.IsPinned(key) || len(ipfs.unpinned) != 0 {
		t.Error("an unconfirmed tourist copy must stay pinned until the target shard confirms")
	}
}

func TestCustodialRetry_BoundsLookupsPerPassAndRotates(t *testing.T) {
	sm, storage, ipfs := newCustodialRig(t, 0)
	now := time.Now()
	total := custodialLookupsPerPass + 5
	keys := make([]string, total)
	start := 0
	for i := range keys {
		var payload cid.Cid
		payload, start = nextPayloadInShard(t, "0", start)
		keys[i] = pinManifest(t, storage, ipfs, payload, fmt.Sprintf("own-%d", i), now.Add(-time.Duration(total-i)*time.Hour))
	}
	cluster := sm.clusterMgr.(*recordingCluster)

	sm.RunCustodialRetryPass()
	if storage.lookups != custodialLookupsPerPass {
		t.Fatalf("first pass: %d lookups, want %d", storage.lookups, custodialLookupsPerPass)
	}
	if len(cluster.pinned) != custodialLookupsPerPass || cluster.pinned[0] != keys[0] {
		t.Fatalf("first pass should reannounce the %d oldest, got %v", custodialLookupsPerPass, cluster.pinned)
	}

	sm.RunCustodialRetryPass()
	if storage.lookups != total {
		t.Fatalf("second pass should check only the %d unchecked files, total lookups %d", total-custodialLookupsPerPass, storage.lookups)
	}
	for i, key := range cluster.pinned[custodialLookupsPerPass:] {
		if key != keys[custodialLookupsPerPass+i] {
			t.Fatalf("second pass reannounced %v, want the remaining files %v", cluster.pinned[custodialLookupsPerPass:], keys[custodialLookupsPerPass:])
		}
	}

	sm.RunCustodialRetryPass()
	if storage.lookups != total {
		t.Fatalf("files checked within custodialRecheckAfter must not be looked up again, total lookups %d", storage.lookups)
	}
}

func TestCustodialRetry_RechecksAfterInterval(t *testing.T) {
	sm, storage, ipfs := newCustodialRig(t, 10)
	payload, _ := nextPayloadInShard(t, "0", 0)
	key := pinManifest(t, storage, ipfs, payload, "own", time.Now().Add(-time.Hour))

	sm.RunCustodialRetryPass()
	sm.RunCustodialRetryPass()
	if storage.lookups != 1 {
		t.Fatalf("a well-replicated file checked moments ago needs no lookup, got %d", storage.lookups)
	}

	sm.custodialChecked[key] = time.Now().Add(-custodialRecheckAfter - time.Minute)
	sm.RunCustodialRetryPass()
	if storage.lookups != 2 {
		t.Fatalf("file should be checked again after custodialRecheckAfter, got %d lookups", storage.lookups)
	}
	if len(sm.clusterMgr.(*recordingCluster).pinned) != 0 {
		t.Error("a file with enough providers must not be reannounced")
	}
}

func TestCustodialRetry_ForgetsUnpinnedFiles(t *testing.T) {
	sm, storage, ipfs := newCustodialRig(t, 10)
	payload, _ := nextPayloadInShard(t, "0", 0)
	key := pinManifest(t, storage, ipfs, payload, "own", time.Now().Add(-time.Hour))

	sm.RunCustodialRetryPass()
	storage.UnpinFile(key)
	sm.RunCustodialRetryPass()
	if _, ok := sm.custodialChecked[key]; ok {
		t.Error("lookup time of an unpinned file should be dropped")
	}
}

func keysOf(items []custodialItem) []string {
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = it.key
	}
	return out
}

func newCustodialRig(t *testing.T, providers int) (*ShardManager, *recordingStorage, *manifestIPFS) {
	t.Helper()
	storage := newRecordingStorage()
	storage.providers = providers
	sm := buildTestSM(context.Background(), storage, &recordingCluster{})
	sm.signer = nil
	sm.currentShard = "0"
	ipfs := &manifestIPFS{blocks: map[string][]byte{}}
	sm.ipfsClient = ipfs
	return sm, storage, ipfs
}

func pinManifest(t *testing.T, storage *recordingStorage, ipfs *manifestIPFS, payload cid.Cid, name string, pinTime time.Time) string {
	t.Helper()
	ro := schema.ResearchObject{MetadataRef: "file://x", IngestedBy: testutil.MustPeerID(t, "ingester"), Payload: payload, TotalSize: 1}
	block, err := ro.MarshalCBOR()
	if err != nil {
		t.Fatal(err)
	}
	manifest := testCID(t, name)
	ipfs.blocks[manifest.String()] = block
	key := manifest.String()
	storage.AddKnownFile(key)
	storage.PinFile(key)
	storage.pinTimes[key] = pinTime
	return key
}

func nextPayloadInShard(t *testing.T, shard string, start int) (cid.Cid, int) {
	t.Helper()
	for i := start; ; i++ {
		c := testCID(t, fmt.Sprintf("payload-%s-%d", shard, i))
		if s, err := common.TargetShardForPayload(c.String(), 1); err == nil && s == shard {
			return c, i + 1
		}
		if i > start+100000 {
			t.Fatalf("no payload CID in shard %s", shard)
		}
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
