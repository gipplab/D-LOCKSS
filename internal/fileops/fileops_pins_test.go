package fileops

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	mh "github.com/multiformats/go-multihash"

	"dlockss/internal/config"
	"dlockss/pkg/ipfs"
	"dlockss/pkg/schema"
)

// pinKubo is an in-memory Kubo: pins with names and dag-cbor blocks
// addressed by their real CIDs.
type pinKubo struct {
	mu        sync.Mutex
	pins      map[cid.Cid]string
	blocks    map[cid.Cid][]byte
	badBlocks map[cid.Cid]bool
	manifests []cid.Cid
}

func newPinKubo() *pinKubo {
	return &pinKubo{pins: map[cid.Cid]string{}, blocks: map[cid.Cid][]byte{}, badBlocks: map[cid.Cid]bool{}}
}

func (k *pinKubo) ImportFile(context.Context, string) (cid.Cid, error) { return cid.Undef, nil }
func (k *pinKubo) PutDagCBOR(_ context.Context, block []byte) (cid.Cid, error) {
	h, err := mh.Sum(block, mh.SHA2_256, -1)
	if err != nil {
		return cid.Undef, err
	}
	c := cid.NewCidV1(cid.DagCBOR, h)
	k.mu.Lock()
	k.blocks[c] = block
	k.mu.Unlock()
	return c, nil
}
func (k *pinKubo) GetBlock(_ context.Context, c cid.Cid) ([]byte, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.badBlocks[c] {
		return nil, fmt.Errorf("transient failure")
	}
	if b, ok := k.blocks[c]; ok {
		return b, nil
	}
	return nil, fmt.Errorf("not found")
}
func (k *pinKubo) PinRecursive(_ context.Context, c cid.Cid) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if _, ok := k.pins[c]; !ok {
		k.pins[c] = ""
	}
	return nil
}
func (k *pinKubo) PinManifest(_ context.Context, c cid.Cid) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.pins[c] = ipfs.ManifestPinName
	k.manifests = append(k.manifests, c)
	return nil
}
func (k *pinKubo) UnpinRecursive(_ context.Context, c cid.Cid) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	delete(k.pins, c)
	return nil
}
func (k *pinKubo) IsPinned(_ context.Context, c cid.Cid) (bool, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	_, ok := k.pins[c]
	return ok, nil
}
func (k *pinKubo) RecursivePins(context.Context) (map[cid.Cid]string, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	out := make(map[cid.Cid]string, len(k.pins))
	for c, n := range k.pins {
		out[c] = n
	}
	return out, nil
}
func (k *pinKubo) DAGSize(context.Context, cid.Cid) (uint64, error) { return 42, nil }

func (k *pinKubo) userPin(c cid.Cid, name string) {
	k.mu.Lock()
	k.pins[c] = name
	k.mu.Unlock()
}

func (k *pinKubo) manifestPins() []cid.Cid {
	k.mu.Lock()
	defer k.mu.Unlock()
	return append([]cid.Cid(nil), k.manifests...)
}

func (k *pinKubo) manifest(t *testing.T, c cid.Cid) schema.ResearchObject {
	t.Helper()
	b, err := k.GetBlock(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	var ro schema.ResearchObject
	if err := ro.UnmarshalCBOR(b); err != nil {
		t.Fatal(err)
	}
	return ro
}

type pinShard struct {
	mu        sync.Mutex
	published int
}

func (s *pinShard) PeerID() peer.ID {
	id, _ := peer.Decode("12D3KooW9syRZpRZKiJbLrrxbcuymbhT5BTF9hjG6bykH42q29Mb")
	return id
}
func (s *pinShard) GetShardInfo() string                                { return "0" }
func (s *pinShard) AnnouncePinned(string)                               {}
func (s *pinShard) AmIResponsibleFor(string) bool                       { return true }
func (s *pinShard) IsLocalNodeIngestor() bool                           { return true }
func (s *pinShard) PinToCluster(context.Context, cid.Cid) error         { return nil }
func (s *pinShard) ResolveTargetShardForCustodial(n, _ string) string   { return n }
func (s *pinShard) JoinShardAsObserver(string) bool                     { return true }
func (s *pinShard) LeaveShardAsObserver(string)                         {}
func (s *pinShard) EnsureClusterForShard(context.Context, string) error { return nil }
func (s *pinShard) PinToShard(context.Context, string, cid.Cid) error   { return nil }
func (s *pinShard) PublishToShardCBOR([]byte, string)                   {}
func (s *pinShard) PublishIngestMessageToCurrentAndChildIfSplit([]byte, string, string) {
	s.mu.Lock()
	s.published++
	s.mu.Unlock()
}

type pinStorage struct{}

func (pinStorage) PinFile(string) bool                 { return true }
func (pinStorage) AddKnownFile(string)                 {}
func (pinStorage) ProvideFile(context.Context, string) {}

type pinSigner struct{}

func (pinSigner) SignProtocolMessage(schema.Signable) error { return nil }

func newPinRig(t *testing.T) (*pinWatcher, *pinKubo, *pinShard) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.FileWatchFolder = t.TempDir()
	cfg.Files.FileImportTimeout = 5 * time.Second
	cfg.Files.DHTProvideTimeout = time.Second
	kubo := newPinKubo()
	shard := &pinShard{}
	fp := NewFileProcessor(FileProcessorConfig{Cfg: cfg, IPFSClient: kubo, Shard: shard, Storage: pinStorage{}, Signer: pinSigner{}})
	t.Cleanup(fp.Stop)
	return newPinWatcher(fp), kubo, shard
}

func dagPB(t *testing.T, label string) cid.Cid {
	t.Helper()
	h, err := mh.Sum([]byte(label), mh.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	return cid.NewCidV1(cid.DagProtobuf, h)
}

// storeManifest puts a D-LOCKSS manifest for payload into kubo without
// pinning it.
func storeManifest(t *testing.T, kubo *pinKubo, payload cid.Cid) cid.Cid {
	t.Helper()
	other, _ := peer.Decode("12D3KooWCFyKj1WuxUpsNdCNqzTAcZpSCNxCZ3ybSH5dVssV4YVT")
	b, err := schema.NewResearchObject("file://x", other, payload, 1).MarshalCBOR()
	if err != nil {
		t.Fatal(err)
	}
	c, err := kubo.PutDagCBOR(context.Background(), b)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestPinWatcher_IngestsUserPinOnSecondPoll(t *testing.T) {
	w, kubo, shard := newPinRig(t)
	dataset := dagPB(t, "dataset")
	kubo.userPin(dataset, "")

	w.poll(context.Background())
	if n := len(kubo.manifestPins()); n != 0 {
		t.Fatalf("first sighting must not ingest, got %d manifests", n)
	}

	w.poll(context.Background())
	manifests := kubo.manifestPins()
	if len(manifests) != 1 {
		t.Fatalf("second sighting should ingest once, got %d manifests", len(manifests))
	}
	ro := kubo.manifest(t, manifests[0])
	if !ro.Payload.Equals(dataset) || ro.MetadataRef != "ipfs://"+dataset.String() || ro.TotalSize != 42 {
		t.Fatalf("unexpected manifest %+v", ro)
	}
	if shard.published != 1 {
		t.Fatalf("ingest should be announced once, got %d", shard.published)
	}

	w.poll(context.Background())
	w.poll(context.Background())
	if n := len(kubo.manifestPins()); n != 1 {
		t.Fatalf("an ingested pin is covered by its manifest, got %d manifests", n)
	}
}

func TestPinWatcher_UsesPinNameAsMetadataRef(t *testing.T) {
	w, kubo, _ := newPinRig(t)
	paper := dagPB(t, "paper")
	kubo.userPin(paper, "paper.pdf")

	w.poll(context.Background())
	w.poll(context.Background())
	manifests := kubo.manifestPins()
	if len(manifests) != 1 || kubo.manifest(t, manifests[0]).MetadataRef != "paper.pdf" {
		t.Fatalf("pin name should become the metadata ref, got %v", manifests)
	}
}

func TestPinWatcher_SkipsDlockssPins(t *testing.T) {
	w, kubo, _ := newPinRig(t)
	h, _ := mh.Sum([]byte("replica"), mh.SHA2_256, -1)
	payloadV1 := cid.NewCidV1(cid.DagProtobuf, h)
	manifest := storeManifest(t, kubo, payloadV1)
	// A manifest pinned without ManifestPinName, and its payload pinned as
	// CIDv0.
	kubo.userPin(manifest, "")
	kubo.userPin(cid.NewCidV0(h), "")
	kubo.userPin(emptyDir, "")

	for i := 0; i < 3; i++ {
		w.poll(context.Background())
	}
	if n := len(kubo.manifestPins()); n != 0 {
		t.Fatalf("manifests, their payloads and the empty dir must not be ingested, got %d", n)
	}
}

func TestPinWatcher_FolderIngestRaceIsNotIngested(t *testing.T) {
	w, kubo, _ := newPinRig(t)
	payload := dagPB(t, "from-folder")
	kubo.userPin(payload, "")
	w.poll(context.Background())

	manifest := storeManifest(t, kubo, payload)
	_ = kubo.PinManifest(context.Background(), manifest)
	w.poll(context.Background())
	w.poll(context.Background())

	if n := len(kubo.manifestPins()); n != 1 {
		t.Fatalf("payload whose manifest appeared before the second poll must not be ingested again, got %d manifests", n)
	}
}

func TestPinWatcher_UnreadablePinBlocksPass(t *testing.T) {
	w, kubo, _ := newPinRig(t)
	payload := dagPB(t, "replica-payload")
	manifest := storeManifest(t, kubo, payload)
	kubo.userPin(manifest, "")
	kubo.userPin(payload, "")
	kubo.badBlocks[manifest] = true

	w.poll(context.Background())
	w.poll(context.Background())
	if n := len(kubo.manifestPins()); n != 0 {
		t.Fatalf("while a pin cannot be classified nothing may be ingested, got %d", n)
	}

	kubo.mu.Lock()
	delete(kubo.badBlocks, manifest)
	kubo.mu.Unlock()
	w.poll(context.Background())
	w.poll(context.Background())
	if n := len(kubo.manifestPins()); n != 0 {
		t.Fatalf("once readable the manifest covers its payload, got %d", n)
	}
}
