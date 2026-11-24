package fileops

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

	"dlockss/internal/config"
	"dlockss/internal/testutil"
	"dlockss/pkg/schema"
)

// nodeShard is a pinShard whose node ID belongs to a key the test holds.
type nodeShard struct {
	*pinShard
	id peer.ID
}

func (s nodeShard) NodeID() peer.ID { return s.id }

type manifestRig struct {
	fp   *FileProcessor
	kubo *pinKubo
	key  testutil.Key // the Kubo node, which also signs new manifests
}

func newManifestRig(t *testing.T) *manifestRig {
	t.Helper()
	key := testutil.NewKey(t)
	cfg := config.DefaultConfig()
	cfg.FileWatchFolder = t.TempDir()
	cfg.Files.FileImportTimeout = 5 * time.Second
	kubo := newPinKubo()
	fp := NewFileProcessor(FileProcessorConfig{
		Cfg: cfg, IPFSClient: kubo, Shard: nodeShard{&pinShard{}, key.ID},
		Storage: pinStorage{}, Signer: pinSigner{}, NodeSigner: key,
	})
	t.Cleanup(fp.Stop)
	return &manifestRig{fp: fp, kubo: kubo, key: key}
}

// pinOld stores and pins ro the way D-LOCKSS did before helpers had their
// own keys: signed directly with the node key, without Kubo's prefix.
func (r *manifestRig) pinOld(t *testing.T, ro *schema.ResearchObject) cid.Cid {
	t.Helper()
	return r.pinSigned(t, ro, r.key.Priv)
}

// pinSigned stores and pins ro signed directly with priv.
func (r *manifestRig) pinSigned(t *testing.T, ro *schema.ResearchObject, priv crypto.PrivKey) cid.Cid {
	t.Helper()
	unsigned, err := ro.MarshalCBORForSigning()
	if err != nil {
		t.Fatal(err)
	}
	if ro.Signature, err = priv.Sign(unsigned); err != nil {
		t.Fatal(err)
	}
	b, err := ro.MarshalCBOR()
	if err != nil {
		t.Fatal(err)
	}
	return r.pinBlock(t, b)
}

func (r *manifestRig) pinBlock(t *testing.T, b []byte) cid.Cid {
	t.Helper()
	c, err := r.kubo.PutDagCBOR(context.Background(), b)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.kubo.PinManifest(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	return c
}

func (r *manifestRig) blockCount() int {
	r.kubo.mu.Lock()
	defer r.kubo.mu.Unlock()
	return len(r.kubo.blocks)
}

func TestStoreManifest_ReusesPreHelperManifest(t *testing.T) {
	r := newManifestRig(t)
	payload := dagPB(t, "paper")
	old := r.pinOld(t, schema.NewResearchObject("file://paper.pdf", r.key.ID, payload, 7))

	r.fp.ScanExistingFiles() // loads the index; the watch folder is empty
	blocks := r.blockCount()

	got, err := r.fp.storeManifest(context.Background(), schema.NewResearchObject("file://paper.pdf", r.key.ID, payload, 7))
	if err != nil {
		t.Fatal(err)
	}
	if !got.Equals(old) {
		t.Fatalf("re-ingest stored %s, want the existing manifest %s", got, old)
	}
	if n := r.blockCount(); n != blocks {
		t.Fatalf("no new manifest block expected, blocks %d -> %d", blocks, n)
	}
}

func TestStoreManifest_NewManifestWhenContentDiffers(t *testing.T) {
	r := newManifestRig(t)
	payload := dagPB(t, "paper")
	old := r.pinOld(t, schema.NewResearchObject("file://paper.pdf", r.key.ID, payload, 7))
	r.fp.loadManifestIndex()

	got, err := r.fp.storeManifest(context.Background(), schema.NewResearchObject("file://renamed.pdf", r.key.ID, payload, 7))
	if err != nil {
		t.Fatal(err)
	}
	if got.Equals(old) {
		t.Fatal("a manifest with another meta_ref must not be reused")
	}
	ro := r.kubo.manifest(t, got)
	if err := ro.VerifySignature(); err != nil {
		t.Fatalf("new manifest should be Kubo-signed by the node: %v", err)
	}
}

func TestStoreManifest_IgnoresUnusableManifests(t *testing.T) {
	r := newManifestRig(t)
	ctx := context.Background()

	// Signed with another key although it names this node.
	forged := dagPB(t, "forged")
	forgedCID := r.pinSigned(t, schema.NewResearchObject("file://forged", r.key.ID, forged, 1), testutil.NewKey(t).Priv)

	// Legacy manifest with a timestamp, validly signed: the legacy cleanup
	// removes it, so it must be replaced.
	legacy := dagPB(t, "legacy")
	legacyCID := r.pinBlock(t, legacyManifest(t, r, "file://legacy", legacy, 2))

	r.fp.loadManifestIndex()
	for _, tc := range []struct {
		name    string
		payload cid.Cid
		metaRef string
		size    uint64
		avoid   cid.Cid
	}{
		{"forged signature", forged, "file://forged", 1, forgedCID},
		{"legacy timestamp", legacy, "file://legacy", 2, legacyCID},
	} {
		got, err := r.fp.storeManifest(ctx, schema.NewResearchObject(tc.metaRef, r.key.ID, tc.payload, tc.size))
		if err != nil {
			t.Fatal(err)
		}
		if got.Equals(tc.avoid) {
			t.Errorf("%s: manifest %s must not be reused", tc.name, got)
		}
	}
}

func TestStoreManifest_NoReuseBeforeIndexLoaded(t *testing.T) {
	r := newManifestRig(t)
	payload := dagPB(t, "paper")
	old := r.pinOld(t, schema.NewResearchObject("file://paper.pdf", r.key.ID, payload, 7))

	got, err := r.fp.storeManifest(context.Background(), schema.NewResearchObject("file://paper.pdf", r.key.ID, payload, 7))
	if err != nil {
		t.Fatal(err)
	}
	if got.Equals(old) {
		t.Fatal("without the index, storeManifest stores a new manifest")
	}
}

// legacyManifest encodes a manifest with the "ts" field of old versions,
// signed directly with the node key.
func legacyManifest(t *testing.T, r *manifestRig, metaRef string, payload cid.Cid, size int64) []byte {
	t.Helper()
	unsigned, err := schema.NewResearchObject(metaRef, r.key.ID, payload, uint64(size)).MarshalCBORForSigning()
	if err != nil {
		t.Fatal(err)
	}
	sig, err := r.key.Priv.Sign(unsigned)
	if err != nil {
		t.Fatal(err)
	}
	n, err := qp.BuildMap(basicnode.Prototype.Any, 6, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "meta_ref", qp.String(metaRef))
		qp.MapEntry(ma, "ingester_id", qp.String(r.key.ID.String()))
		qp.MapEntry(ma, "sig", qp.Bytes(sig))
		qp.MapEntry(ma, "ts", qp.Int(1700000000))
		qp.MapEntry(ma, "payload", qp.String(payload.String()))
		qp.MapEntry(ma, "size", qp.Int(size))
	})
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := dagcbor.Encode(n, &buf); err != nil {
		t.Fatal(err)
	}
	var ro schema.ResearchObject
	if err := ro.UnmarshalCBOR(buf.Bytes()); err != nil || !ro.HasLegacyTimestamp || ro.VerifySignature() != nil {
		t.Fatalf("legacy fixture: err=%v legacy=%v sig=%v", err, ro.HasLegacyTimestamp, ro.VerifySignature())
	}
	return buf.Bytes()
}
