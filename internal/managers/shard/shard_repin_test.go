package shard

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
)

// repinIPFS counts re-pins of manifests and payloads.
type repinIPFS struct {
	*manifestIPFS
	mu       sync.Mutex
	fail     bool
	manifest int
	payloads []string
}

func (r *repinIPFS) PinManifest(context.Context, cid.Cid) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.manifest++
	if r.fail {
		return errors.New("timeout")
	}
	return nil
}

func (r *repinIPFS) PinRecursive(_ context.Context, c cid.Cid) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.payloads = append(r.payloads, c.String())
	return nil
}

func (r *repinIPFS) counts() (int, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.manifest, len(r.payloads)
}

func TestCompleteNextPinnedFile(t *testing.T) {
	sm, storage, mipfs := newCustodialRig(t, 0)
	ipfs := &repinIPFS{manifestIPFS: mipfs, fail: true}
	sm.ipfsClient = ipfs
	payload := testCID(t, "payload")
	key := pinManifest(t, storage, mipfs, payload, "manifest", time.Now())
	storage.next = key

	run := func() {
		t.Helper()
		sm.completeNextPinnedFile()
		deadline := time.Now().Add(2 * time.Second)
		for sm.repinInFlight.Load() && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
	}

	run()
	if m, p := ipfs.counts(); m != 1 || p != 0 || storage.IsComplete(key) {
		t.Fatalf("after a failed re-pin: manifest pins %d, payload pins %d, complete %v; want 1, 0, false", m, p, storage.IsComplete(key))
	}

	ipfs.mu.Lock()
	ipfs.fail = false
	ipfs.mu.Unlock()
	run()
	if m, p := ipfs.counts(); m != 2 || p != 1 || ipfs.payloads[0] != payload.String() || !storage.IsComplete(key) {
		t.Fatalf("after a successful re-pin: manifest pins %d, payload pins %d, complete %v; want 2, 1, true", m, p, storage.IsComplete(key))
	}

	run()
	if m, _ := ipfs.counts(); m != 2 {
		t.Fatalf("a complete file must not be re-pinned, got %d manifest pins", m)
	}
}
