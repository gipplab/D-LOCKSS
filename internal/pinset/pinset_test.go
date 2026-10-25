package pinset

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"

	"dlockss/internal/badbits"
	"dlockss/internal/kubo"
	"dlockss/internal/schema"
	"dlockss/internal/trust"
)

// mockKubo fakes the handful of Kubo RPC endpoints the pinset tracker uses.
type mockKubo struct {
	mu     sync.Mutex
	pinned map[string]bool
	blocks map[string][]byte
	pins   []string // pin/add calls observed
}

func (m *mockKubo) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v0/version", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"Version": "0.43.1-mock"})
	})
	mux.HandleFunc("/api/v0/pin/ls", func(w http.ResponseWriter, r *http.Request) {
		arg := r.URL.Query().Get("arg")
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.pinned[arg] {
			fmt.Fprintf(w, `{"Keys":{%q:{"Type":"recursive"}}}`, arg)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, `{"Message":"path %q is not pinned"}`, arg)
	})
	mux.HandleFunc("/api/v0/pin/add", func(w http.ResponseWriter, r *http.Request) {
		arg := r.URL.Query().Get("arg")
		m.mu.Lock()
		m.pinned[arg] = true
		m.pins = append(m.pins, arg+"|name="+r.URL.Query().Get("name"))
		m.mu.Unlock()
		fmt.Fprintf(w, `{"Pins":[%q]}`, arg)
	})
	mux.HandleFunc("/api/v0/block/get", func(w http.ResponseWriter, r *http.Request) {
		arg := r.URL.Query().Get("arg")
		m.mu.Lock()
		block, ok := m.blocks[arg]
		m.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, `{"Message":"block not found"}`)
			return
		}
		w.Write(block)
	})
	return mux
}

func startMock(t *testing.T) (*mockKubo, *kubo.Client) {
	t.Helper()
	m := &mockKubo{pinned: map[string]bool{}, blocks: map[string][]byte{}}
	srv := httptest.NewServer(m.handler())
	t.Cleanup(srv.Close)
	u, _ := url.Parse(srv.URL)
	addr := "/ip4/" + strings.Split(u.Host, ":")[0] + "/tcp/" + u.Port()
	client, err := kubo.New(context.Background(), addr)
	if err != nil {
		t.Fatalf("mock kubo client: %v", err)
	}
	return m, client
}

func makeCID(t *testing.T, data string) cid.Cid {
	t.Helper()
	mh, err := multihash.Sum([]byte(data), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	return cid.NewCidV1(cid.DagCBOR, mh)
}

func makePeer(t *testing.T) peer.ID {
	t.Helper()
	_, pub, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	id, err := peer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func manifestBlock(t *testing.T, ingester peer.ID, payload cid.Cid) []byte {
	t.Helper()
	ro := &schema.ResearchObject{MetadataRef: "m", IngestedBy: ingester, Payload: payload}
	raw, err := ro.MarshalCBOR()
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("timed out waiting: " + msg)
}

func TestTrackerPinsMissingManifests(t *testing.T) {
	mock, client := startMock(t)
	ingester := makePeer(t)
	payload := makeCID(t, "payload-1")
	manifest := makeCID(t, "manifest-1")
	mock.blocks[manifest.String()] = manifestBlock(t, ingester, payload)

	mgr, err := NewManager(context.Background(), t.TempDir(), client, nil, trust.NewManager("open"))
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	synced := map[cid.Cid]int{}
	mgr.OnSynced = func(k cid.Cid) {
		mu.Lock()
		synced[k]++
		mu.Unlock()
	}
	if err := mgr.Join("0"); err != nil {
		t.Fatal(err)
	}
	if err := mgr.Add("0", manifest); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return synced[manifest] > 0
	}, "manifest to sync")

	mock.mu.Lock()
	defer mock.mu.Unlock()
	if !mock.pinned[manifest.String()] {
		t.Fatal("tracker did not pin the manifest")
	}
	found := false
	for _, p := range mock.pins {
		if p == manifest.String()+"|name="+kubo.ManifestPinName {
			found = true
		}
	}
	if !found {
		t.Fatalf("manifest pin missing the %q name: %v", kubo.ManifestPinName, mock.pins)
	}
	mu.Lock()
	if synced[manifest] != 1 {
		t.Fatalf("OnSynced fired %d times, want once", synced[manifest])
	}
	mu.Unlock()
}

func TestTrackerRejectsUntrustedOrigins(t *testing.T) {
	mock, client := startMock(t)
	trusted := makePeer(t)
	stranger := makePeer(t)
	payload := makeCID(t, "p")

	foreign := makeCID(t, "foreign-manifest")
	mock.blocks[foreign.String()] = manifestBlock(t, stranger, payload)
	ok := makeCID(t, "ok-manifest")
	mock.blocks[ok.String()] = manifestBlock(t, trusted, payload)

	tm := trust.NewManager("open")
	tm.AddOrigins([]string{trusted.String()})

	mgr, err := NewManager(context.Background(), t.TempDir(), client, nil, tm)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	synced := map[cid.Cid]bool{}
	mgr.OnSynced = func(k cid.Cid) { mu.Lock(); synced[k] = true; mu.Unlock() }
	if err := mgr.Join(""); err != nil {
		t.Fatal(err)
	}
	mgr.Add("", foreign)
	mgr.Add("", ok)

	waitFor(t, 5*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return synced[ok]
	}, "trusted manifest to sync")

	mu.Lock()
	if synced[foreign] {
		t.Fatal("untrusted manifest must not sync")
	}
	mu.Unlock()
	mock.mu.Lock()
	if mock.pinned[foreign.String()] {
		t.Fatal("untrusted manifest must not be pinned")
	}
	mock.mu.Unlock()
}

func TestPersistenceAcrossRestart(t *testing.T) {
	_, client := startMock(t)
	dir := t.TempDir()
	k := makeCID(t, "persisted")

	mgr, err := NewManager(context.Background(), dir, client, nil, trust.NewManager("open"))
	if err != nil {
		t.Fatal(err)
	}
	if err := mgr.Join("01"); err != nil {
		t.Fatal(err)
	}
	if err := mgr.Add("01", k); err != nil {
		t.Fatal(err)
	}
	mgr.Leave("01")

	mgr2, err := NewManager(context.Background(), dir, client, nil, trust.NewManager("open"))
	if err != nil {
		t.Fatal(err)
	}
	if err := mgr2.Join("01"); err != nil {
		t.Fatal(err)
	}
	pins, err := mgr2.List("01")
	if err != nil {
		t.Fatal(err)
	}
	if len(pins) != 1 || pins[0] != k {
		t.Fatalf("persisted pins = %v, want [%s]", pins, k)
	}
}

func TestMigratePins(t *testing.T) {
	mock, client := startMock(t)
	ingester := makePeer(t)

	stay := makeCID(t, "stay-manifest")
	move := makeCID(t, "move-manifest")
	stayPayload := makeCID(t, "stay-payload")
	movePayload := makeCID(t, "move-payload")
	mock.blocks[stay.String()] = manifestBlock(t, ingester, stayPayload)
	mock.blocks[move.String()] = manifestBlock(t, ingester, movePayload)

	mgr, err := NewManager(context.Background(), t.TempDir(), client, nil, trust.NewManager("open"))
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{"", "1"} {
		if err := mgr.Join(s); err != nil {
			t.Fatal(err)
		}
	}
	mgr.Add("", stay)
	mgr.Add("", move)

	failures := mgr.MigratePins(context.Background(), "", "1", func(payloadKey string) bool {
		return payloadKey == movePayload.String()
	})
	if failures != 0 {
		t.Fatalf("migration failures: %d", failures)
	}
	dest, _ := mgr.List("1")
	src, _ := mgr.List("")
	if len(dest) != 1 || dest[0] != move {
		t.Fatalf("dest = %v, want [%s]", dest, move)
	}
	if len(src) != 1 || src[0] != stay {
		t.Fatalf("src = %v, want [%s]", src, stay)
	}
}

func TestAddRequiresMembership(t *testing.T) {
	_, client := startMock(t)
	mgr, err := NewManager(context.Background(), t.TempDir(), client, nil, trust.NewManager("open"))
	if err != nil {
		t.Fatal(err)
	}
	if err := mgr.Add("00", makeCID(t, "x")); err == nil {
		t.Fatal("Add to an unjoined shard must fail")
	}
}

func TestBadbitsSkipped(t *testing.T) {
	mock, client := startMock(t)
	blocked := makeCID(t, "blocked-manifest")
	bbPath := t.TempDir() + "/bb.csv"
	writeFile(t, bbPath, blocked.String()+"\n")
	bb, err := badbits.Load(bbPath)
	if err != nil {
		t.Fatal(err)
	}
	mgr, err := NewManager(context.Background(), t.TempDir(), client, bb, trust.NewManager("open"))
	if err != nil {
		t.Fatal(err)
	}
	if err := mgr.Join(""); err != nil {
		t.Fatal(err)
	}
	mgr.Add("", blocked)
	time.Sleep(300 * time.Millisecond)
	mock.mu.Lock()
	defer mock.mu.Unlock()
	if mock.pinned[blocked.String()] {
		t.Fatal("denylisted manifest must never be pinned")
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := writeFileErr(path, content); err != nil {
		t.Fatal(err)
	}
}

func writeFileErr(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o600)
}
