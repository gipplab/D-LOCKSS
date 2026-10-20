package ipfs

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	ipfsapi "github.com/ipfs/go-ipfs-api"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
)

const (
	provA  = "12D3KooW9syRZpRZKiJbLrrxbcuymbhT5BTF9hjG6bykH42q29Mb"
	provB  = "12D3KooWCFyKj1WuxUpsNdCNqzTAcZpSCNxCZ3ybSH5dVssV4YVT"
	closer = "12D3KooWLb7WFH45GCF4ZKvxvH1ZbMvWDqBvsGYtMpsCACvuLSiC"
)

// fakeKubo serves canned RPC responses keyed by command path and records
// which commands were called.
type fakeKubo struct {
	mu     sync.Mutex
	calls  []string
	routes map[string]func(w http.ResponseWriter)
}

func (f *fakeKubo) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	cmd := strings.TrimPrefix(r.URL.Path, "/api/v0/")
	f.mu.Lock()
	f.calls = append(f.calls, cmd)
	f.mu.Unlock()
	if h, ok := f.routes[cmd]; ok {
		h(w)
		return
	}
	http.NotFound(w, r)
}

func (f *fakeKubo) called() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func kuboError(msg string) func(w http.ResponseWriter) {
	return func(w http.ResponseWriter) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"Message":"` + msg + `","Code":0,"Type":"error"}`))
	}
}

func kuboStream(lines ...string) func(w http.ResponseWriter) {
	return func(w http.ResponseWriter) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Chunked-Output", "1")
		_, _ = w.Write([]byte(strings.Join(lines, "\n") + "\n"))
	}
}

func emptyOK(w http.ResponseWriter) { w.WriteHeader(http.StatusOK) }

func newTestAdapter(t *testing.T, f *fakeKubo) *IPFSDHTAdapter {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	a := NewIPFSDHTAdapterFromClient(&Client{api: ipfsapi.NewShell(srv.URL)})
	a.retryDelay = time.Millisecond
	t.Cleanup(a.workerCancel)
	return a
}

func testKey(t *testing.T) cid.Cid {
	t.Helper()
	h, err := multihash.Sum([]byte("dht-adapter"), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	return cid.NewCidV1(cid.Raw, h)
}

func TestProvide_EmptySuccessBodyIsSuccess(t *testing.T) {
	f := &fakeKubo{routes: map[string]func(http.ResponseWriter){
		"provide/once":    emptyOK,
		"routing/provide": emptyOK,
		"dht/provide":     kuboError("removed, use 'ipfs routing' instead"),
	}}
	a := newTestAdapter(t, f)

	if err := a.Provide(context.Background(), testKey(t), true); err != nil {
		t.Fatalf("provide with an empty 200 reply must succeed, got %v", err)
	}
	if got := f.called(); len(got) != 1 || got[0] != "provide/once" {
		t.Fatalf("want a single provide/once call, got %v", got)
	}
}

func TestProvide_FallsBackToRoutingProvide(t *testing.T) {
	f := &fakeKubo{routes: map[string]func(http.ResponseWriter){
		"routing/provide": emptyOK,
	}}
	a := newTestAdapter(t, f)

	if err := a.Provide(context.Background(), testKey(t), true); err != nil {
		t.Fatalf("a Kubo without provide/once must fall back to routing/provide, got %v", err)
	}
	if got := f.called(); len(got) != 2 || got[1] != "routing/provide" {
		t.Fatalf("want provide/once then routing/provide, got %v", got)
	}
}

func TestProvide_ReportsKuboError(t *testing.T) {
	f := &fakeKubo{routes: map[string]func(http.ResponseWriter){
		"provide/once":    kuboError("block not found locally, cannot provide"),
		"routing/provide": kuboError("block not found locally, cannot provide"),
	}}
	a := newTestAdapter(t, f)

	err := a.Provide(context.Background(), testKey(t), true)
	if err == nil || !strings.Contains(err.Error(), "not found locally") {
		t.Fatalf("want Kubo's error, got %v", err)
	}
	if got := f.called(); len(got) != 2 {
		t.Fatalf("a permanent error must not be retried, got %v", got)
	}
}

func TestFindProvidersAsync_OnlyProviderEvents(t *testing.T) {
	f := &fakeKubo{routes: map[string]func(http.ResponseWriter){
		"routing/findprovs": kuboStream(
			`{"Extra":"","ID":"`+closer+`","Responses":null,"Type":0}`,
			`{"Extra":"","ID":"`+closer+`","Responses":[{"ID":"`+closer+`","Addrs":["/ip4/1.2.3.4/tcp/4001"]}],"Type":1}`,
			`{"Extra":"","ID":"","Responses":[{"ID":"`+provA+`","Addrs":["/ip4/5.6.7.8/tcp/4001"]}],"Type":4}`,
			`{"Extra":"","ID":"`+closer+`","Responses":null,"Type":7}`,
			`{"Extra":"","ID":"","Responses":[{"ID":"`+provB+`","Addrs":[]}],"Type":4}`,
		),
	}}
	a := newTestAdapter(t, f)

	var got []string
	for info := range a.FindProvidersAsync(context.Background(), testKey(t), 5) {
		got = append(got, info.ID.String())
	}
	if len(got) != 2 || got[0] != provA || got[1] != provB {
		t.Fatalf("want providers [%s %s], got %v", provA, provB, got)
	}
}

func TestFindPeer_UsesFinalPeerEvent(t *testing.T) {
	target, err := peer.Decode(provA)
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeKubo{routes: map[string]func(http.ResponseWriter){
		"routing/findpeer": kuboStream(
			`{"Extra":"","ID":"`+closer+`","Responses":null,"Type":0}`,
			`{"Extra":"","ID":"`+closer+`","Responses":[{"ID":"`+provB+`","Addrs":["/ip4/1.2.3.4/tcp/4001"]}],"Type":1}`,
			`{"Extra":"","ID":"","Responses":[{"ID":"`+provA+`","Addrs":["/ip4/5.6.7.8/tcp/4001"]}],"Type":2}`,
		),
	}}
	a := newTestAdapter(t, f)

	info, err := a.FindPeer(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}
	if info.ID != target || len(info.Addrs) != 1 || info.Addrs[0].String() != "/ip4/5.6.7.8/tcp/4001" {
		t.Fatalf("got %v", info)
	}
}

func TestFindPeer_NotFound(t *testing.T) {
	target, _ := peer.Decode(provA)
	f := &fakeKubo{routes: map[string]func(http.ResponseWriter){
		"routing/findpeer": kuboStream(
			`{"Extra":"","ID":"` + closer + `","Responses":[{"ID":"` + provB + `","Addrs":["/ip4/1.2.3.4/tcp/4001"]}],"Type":1}`,
		),
	}}
	a := newTestAdapter(t, f)

	if _, err := a.FindPeer(context.Background(), target); err == nil {
		t.Fatal("closer peers in a PeerResponse are not the target")
	}
}
