package ipfs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/ipfs/go-cid"
	ipfsapi "github.com/ipfs/go-ipfs-api"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	ma "github.com/multiformats/go-multiaddr"
)

var errAdapterClosed = errors.New("DHT adapter is closed")

type provideRequest struct {
	ctx      context.Context
	key      cid.Cid
	resultCh chan error
}

// IPFSDHTAdapter implements common.DHTProvider on top of the Kubo RPC API.
// It also implements routing.Routing for use by libp2p and ipfs-cluster.
// Provides are serialized through a single worker goroutine.
type IPFSDHTAdapter struct {
	api            *ipfsapi.Shell
	retryAttempts  int
	retryDelay     time.Duration
	provideTimeout time.Duration
	provideQueue   chan *provideRequest
	workerCtx      context.Context
	workerCancel   context.CancelFunc
}

var _ routing.Routing = (*IPFSDHTAdapter)(nil)

// NewIPFSDHTAdapterFromClient creates a DHT adapter from an ipfs.Client,
// avoiding the need for callers to access the raw Shell.
func NewIPFSDHTAdapterFromClient(c *Client) *IPFSDHTAdapter {
	ctx, cancel := context.WithCancel(context.Background())
	adapter := &IPFSDHTAdapter{
		api:            c.api,
		retryAttempts:  3,
		retryDelay:     500 * time.Millisecond,
		provideTimeout: 60 * time.Second,
		provideQueue:   make(chan *provideRequest, 100),
		workerCtx:      ctx,
		workerCancel:   cancel,
	}
	go adapter.worker()
	return adapter
}

func (a *IPFSDHTAdapter) worker() {
	for {
		select {
		case <-a.workerCtx.Done():
			return
		case req := <-a.provideQueue:
			a.processProvideRequest(req)
		}
	}
}

func (a *IPFSDHTAdapter) processProvideRequest(req *provideRequest) {
	// A request without a deadline, or with little time left after waiting in
	// the queue, runs detached under provideTimeout.
	opCtx := req.ctx
	if deadline, ok := req.ctx.Deadline(); !ok || time.Until(deadline) < a.provideTimeout/2 {
		var cancel context.CancelFunc
		opCtx, cancel = context.WithTimeout(context.Background(), a.provideTimeout)
		defer cancel()
	}

	err := a.provideWithRetry(opCtx, req.key)

	select {
	case req.resultCh <- err:
	case <-req.ctx.Done():
	}
}

// routingEvent is one line of the newline-delimited JSON stream returned by
// routing/findprovs and routing/findpeer. Most lines report query progress
// and list closer peers; only Provider and FinalPeer events carry answers.
type routingEvent struct {
	Type      routing.QueryEventType `json:"Type"`
	Responses []struct {
		ID    string   `json:"ID"`
		Addrs []string `json:"Addrs"`
	} `json:"Responses"`
}

// streamRoutingEvents passes each event of req's response to fn until the
// stream ends or fn returns false.
func streamRoutingEvents(ctx context.Context, req *ipfsapi.RequestBuilder, fn func(routingEvent) bool) error {
	res, err := req.Send(ctx)
	if err != nil {
		return err
	}
	defer res.Close()
	if res.Error != nil {
		return res.Error
	}
	dec := json.NewDecoder(res.Output)
	for {
		var ev routingEvent
		if err := dec.Decode(&ev); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if !fn(ev) {
			return nil
		}
	}
}

func parseAddrInfo(id string, addrStrs []string) (peer.AddrInfo, bool) {
	peerID, err := peer.Decode(id)
	if err != nil {
		return peer.AddrInfo{}, false
	}
	var addrs []ma.Multiaddr
	for _, s := range addrStrs {
		if addr, err := ma.NewMultiaddr(s); err == nil {
			addrs = append(addrs, addr)
		}
	}
	return peer.AddrInfo{ID: peerID, Addrs: addrs}, true
}

// FindProvidersAsync streams the providers of key that Kubo finds.
func (a *IPFSDHTAdapter) FindProvidersAsync(ctx context.Context, key cid.Cid, count int) <-chan peer.AddrInfo {
	ch := make(chan peer.AddrInfo, 10)

	go func() {
		defer close(ch)

		req := a.api.Request("routing/findprovs", key.String())
		if count > 0 {
			req.Option("num-providers", count)
		}

		_ = streamRoutingEvents(ctx, req, func(ev routingEvent) bool {
			if ev.Type != routing.Provider {
				return true
			}
			for _, resp := range ev.Responses {
				info, ok := parseAddrInfo(resp.ID, resp.Addrs)
				if !ok {
					continue
				}
				select {
				case ch <- info:
				case <-ctx.Done():
					return false
				}
			}
			return true
		})
	}()

	return ch
}

func isTransientError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) {
		return true
	}
	errStr := strings.ToLower(err.Error())
	if strings.Contains(errStr, "eof") {
		return true
	}
	return strings.Contains(errStr, "connection") &&
		(strings.Contains(errStr, "reset") || strings.Contains(errStr, "closed") || strings.Contains(errStr, "refused"))
}

// Provide asks Kubo to announce key to the DHT and waits for its reply.
// broadcast is ignored.
func (a *IPFSDHTAdapter) Provide(ctx context.Context, key cid.Cid, broadcast bool) error {
	req := &provideRequest{
		ctx:      ctx,
		key:      key,
		resultCh: make(chan error, 1),
	}
	select {
	case a.provideQueue <- req:
	case <-ctx.Done():
		return ctx.Err()
	case <-a.workerCtx.Done():
		return errAdapterClosed
	}
	select {
	case err := <-req.resultCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-a.workerCtx.Done():
		return errAdapterClosed
	}
}

// provideWithRetry retries transient failures with exponential backoff
// capped at 5s.
func (a *IPFSDHTAdapter) provideWithRetry(ctx context.Context, key cid.Cid) error {
	var lastErr error
	for attempt := 0; attempt <= a.retryAttempts; attempt++ {
		if attempt > 0 {
			backoffDelay := min(a.retryDelay<<(attempt-1), 5*time.Second)
			if deadline, ok := ctx.Deadline(); ok {
				timeRemaining := time.Until(deadline)
				if timeRemaining < backoffDelay*2 {
					return fmt.Errorf("context deadline too soon for retry: %v remaining", timeRemaining)
				}
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoffDelay):
			}
		}

		err := a.provideOnce(ctx, key)
		if err == nil {
			return nil
		}
		lastErr = err
		if !isTransientError(err) {
			return err
		}
	}
	return fmt.Errorf("provide failed after %d retries: %w", a.retryAttempts, lastErr)
}

// provideOnce uses 'provide once', which queues the CID with Kubo's provider
// and returns. Kubo releases without it only have routing/provide, which
// announces synchronously. Both reply with an empty body on success, so the
// response must not be decoded.
func (a *IPFSDHTAdapter) provideOnce(ctx context.Context, key cid.Cid) error {
	err := a.api.Request("provide/once", key.String()).Exec(ctx, nil)
	if err == nil || ctx.Err() != nil || isTransientError(err) {
		return err
	}
	if err2 := a.api.Request("routing/provide", key.String()).Exec(ctx, nil); err2 != nil {
		return fmt.Errorf("provide/once: %w; routing/provide: %w", err, err2)
	}
	return nil
}

// FindPeer looks up the addresses of id through Kubo.
func (a *IPFSDHTAdapter) FindPeer(ctx context.Context, id peer.ID) (peer.AddrInfo, error) {
	var found peer.AddrInfo
	req := a.api.Request("routing/findpeer", id.String())
	err := streamRoutingEvents(ctx, req, func(ev routingEvent) bool {
		if ev.Type != routing.FinalPeer {
			return true
		}
		for _, resp := range ev.Responses {
			if info, ok := parseAddrInfo(resp.ID, resp.Addrs); ok && info.ID == id && len(info.Addrs) > 0 {
				found = info
				return false
			}
		}
		return true
	})
	if err != nil {
		return peer.AddrInfo{}, fmt.Errorf("peer lookup failed: %w", err)
	}
	if found.ID == "" {
		return peer.AddrInfo{}, errors.New("peer not found")
	}
	return found, nil
}

// PutValue implements routing.ValueStore. opts are ignored.
func (a *IPFSDHTAdapter) PutValue(ctx context.Context, key string, val []byte, opts ...routing.Option) error {
	return a.api.Request("dht/put", key).Body(bytes.NewReader(val)).Exec(ctx, nil)
}

// GetValue implements routing.ValueStore. It returns the first value found.
func (a *IPFSDHTAdapter) GetValue(ctx context.Context, key string, opts ...routing.Option) ([]byte, error) {
	var result struct {
		Values [][]byte `json:"Values"`
	}
	if err := a.api.Request("dht/get", key).Exec(ctx, &result); err != nil {
		return nil, err
	}
	if len(result.Values) == 0 {
		return nil, routing.ErrNotFound
	}
	return result.Values[0], nil
}

// SearchValue implements routing.ValueStore by delivering the GetValue result
// on a channel.
func (a *IPFSDHTAdapter) SearchValue(ctx context.Context, key string, opts ...routing.Option) (<-chan []byte, error) {
	out := make(chan []byte, 1)
	go func() {
		defer close(out)
		val, err := a.GetValue(ctx, key, opts...)
		if err == nil {
			select {
			case out <- val:
			case <-ctx.Done():
			}
		}
	}()
	return out, nil
}

// Bootstrap implements routing.Routing. IPFS daemon bootstraps itself.
func (a *IPFSDHTAdapter) Bootstrap(ctx context.Context) error {
	return nil
}
