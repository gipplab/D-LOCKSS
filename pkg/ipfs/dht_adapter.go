package ipfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	ipfsapi "github.com/ipfs/go-ipfs-api"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	ma "github.com/multiformats/go-multiaddr"
)

// provideRequest represents a queued provide operation
type provideRequest struct {
	ctx       context.Context
	key       cid.Cid
	broadcast bool
	resultCh  chan error
}

// IPFSDHTAdapter implements DHTProvider using IPFS's DHT via HTTP API
// It also implements routing.Routing to be compatible with libp2p/ipfs-cluster.
type IPFSDHTAdapter struct {
	api             *ipfsapi.Shell
	retryAttempts   int
	retryDelay      time.Duration
	provideTimeout  time.Duration // Timeout for provide operations
	provideInterval time.Duration // Min delay between provide ops (0 = no delay) to avoid overwhelming the DHT
	provideQueue    chan *provideRequest
	workerCtx       context.Context
	workerCancel    context.CancelFunc
	workerStarted   bool
	workerMu        sync.Mutex
	intervalMu      sync.RWMutex
}

// Ensure IPFSDHTAdapter implements routing.Routing
var _ routing.Routing = (*IPFSDHTAdapter)(nil)

// NewIPFSDHTAdapter creates a new DHT adapter that uses IPFS's DHT
func NewIPFSDHTAdapter(api *ipfsapi.Shell) *IPFSDHTAdapter {
	ctx, cancel := context.WithCancel(context.Background())
	adapter := &IPFSDHTAdapter{
		api:            api,
		retryAttempts:  3,                               // Default retry attempts
		retryDelay:     500 * time.Millisecond,          // Default retry delay
		provideTimeout: 60 * time.Second,                // Default timeout
		provideQueue:   make(chan *provideRequest, 100), // Buffer up to 100 queued operations
		workerCtx:      ctx,
		workerCancel:   cancel,
	}
	adapter.startWorker()
	return adapter
}

// NewIPFSDHTAdapterWithRetry creates a new DHT adapter with custom retry configuration
func NewIPFSDHTAdapterWithRetry(api *ipfsapi.Shell, retryAttempts int, retryDelay time.Duration) *IPFSDHTAdapter {
	return NewIPFSDHTAdapterWithTimeout(api, retryAttempts, retryDelay, 60*time.Second)
}

// NewIPFSDHTAdapterWithTimeout creates a new DHT adapter with custom retry and timeout configuration
func NewIPFSDHTAdapterWithTimeout(api *ipfsapi.Shell, retryAttempts int, retryDelay time.Duration, provideTimeout time.Duration) *IPFSDHTAdapter {
	ctx, cancel := context.WithCancel(context.Background())
	adapter := &IPFSDHTAdapter{
		api:            api,
		retryAttempts:  retryAttempts,
		retryDelay:     retryDelay,
		provideTimeout: provideTimeout,
		provideQueue:   make(chan *provideRequest, 100), // Buffer up to 100 queued operations
		workerCtx:      ctx,
		workerCancel:   cancel,
	}
	adapter.startWorker()
	return adapter
}

// SetProvideInterval sets the minimum delay between processing provide operations.
// Use > 0 (e.g. 3s) to avoid overwhelming the DHT when many files are announced.
func (a *IPFSDHTAdapter) SetProvideInterval(d time.Duration) {
	a.intervalMu.Lock()
	defer a.intervalMu.Unlock()
	a.provideInterval = d
}

// startWorker starts the worker goroutine that processes provide operations one at a time
func (a *IPFSDHTAdapter) startWorker() {
	a.workerMu.Lock()
	defer a.workerMu.Unlock()

	if a.workerStarted {
		return
	}

	go a.worker()
	a.workerStarted = true
}

// worker processes provide operations from the queue one at a time
func (a *IPFSDHTAdapter) worker() {
	for {
		select {
		case <-a.workerCtx.Done():
			return
		case req := <-a.provideQueue:
			opCtx := req.ctx
			if deadline, ok := req.ctx.Deadline(); ok {
				remaining := time.Until(deadline)
				if remaining < a.provideTimeout/2 {
					var cancel context.CancelFunc
					opCtx, cancel = context.WithTimeout(context.Background(), a.provideTimeout)
					defer cancel()
				}
			} else {
				var cancel context.CancelFunc
				opCtx, cancel = context.WithTimeout(context.Background(), a.provideTimeout)
				defer cancel()
			}

			err := a.provideInternal(opCtx, req.key, req.broadcast)

			select {
			case req.resultCh <- err:
			case <-req.ctx.Done():
			}

			a.intervalMu.RLock()
			interval := a.provideInterval
			a.intervalMu.RUnlock()
			if interval > 0 {
				select {
				case <-a.workerCtx.Done():
					return
				case <-time.After(interval):
				}
			}
		}
	}
}

// Close shuts down the worker goroutine
func (a *IPFSDHTAdapter) Close() {
	a.workerCancel()
}

// FindProvidersAsync finds providers of a CID using IPFS DHT
func (a *IPFSDHTAdapter) FindProvidersAsync(ctx context.Context, key cid.Cid, count int) <-chan peer.AddrInfo {
	ch := make(chan peer.AddrInfo, 10)

	go func() {
		defer close(ch)

		var result struct {
			Responses []struct {
				ID    string   `json:"ID"`
				Addrs []string `json:"Addrs"`
			} `json:"Responses"`
		}

		req := a.api.Request("routing/findprovs", key.String())
		if count > 0 {
			req.Option("num-providers", count)
		}

		if err := req.Exec(ctx, &result); err != nil {
			req2 := a.api.Request("dht/findprovs", key.String())
			if count > 0 {
				req2.Option("num-providers", count)
			}
			if err2 := req2.Exec(ctx, &result); err2 != nil {
				return
			}
		}

		for _, resp := range result.Responses {
			peerID, err := peer.Decode(resp.ID)
			if err != nil {
				continue
			}

			var addrs []ma.Multiaddr
			for _, addrStr := range resp.Addrs {
				addr, err := ma.NewMultiaddr(addrStr)
				if err != nil {
					continue
				}
				addrs = append(addrs, addr)
			}

			select {
			case ch <- peer.AddrInfo{ID: peerID, Addrs: addrs}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch
}

// isTransientError checks if an error is transient and should be retried
func (a *IPFSDHTAdapter) isTransientError(err error) bool {
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
	if strings.Contains(errStr, "connection") &&
		(strings.Contains(errStr, "reset") || strings.Contains(errStr, "closed") || strings.Contains(errStr, "refused")) {
		return true
	}
	return false
}

// Provide announces that this node provides a CID using IPFS DHT
func (a *IPFSDHTAdapter) Provide(ctx context.Context, key cid.Cid, broadcast bool) error {
	a.startWorker()
	req := &provideRequest{
		ctx:       ctx,
		key:       key,
		broadcast: broadcast,
		resultCh:  make(chan error, 1),
	}
	select {
	case a.provideQueue <- req:
	case <-ctx.Done():
		return ctx.Err()
	case <-a.workerCtx.Done():
		return fmt.Errorf("DHT adapter is closed")
	}
	select {
	case err := <-req.resultCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-a.workerCtx.Done():
		return fmt.Errorf("DHT adapter is closed")
	}
}

// provideInternal performs the actual provide operation (called by worker)
func (a *IPFSDHTAdapter) provideInternal(ctx context.Context, key cid.Cid, broadcast bool) error {
	var result struct {
		Extra string `json:"Extra"`
	}

	var lastErr error
	var lastErr2 error

	for attempt := 0; attempt <= a.retryAttempts; attempt++ {
		if attempt > 0 {
			backoffMultiplier := 1 << uint(attempt-1)
			backoffDelay := time.Duration(int64(a.retryDelay) * int64(backoffMultiplier))
			if backoffDelay > 5*time.Second {
				backoffDelay = 5 * time.Second
			}
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

		req := a.api.Request("routing/provide", key.String())
		if broadcast {
			req.Option("recursive", false)
		}

		err := req.Exec(ctx, &result)
		if err == nil {
			return nil
		}

		lastErr = err

		if a.isTransientError(err) && attempt < a.retryAttempts {
			continue
		}

		req2 := a.api.Request("dht/provide", key.String())
		if broadcast {
			req2.Option("recursive", false)
		}
		err2 := req2.Exec(ctx, &result)
		if err2 == nil {
			return nil
		}

		lastErr2 = err2

		if a.isTransientError(err2) && attempt < a.retryAttempts {
			continue
		}

		if !a.isTransientError(err) && !a.isTransientError(err2) {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return err
			}
			return fmt.Errorf("both routing/provide and dht/provide failed: routing=%v, dht=%v", err, err2)
		}
	}
	return fmt.Errorf("both routing/provide and dht/provide failed after %d retries: routing=%v, dht=%v", a.retryAttempts, lastErr, lastErr2)
}

// FindPeer finds a peer by ID using IPFS DHT
func (a *IPFSDHTAdapter) FindPeer(ctx context.Context, id peer.ID) (peer.AddrInfo, error) {
	var result struct {
		Responses []struct {
			ID    string   `json:"ID"`
			Addrs []string `json:"Addrs"`
		} `json:"Responses"`
		Extra string `json:"Extra,omitempty"`
	}

	req := a.api.Request("routing/findpeer", id.String())
	if err := req.Exec(ctx, &result); err != nil {
		req2 := a.api.Request("dht/findpeer", id.String())
		if err2 := req2.Exec(ctx, &result); err2 != nil {
			return peer.AddrInfo{}, fmt.Errorf("peer lookup failed: %w (routing: %v)", err2, err)
		}
	}

	if len(result.Responses) == 0 {
		return peer.AddrInfo{}, fmt.Errorf("peer not found")
	}

	resp := result.Responses[0]
	peerID, err := peer.Decode(resp.ID)
	if err != nil {
		return peer.AddrInfo{}, fmt.Errorf("invalid peer ID in response: %w", err)
	}

	var addrs []ma.Multiaddr
	for _, addrStr := range resp.Addrs {
		addr, err := ma.NewMultiaddr(addrStr)
		if err != nil {
			continue
		}
		addrs = append(addrs, addr)
	}

	if len(addrs) == 0 {
		return peer.AddrInfo{}, fmt.Errorf("no valid addresses found for peer")
	}

	return peer.AddrInfo{ID: peerID, Addrs: addrs}, nil
}

// PutValue implements routing.ValueStore.
func (a *IPFSDHTAdapter) PutValue(ctx context.Context, key string, val []byte, opts ...routing.Option) error {
	// IPFS API dht/put
	// Note: We ignore opts for now
	return a.api.Request("dht/put", key).Body(strings.NewReader(string(val))).Exec(ctx, nil)
}

// GetValue implements routing.ValueStore.
func (a *IPFSDHTAdapter) GetValue(ctx context.Context, key string, opts ...routing.Option) ([]byte, error) {
	// IPFS API dht/get
	var result struct {
		Values [][]byte `json:"Values"`
	}
	if err := a.api.Request("dht/get", key).Exec(ctx, &result); err != nil {
		return nil, err
	}
	if len(result.Values) == 0 {
		return nil, routing.ErrNotFound
	}
	// Return first value
	return result.Values[0], nil
}

// SearchValue implements routing.ValueStore.
func (a *IPFSDHTAdapter) SearchValue(ctx context.Context, key string, opts ...routing.Option) (<-chan []byte, error) {
	// IPFS API doesn't easily expose streaming search values via HTTP client in this way,
	// but GetValue (dht/get) returns the best value.
	// We'll just return a channel with the result of GetValue for compatibility.
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

// Bootstrap implements routing.Routing.
func (a *IPFSDHTAdapter) Bootstrap(ctx context.Context) error {
	// IPFS daemon handles its own bootstrap. We can just return nil.
	return nil
}
