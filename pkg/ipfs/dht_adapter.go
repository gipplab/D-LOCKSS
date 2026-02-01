package ipfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"strings"
	"time"

	"github.com/ipfs/go-cid"
	ipfsapi "github.com/ipfs/go-ipfs-api"
	"github.com/libp2p/go-libp2p/core/peer"
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

// NewIPFSDHTAdapter creates a new DHT adapter that uses IPFS's DHT
func NewIPFSDHTAdapter(api *ipfsapi.Shell) *IPFSDHTAdapter {
	ctx, cancel := context.WithCancel(context.Background())
	adapter := &IPFSDHTAdapter{
		api:            api,
		retryAttempts:  3, // Default retry attempts
		retryDelay:     500 * time.Millisecond, // Default retry delay
		provideTimeout: 60 * time.Second, // Default timeout
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
			// Create a fresh context for the actual operation
			// This ensures each operation gets adequate time to complete,
			// regardless of how long it waited in the queue.
			// Use the configured timeout to ensure consistency.
			opCtx := req.ctx
			if deadline, ok := req.ctx.Deadline(); ok {
				remaining := time.Until(deadline)
				// If less than half the configured timeout remaining (likely waited in queue), give fresh timeout
				// This ensures operations get adequate time even if they waited in the queue
				if remaining < a.provideTimeout/2 {
					var cancel context.CancelFunc
					opCtx, cancel = context.WithTimeout(context.Background(), a.provideTimeout)
					defer cancel()
				}
			} else {
				// No deadline set, create one to prevent operations from hanging forever
				var cancel context.CancelFunc
				opCtx, cancel = context.WithTimeout(context.Background(), a.provideTimeout)
				defer cancel()
			}
			
			// Process the provide operation with the fresh context
			err := a.provideInternal(opCtx, req.key, req.broadcast)
			
			// Send result back to caller
			select {
			case req.resultCh <- err:
			case <-req.ctx.Done():
				// Caller cancelled, result channel might be closed
			}
			
			// Rate limit: wait before next provide so the DHT/host is not overwhelmed
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
// This operation uses the local gateway and is not expensive, so it runs directly
// without queuing (unlike Provide operations which are queued).
func (a *IPFSDHTAdapter) FindProvidersAsync(ctx context.Context, key cid.Cid, count int) <-chan peer.AddrInfo {
	ch := make(chan peer.AddrInfo, 10)
	
	go func() {
		defer close(ch)
		
		// Use IPFS routing findprovs API (dht/findprovs was deprecated)
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
		// If routing API fails, try old dht API as fallback
		req2 := a.api.Request("dht/findprovs", key.String())
		if count > 0 {
			req2.Option("num-providers", count)
		}
		if err2 := req2.Exec(ctx, &result); err2 != nil {
			return // Both failed, give up
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
	
	// Check for EOF errors (connection closed prematurely)
	if errors.Is(err, io.EOF) {
		return true
	}
	
	// Check for EOF in error string (go-ipfs-api might wrap it)
	errStr := strings.ToLower(err.Error())
	if strings.Contains(errStr, "eof") {
		return true
	}
	
	// Check for connection-related errors
	if strings.Contains(errStr, "connection") && 
	   (strings.Contains(errStr, "reset") || strings.Contains(errStr, "closed") || strings.Contains(errStr, "refused")) {
		return true
	}
	
	return false
}

// Provide announces that this node provides a CID using IPFS DHT
// This method queues the operation to ensure only one provide operation runs at a time.
// Provide operations are expensive and can overwhelm the IPFS node if run concurrently.
// Note: DHT queries (FindProvidersAsync, FindPeer) are NOT queued as they use the
// cheaper local gateway and can safely run concurrently.
func (a *IPFSDHTAdapter) Provide(ctx context.Context, key cid.Cid, broadcast bool) error {
	// Ensure worker is started
	a.startWorker()
	
	// Create request with result channel
	req := &provideRequest{
		ctx:       ctx,
		key:       key,
		broadcast: broadcast,
		resultCh:  make(chan error, 1),
	}
	
	// Enqueue the request
	select {
	case a.provideQueue <- req:
		// Successfully queued
	case <-ctx.Done():
		return ctx.Err()
	case <-a.workerCtx.Done():
		return fmt.Errorf("DHT adapter is closed")
	}
	
	// Wait for result
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
	// Use IPFS routing provide API (dht/provide was deprecated)
	var result struct {
		Extra string `json:"Extra"`
	}
	
	var lastErr error
	var lastErr2 error
	
	// Retry logic for transient errors with exponential backoff
	for attempt := 0; attempt <= a.retryAttempts; attempt++ {
		if attempt > 0 {
			// Exponential backoff: delay increases with each retry
			// delay = baseDelay * 2^(attempt-1)
			backoffMultiplier := 1 << uint(attempt-1) // 2^(attempt-1)
			backoffDelay := time.Duration(int64(a.retryDelay) * int64(backoffMultiplier))
			// Cap at 5 seconds to avoid excessive delays
			if backoffDelay > 5*time.Second {
				backoffDelay = 5 * time.Second
			}
			
			// Check if we have enough time left in context
			if deadline, ok := ctx.Deadline(); ok {
				timeRemaining := time.Until(deadline)
				// If we don't have at least 2x the backoff delay remaining, don't retry
				if timeRemaining < backoffDelay*2 {
					return fmt.Errorf("context deadline too soon for retry: %v remaining", timeRemaining)
				}
			}
			
			// Wait before retrying with exponential backoff
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
			// Success!
			return nil
		}
		
		lastErr = err
		
		// If routing API fails with a transient error, retry
		if a.isTransientError(err) && attempt < a.retryAttempts {
			continue
		}
		
		// If routing API fails, try old dht API as fallback
		req2 := a.api.Request("dht/provide", key.String())
		if broadcast {
			req2.Option("recursive", false)
		}
		err2 := req2.Exec(ctx, &result)
		if err2 == nil {
			// Old API succeeded
			return nil
		}
		
		lastErr2 = err2
		
		// If dht API also fails with a transient error, retry
		if a.isTransientError(err2) && attempt < a.retryAttempts {
			continue
		}
		
		// If both failed and error is not transient, return error immediately
		if !a.isTransientError(err) && !a.isTransientError(err2) {
			// Check if error is context deadline or cancelled
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return err // Don't retry, just fail
			}
			return fmt.Errorf("both routing/provide and dht/provide failed: routing=%v, dht=%v", err, err2)
		}
	}
	
	// All retries exhausted
	return fmt.Errorf("both routing/provide and dht/provide failed after %d retries: routing=%v, dht=%v", a.retryAttempts, lastErr, lastErr2)
}

// FindPeer finds a peer by ID using IPFS DHT
// This operation uses the local gateway and is not expensive, so it runs directly
// without queuing (unlike Provide operations which are queued).
func (a *IPFSDHTAdapter) FindPeer(ctx context.Context, id peer.ID) (peer.AddrInfo, error) {
	// Use IPFS routing findpeer API (dht/findpeer was deprecated)
	// The routing API returns a different format - it streams responses
	var result struct {
		Responses []struct {
			ID    string   `json:"ID"`
			Addrs []string `json:"Addrs"`
		} `json:"Responses"`
		Extra string `json:"Extra,omitempty"`
	}
	
	req := a.api.Request("routing/findpeer", id.String())
	if err := req.Exec(ctx, &result); err != nil {
		// If routing API fails, try the old dht/findpeer as fallback
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
