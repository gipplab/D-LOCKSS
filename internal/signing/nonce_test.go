package signing

import (
	"testing"
	"time"

	"dlockss/internal/common"
	"dlockss/internal/testutil"
)

func TestNewNonce_ReturnsCorrectLength(t *testing.T) {
	for _, size := range []int{1, 16, 32, 64} {
		nonce, err := common.NewNonce(size)
		if err != nil {
			t.Fatalf("NewNonce(%d): %v", size, err)
		}
		if len(nonce) != size {
			t.Errorf("NewNonce(%d) len=%d, want %d", size, len(nonce), size)
		}
	}
}

func TestNewNonce_TwoCallsProduceDifferentValues(t *testing.T) {
	n1, err := common.NewNonce(32)
	if err != nil {
		t.Fatalf("NewNonce: %v", err)
	}
	n2, err := common.NewNonce(32)
	if err != nil {
		t.Fatalf("NewNonce: %v", err)
	}
	if string(n1) == string(n2) {
		t.Error("two NewNonce calls produced identical values")
	}
}

func TestNewNonce_ZeroSizeWorks(t *testing.T) {
	nonce, err := common.NewNonce(0)
	if err != nil {
		t.Fatalf("NewNonce(0): %v", err)
	}
	if nonce == nil {
		t.Error("NewNonce(0) returned nil")
	}
	if len(nonce) != 0 {
		t.Errorf("NewNonce(0) len=%d, want 0", len(nonce))
	}
}

func TestSeenBefore_RejectsSameNonceTwice(t *testing.T) {
	ns := newNonceStore()
	pid := testutil.MustPeerID(t, "sender-1")
	nonce := []byte("fixed-nonce-for-replay-test")
	ttl := 10 * time.Minute

	// First call: not seen before, records it
	if ns.seenBefore(pid, nonce, ttl) {
		t.Error("first seenBefore should return false (fresh nonce)")
	}
	// Second call: replay detected
	if !ns.seenBefore(pid, nonce, ttl) {
		t.Error("second seenBefore should return true (replay)")
	}
}

func TestSeenBefore_AllowsFreshNonce(t *testing.T) {
	ns := newNonceStore()
	pid := testutil.MustPeerID(t, "sender-2")
	nonce, err := common.NewNonce(16)
	if err != nil {
		t.Fatalf("NewNonce: %v", err)
	}
	ttl := 10 * time.Minute

	if ns.seenBefore(pid, nonce, ttl) {
		t.Error("fresh nonce should not be seen before")
	}
}

func TestSeenBefore_NonceCacheEviction(t *testing.T) {
	ns := newNonceStore()
	pid := testutil.MustPeerID(t, "sender-evict")
	ttl := -1 * time.Hour // expired immediately

	// Record one nonce that will be expired
	nonce := []byte("expired-nonce")
	if ns.seenBefore(pid, nonce, ttl) {
		t.Error("first seenBefore should return false")
	}

	// Trigger cleanup: cleanup runs every 256 calls
	const cleanupEveryN = 256
	for i := 0; i < cleanupEveryN; i++ {
		dummyNonce := make([]byte, 8)
		dummyNonce[0] = byte(i)
		ns.seenBefore(pid, dummyNonce, ttl)
	}

	// The expired nonce should have been evicted. Seeing it again should
	// return false (not seen before) since it was removed.
	if ns.seenBefore(pid, nonce, 10*time.Minute) {
		t.Error("evicted nonce should be treated as fresh (seenBefore=false)")
	}
}
