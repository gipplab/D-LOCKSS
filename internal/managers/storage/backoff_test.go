package storage

import (
	"testing"
	"time"
)

func TestNewBackoffTable(t *testing.T) {
	initial := 10 * time.Millisecond
	max := 5 * time.Minute
	multiplier := 2.0

	bt := newBackoffTable(initial, max, multiplier)
	if bt == nil {
		t.Fatal("newBackoffTable returned nil")
	}
	if bt.size() != 0 {
		t.Errorf("new table size = %d, want 0", bt.size())
	}
}

func TestRecordFailureIncrementsTable(t *testing.T) {
	bt := newBackoffTable(10*time.Millisecond, time.Minute, 2.0)

	if bt.size() != 0 {
		t.Errorf("initial size = %d, want 0", bt.size())
	}

	bt.recordFailure("key1")
	if bt.size() != 1 {
		t.Errorf("after 1 failure size = %d, want 1", bt.size())
	}

	bt.recordFailure("key1")
	if bt.size() != 1 {
		t.Errorf("after 2 failures same key size = %d, want 1", bt.size())
	}

	bt.recordFailure("key2")
	if bt.size() != 2 {
		t.Errorf("after adding key2 size = %d, want 2", bt.size())
	}
}

func TestShouldSkipReturnsFalseInitially(t *testing.T) {
	bt := newBackoffTable(10*time.Millisecond, time.Minute, 2.0)

	if bt.shouldSkip("nonexistent") {
		t.Error("shouldSkip for unknown key should be false, got true")
	}
}

func TestShouldSkipReturnsTrueForRecentlyFailedKeys(t *testing.T) {
	bt := newBackoffTable(50*time.Millisecond, time.Minute, 2.0)

	bt.recordFailure("key1")
	if !bt.shouldSkip("key1") {
		t.Error("shouldSkip for recently failed key should be true, got false")
	}
}

func TestClearResetsBackoff(t *testing.T) {
	bt := newBackoffTable(50*time.Millisecond, time.Minute, 2.0)

	bt.recordFailure("key1")
	if !bt.shouldSkip("key1") {
		t.Error("before clear: shouldSkip should be true")
	}

	bt.clear("key1")
	if bt.shouldSkip("key1") {
		t.Error("after clear: shouldSkip should be false")
	}
}

func TestSizeReturnsCorrectCount(t *testing.T) {
	bt := newBackoffTable(10*time.Millisecond, time.Minute, 2.0)

	keys := []string{"a", "b", "c"}
	for i, k := range keys {
		bt.recordFailure(k)
		if got := bt.size(); got != i+1 {
			t.Errorf("after adding %q size = %d, want %d", k, got, i+1)
		}
	}

	if got := bt.size(); got != 3 {
		t.Errorf("final size = %d, want 3", got)
	}
}

func TestBackoffDelayGrowsExponentially(t *testing.T) {
	initial := 1 * time.Millisecond
	max := 100 * time.Millisecond
	multiplier := 2.0
	bt := newBackoffTable(initial, max, multiplier)

	// After 1 failure: delay ~2ms. After 5 failures: delay ~32ms (2^5).
	// Sleep 5ms: 1 failure would have expired, 5 failures would not.
	bt.recordFailure("key1")
	time.Sleep(5 * time.Millisecond)
	if bt.shouldSkip("key1") {
		t.Error("after 1 failure and 5ms: shouldSkip should be false (short delay)")
	}

	bt.recordFailure("key2")
	bt.recordFailure("key2")
	bt.recordFailure("key2")
	bt.recordFailure("key2")
	bt.recordFailure("key2")
	time.Sleep(5 * time.Millisecond)
	if !bt.shouldSkip("key2") {
		t.Error("after 5 failures and 5ms: shouldSkip should be true (exponential delay)")
	}
}

func TestPurgeExpiredRemovesOldEntries(t *testing.T) {
	bt := newBackoffTable(1*time.Millisecond, time.Minute, 2.0)

	bt.recordFailure("key1")
	bt.recordFailure("key2")
	if bt.size() != 2 {
		t.Fatalf("before purge size = %d, want 2", bt.size())
	}

	time.Sleep(20 * time.Millisecond)
	bt.purgeExpired()

	if bt.size() != 0 {
		t.Errorf("after purgeExpired size = %d, want 0", bt.size())
	}
}
