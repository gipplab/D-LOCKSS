package syncmap

import (
	"sync"
	"testing"
)

func TestUpsertAndGet(t *testing.T) {
	m := New[string, int]()
	m.Upsert("a", 1)
	m.Upsert("b", 2)

	if v, ok := m.Get("a"); !ok || v != 1 {
		t.Errorf("Get(\"a\") = %v, %v; want 1, true", v, ok)
	}
	if v, ok := m.Get("b"); !ok || v != 2 {
		t.Errorf("Get(\"b\") = %v, %v; want 2, true", v, ok)
	}
}

func TestGet_MissingKeyReturnsZeroValueAndFalse(t *testing.T) {
	m := New[string, int]()
	m.Upsert("a", 1)

	v, ok := m.Get("missing")
	if ok {
		t.Error("Get(missing) ok=true, want false")
	}
	if v != 0 {
		t.Errorf("Get(missing) = %v, want zero value 0", v)
	}
}

func TestDeleteAndGet_RemovesEntry(t *testing.T) {
	m := New[string, int]()
	m.Upsert("a", 1)
	m.Upsert("b", 2)

	v, ok := m.DeleteAndGet("a")
	if !ok || v != 1 {
		t.Errorf("DeleteAndGet(\"a\") = %v, %v; want 1, true", v, ok)
	}

	if _, ok := m.Get("a"); ok {
		t.Error("Get(\"a\") after DeleteAndGet: ok=true, want false")
	}
	if v, ok := m.Get("b"); !ok || v != 2 {
		t.Errorf("Get(\"b\") after DeleteAndGet(\"a\") = %v, %v; want 2, true", v, ok)
	}
}

func TestSnapshot_IteratesAllEntries(t *testing.T) {
	m := New[string, int]()
	m.Upsert("a", 1)
	m.Upsert("b", 2)
	m.Upsert("c", 3)

	seen := make(map[string]int)
	for k, v := range m.Snapshot() {
		seen[k] = v
	}

	if len(seen) != 3 {
		t.Errorf("Snapshot: saw %d entries, want 3", len(seen))
	}
	for k, want := range map[string]int{"a": 1, "b": 2, "c": 3} {
		if got := seen[k]; got != want {
			t.Errorf("Snapshot: %q = %v, want %v", k, got, want)
		}
	}
}

func TestLen_ReturnsCorrectCount(t *testing.T) {
	m := New[string, int]()
	if m.Len() != 0 {
		t.Errorf("empty map Len() = %d, want 0", m.Len())
	}

	m.Upsert("a", 1)
	m.Upsert("b", 2)
	if m.Len() != 2 {
		t.Errorf("Len() = %d, want 2", m.Len())
	}

	m.DeleteAndGet("a")
	if m.Len() != 1 {
		t.Errorf("after DeleteAndGet Len() = %d, want 1", m.Len())
	}
}

func TestConcurrentAccess(t *testing.T) {
	m := New[int, int]()
	var wg sync.WaitGroup

	// Concurrent writers
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			m.Upsert(k, k*2)
		}(i)
	}

	// Concurrent readers
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			_, _ = m.Get(k)
		}(i)
	}

	wg.Wait()

	// Verify all writes are visible
	for i := 0; i < 100; i++ {
		if v, ok := m.Get(i); !ok || v != i*2 {
			t.Errorf("Get(%d) = %v, %v; want %d, true", i, v, ok, i*2)
		}
	}
}
