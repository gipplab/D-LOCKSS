package syncmap

import "sync"

// Map is a generic thread-safe map. It replaces the many hand-rolled
// sync.RWMutex + map wrappers throughout the codebase.
type Map[K comparable, V any] struct {
	mu sync.RWMutex
	m  map[K]V
}

func New[K comparable, V any]() *Map[K, V] {
	return &Map[K, V]{m: make(map[K]V)}
}

func (s *Map[K, V]) Get(key K) (V, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.m[key]
	return v, ok
}

// Upsert sets the value and returns true if the key was new.
func (s *Map[K, V]) Upsert(key K, val V) (isNew bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, exists := s.m[key]
	s.m[key] = val
	return !exists
}

// SetIfAbsent sets the value only if the key does not exist.
// Returns true if the value was set (key was new).
func (s *Map[K, V]) SetIfAbsent(key K, val V) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.m[key]; exists {
		return false
	}
	s.m[key] = val
	return true
}

// DeleteAndGet atomically removes and returns the value.
func (s *Map[K, V]) DeleteAndGet(key K) (V, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[key]
	if ok {
		delete(s.m, key)
	}
	return v, ok
}

func (s *Map[K, V]) Has(key K) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.m[key]
	return ok
}

func (s *Map[K, V]) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.m)
}

// Snapshot returns a shallow copy of the map.
func (s *Map[K, V]) Snapshot() map[K]V {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cp := make(map[K]V, len(s.m))
	for k, v := range s.m {
		cp[k] = v
	}
	return cp
}

// Keys returns all keys as a slice.
func (s *Map[K, V]) Keys() []K {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]K, 0, len(s.m))
	for k := range s.m {
		keys = append(keys, k)
	}
	return keys
}

// ReplaceAll atomically replaces the entire map contents.
func (s *Map[K, V]) ReplaceAll(m map[K]V) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make(map[K]V, len(m))
	for k, v := range m {
		cp[k] = v
	}
	s.m = cp
}
