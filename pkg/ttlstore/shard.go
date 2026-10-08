package ttlstore

import (
	"errors"
	"reflect"
	"sync"
	"time"
)

var (
	ErrKeyNotFound = errors.New("key not found or expired")
	ErrStoreClosed = errors.New("store is closed")
)

// shard encapsulates a partition of key-value items guarded by its own RWMutex.
//
// Concurrency Trap Solved: Read-Write Contention.
// By partitioning keys across multiple shards, concurrent operations on different keys
// execute in parallel without acquiring a single bottleneck mutex.
type shard struct {
	mu    sync.RWMutex
	items map[string]*Item
}

func newShard() *shard {
	return &shard{
		items: make(map[string]*Item),
	}
}

// get retrieves a key with lazy expiration.
func (s *shard) get(key string, now time.Time) (any, bool) {
	s.mu.RLock()
	item, exists := s.items[key]
	if !exists {
		s.mu.RUnlock()
		return nil, false
	}

	if !item.IsExpired(now) {
		val := item.Value
		s.mu.RUnlock()
		return val, true
	}
	s.mu.RUnlock()

	// Lazy Cleanup: Key is expired; promote to write lock and remove it
	s.mu.Lock()
	defer s.mu.Unlock()
	if curItem, stillExists := s.items[key]; stillExists && curItem.IsExpired(now) {
		delete(s.items, key)
	}
	return nil, false
}

// set inserts or overwrites a key.
func (s *shard) set(key string, val any, expiresAt time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items[key] = &Item{
		Value:     val,
		ExpiresAt: expiresAt,
	}
}

// setNX sets key only if it does not exist or has expired.
func (s *shard) setNX(key string, val any, expiresAt time.Time, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	item, exists := s.items[key]
	if exists && !item.IsExpired(now) {
		return false
	}

	s.items[key] = &Item{
		Value:     val,
		ExpiresAt: expiresAt,
	}
	return true
}

// delete removes a key if present and not expired.
func (s *shard) delete(key string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	item, exists := s.items[key]
	if !exists {
		return false
	}
	delete(s.items, key)
	return !item.IsExpired(now)
}

// cas atomically compares current value with expected and updates if matching.
//
// Concurrency Trap Solved: Atomic Operations (CAS).
// Evaluation and mutation occur under the shard write lock, eliminating race windows.
func (s *shard) cas(key string, expected, newVal any, now time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	item, exists := s.items[key]
	if !exists || item.IsExpired(now) {
		if exists {
			delete(s.items, key)
		}
		return false, ErrKeyNotFound
	}

	if !reflect.DeepEqual(item.Value, expected) {
		return false, nil
	}

	item.Value = newVal
	return true, nil
}

// cad atomically compares current value with expected and deletes if matching.
func (s *shard) cad(key string, expected any, now time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	item, exists := s.items[key]
	if !exists || item.IsExpired(now) {
		if exists {
			delete(s.items, key)
		}
		return false, ErrKeyNotFound
	}

	if !reflect.DeepEqual(item.Value, expected) {
		return false, nil
	}

	delete(s.items, key)
	return true, nil
}

// ttl returns remaining time-to-live.
func (s *shard) ttl(key string, now time.Time) (time.Duration, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	item, exists := s.items[key]
	if !exists || item.IsExpired(now) {
		return 0, false
	}
	return item.RemainingTTL(now), true
}

// sweep prunes expired keys within this single shard.
//
// Concurrency Trap Solved: Active vs. Lazy Cleanup Contention.
// Instead of taking a global lock across the entire store, we lock only one shard
// at a time, check at most maxPerShard keys, and release the lock immediately.
func (s *shard) sweep(maxPerShard int, now time.Time) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	checked := 0
	deleted := 0

	for k, item := range s.items {
		if item.IsExpired(now) {
			delete(s.items, k)
			deleted++
		}
		checked++
		if maxPerShard > 0 && checked >= maxPerShard {
			break
		}
	}
	return deleted
}

// size returns count of currently stored items (including potentially expired ones).
func (s *shard) size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.items)
}
