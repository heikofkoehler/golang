package ttlstore

import (
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultShards          = 32
	defaultCleanupInterval = 100 * time.Millisecond
	defaultMaxSweepBatch   = 100
)

// Store is a sharded, thread-safe in-memory key-value database supporting TTL expiration,
// atomic CAS/CAD, and low-latency concurrent read/write operations.
type Store struct {
	shards          []*shard
	numShards       uint64
	cleanupInterval time.Duration
	stopReaper      chan struct{}
	reaperWg        sync.WaitGroup
	closed          atomic.Bool
}

// Option configures Store behavior.
type Option func(*Store)

// WithShards sets the number of concurrent shards (defaults to 32).
func WithShards(n int) Option {
	return func(s *Store) {
		if n > 0 {
			s.numShards = uint64(n)
		}
	}
}

// WithCleanupInterval sets the active background sweep interval.
func WithCleanupInterval(d time.Duration) Option {
	return func(s *Store) {
		if d > 0 {
			s.cleanupInterval = d
		}
	}
}

// New initializes a Store with the given options and launches the background reaper.
func New(opts ...Option) *Store {
	s := &Store{
		numShards:       defaultShards,
		cleanupInterval: defaultCleanupInterval,
		stopReaper:      make(chan struct{}),
	}

	for _, opt := range opts {
		opt(s)
	}

	s.shards = make([]*shard, s.numShards)
	for i := uint64(0); i < s.numShards; i++ {
		s.shards[i] = newShard()
	}

	// Start background active reaper
	s.reaperWg.Add(1)
	go s.activeSweepLoop()

	return s
}

func (s *Store) getShard(key string) *shard {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	idx := h.Sum64() % s.numShards
	return s.shards[idx]
}

// Set stores a key-value pair without expiration.
func (s *Store) Set(key string, val any) error {
	if s.closed.Load() {
		return ErrStoreClosed
	}
	s.getShard(key).set(key, val, time.Time{})
	return nil
}

// SetWithTTL stores a key-value pair with a time-to-live.
func (s *Store) SetWithTTL(key string, val any, ttl time.Duration) error {
	if s.closed.Load() {
		return ErrStoreClosed
	}
	expiresAt := time.Time{}
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}
	s.getShard(key).set(key, val, expiresAt)
	return nil
}

// SetNX stores a key-value pair only if it does not already exist (or has expired).
func (s *Store) SetNX(key string, val any, ttl time.Duration) (bool, error) {
	if s.closed.Load() {
		return false, ErrStoreClosed
	}
	expiresAt := time.Time{}
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}
	ok := s.getShard(key).setNX(key, val, expiresAt, time.Now())
	return ok, nil
}

// Get retrieves a key's value. Returns false if not found or expired.
func (s *Store) Get(key string) (any, bool) {
	if s.closed.Load() {
		return nil, false
	}
	return s.getShard(key).get(key, time.Now())
}

// Delete removes a key. Returns true if key was present and unexpired.
func (s *Store) Delete(key string) (bool, error) {
	if s.closed.Load() {
		return false, ErrStoreClosed
	}
	return s.getShard(key).delete(key, time.Now()), nil
}

// CAS executes an atomic Compare-And-Swap.
func (s *Store) CAS(key string, expected, newVal any) (bool, error) {
	if s.closed.Load() {
		return false, ErrStoreClosed
	}
	return s.getShard(key).cas(key, expected, newVal, time.Now())
}

// CAD executes an atomic Compare-And-Delete.
func (s *Store) CAD(key string, expected any) (bool, error) {
	if s.closed.Load() {
		return false, ErrStoreClosed
	}
	return s.getShard(key).cad(key, expected, time.Now())
}

// TTL returns remaining duration before expiration for key.
func (s *Store) TTL(key string) (time.Duration, bool) {
	if s.closed.Load() {
		return 0, false
	}
	return s.getShard(key).ttl(key, time.Now())
}

// Size returns total items currently stored across all shards.
func (s *Store) Size() int {
	total := 0
	for _, shard := range s.shards {
		total += shard.size()
	}
	return total
}

// Close stops the background reaper and frees resources.
func (s *Store) Close() error {
	if s.closed.Swap(true) {
		return nil // already closed
	}
	close(s.stopReaper)
	s.reaperWg.Wait()
	return nil
}

// activeSweepLoop periodically sweeps expired keys across shards without lock starvation.
func (s *Store) activeSweepLoop() {
	defer s.reaperWg.Done()

	ticker := time.NewTicker(s.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopReaper:
			return
		case <-ticker.C:
			s.sweepShards()
		}
	}
}

// sweepShards visits each shard sequentially, acquiring write locks only for brief batches.
func (s *Store) sweepShards() {
	now := time.Now()
	for _, sh := range s.shards {
		select {
		case <-s.stopReaper:
			return
		default:
			sh.sweep(defaultMaxSweepBatch, now)
		}
	}
}
