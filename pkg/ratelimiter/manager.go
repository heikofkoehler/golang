package ratelimiter

import (
	"context"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"
)

type tenantEntry struct {
	limiter            Limiter
	lastAccessUnixNano atomic.Int64
}

type tenantShard struct {
	mu      sync.RWMutex
	entries map[string]*tenantEntry
}

// Manager orchestrates multi-tenant rate limiters across sharded partitions with idle eviction.
//
// Concurrency Traps Solved:
//  1. Sharded Partitioning: Tenant lookups and updates are partitioned across N shards to eliminate
//     global mutex contention under high traffic bursts across different tenants.
//  2. Memory Leak Prevention: Inactive tenants are safely evicted after idleTTL.
type Manager struct {
	shards     []*tenantShard
	numShards  uint64
	factory    func() Limiter
	idleTTL    time.Duration
	stopReaper chan struct{}
	reaperWg   sync.WaitGroup
	closed     atomic.Bool
}

// ManagerConfig configures the multi-tenant Manager.
type ManagerConfig struct {
	NumShards       int
	IdleTTL         time.Duration
	CleanupInterval time.Duration
	LimiterFactory  func() Limiter
}

// NewManager creates a sharded multi-tenant rate limiter manager.
func NewManager(cfg ManagerConfig) *Manager {
	if cfg.NumShards <= 0 {
		cfg.NumShards = 16
	}
	if cfg.IdleTTL <= 0 {
		cfg.IdleTTL = 10 * time.Minute
	}
	if cfg.CleanupInterval <= 0 {
		cfg.CleanupInterval = time.Minute
	}
	if cfg.LimiterFactory == nil {
		cfg.LimiterFactory = func() Limiter {
			return NewTokenBucket(100, 20) // Default: 100 burst, 20/sec
		}
	}

	m := &Manager{
		numShards:  uint64(cfg.NumShards),
		factory:    cfg.LimiterFactory,
		idleTTL:    cfg.IdleTTL,
		stopReaper: make(chan struct{}),
	}

	m.shards = make([]*tenantShard, m.numShards)
	for i := uint64(0); i < m.numShards; i++ {
		m.shards[i] = &tenantShard{
			entries: make(map[string]*tenantEntry),
		}
	}

	m.reaperWg.Add(1)
	go m.evictionLoop(cfg.CleanupInterval)

	return m
}

func (m *Manager) getShard(tenantID string) *tenantShard {
	h := fnv.New64a()
	_, _ = h.Write([]byte(tenantID))
	idx := h.Sum64() % m.numShards
	return m.shards[idx]
}

// GetLimiter retrieves or lazily instantiates the limiter for tenantID.
func (m *Manager) GetLimiter(tenantID string) (Limiter, error) {
	if m.closed.Load() {
		return nil, ErrManagerClosed
	}

	shard := m.getShard(tenantID)
	nowNano := time.Now().UnixNano()

	// Fast path: read lock with atomic timestamp store
	shard.mu.RLock()
	entry, exists := shard.entries[tenantID]
	if exists {
		entry.lastAccessUnixNano.Store(nowNano)
		lim := entry.limiter
		shard.mu.RUnlock()
		return lim, nil
	}
	shard.mu.RUnlock()

	// Slow path: write lock to instantiate
	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Double-check after acquiring write lock
	if entry, exists = shard.entries[tenantID]; exists {
		entry.lastAccessUnixNano.Store(nowNano)
		return entry.limiter, nil
	}

	newLim := m.factory()
	newEntry := &tenantEntry{
		limiter: newLim,
	}
	newEntry.lastAccessUnixNano.Store(nowNano)
	shard.entries[tenantID] = newEntry

	return newLim, nil
}

// Allow checks if a request is permitted for tenantID.
func (m *Manager) Allow(tenantID string) bool {
	lim, err := m.GetLimiter(tenantID)
	if err != nil {
		return false
	}
	return lim.Allow()
}

// AllowN checks if n requests are permitted for tenantID.
func (m *Manager) AllowN(tenantID string, n int) bool {
	lim, err := m.GetLimiter(tenantID)
	if err != nil {
		return false
	}
	return lim.AllowN(time.Now(), n)
}

// Wait blocks until tenantID has capacity or context is cancelled.
func (m *Manager) Wait(ctx context.Context, tenantID string) error {
	lim, err := m.GetLimiter(tenantID)
	if err != nil {
		return err
	}
	return lim.Wait(ctx)
}

// ActiveTenantCount returns total tenants currently tracked across all shards.
func (m *Manager) ActiveTenantCount() int {
	total := 0
	for _, sh := range m.shards {
		sh.mu.RLock()
		total += len(sh.entries)
		sh.mu.RUnlock()
	}
	return total
}

// Close gracefully stops the idle eviction routine.
func (m *Manager) Close() error {
	if m.closed.Swap(true) {
		return nil
	}
	close(m.stopReaper)
	m.reaperWg.Wait()
	return nil
}

func (m *Manager) evictionLoop(interval time.Duration) {
	defer m.reaperWg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopReaper:
			return
		case <-ticker.C:
			m.pruneIdleTenants()
		}
	}
}

func (m *Manager) pruneIdleTenants() {
	cutoffNano := time.Now().Add(-m.idleTTL).UnixNano()

	for _, sh := range m.shards {
		select {
		case <-m.stopReaper:
			return
		default:
			sh.mu.Lock()
			for tenantID, entry := range sh.entries {
				if entry.lastAccessUnixNano.Load() < cutoffNano {
					delete(sh.entries, tenantID)
				}
			}
			sh.mu.Unlock()
		}
	}
}
