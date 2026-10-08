package ratelimiter

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTokenBucket_BurstAndRefill(t *testing.T) {
	// Capacity = 5, Refill = 10 tokens/sec (1 token every 100ms)
	tb := NewTokenBucket(5, 10)

	// Consume entire burst
	for i := 0; i < 5; i++ {
		if !tb.Allow() {
			t.Fatalf("expected token %d to be allowed", i)
		}
	}

	// 6th request should fail
	if tb.Allow() {
		t.Fatalf("expected 6th request to be rejected")
	}

	// Wait 250ms -> should refill ~2.5 tokens (at least 2 tokens)
	time.Sleep(250 * time.Millisecond)

	if !tb.AllowN(time.Now(), 2) {
		t.Fatalf("expected 2 tokens to have refilled")
	}
}

func TestTokenBucket_Wait_ContextCancel(t *testing.T) {
	tb := NewTokenBucket(1, 1) // 1 token per second
	_ = tb.Allow()             // Deplete bucket

	// Context with very short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := tb.Wait(ctx)
	if err == nil {
		t.Errorf("expected context cancellation error, got nil")
	}
}

func TestSlidingWindow_EdgeBurstPrevention(t *testing.T) {
	// Limit = 5 requests per 200ms
	sw := NewSlidingWindow(5, 200*time.Millisecond)

	now := time.Now()
	for i := 0; i < 5; i++ {
		if !sw.AllowN(now, 1) {
			t.Fatalf("expected request %d to be allowed", i)
		}
	}

	// 6th request in same window must fail
	if sw.AllowN(now, 1) {
		t.Fatalf("expected 6th request to fail")
	}

	// Advance time by 250ms (beyond window)
	future := now.Add(250 * time.Millisecond)
	if !sw.AllowN(future, 1) {
		t.Fatalf("expected request to succeed in new window")
	}
}

func TestSlidingWindow_Wait(t *testing.T) {
	sw := NewSlidingWindow(2, 100*time.Millisecond)

	// Consume 2 tokens
	_ = sw.Allow()
	_ = sw.Allow()

	// Wait should block and succeed once window slides
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err := sw.Wait(ctx)
	if err != nil {
		t.Fatalf("unexpected wait error: %v", err)
	}
}

func TestManager_MultiTenantIsolation(t *testing.T) {
	mgr := NewManager(ManagerConfig{
		NumShards: 4,
		LimiterFactory: func() Limiter {
			return NewTokenBucket(2, 1) // 2 burst
		},
	})
	defer mgr.Close()

	// Exhaust Tenant A
	if !mgr.Allow("tenant-A") || !mgr.Allow("tenant-A") {
		t.Fatalf("expected tenant-A initial requests to pass")
	}
	if mgr.Allow("tenant-A") {
		t.Fatalf("expected tenant-A to be throttled")
	}

	// Tenant B must NOT be affected by Tenant A's exhaustion
	if !mgr.Allow("tenant-B") {
		t.Fatalf("tenant-B should be allowed independently")
	}
}

func TestManager_IdleTenantEviction(t *testing.T) {
	mgr := NewManager(ManagerConfig{
		NumShards:       2,
		IdleTTL:         50 * time.Millisecond,
		CleanupInterval: 25 * time.Millisecond,
		LimiterFactory: func() Limiter {
			return NewTokenBucket(10, 10)
		},
	})
	defer mgr.Close()

	_ = mgr.Allow("active-user")
	_ = mgr.Allow("idle-user")

	if mgr.ActiveTenantCount() != 2 {
		t.Fatalf("expected 2 active tenants, got %d", mgr.ActiveTenantCount())
	}

	// Keep "active-user" alive while letting "idle-user" expire
	for i := 0; i < 4; i++ {
		time.Sleep(20 * time.Millisecond)
		_ = mgr.Allow("active-user")
	}

	time.Sleep(50 * time.Millisecond)

	// Idle user should have been evicted
	count := mgr.ActiveTenantCount()
	if count != 1 {
		t.Fatalf("expected exactly 1 active tenant after idle eviction, got %d", count)
	}
}

func TestManager_HighConcurrencyStress_RaceDetector(t *testing.T) {
	mgr := NewManager(ManagerConfig{
		NumShards: 8,
		LimiterFactory: func() Limiter {
			return NewTokenBucket(50, 100)
		},
	})
	defer mgr.Close()

	const numTenants = 10
	const goroutines = 30
	const requestsPerRoutine = 50

	var allowedTotal atomic.Int64
	var throttledTotal atomic.Int64

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(gid int) {
			defer wg.Done()
			for r := 0; r < requestsPerRoutine; r++ {
				tenantID := fmt.Sprintf("tenant-%d", (gid+r)%numTenants)
				if mgr.Allow(tenantID) {
					allowedTotal.Add(1)
				} else {
					throttledTotal.Add(1)
				}
			}
		}(g)
	}

	wg.Wait()

	if allowedTotal.Load() == 0 {
		t.Errorf("expected some requests to be allowed")
	}
}
