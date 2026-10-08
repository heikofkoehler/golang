package ratelimiter

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"
)

// TokenBucket implements a thread-safe token bucket rate limiter.
//
// Concurrency Traps Solved:
//  1. Clock Precision & Drift: Uses Go's monotonic time reading (via time.Now() and time.Sub)
//     to compute elapsed fractions of seconds continuously, avoiding cumulative drift and OS jumps.
//  2. Minimal Lock Contention: Updates are computed purely analytically on-demand without
//     requiring a continuous ticking background goroutine per bucket.
type TokenBucket struct {
	mu         sync.Mutex
	capacity   float64
	refillRate float64 // tokens per second
	tokens     float64
	lastRefill time.Time
}

// NewTokenBucket creates a new token bucket with specified burst capacity and refill rate (tokens/sec).
func NewTokenBucket(capacity float64, refillRate float64) *TokenBucket {
	if capacity <= 0 || refillRate <= 0 {
		panic(fmt.Sprintf("capacity and refillRate must be strictly positive: cap=%f, rate=%f", capacity, refillRate))
	}
	return &TokenBucket{
		capacity:   capacity,
		refillRate: refillRate,
		tokens:     capacity, // Bucket starts full
		lastRefill: time.Now(),
	}
}

// Allow reports whether 1 token is immediately available.
func (tb *TokenBucket) Allow() bool {
	return tb.AllowN(time.Now(), 1)
}

// AllowN reports whether n tokens are available at the given timestamp.
func (tb *TokenBucket) AllowN(now time.Time, n int) bool {
	if n <= 0 {
		return true
	}

	tb.mu.Lock()
	defer tb.mu.Unlock()

	if float64(n) > tb.capacity {
		return false // request exceeds total capacity
	}

	tb.refillLocked(now)

	if tb.tokens >= float64(n) {
		tb.tokens -= float64(n)
		return true
	}

	return false
}

// Wait blocks until 1 token is available or context is cancelled.
func (tb *TokenBucket) Wait(ctx context.Context) error {
	return tb.WaitN(ctx, 1)
}

// WaitN blocks until n tokens are available or context is cancelled.
func (tb *TokenBucket) WaitN(ctx context.Context, n int) error {
	if n <= 0 {
		return nil
	}
	if float64(n) > tb.capacity {
		return fmt.Errorf("requested %d tokens exceeds capacity of %d", n, int(tb.capacity))
	}

	for {
		tb.mu.Lock()
		now := time.Now()
		tb.refillLocked(now)

		if tb.tokens >= float64(n) {
			tb.tokens -= float64(n)
			tb.mu.Unlock()
			return nil
		}

		missing := float64(n) - tb.tokens
		waitDuration := time.Duration(math.Ceil((missing / tb.refillRate) * float64(time.Second)))
		tb.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(waitDuration):
		}
	}
}

// AvailableTokens returns current available tokens.
func (tb *TokenBucket) AvailableTokens() float64 {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.refillLocked(time.Now())
	return tb.tokens
}

// refillLocked updates available tokens according to elapsed monotonic time.
func (tb *TokenBucket) refillLocked(now time.Time) {
	elapsed := now.Sub(tb.lastRefill)
	if elapsed <= 0 {
		return
	}

	tokensToAdd := elapsed.Seconds() * tb.refillRate
	tb.tokens = math.Min(tb.capacity, tb.tokens+tokensToAdd)
	tb.lastRefill = now
}
