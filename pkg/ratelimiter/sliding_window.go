package ratelimiter

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// SlidingWindow implements an exact sliding window log rate limiter.
//
// Concurrency Design:
// Eliminates the edge-burst vulnerability of fixed window counters by sliding
// a continuous time window and pruning expired request records atomically under lock.
type SlidingWindow struct {
	mu         sync.Mutex
	limit      int
	window     time.Duration
	timestamps []time.Time
}

// NewSlidingWindow creates a sliding window limiter with max requests per window duration.
func NewSlidingWindow(limit int, window time.Duration) *SlidingWindow {
	if limit <= 0 || window <= 0 {
		panic(fmt.Sprintf("limit and window must be strictly positive: limit=%d, window=%v", limit, window))
	}
	return &SlidingWindow{
		limit:      limit,
		window:     window,
		timestamps: make([]time.Time, 0, limit),
	}
}

// Allow reports whether a single request is allowed.
func (sw *SlidingWindow) Allow() bool {
	return sw.AllowN(time.Now(), 1)
}

// AllowN reports whether n requests are allowed at the provided timestamp.
func (sw *SlidingWindow) AllowN(now time.Time, n int) bool {
	if n <= 0 {
		return true
	}
	if n > sw.limit {
		return false
	}

	sw.mu.Lock()
	defer sw.mu.Unlock()

	sw.pruneExpiredLocked(now)

	if len(sw.timestamps)+n <= sw.limit {
		for i := 0; i < n; i++ {
			sw.timestamps = append(sw.timestamps, now)
		}
		return true
	}

	return false
}

// Wait blocks until 1 request is allowed or context is cancelled.
func (sw *SlidingWindow) Wait(ctx context.Context) error {
	return sw.WaitN(ctx, 1)
}

// WaitN blocks until n requests are allowed or context is cancelled.
func (sw *SlidingWindow) WaitN(ctx context.Context, n int) error {
	if n <= 0 {
		return nil
	}
	if n > sw.limit {
		return fmt.Errorf("requested %d exceeds sliding window limit %d", n, sw.limit)
	}

	for {
		sw.mu.Lock()
		now := time.Now()
		sw.pruneExpiredLocked(now)

		if len(sw.timestamps)+n <= sw.limit {
			for i := 0; i < n; i++ {
				sw.timestamps = append(sw.timestamps, now)
			}
			sw.mu.Unlock()
			return nil
		}

		// Calculate sleep duration until enough requests expire out of the window
		idxToFree := len(sw.timestamps) + n - sw.limit - 1
		earliestRelevant := sw.timestamps[idxToFree]
		waitDuration := earliestRelevant.Add(sw.window).Sub(now)
		sw.mu.Unlock()

		if waitDuration <= 0 {
			waitDuration = time.Millisecond
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(waitDuration):
		}
	}
}

// CurrentCount returns active request count in current window.
func (sw *SlidingWindow) CurrentCount() int {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	sw.pruneExpiredLocked(time.Now())
	return len(sw.timestamps)
}

func (sw *SlidingWindow) pruneExpiredLocked(now time.Time) {
	threshold := now.Add(-sw.window)
	startIdx := 0
	for startIdx < len(sw.timestamps) && sw.timestamps[startIdx].Before(threshold) {
		startIdx++
	}

	if startIdx > 0 {
		// Prune expired records in-place
		copy(sw.timestamps, sw.timestamps[startIdx:])
		sw.timestamps = sw.timestamps[:len(sw.timestamps)-startIdx]
	}
}
