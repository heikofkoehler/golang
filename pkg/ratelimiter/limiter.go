package ratelimiter

import (
	"context"
	"errors"
	"time"
)

var (
	ErrLimitExceeded = errors.New("rate limit exceeded")
	ErrManagerClosed = errors.New("rate limiter manager closed")
)

// Limiter defines the standard contract for concurrent rate limiters.
type Limiter interface {
	// Allow checks if a single request is permitted at the current moment.
	Allow() bool

	// AllowN checks if n requests are permitted at the provided timestamp.
	AllowN(now time.Time, n int) bool

	// Wait blocks until 1 token is available or context is cancelled.
	Wait(ctx context.Context) error

	// WaitN blocks until n tokens are available or context is cancelled.
	WaitN(ctx context.Context, n int) error
}
