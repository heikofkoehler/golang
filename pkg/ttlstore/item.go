package ttlstore

import (
	"time"
)

// Item holds a stored value alongside its optional expiration time.
type Item struct {
	Value     any
	ExpiresAt time.Time // IsZero() indicates no expiration
}

// IsExpired checks whether the item has expired relative to the given timestamp.
func (i *Item) IsExpired(now time.Time) bool {
	if i.ExpiresAt.IsZero() {
		return false
	}
	return now.After(i.ExpiresAt)
}

// RemainingTTL returns time left until expiration relative to now.
// Returns -1 if no expiration is configured, and 0 if expired.
func (i *Item) RemainingTTL(now time.Time) time.Duration {
	if i.ExpiresAt.IsZero() {
		return -1
	}
	rem := i.ExpiresAt.Sub(now)
	if rem < 0 {
		return 0
	}
	return rem
}
