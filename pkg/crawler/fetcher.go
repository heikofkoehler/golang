package crawler

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Fetcher defines the contract for fetching a web page's content and its links.
type Fetcher interface {
	Fetch(ctx context.Context, targetURL string) (body string, urls []string, err error)
}

// MockPage represents a mock web page for testing.
type MockPage struct {
	Body  string
	URLs  []string
	Err   error
	Delay time.Duration
}

// MockFetcher is a thread-safe mock implementation of Fetcher for testing and benchmarks.
type MockFetcher struct {
	mu          sync.Mutex
	pages       map[string]MockPage
	callCounts  map[string]int
	maxInFlight int
	curInFlight int
}

// NewMockFetcher creates a new MockFetcher.
func NewMockFetcher(pages map[string]MockPage) *MockFetcher {
	return &MockFetcher{
		pages:      pages,
		callCounts: make(map[string]int),
	}
}

// Fetch simulates fetching a web page.
func (f *MockFetcher) Fetch(ctx context.Context, targetURL string) (string, []string, error) {
	f.mu.Lock()
	f.curInFlight++
	if f.curInFlight > f.maxInFlight {
		f.maxInFlight = f.curInFlight
	}
	f.callCounts[targetURL]++
	page, exists := f.pages[targetURL]
	f.mu.Unlock()

	defer func() {
		f.mu.Lock()
		f.curInFlight--
		f.mu.Unlock()
	}()

	if page.Delay > 0 {
		select {
		case <-time.After(page.Delay):
		case <-ctx.Done():
			return "", nil, ctx.Err()
		}
	}

	if !exists {
		return "", nil, fmt.Errorf("404 not found: %s", targetURL)
	}

	if page.Err != nil {
		return "", nil, page.Err
	}

	return page.Body, page.URLs, nil
}

// CallCount returns the number of times targetURL was fetched.
func (f *MockFetcher) CallCount(targetURL string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.callCounts[targetURL]
}

// TotalCalls returns the total number of fetch invocations across all URLs.
func (f *MockFetcher) TotalCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	total := 0
	for _, c := range f.callCounts {
		total += c
	}
	return total
}

// MaxConcurrentFetches returns the peak number of simultaneous fetches observed.
func (f *MockFetcher) MaxConcurrentFetches() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.maxInFlight
}

// NormalizeURL cleans and resolves a discovered URL relative to the base URL.
// It strips URL fragments, converts scheme/host to lowercase, and resolves relative paths.
func NormalizeURL(base *url.URL, rawURL string) (string, error) {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return "", fmt.Errorf("empty url")
	}

	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("failed to parse url %q: %w", rawURL, err)
	}

	// Resolve against base URL if relative
	resolved := base.ResolveReference(parsed)
	resolved.Fragment = "" // Strip anchor/fragments

	// Normalize scheme and host to lower case
	resolved.Scheme = strings.ToLower(resolved.Scheme)
	resolved.Host = strings.ToLower(resolved.Host)

	// Remove trailing slash for path consistency (except for root path "/")
	if len(resolved.Path) > 1 && strings.HasSuffix(resolved.Path, "/") {
		resolved.Path = strings.TrimRight(resolved.Path, "/")
	}

	return resolved.String(), nil
}
