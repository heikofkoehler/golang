package crawler

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestCrawler_BasicAndCycleHandling(t *testing.T) {
	// A points to B and C
	// B points to A (cycle!) and D
	// C points to B
	// D points to C
	pages := map[string]MockPage{
		"http://example.com/": {
			Body: "root",
			URLs: []string{"http://example.com/b", "http://example.com/c"},
		},
		"http://example.com/b": {
			Body: "page b",
			URLs: []string{"http://example.com/", "http://example.com/d"},
		},
		"http://example.com/c": {
			Body: "page c",
			URLs: []string{"http://example.com/b"},
		},
		"http://example.com/d": {
			Body: "page d",
			URLs: []string{"http://example.com/c"},
		},
	}

	fetcher := NewMockFetcher(pages)
	c := New(fetcher, Config{
		MaxWorkers:   4,
		MaxDepth:     10,
		SameHostOnly: true,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := c.Crawl(ctx, "http://example.com/")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Pages) != 4 {
		t.Errorf("expected 4 pages, got %d", len(result.Pages))
	}

	// Verify every page was fetched exactly once (Visited Set race condition trap)
	for url := range pages {
		if fetcher.CallCount(url) != 1 {
			t.Errorf("url %s called %d times, expected 1", url, fetcher.CallCount(url))
		}
	}
}

func TestCrawler_TerminationWithLaggingWorkers(t *testing.T) {
	// Trap: Queue empties quickly, but in-flight worker takes 100ms and discovers new links.
	// Crawler must NOT exit prematurely!
	pages := map[string]MockPage{
		"http://test.com/": {
			Body:  "root",
			URLs:  []string{"http://test.com/slow"},
			Delay: 10 * time.Millisecond,
		},
		"http://test.com/slow": {
			Body:  "slow page",
			URLs:  []string{"http://test.com/delayed-child-1", "http://test.com/delayed-child-2"},
			Delay: 150 * time.Millisecond,
		},
		"http://test.com/delayed-child-1": {
			Body:  "child 1",
			URLs:  nil,
			Delay: 20 * time.Millisecond,
		},
		"http://test.com/delayed-child-2": {
			Body:  "child 2",
			URLs:  nil,
			Delay: 20 * time.Millisecond,
		},
	}

	fetcher := NewMockFetcher(pages)
	c := New(fetcher, Config{
		MaxWorkers:   4,
		MaxDepth:     5,
		SameHostOnly: true,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	result, err := c.Crawl(ctx, "http://test.com/")
	if err != nil {
		t.Fatalf("crawl failed: %v", err)
	}

	if len(result.Pages) != 4 {
		t.Fatalf("expected 4 pages to be discovered, got %d", len(result.Pages))
	}

	if fetcher.CallCount("http://test.com/delayed-child-1") != 1 {
		t.Errorf("delayed child 1 was not fetched")
	}
	if fetcher.CallCount("http://test.com/delayed-child-2") != 1 {
		t.Errorf("delayed child 2 was not fetched")
	}
}

func TestCrawler_BoundedConcurrency(t *testing.T) {
	// Generate 50 pages linked in parallel
	pages := make(map[string]MockPage)
	var rootLinks []string
	for i := 0; i < 50; i++ {
		url := fmt.Sprintf("http://bounded.com/page/%d", i)
		rootLinks = append(rootLinks, url)
		pages[url] = MockPage{
			Body:  fmt.Sprintf("content %d", i),
			Delay: 25 * time.Millisecond,
		}
	}
	pages["http://bounded.com/"] = MockPage{
		Body: "root",
		URLs: rootLinks,
	}

	fetcher := NewMockFetcher(pages)
	maxWorkers := 5
	c := New(fetcher, Config{
		MaxWorkers:   maxWorkers,
		MaxDepth:     2,
		SameHostOnly: true,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := c.Crawl(ctx, "http://bounded.com/")
	if err != nil {
		t.Fatalf("crawl failed: %v", err)
	}

	if len(result.Pages) != 51 {
		t.Errorf("expected 51 pages crawled, got %d", len(result.Pages))
	}

	if fetcher.MaxConcurrentFetches() > maxWorkers {
		t.Errorf("concurrency bound violated: max in flight was %d, limit was %d",
			fetcher.MaxConcurrentFetches(), maxWorkers)
	}
}

func TestCrawler_ContextCancellation(t *testing.T) {
	pages := map[string]MockPage{
		"http://slow.com/": {
			Body:  "root",
			URLs:  []string{"http://slow.com/hang"},
			Delay: 10 * time.Millisecond,
		},
		"http://slow.com/hang": {
			Body:  "hanging",
			Delay: 5 * time.Second,
		},
	}

	fetcher := NewMockFetcher(pages)
	c := New(fetcher, Config{
		MaxWorkers: 2,
		MaxDepth:   3,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := c.Crawl(ctx, "http://slow.com/")
	if err == nil {
		t.Errorf("expected context deadline error, got nil")
	}
}

func TestVisitedSet_ConcurrentSafety(t *testing.T) {
	set := NewVisitedSet()
	var duplicates atomic.Int64
	var successes atomic.Int64

	url := "http://race.com/target"
	concurrency := 100

	done := make(chan struct{})
	for i := 0; i < concurrency; i++ {
		go func() {
			if set.TryAdd(url) {
				successes.Add(1)
			} else {
				duplicates.Add(1)
			}
			done <- struct{}{}
		}()
	}

	for i := 0; i < concurrency; i++ {
		<-done
	}

	if successes.Load() != 1 {
		t.Fatalf("expected exactly 1 success, got %d", successes.Load())
	}
	if duplicates.Load() != int64(concurrency-1) {
		t.Fatalf("expected %d duplicates, got %d", concurrency-1, duplicates.Load())
	}
	if set.Size() != 1 {
		t.Fatalf("expected set size 1, got %d", set.Size())
	}
}
