package crawler

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"time"
)

// VisitedSet provides thread-safe check-and-add operations.
//
// Concurrency Trap Solved: Visited Set Race Conditions.
// Naive check-then-set (if !visited[u] { visited[u] = true; crawl(u) }) allows two workers
// to see false concurrently and fetch the same URL twice.
// TryAdd combines lookup and insertion atomically under a single lock.
type VisitedSet struct {
	mu   sync.RWMutex
	seen map[string]struct{}
}

// NewVisitedSet initializes an empty VisitedSet.
func NewVisitedSet() *VisitedSet {
	return &VisitedSet{seen: make(map[string]struct{})}
}

// TryAdd atomically checks if a URL is present. If absent, it records it and returns true.
// If already present, it returns false.
func (v *VisitedSet) TryAdd(u string) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	if _, exists := v.seen[u]; exists {
		return false
	}
	v.seen[u] = struct{}{}
	return true
}

// Has checks if a URL has already been recorded.
func (v *VisitedSet) Has(u string) bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	_, exists := v.seen[u]
	return exists
}

// Size returns the count of visited URLs.
func (v *VisitedSet) Size() int {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return len(v.seen)
}

// Elements returns a slice copy of all visited URLs.
func (v *VisitedSet) Elements() []string {
	v.mu.RLock()
	defer v.mu.RUnlock()
	urls := make([]string, 0, len(v.seen))
	for u := range v.seen {
		urls = append(urls, u)
	}
	return urls
}

// Config specifies the runtime settings for the crawler.
type Config struct {
	// MaxWorkers bounds the maximum concurrent fetch operations (bounded concurrency).
	MaxWorkers int
	// MaxDepth limits the recursion depth from the root URL (0 = root only).
	MaxDepth int
	// SameHostOnly enforces that only URLs with the same hostname as the root are crawled.
	SameHostOnly bool
	// Delay is an optional throttle between requests to prevent overwhelming the target.
	Delay time.Duration
}

// DefaultConfig returns sensible defaults for the crawler.
func DefaultConfig() Config {
	return Config{
		MaxWorkers:   8,
		MaxDepth:     3,
		SameHostOnly: true,
		Delay:        0,
	}
}

// PageResult stores the crawl data discovered for a single URL.
type PageResult struct {
	URL        string
	Depth      int
	Body       string
	Links      []string
	Error      error
	Discovered time.Time
}

// CrawlResult encapsulates the complete outcome of a crawl job.
type CrawlResult struct {
	RootURL  string
	Visited  []string
	Pages    map[string]*PageResult
	Errors   map[string]error
	Duration time.Duration
}

// Crawler executes concurrent web crawls adhering to concurrency safety constraints.
type Crawler struct {
	fetcher Fetcher
	cfg     Config
}

// New creates a new Crawler instance with the specified Fetcher and Config.
func New(fetcher Fetcher, cfg Config) *Crawler {
	if cfg.MaxWorkers <= 0 {
		cfg.MaxWorkers = 1
	}
	return &Crawler{
		fetcher: fetcher,
		cfg:     cfg,
	}
}

type queueTask struct {
	url   string
	depth int
}

type fetchOutcome struct {
	task queueTask
	body string
	urls []string
	err  error
}

// Crawl initiates a concurrent crawl starting from startURL.
//
// Concurrency Architecture:
//  1. Bounded Concurrency: We spin up exactly cfg.MaxWorkers worker goroutines.
//  2. Visited Set Race Condition: Atomically guarded by VisitedSet.TryAdd.
//  3. Termination Condition: An empty queue alone does NOT signify completion.
//     We maintain an activeWorker counter in the coordinator. The crawl only terminates
//     when (len(queue) == 0 && activeWorkers == 0).
//  4. Deadlock Prevention: Instead of workers sending directly to an unbuffered work channel
//     (which causes deadlock when all workers block on writes), workers send results back
//     to the coordinator via a result channel, and the coordinator manages an unbounded
//     in-memory slice queue, feeding workers as they become available.
func (c *Crawler) Crawl(ctx context.Context, startURL string) (*CrawlResult, error) {
	startTime := time.Now()

	baseURL, err := url.Parse(startURL)
	if err != nil {
		return nil, fmt.Errorf("invalid root URL %q: %w", startURL, err)
	}

	normStartURL, err := NormalizeURL(baseURL, startURL)
	if err != nil {
		return nil, fmt.Errorf("failed to normalize root URL: %w", err)
	}

	visited := NewVisitedSet()
	if !visited.TryAdd(normStartURL) {
		return nil, fmt.Errorf("failed to seed root URL into visited set")
	}

	result := &CrawlResult{
		RootURL: normStartURL,
		Pages:   make(map[string]*PageResult),
		Errors:  make(map[string]error),
	}

	workCh := make(chan queueTask)
	resultCh := make(chan fetchOutcome, c.cfg.MaxWorkers)

	// Launch fixed worker pool
	var workerWg sync.WaitGroup
	for i := 0; i < c.cfg.MaxWorkers; i++ {
		workerWg.Add(1)
		go func() {
			defer workerWg.Done()
			for task := range workCh {
				if c.cfg.Delay > 0 {
					select {
					case <-time.After(c.cfg.Delay):
					case <-ctx.Done():
						return
					}
				}

				body, links, fetchErr := c.fetcher.Fetch(ctx, task.url)
				select {
				case resultCh <- fetchOutcome{
					task: task,
					body: body,
					urls: links,
					err:  fetchErr,
				}:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	// Coordinator manages the dynamic queue and tracks active workers.
	queue := []queueTask{{url: normStartURL, depth: 0}}
	activeWorkers := 0

	for {
		// When queue has items, we can either dispatch to an idle worker or collect a result.
		// When queue is empty, we must wait for in-flight fetches to produce more work.
		if len(queue) > 0 {
			nextTask := queue[0]
			select {
			case <-ctx.Done():
				// Context cancelled / timeout: abort cleanly
				close(workCh)
				workerWg.Wait()
				result.Duration = time.Since(startTime)
				result.Visited = visited.Elements()
				return result, ctx.Err()

			case workCh <- nextTask:
				// Successfully dispatched a task to a worker
				queue = queue[1:]
				activeWorkers++

			case outcome := <-resultCh:
				activeWorkers--
				c.processOutcome(baseURL, outcome, visited, &queue, result)
			}
		} else {
			// Queue is empty. If no workers are running, we are 100% finished.
			// If workers are still running, they might produce new URLs.
			if activeWorkers == 0 {
				// Crawl is fully terminated!
				break
			}

			select {
			case <-ctx.Done():
				close(workCh)
				workerWg.Wait()
				result.Duration = time.Since(startTime)
				result.Visited = visited.Elements()
				return result, ctx.Err()

			case outcome := <-resultCh:
				activeWorkers--
				c.processOutcome(baseURL, outcome, visited, &queue, result)
			}
		}
	}

	// Shutdown workers gracefully
	close(workCh)
	workerWg.Wait()

	result.Duration = time.Since(startTime)
	result.Visited = visited.Elements()
	return result, nil
}

func (c *Crawler) processOutcome(
	baseURL *url.URL,
	outcome fetchOutcome,
	visited *VisitedSet,
	queue *[]queueTask,
	result *CrawlResult,
) {
	u := outcome.task.url
	depth := outcome.task.depth

	if outcome.err != nil {
		result.Errors[u] = outcome.err
		result.Pages[u] = &PageResult{
			URL:        u,
			Depth:      depth,
			Error:      outcome.err,
			Discovered: time.Now(),
		}
		return
	}

	var validLinks []string
	for _, rawLink := range outcome.urls {
		normLink, err := NormalizeURL(baseURL, rawLink)
		if err != nil {
			continue
		}

		parsedLink, err := url.Parse(normLink)
		if err != nil {
			continue
		}

		// Enforce domain boundary if SameHostOnly is enabled
		if c.cfg.SameHostOnly && parsedLink.Host != baseURL.Host {
			continue
		}

		validLinks = append(validLinks, normLink)

		// Check depth constraint before enqueueing
		if depth+1 <= c.cfg.MaxDepth {
			// VisitedSet.TryAdd guarantees atomic check-and-insert
			if visited.TryAdd(normLink) {
				*queue = append(*queue, queueTask{
					url:   normLink,
					depth: depth + 1,
				})
			}
		}
	}

	result.Pages[u] = &PageResult{
		URL:        u,
		Depth:      depth,
		Body:       outcome.body,
		Links:      validLinks,
		Discovered: time.Now(),
	}
}
