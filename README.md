# Production Go Concurrency Suite: Solutions to Anthropic Concurrency Problems

A production-grade, race-free Go implementation of the five core concurrency challenges frequently evaluated in top-tier systems interviews (specifically Anthropic's distributed and concurrent systems tracks).

Every module addresses the exact concurrency traps, failure modes, race conditions, and architectural criteria outlined in the problem specifications, and is verified with Go's race detector (`go test -race ./...`).

---

## Architecture & Concurrency Traps Overview

### 1. The Concurrent Web Crawler (`pkg/crawler`)
- **Core Problem:** Given a root URL and a high-latency fetch API, crawl unique internal links concurrently without infinite loops, deadlocks, or redundant network calls.
- **Trap 1: Termination Condition:** An empty task queue does **not** signify crawl completion, because in-flight workers may discover new links. Solved via a **Coordinator & In-Memory Queue** pattern that tracks `activeWorkers` and `len(queue)`. The crawl only terminates when `activeWorkers == 0 && len(queue) == 0`.
- **Trap 2: Visited Set Race Conditions:** Naive `if !visited[url]` checks allow concurrent workers to fetch duplicate pages. Solved with atomic check-and-insert (`VisitedSet.TryAdd(url)`) under a mutex.
- **Trap 3: Bounded Concurrency:** Fixed pool of $N$ workers (`cfg.MaxWorkers`) prevents unconstrained goroutine creation. Workers communicate results back over buffered channels to avoid deadlocks.

### 2. The Multi-Worker Job / Image Processor (`pkg/jobprocessor`)
- **Core Problem:** Asynchronous batch execution engine supporting task submissions (`Submit`), inspection (`GetStatus`, `GetJob`), and cooperative cancellation (`CancelJob`).
- **Trap 1: Lifecycle State Races:** Atomic transitions between `QUEUED`, `RUNNING`, `CANCELLED`, `COMPLETED`, and `FAILED`. When a worker pops a task off the queue, it validates that `status == QUEUED` under a lock; if cancelled while queued, it immediately aborts. If cancelled while running, the task's context is cancelled.
- **Trap 2: Failure Isolation:** Unhandled panics/crashes in jobs (e.g. corrupt images) are captured via `defer recover()`, marking the job `FAILED` without terminating the worker goroutine or stalling the pool.
- **Trap 3: Graceful Teardown:** `Shutdown(ctx, wait=true/false)` stops accepting submissions, drains queued tasks or cancels in-flight tasks, and waits cleanly for workers to exit without hanging.

### 3. Banking Ledger & Concurrent Account Transfers (`pkg/ledger`)
- **Core Problem:** Multi-account banking ledger supporting deposits, withdrawals, and multi-party transfers under high concurrent load.
- **Trap 1: Deadlock Prevention via Lock Ordering:** Circular wait ($A \to B$ vs $B \to A$) is eliminated by lexicographically sorting account IDs and acquiring locks in strict ascending order (`sort.Strings(accountIDs)`). Self-transfers are detected and rejected.
- **Trap 2: Balance Invariant Violations:** Fine-grained per-account mutexes prevent global bottlenecks while atomically verifying `balance >= amount` before debiting.
- **Trap 3: Atomic Rollback on Partial Failure:** In multi-leg transfers (e.g. $A \to B$ and $A \to C$), net deltas are pre-validated before committing. If any balance invariant is violated, all modifications are aborted and status is recorded as `ROLLED_BACK`.
- **Auditability:** Append-only immutable transaction log and `TotalSystemBalance()` for conservation of money verification.

### 4. Thread-Safe In-Memory Store with TTL (`pkg/ttlstore`)
- **Core Problem:** In-memory key-value database with TTL expiration, CRUD, and atomic conditional writes.
- **Trap 1: Read-Write Contention:** Keys are partitioned across 32 or 64 independent shards using 64-bit FNV-1a hashing. Each shard has an isolated `sync.RWMutex`, allowing concurrent reads and writes across different keys.
- **Trap 2: Active vs. Lazy Cleanup Contention:** 
  - *Lazy Cleanup:* Expired keys encountered on `Get` are deleted on the fly.
  - *Active Cleanup:* A background reaper periodically sweeps shards one-by-one, inspecting bounded batches (`defaultMaxSweepBatch`) to eliminate lock starvation for active operations.
- **Trap 3: Atomic Operations:** Compare-And-Swap (`CAS`) and Compare-And-Delete (`CAD`) evaluate and mutate values inside the shard write lock, preventing race windows between read and write.

### 5. Distributed / Concurrent Rate Limiter (`pkg/ratelimiter`)
- **Core Problem:** Token bucket and sliding window rate limiters to throttle API requests per tenant/user.
- **Trap 1: Clock Precision & Drift:** Refills are calculated analytically on-demand using Go's monotonic clock (`time.Now()` and `time.Sub()`). This prevents cumulative drift and leap-second jumps without requiring per-limiter background ticker goroutines.
- **Trap 2: Lock Contention & Burst Handling:** Token Bucket allows bursts up to capacity $C$ and refills at rate $R$. Sliding Window provides an exact sliding log to eliminate end-of-window burst exploitation.
- **Trap 3: Multi-Tenant Sharding & Idle Eviction:** A sharded `Manager` partitions tenants across multiple shards with lock-free atomic timestamp updates (`atomic.Int64`) on read paths, and safely evicts idle tenants to prevent memory leaks.

---

## Go Concurrency vs. Python Concurrency Deep-Dive

Interviewers at Anthropic frequently contrast Go's concurrency runtime with Python's internals:

| Aspect | Go Runtime | Python (CPython) |
|---|---|---|
| **Execution Model** | **M:N Scheduler:** Thousands of lightweight goroutines multiplexed onto $M$ OS threads across $N$ CPU cores. | **1:1 OS Threading** or single-threaded **Event Loop** (`asyncio`). |
| **GIL (Global Interpreter Lock)** | **None.** True parallel multi-core CPU execution out of the box. | **Global GIL:** Only one thread can execute Python bytecode at a time, restricting `threading` to I/O-bound tasks. CPU-bound concurrency requires `multiprocessing`. |
| **I/O vs CPU Workloads** | The Go runtime intercepts blocking syscalls via the network poller (kqueue/epoll) transparently behind synchronous-looking Go code. | `asyncio` requires cooperative coroutines (`async`/`await`). A single blocking CPU call or sync I/O call blocks the entire event loop. `multiprocessing` incurs serialization/IPC overhead. |
| **Synchronization Primitives** | Channels (`chan`), `select`, `sync.Mutex`, `sync.RWMutex`, `sync.WaitGroup`, `sync/atomic`, `context.Context`. | `threading.Lock`, `asyncio.Lock`, `multiprocessing.Queue`, `queue.Queue`. |
| **Memory Footprint** | Goroutines start with a **2 KB dynamically resizeable stack**, allowing millions of goroutines per process. | Python threads require substantial OS stack memory (typically 1–8 MB per thread), limiting concurrency to hundreds of threads. |

---

## Running the Verification Suite

Run all unit tests, stress benchmarks, and race detector checks:

```bash
# Execute all tests with the Go race detector enabled
go test -v -race ./...
```
