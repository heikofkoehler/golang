package jobprocessor

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Engine is an asynchronous batch job execution framework supporting parallel workers,
// atomic lifecycle transitions, failure isolation, and graceful teardown.
type Engine struct {
	mu           sync.RWMutex
	jobs         map[string]*JobRecord
	taskQueue    chan *JobRecord
	numWorkers   int
	workerWg     sync.WaitGroup
	closed       bool
	engineCtx    context.Context
	cancelEngine context.CancelFunc
}

// Config configures the job engine.
type Config struct {
	NumWorkers    int
	QueueCapacity int
}

// NewEngine creates and starts a multi-worker job execution engine.
func NewEngine(cfg Config) *Engine {
	if cfg.NumWorkers <= 0 {
		cfg.NumWorkers = 4
	}
	if cfg.QueueCapacity <= 0 {
		cfg.QueueCapacity = 128
	}

	ctx, cancel := context.WithCancel(context.Background())

	e := &Engine{
		jobs:         make(map[string]*JobRecord),
		taskQueue:    make(chan *JobRecord, cfg.QueueCapacity),
		numWorkers:   cfg.NumWorkers,
		engineCtx:    ctx,
		cancelEngine: cancel,
	}

	// Start worker pool
	for i := 0; i < e.numWorkers; i++ {
		e.workerWg.Add(1)
		go e.workerLoop(i)
	}

	return e
}

// Submit enqueues a job for asynchronous execution.
// Returns ErrEngineClosed if the engine is shutting down or ErrJobAlreadyExists if ID is duplicate.
func (e *Engine) Submit(job Job) error {
	if job == nil || job.ID() == "" {
		return fmt.Errorf("job and job ID must not be empty")
	}

	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return ErrEngineClosed
	}

	if _, exists := e.jobs[job.ID()]; exists {
		e.mu.Unlock()
		return ErrJobAlreadyExists
	}

	jobCtx, jobCancel := context.WithCancel(e.engineCtx)

	record := &JobRecord{
		job:       job,
		status:    StatusQueued,
		createdAt: time.Now(),
		ctx:       jobCtx,
		cancel:    jobCancel,
	}

	e.jobs[job.ID()] = record
	e.mu.Unlock()

	// Enqueue task for worker pool
	select {
	case e.taskQueue <- record:
		return nil
	case <-e.engineCtx.Done():
		return ErrEngineClosed
	}
}

// CancelJob attempts to cancel a job by its ID.
//
// Concurrency Trap Solved: Lifecycle State Races.
// - If QUEUED: atomically marks as CANCELLED; when worker pops it later, it skips execution.
// - If RUNNING: triggers context cancellation; worker aborts cooperative execution safely.
// - If already COMPLETED / FAILED / CANCELLED: returns false with no invalid state overwrite.
func (e *Engine) CancelJob(id string) (bool, error) {
	e.mu.RLock()
	record, exists := e.jobs[id]
	e.mu.RUnlock()

	if !exists {
		return false, ErrJobNotFound
	}

	record.mu.Lock()
	defer record.mu.Unlock()

	switch record.status {
	case StatusCompleted, StatusFailed, StatusCancelled:
		// Terminal state reached: cannot cancel
		return false, nil

	case StatusQueued:
		record.status = StatusCancelled
		record.err = ErrJobCancelled
		record.completedAt = time.Now()
		if record.cancel != nil {
			record.cancel()
		}
		return true, nil

	case StatusRunning:
		record.status = StatusCancelled
		record.err = ErrJobCancelled
		if record.cancel != nil {
			record.cancel()
		}
		return true, nil

	default:
		return false, ErrInvalidState
	}
}

// GetStatus returns the current lifecycle status of a job.
func (e *Engine) GetStatus(id string) (Status, error) {
	e.mu.RLock()
	record, exists := e.jobs[id]
	e.mu.RUnlock()

	if !exists {
		return "", ErrJobNotFound
	}

	record.mu.RLock()
	defer record.mu.RUnlock()
	return record.status, nil
}

// GetJob returns a complete snapshot of the job's state and execution metrics.
func (e *Engine) GetJob(id string) (JobSnapshot, error) {
	e.mu.RLock()
	record, exists := e.jobs[id]
	e.mu.RUnlock()

	if !exists {
		return JobSnapshot{}, ErrJobNotFound
	}

	return record.Snapshot(), nil
}

// workerLoop is executed by each worker goroutine in the pool.
func (e *Engine) workerLoop(workerID int) {
	defer e.workerWg.Done()

	for record := range e.taskQueue {
		// Atomic check before execution to resolve the pop-vs-cancel race
		shouldRun := false
		record.mu.Lock()
		if record.status == StatusQueued {
			record.status = StatusRunning
			record.startedAt = time.Now()
			shouldRun = true
		}
		record.mu.Unlock()

		if !shouldRun {
			// Job was cancelled while waiting in the queue
			continue
		}

		e.executeJobSafely(record)
	}
}

// executeJobSafely runs the job wrapped with panic recovery and context tracking.
//
// Concurrency Trap Solved: Failure Isolation.
// An unhandled panic or crash inside a task MUST NOT crash the worker goroutine.
// The panic is caught, the job is marked FAILED, and the worker survives for next tasks.
func (e *Engine) executeJobSafely(record *JobRecord) {
	defer func() {
		if r := recover(); r != nil {
			record.mu.Lock()
			record.status = StatusFailed
			record.err = fmt.Errorf("job crashed with panic: %v", r)
			record.completedAt = time.Now()
			record.mu.Unlock()
		}
	}()

	res, err := record.job.Execute(record.ctx)

	record.mu.Lock()
	defer record.mu.Unlock()

	// If cancelled during execution, keep CANCELLED status
	if record.status == StatusCancelled {
		record.completedAt = time.Now()
		return
	}

	record.completedAt = time.Now()
	if err != nil {
		if record.ctx.Err() != nil {
			record.status = StatusCancelled
			record.err = ErrJobCancelled
		} else {
			record.status = StatusFailed
			record.err = err
		}
	} else {
		record.status = StatusCompleted
		record.result = res
	}
}

// Shutdown initiates a graceful teardown of the engine.
//
// Concurrency Trap Solved: Graceful Teardown.
// - Stops accepting new tasks immediately.
// - If wait=true: drains the queue and allows active workers to finish within ctx.
// - If wait=false: cancels all in-flight and queued tasks immediately.
func (e *Engine) Shutdown(ctx context.Context, wait bool) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	e.mu.Unlock()

	if !wait {
		// Stop in-flight tasks via engine context
		e.cancelEngine()

		// Drain remaining queued jobs and mark them CANCELLED
		close(e.taskQueue)
		for record := range e.taskQueue {
			record.mu.Lock()
			if record.status == StatusQueued {
				record.status = StatusCancelled
				record.err = ErrJobCancelled
				record.completedAt = time.Now()
			}
			record.mu.Unlock()
		}
	} else {
		// Draining shutdown: close queue so workers exit once queue is empty
		close(e.taskQueue)
	}

	// Wait for workers with context timeout
	done := make(chan struct{})
	go func() {
		e.workerWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		e.cancelEngine() // Force cancel on timeout
		return ctx.Err()
	}
}
