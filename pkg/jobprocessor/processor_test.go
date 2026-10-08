package jobprocessor

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestEngine_BasicJobExecution(t *testing.T) {
	engine := NewEngine(Config{NumWorkers: 3, QueueCapacity: 10})
	defer engine.Shutdown(context.Background(), true)

	job := NewImageJob("img-1", "path/to/cat.png", OpResize, 20*time.Millisecond)
	err := engine.Submit(job)
	if err != nil {
		t.Fatalf("failed to submit job: %v", err)
	}

	// Poll until completed
	deadline := time.After(1 * time.Second)
	for {
		status, err := engine.GetStatus("img-1")
		if err != nil {
			t.Fatalf("unexpected error getting status: %v", err)
		}
		if status == StatusCompleted {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("job did not complete in time, status: %s", status)
		case <-time.After(10 * time.Millisecond):
		}
	}

	snapshot, err := engine.GetJob("img-1")
	if err != nil {
		t.Fatalf("unexpected error getting job snapshot: %v", err)
	}
	if snapshot.Status != StatusCompleted {
		t.Errorf("expected COMPLETED, got %s", snapshot.Status)
	}
	if snapshot.Result == nil {
		t.Errorf("expected non-nil result")
	}
}

func TestEngine_LifecycleStateRaces_CancelQueued(t *testing.T) {
	// Worker count 1, saturated by a slow running job
	engine := NewEngine(Config{NumWorkers: 1, QueueCapacity: 10})
	defer engine.Shutdown(context.Background(), false)

	// Block the single worker
	slowJob := NewFuncJob("slow", func(ctx context.Context) (any, error) {
		select {
		case <-time.After(300 * time.Millisecond):
			return "done", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	engine.Submit(slowJob)

	// Give worker time to pick up the slow job
	time.Sleep(20 * time.Millisecond)

	var executed atomic.Bool
	queuedJob := NewFuncJob("queued-to-cancel", func(ctx context.Context) (any, error) {
		executed.Store(true)
		return "should not run", nil
	})

	err := engine.Submit(queuedJob)
	if err != nil {
		t.Fatalf("failed to submit: %v", err)
	}

	// Verify it's queued
	status, err := engine.GetStatus("queued-to-cancel")
	if err != nil || status != StatusQueued {
		t.Fatalf("expected QUEUED, got %s, err: %v", status, err)
	}

	// Cancel while QUEUED
	ok, err := engine.CancelJob("queued-to-cancel")
	if err != nil || !ok {
		t.Fatalf("cancel failed: ok=%v, err=%v", ok, err)
	}

	// Status should immediately be CANCELLED
	status, _ = engine.GetStatus("queued-to-cancel")
	if status != StatusCancelled {
		t.Errorf("expected CANCELLED, got %s", status)
	}

	// Wait for the slow job to finish so worker pulls the queued job
	time.Sleep(350 * time.Millisecond)

	// Verify the cancelled job was never executed by the worker
	if executed.Load() {
		t.Errorf("cancelled job should NEVER have executed!")
	}
}

func TestEngine_LifecycleStateRaces_CancelRunning(t *testing.T) {
	engine := NewEngine(Config{NumWorkers: 2, QueueCapacity: 10})
	defer engine.Shutdown(context.Background(), false)

	started := make(chan struct{})
	runningJob := NewFuncJob("running-cancel", func(ctx context.Context) (any, error) {
		close(started)
		select {
		case <-time.After(2 * time.Second):
			return "finished", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})

	engine.Submit(runningJob)
	<-started // Wait until job is actively executing

	// Verify running status
	status, _ := engine.GetStatus("running-cancel")
	if status != StatusRunning {
		t.Fatalf("expected RUNNING, got %s", status)
	}

	// Cancel running job
	ok, err := engine.CancelJob("running-cancel")
	if err != nil || !ok {
		t.Fatalf("failed to cancel running job: ok=%v, err=%v", ok, err)
	}

	time.Sleep(50 * time.Millisecond)
	status, _ = engine.GetStatus("running-cancel")
	if status != StatusCancelled {
		t.Errorf("expected CANCELLED, got %s", status)
	}

	// Subsequent cancel on already cancelled job returns false
	ok, err = engine.CancelJob("running-cancel")
	if ok {
		t.Errorf("expected cancel on cancelled job to return false")
	}
}

func TestEngine_FailureIsolation(t *testing.T) {
	// Concurrency Trap: A panic in a job MUST NOT kill the worker thread
	// nor affect adjacent jobs.
	engine := NewEngine(Config{NumWorkers: 1, QueueCapacity: 10})
	defer engine.Shutdown(context.Background(), true)

	panicJob := &ImageJob{
		id:          "panic-image",
		ImagePath:   "corrupt.raw",
		ShouldPanic: true,
	}

	if err := engine.Submit(panicJob); err != nil {
		t.Fatalf("failed to submit panic job: %v", err)
	}

	// Wait for panic job to be processed
	deadline := time.After(1 * time.Second)
	for {
		status, _ := engine.GetStatus("panic-image")
		if status == StatusFailed {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("panic job did not transition to FAILED")
		case <-time.After(10 * time.Millisecond):
		}
	}

	snapshot, _ := engine.GetJob("panic-image")
	if snapshot.Error == nil {
		t.Errorf("expected snapshot error describing panic, got nil")
	}

	// Now submit a healthy job to verify the worker is still ALIVE and processing
	healthyJob := NewImageJob("healthy-image", "normal.png", OpGrayscale, 10*time.Millisecond)
	if err := engine.Submit(healthyJob); err != nil {
		t.Fatalf("failed to submit healthy job: %v", err)
	}

	deadline = time.After(1 * time.Second)
	for {
		status, _ := engine.GetStatus("healthy-image")
		if status == StatusCompleted {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("worker did not process healthy job after previous panic! Worker thread may have crashed.")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestEngine_GracefulTeardown_Wait(t *testing.T) {
	engine := NewEngine(Config{NumWorkers: 2, QueueCapacity: 10})

	var count atomic.Int32
	for i := 0; i < 5; i++ {
		jobID := fmt.Sprintf("drain-%d", i)
		engine.Submit(NewFuncJob(jobID, func(ctx context.Context) (any, error) {
			time.Sleep(30 * time.Millisecond)
			count.Add(1)
			return "ok", nil
		}))
	}

	// Shutdown with wait=true
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := engine.Shutdown(ctx, true)
	if err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}

	// Submitting after shutdown must be rejected
	err = engine.Submit(NewFuncJob("after-shutdown", func(ctx context.Context) (any, error) {
		return nil, nil
	}))
	if err != ErrEngineClosed {
		t.Errorf("expected ErrEngineClosed, got %v", err)
	}

	if count.Load() != 5 {
		t.Errorf("expected all 5 queued jobs to finish, got %d", count.Load())
	}
}

func TestEngine_HighConcurrencyStress(t *testing.T) {
	// Concurrent submissions, cancellations, and status reads
	engine := NewEngine(Config{NumWorkers: 8, QueueCapacity: 200})
	defer engine.Shutdown(context.Background(), false)

	const totalJobs = 100
	var wg sync.WaitGroup

	for i := 0; i < totalJobs; i++ {
		wg.Add(1)
		id := fmt.Sprintf("stress-%d", i)
		go func(jobID string) {
			defer wg.Done()
			job := NewFuncJob(jobID, func(ctx context.Context) (any, error) {
				time.Sleep(5 * time.Millisecond)
				return "done", nil
			})

			_ = engine.Submit(job)

			// Randomly cancel some jobs concurrently
			if jobID == "stress-10" || jobID == "stress-25" || jobID == "stress-50" {
				_, _ = engine.CancelJob(jobID)
			}

			_, _ = engine.GetStatus(jobID)
			_, _ = engine.GetJob(jobID)
		}(id)
	}

	wg.Wait()
}
