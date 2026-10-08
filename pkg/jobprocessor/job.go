package jobprocessor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrEngineClosed     = errors.New("job engine is shut down")
	ErrJobNotFound      = errors.New("job not found")
	ErrJobAlreadyExists = errors.New("job ID already exists")
	ErrJobCancelled     = errors.New("job was cancelled")
	ErrInvalidState     = errors.New("invalid job state transition")
)

// Status represents the lifecycle states of a Job.
type Status string

const (
	StatusQueued    Status = "QUEUED"
	StatusRunning   Status = "RUNNING"
	StatusCompleted Status = "COMPLETED"
	StatusFailed    Status = "FAILED"
	StatusCancelled Status = "CANCELLED"
)

// Job defines the executable workload.
type Job interface {
	ID() string
	Execute(ctx context.Context) (any, error)
}

// FuncJob is an adapter that enables normal functions to act as Jobs.
type FuncJob struct {
	id string
	fn func(ctx context.Context) (any, error)
}

// NewFuncJob creates a new Job from an ID and execution function.
func NewFuncJob(id string, fn func(ctx context.Context) (any, error)) *FuncJob {
	return &FuncJob{id: id, fn: fn}
}

func (j *FuncJob) ID() string {
	return j.id
}

func (j *FuncJob) Execute(ctx context.Context) (any, error) {
	return j.fn(ctx)
}

// ImageOperation defines the image transform type.
type ImageOperation string

const (
	OpResize    ImageOperation = "RESIZE"
	OpGrayscale ImageOperation = "GRAYSCALE"
	OpBlur      ImageOperation = "BLUR"
)

// ImageJob simulates an asynchronous batch image processing task.
type ImageJob struct {
	id          string
	ImagePath   string
	Operation   ImageOperation
	SimulatedMs time.Duration
	ShouldPanic bool
}

// NewImageJob creates an image processing job.
func NewImageJob(id, imagePath string, op ImageOperation, delay time.Duration) *ImageJob {
	return &ImageJob{
		id:          id,
		ImagePath:   imagePath,
		Operation:   op,
		SimulatedMs: delay,
	}
}

func (j *ImageJob) ID() string {
	return j.id
}

func (j *ImageJob) Execute(ctx context.Context) (any, error) {
	if j.ShouldPanic {
		panic(fmt.Sprintf("fatal segmentation fault processing image %s", j.ImagePath))
	}

	select {
	case <-time.After(j.SimulatedMs):
		return fmt.Sprintf("processed %s with op %s", j.ImagePath, j.Operation), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// JobRecord stores runtime execution metadata and state for a Job.
type JobRecord struct {
	mu          sync.RWMutex
	job         Job
	status      Status
	result      any
	err         error
	createdAt   time.Time
	startedAt   time.Time
	completedAt time.Time
	ctx         context.Context
	cancel      context.CancelFunc
}

// Snapshot returns a point-in-time copy of the job record details.
type JobSnapshot struct {
	ID          string
	Status      Status
	Result      any
	Error       error
	CreatedAt   time.Time
	StartedAt   time.Time
	CompletedAt time.Time
	Duration    time.Duration
}

func (r *JobRecord) Snapshot() JobSnapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var duration time.Duration
	if !r.startedAt.IsZero() {
		if !r.completedAt.IsZero() {
			duration = r.completedAt.Sub(r.startedAt)
		} else {
			duration = time.Since(r.startedAt)
		}
	}

	return JobSnapshot{
		ID:          r.job.ID(),
		Status:      r.status,
		Result:      r.result,
		Error:       r.err,
		CreatedAt:   r.createdAt,
		StartedAt:   r.startedAt,
		CompletedAt: r.completedAt,
		Duration:    duration,
	}
}
