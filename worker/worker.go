package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/lande26/anvil/queue"
)

// HandlerFunc is the signature job handlers must implement.
type HandlerFunc func(ctx context.Context, payload json.RawMessage) error

// Registry maps job types to their handlers.
type Registry map[string]HandlerFunc

// Worker is a single job consumer goroutine.
type Worker struct {
	id         int
	q          *queue.Queue
	registry   Registry
	jobTimeout time.Duration
	hbInterval time.Duration
	logger     *slog.Logger
}

func newWorker(id int, q *queue.Queue, registry Registry, jobTimeout, hbInterval time.Duration) *Worker {
	return &Worker{
		id:         id,
		q:          q,
		registry:   registry,
		jobTimeout: jobTimeout,
		hbInterval: hbInterval,
		logger:     slog.With("worker_id", id),
	}
}

// run is the main worker loop. It blocks on Dequeue and processes jobs
// until ctx is cancelled.
func (w *Worker) run(ctx context.Context) {
	w.logger.Info("worker started")
	for {
		job, err := w.q.Dequeue(ctx)
		if err != nil {
			// ctx cancelled — clean shutdown
			w.logger.Info("worker stopping")
			return
		}
		w.process(ctx, job)
	}
}

func (w *Worker) process(ctx context.Context, job *queue.Job) {
	w.logger.Info("processing job", "job_id", job.ID, "type", job.Type, "retry", job.RetryCount)

	// Start heartbeat goroutine — updates HeartbeatAt in memory so the
	// Reaper knows this worker is alive.
	hbCtx, cancelHB := context.WithCancel(ctx)
	defer cancelHB()
	go w.heartbeat(hbCtx, job.ID)

	// Execute with per-job timeout
	execCtx, cancelExec := context.WithTimeout(ctx, w.jobTimeout)
	defer cancelExec()

	start := time.Now()
	execErr := w.execute(execCtx, job)
	duration := time.Since(start)

	if execErr != nil {
		w.logger.Error("job failed", "job_id", job.ID, "error", execErr, "duration", duration)
		if err := w.q.Fail(context.Background(), job.ID, execErr.Error()); err != nil {
			w.logger.Error("failed to record failure", "job_id", job.ID, "error", err)
		}
	} else {
		w.logger.Info("job complete", "job_id", job.ID, "duration", duration)
		if err := w.q.Complete(context.Background(), job.ID); err != nil {
			w.logger.Error("failed to mark complete", "job_id", job.ID, "error", err)
		}
	}
}

func (w *Worker) execute(ctx context.Context, job *queue.Job) error {
	handler, ok := w.registry[job.Type]
	if !ok {
		return fmt.Errorf("no handler registered for job type %q", job.Type)
	}
	return handler(ctx, job.Payload)
}

func (w *Worker) heartbeat(ctx context.Context, jobID string) {
	ticker := time.NewTicker(w.hbInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.q.UpdateHeartbeat(jobID)
		}
	}
}
