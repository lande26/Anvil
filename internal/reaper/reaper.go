// Package reaper scans the processing queue for jobs whose workers have
// stopped sending heartbeats and requeues them back to pending.
// It uses queue.Requeue() — the same exported method as all other queue ops —
// so it is serialized through the same queue mutex with zero special casing.
package reaper

import (
	"context"
	"log/slog"
	"time"

	"github.com/lande26/anvil/queue"
)

// Reaper is a background goroutine that rescues stale jobs.
type Reaper struct {
	q                  *queue.Queue
	interval           time.Duration
	stalenessThreshold time.Duration
	logger             *slog.Logger
}

// New creates a Reaper. Call Run() to start the sweep loop.
func New(q *queue.Queue, interval, stalenessThreshold time.Duration) *Reaper {
	return &Reaper{
		q:                  q,
		interval:           interval,
		stalenessThreshold: stalenessThreshold,
		logger:             slog.With("component", "reaper"),
	}
}

// Run starts the sweep loop. It blocks until ctx is cancelled.
func (r *Reaper) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	r.logger.Info("reaper started",
		"interval", r.interval,
		"staleness_threshold", r.stalenessThreshold,
	)
	for {
		select {
		case <-ctx.Done():
			r.logger.Info("reaper stopping")
			return
		case <-ticker.C:
			r.sweep(ctx)
		}
	}
}

func (r *Reaper) sweep(ctx context.Context) {
	ids := r.q.ProcessingJobs()
	rescued := 0
	for _, jobID := range ids {
		hb, ok := r.q.HeartbeatAt(jobID)
		if !ok {
			continue
		}
		if time.Since(hb) > r.stalenessThreshold {
			r.logger.Warn("rescuing stale job", "job_id", jobID, "last_heartbeat", hb)
			if err := r.q.Requeue(ctx, jobID); err != nil {
				r.logger.Error("requeue failed", "job_id", jobID, "error", err)
				continue
			}
			rescued++
		}
	}
	if rescued > 0 {
		r.logger.Info("sweep complete", "checked", len(ids), "rescued", rescued)
	}
}
