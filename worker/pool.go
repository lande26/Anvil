package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/lande26/anvil/queue"
)

// Pool manages N concurrent workers.
type Pool struct {
	q           *queue.Queue
	registry    Registry
	concurrency int
	jobTimeout  time.Duration
	hbInterval  time.Duration
	wg          sync.WaitGroup
	logger      *slog.Logger
}

// NewPool creates a pool. Start() must be called to launch workers.
func NewPool(q *queue.Queue, registry Registry, concurrency int, jobTimeout, hbInterval time.Duration) *Pool {
	return &Pool{
		q:           q,
		registry:    registry,
		concurrency: concurrency,
		jobTimeout:  jobTimeout,
		hbInterval:  hbInterval,
		logger:      slog.With("component", "pool"),
	}
}

// Start launches all workers. When ctx is cancelled, workers finish their
// current job and exit. Call Wait() to block until all have exited.
func (p *Pool) Start(ctx context.Context) {
	p.logger.Info("starting worker pool", "concurrency", p.concurrency)
	for i := 0; i < p.concurrency; i++ {
		p.wg.Add(1)
		go func(id int) {
			defer p.wg.Done()
			w := newWorker(id, p.q, p.registry, p.jobTimeout, p.hbInterval)
			w.run(ctx)
		}(i)
	}
}

// Wait blocks until all workers have exited cleanly.
func (p *Pool) Wait() {
	p.wg.Wait()
	p.logger.Info("all workers stopped")
}
