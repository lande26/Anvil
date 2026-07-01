// Package anvil is the public SDK entry point for embedding Anvil into
// a Go application. It wires together the store, AOF, queue, worker pool,
// reaper, and HTTP API into a single Run() call.
//
// Usage:
//
//	a, err := anvil.New(anvil.Config{DataDir: "./data", Concurrency: 10})
//	a.RegisterHandler("send-email", myEmailHandler)
//	a.Run(ctx) // blocks until ctx cancelled
package anvil

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/lande26/anvil/api"
	intaof "github.com/lande26/anvil/internal/aof"
	"github.com/lande26/anvil/internal/reaper"
	"github.com/lande26/anvil/queue"
	"github.com/lande26/anvil/store"
	"github.com/lande26/anvil/worker"
)

// HandlerFunc is the signature for job processing functions.
// This is an alias of worker.HandlerFunc for convenience at the SDK surface.
type HandlerFunc = worker.HandlerFunc

// Config holds all Anvil runtime configuration.
type Config struct {
	// DataDir is the directory where the AOF file will be written.
	// Defaults to "./anvil-data".
	DataDir string

	// AOFSync controls fsync behaviour: "always", "everysec" (default), "no".
	AOFSync string

	// Concurrency is the number of concurrent workers. Defaults to 5.
	Concurrency int

	// HeartbeatInterval is how often workers update the heartbeat timestamp.
	// Defaults to 5s.
	HeartbeatInterval time.Duration

	// StalenessThreshold is how long a job can go without a heartbeat before
	// the Reaper considers its worker dead. Defaults to 60s.
	StalenessThreshold time.Duration

	// JobTimeout is the maximum duration a single job execution may take.
	// Defaults to 30s.
	JobTimeout time.Duration

	// ReaperInterval is how often the Reaper sweeps the processing queue.
	// Defaults to 10s.
	ReaperInterval time.Duration

	// HTTPAddr is the address the HTTP server listens on. Defaults to ":8080".
	HTTPAddr string
}

func (c *Config) withDefaults() {
	if c.DataDir == "" {
		c.DataDir = "./anvil-data"
	}
	if c.AOFSync == "" {
		c.AOFSync = "everysec"
	}
	if c.Concurrency <= 0 {
		c.Concurrency = 5
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 5 * time.Second
	}
	if c.StalenessThreshold == 0 {
		c.StalenessThreshold = 60 * time.Second
	}
	if c.JobTimeout == 0 {
		c.JobTimeout = 30 * time.Second
	}
	if c.ReaperInterval == 0 {
		c.ReaperInterval = 10 * time.Second
	}
	if c.HTTPAddr == "" {
		c.HTTPAddr = ":8080"
	}
}

// Anvil is the top-level coordinator. Create it with New(), register handlers
// with RegisterHandler(), and call Run() to start everything.
type Anvil struct {
	cfg      Config
	store    *store.Store
	aof      *intaof.AOF
	queue    *queue.Queue
	pool     *worker.Pool
	reaper   *reaper.Reaper
	registry worker.Registry
	logger   *slog.Logger
}

// New initialises Anvil: creates the data directory, opens the AOF,
// replays existing state, and wires all components together.
// It does NOT start any goroutines — call Run() for that.
func New(cfg Config) (*Anvil, error) {
	cfg.withDefaults()

	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("anvil: create data dir: %w", err)
	}

	aofPath := filepath.Join(cfg.DataDir, "anvil.aof")

	// 1. Open AOF (starts the write loop goroutine internally)
	syncPolicy := intaof.SyncPolicy(cfg.AOFSync)
	a, err := intaof.Open(aofPath, syncPolicy, 512)
	if err != nil {
		return nil, fmt.Errorf("anvil: open aof: %w", err)
	}

	// 2. Create store
	st := store.New()

	// 3. Create queue (replay wires the queue's Replay method as the AOF callback)
	q := queue.New(st, a, cfg.Concurrency*2)

	// 4. Replay AOF to restore state
	count, err := intaof.Replay(aofPath, q.Replay)
	if err != nil {
		return nil, fmt.Errorf("anvil: aof replay: %w", err)
	}
	slog.Info("anvil: state restored", "commands_replayed", count)

	registry := make(worker.Registry)

	pool := worker.NewPool(q, registry, cfg.Concurrency, cfg.JobTimeout, cfg.HeartbeatInterval)
	r := reaper.New(q, cfg.ReaperInterval, cfg.StalenessThreshold)

	return &Anvil{
		cfg:      cfg,
		store:    st,
		aof:      a,
		queue:    q,
		pool:     pool,
		reaper:   r,
		registry: registry,
		logger:   slog.With("component", "anvil"),
	}, nil
}

// RegisterHandler registers a handler function for a given job type.
// Must be called before Run().
func (a *Anvil) RegisterHandler(jobType string, fn worker.HandlerFunc) {
	a.registry[jobType] = fn
}

// Enqueue submits a new job. Can be called before or after Run().
func (a *Anvil) Enqueue(ctx context.Context, job *queue.Job) error {
	return a.queue.Enqueue(ctx, job)
}

// Run starts the worker pool, reaper, and HTTP server.
// It blocks until ctx is cancelled, then performs graceful shutdown:
//  1. HTTP server stops accepting new connections.
//  2. Worker pool drains in-flight jobs.
//  3. Reaper exits.
//  4. AOF writeLoop flushes and fsyncs regardless of policy.
func (a *Anvil) Run(ctx context.Context) error {
	// Start background components
	a.pool.Start(ctx)
	go a.reaper.Run(ctx)

	// HTTP server
	handler := api.NewHandler(a.queue)
	mux := api.NewRouter(handler)
	srv := &http.Server{
		Addr:    a.cfg.HTTPAddr,
		Handler: mux,
	}

	// Serve in background
	srvErr := make(chan error, 1)
	go func() {
		a.logger.Info("http server starting", "addr", a.cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			srvErr <- err
		}
	}()

	// Block until ctx cancelled or server error
	select {
	case <-ctx.Done():
	case err := <-srvErr:
		return err
	}

	a.logger.Info("shutting down gracefully")

	// 1. Stop HTTP server (30s deadline for in-flight requests)
	shutCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	srv.Shutdown(shutCtx)

	// 2. Workers drain — they exit when ctx is cancelled and their current
	//    job finishes (bounded by JobTimeout)
	a.pool.Wait()

	// 3. Close AOF — flushes channel and fsyncs regardless of policy
	a.aof.Close()

	a.logger.Info("shutdown complete")
	return nil
}
