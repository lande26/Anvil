package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/lande26/anvil/internal/aof"
	"github.com/lande26/anvil/store"
)

// Status represents the lifecycle state of a job.
type Status string

const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusComplete   Status = "complete"
	StatusFailed     Status = "failed"
	StatusDead       Status = "dead"
)

// Key constants for the three queues.
const (
	KeyPending    = "anvil:pending"
	KeyProcessing = "anvil:processing"
	KeyDead       = "anvil:dead"
)

// Job is the unit of work managed by Anvil.
type Job struct {
	ID             string          `json:"id"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Status         Status          `json:"status"`
	RetryCount     int             `json:"retry_count"`
	MaxRetries     int             `json:"max_retries"`
	LastError      string          `json:"last_error,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	HeartbeatAt    time.Time       `json:"heartbeat_at"`
}

// ErrDuplicateJob is returned by Enqueue when a job with the same
// idempotency key already exists.
var ErrDuplicateJob = fmt.Errorf("job with this idempotency key already exists")

// ErrJobNotFound is returned when a job ID does not exist in the store.
var ErrJobNotFound = fmt.Errorf("job not found")

// Queue is the core coordinator. It owns the store reference and the AOF
// writer. A single mutex serializes all state-changing operations so that
// in-memory changes and AOF log entries are always consistent.
//
// Workers block on the notify channel when the pending queue is empty.
// Enqueue signals the channel to wake them up.
type Queue struct {
	mu     sync.Mutex
	store  *store.Store
	aof    *aof.AOF
	notify chan struct{} // buffered signal: new job available
}

// New creates a new Queue backed by st, logging mutations to a.
// bufSize controls the depth of the worker notification channel.
func New(st *store.Store, a *aof.AOF, bufSize int) *Queue {
	return &Queue{
		store:  st,
		aof:    a,
		notify: make(chan struct{}, bufSize),
	}
}

// Enqueue adds a new job to the pending queue.
// If job.IdempotencyKey is non-empty, the call is a no-op (and returns
// ErrDuplicateJob) if a job with that key has already been submitted.
// The operation is atomic: the in-memory state change and the AOF entry
// are both made inside the same mutex critical section before release.
func (q *Queue) Enqueue(_ context.Context, job *Job) error {
	if job.ID == "" {
		job.ID = uuid.New().String()
	}
	job.Status = StatusPending
	job.CreatedAt = time.Now()
	job.HeartbeatAt = time.Now()

	q.mu.Lock()

	// Idempotency check
	if job.IdempotencyKey != "" {
		if _, exists := q.store.Strings.Get("idem:" + job.IdempotencyKey); exists {
			q.mu.Unlock()
			return ErrDuplicateJob
		}
		q.store.Strings.SetNX("idem:"+job.IdempotencyKey, job.ID)
	}

	// Persist metadata to the hash store
	q.store.Hashes.HSet(jobKey(job.ID), jobToFields(job))

	// Append to pending list
	q.store.Lists.RPush(KeyPending, job.ID)

	// Write AOF entry — sent to channel, does NOT block on disk
	payloadBytes, _ := json.Marshal(job)
	q.aof.Log(aof.Entry{Verb: "ENQUEUE", JobID: job.ID, Payload: string(payloadBytes)})

	q.mu.Unlock()

	// Signal waiting workers (non-blocking: if the channel is full, workers
	// are already awake and will pick up the job on their next iteration)
	select {
	case q.notify <- struct{}{}:
	default:
	}

	return nil
}

// Dequeue atomically moves the next job from pending to processing.
// It blocks until a job is available or ctx is cancelled.
func (q *Queue) Dequeue(ctx context.Context) (*Job, error) {
	for {
		q.mu.Lock()
		jobID, ok := q.store.Lists.LMove(KeyPending, KeyProcessing)
		if ok {
			// Update status in hash store
			q.store.Hashes.HSet(jobKey(jobID), map[string]string{
				"status":       string(StatusProcessing),
				"heartbeat_at": fmt.Sprintf("%d", time.Now().Unix()),
			})
			fields := q.store.Hashes.HGetAll(jobKey(jobID))
			q.aof.Log(aof.Entry{Verb: "DEQUEUE", JobID: jobID})
			q.mu.Unlock()

			job, err := fieldsToJob(jobID, fields)
			if err != nil {
				return nil, err
			}
			return job, nil
		}
		q.mu.Unlock()

		// Queue empty — wait for a signal or context cancellation
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-q.notify:
			// New job arrived, loop to try again
		}
	}
}

// Complete marks a job as successfully finished and removes it from processing.
func (q *Queue) Complete(_ context.Context, jobID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.store.Lists.LRem(KeyProcessing, jobID)
	q.store.Hashes.HSet(jobKey(jobID), map[string]string{
		"status": string(StatusComplete),
	})
	q.aof.Log(aof.Entry{Verb: "COMPLETE", JobID: jobID})
	return nil
}

// Fail records a failed execution attempt. If the job has retries remaining
// it is placed back in pending; otherwise it is moved to the dead queue.
func (q *Queue) Fail(_ context.Context, jobID, reason string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	fields := q.store.Hashes.HGetAll(jobKey(jobID))
	if fields == nil {
		return ErrJobNotFound
	}

	job, err := fieldsToJob(jobID, fields)
	if err != nil {
		return err
	}

	q.store.Lists.LRem(KeyProcessing, jobID)
	job.RetryCount++
	job.LastError = reason

	if job.RetryCount <= job.MaxRetries {
		job.Status = StatusPending
		q.store.Hashes.HSet(jobKey(jobID), jobToFields(job))
		q.store.Lists.RPush(KeyPending, jobID)
		q.aof.Log(aof.Entry{Verb: "REQUEUE", JobID: jobID, Payload: fmt.Sprintf("%d", job.RetryCount)})

		// Wake up a worker for the requeued job
		select {
		case q.notify <- struct{}{}:
		default:
		}
	} else {
		job.Status = StatusDead
		q.store.Hashes.HSet(jobKey(jobID), jobToFields(job))
		q.store.Lists.RPush(KeyDead, jobID)
		q.aof.Log(aof.Entry{Verb: "DEAD", JobID: jobID, Payload: reason})
	}

	return nil
}

// Requeue is called by the Reaper to move a stale processing job back
// to pending. It uses the same queue mutex as all other operations,
// so there is no separate story needed for reaper/queue consistency.
func (q *Queue) Requeue(_ context.Context, jobID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.store.Lists.LRem(KeyProcessing, jobID)
	q.store.Hashes.HSet(jobKey(jobID), map[string]string{
		"status": string(StatusPending),
	})
	q.store.Lists.RPush(KeyPending, jobID)
	q.aof.Log(aof.Entry{Verb: "REQUEUE", JobID: jobID})

	select {
	case q.notify <- struct{}{}:
	default:
	}
	return nil
}

// UpdateHeartbeat updates a job's heartbeat timestamp in memory.
// Called periodically by the worker goroutine. Uses the queue mutex.
func (q *Queue) UpdateHeartbeat(jobID string) {
	q.mu.Lock()
	q.store.Hashes.HSet(jobKey(jobID), map[string]string{
		"heartbeat_at": fmt.Sprintf("%d", time.Now().Unix()),
	})
	q.mu.Unlock()
}

// GetJob loads and returns a job's metadata by ID.
func (q *Queue) GetJob(_ context.Context, jobID string) (*Job, error) {
	q.mu.Lock()
	fields := q.store.Hashes.HGetAll(jobKey(jobID))
	q.mu.Unlock()

	if fields == nil {
		return nil, ErrJobNotFound
	}
	return fieldsToJob(jobID, fields)
}

// Stats returns the current depth of the three queues.
func (q *Queue) Stats() (pending, processing, dead int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.store.Lists.LLen(KeyPending),
		q.store.Lists.LLen(KeyProcessing),
		q.store.Lists.LLen(KeyDead)
}

// ProcessingJobs returns all job IDs currently in the processing queue.
// Used by the Reaper to scan for stale jobs.
func (q *Queue) ProcessingJobs() []string {
	q.mu.Lock()
	ids := q.store.Lists.LRange(KeyProcessing, 0, -1)
	q.mu.Unlock()
	return ids
}

// HeartbeatAt returns the last heartbeat time for a job.
// Used by the Reaper without loading the entire job struct.
func (q *Queue) HeartbeatAt(jobID string) (time.Time, bool) {
	q.mu.Lock()
	v, ok := q.store.Hashes.HGet(jobKey(jobID), "heartbeat_at")
	q.mu.Unlock()
	if !ok {
		return time.Time{}, false
	}
	var ts int64
	fmt.Sscanf(v, "%d", &ts)
	return time.Unix(ts, 0), true
}

// CompactionSnapshot returns a snapshot suitable for AOF compaction.
func (q *Queue) CompactionSnapshot() aof.CompactionSnapshot {
	q.mu.Lock()
	defer q.mu.Unlock()

	pending := q.store.Lists.LRange(KeyPending, 0, -1)
	processing := q.store.Lists.LRange(KeyProcessing, 0, -1)

	snap := aof.CompactionSnapshot{
		PendingJobs:    make(map[string]string, len(pending)),
		ProcessingJobs: make(map[string]string, len(processing)),
	}
	for _, id := range pending {
		fields := q.store.Hashes.HGetAll(jobKey(id))
		if p, ok := fields["payload"]; ok {
			snap.PendingJobs[id] = p
		}
	}
	for _, id := range processing {
		fields := q.store.Hashes.HGetAll(jobKey(id))
		if p, ok := fields["payload"]; ok {
			snap.ProcessingJobs[id] = p
		}
	}
	return snap
}

// --- AOF Replay ---

// Replay reconstructs in-memory state by replaying an AOF log.
// Must be called before the queue accepts any operations.
func (q *Queue) Replay(verb, jobID, payload string) error {
	switch verb {
	case "ENQUEUE":
		var job Job
		if err := json.Unmarshal([]byte(payload), &job); err != nil {
			return fmt.Errorf("replay ENQUEUE unmarshal: %w", err)
		}
		job.Status = StatusPending
		q.store.Hashes.HSet(jobKey(jobID), jobToFields(&job))
		q.store.Lists.RPush(KeyPending, jobID)
		if job.IdempotencyKey != "" {
			q.store.Strings.SetNX("idem:"+job.IdempotencyKey, jobID)
		}

	case "DEQUEUE":
		q.store.Lists.LMove(KeyPending, KeyProcessing)
		q.store.Hashes.HSet(jobKey(jobID), map[string]string{
			"status": string(StatusProcessing),
		})

	case "COMPLETE":
		q.store.Lists.LRem(KeyProcessing, jobID)
		q.store.Hashes.HSet(jobKey(jobID), map[string]string{
			"status": string(StatusComplete),
		})

	case "REQUEUE":
		q.store.Lists.LRem(KeyProcessing, jobID)
		q.store.Hashes.HSet(jobKey(jobID), map[string]string{
			"status": string(StatusPending),
		})
		q.store.Lists.RPush(KeyPending, jobID)

	case "FAIL":
		q.store.Hashes.HSet(jobKey(jobID), map[string]string{
			"status":     string(StatusFailed),
			"last_error": payload,
		})

	case "DEAD":
		q.store.Lists.LRem(KeyProcessing, jobID)
		q.store.Hashes.HSet(jobKey(jobID), map[string]string{
			"status": string(StatusDead),
		})
		q.store.Lists.RPush(KeyDead, jobID)
	}
	return nil
}

// --- helpers ---

func jobKey(id string) string { return "anvil:job:" + id }

func jobToFields(j *Job) map[string]string {
	payload, _ := json.Marshal(j.Payload)
	return map[string]string{
		"id":              j.ID,
		"type":            j.Type,
		"payload":         string(payload),
		"status":          string(j.Status),
		"retry_count":     fmt.Sprintf("%d", j.RetryCount),
		"max_retries":     fmt.Sprintf("%d", j.MaxRetries),
		"last_error":      j.LastError,
		"idempotency_key": j.IdempotencyKey,
		"created_at":      fmt.Sprintf("%d", j.CreatedAt.Unix()),
		"heartbeat_at":    fmt.Sprintf("%d", j.HeartbeatAt.Unix()),
	}
}

func fieldsToJob(id string, fields map[string]string) (*Job, error) {
	var retryCount, maxRetries int
	fmt.Sscanf(fields["retry_count"], "%d", &retryCount)
	fmt.Sscanf(fields["max_retries"], "%d", &maxRetries)
	var createdAt, heartbeatAt int64
	fmt.Sscanf(fields["created_at"], "%d", &createdAt)
	fmt.Sscanf(fields["heartbeat_at"], "%d", &heartbeatAt)

	var payload json.RawMessage
	if p := fields["payload"]; p != "" {
		payload = json.RawMessage(p)
	}

	return &Job{
		ID:             id,
		Type:           fields["type"],
		Payload:        payload,
		Status:         Status(fields["status"]),
		RetryCount:     retryCount,
		MaxRetries:     maxRetries,
		LastError:      fields["last_error"],
		IdempotencyKey: fields["idempotency_key"],
		CreatedAt:      time.Unix(createdAt, 0),
		HeartbeatAt:    time.Unix(heartbeatAt, 0),
	}, nil
}
