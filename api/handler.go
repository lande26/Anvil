package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/google/uuid"
	"github.com/lande26/anvil/queue"
)

// Handler holds the dependencies needed to serve HTTP requests.
type Handler struct {
	q *queue.Queue
}

// NewHandler creates a new Handler.
func NewHandler(q *queue.Queue) *Handler {
	return &Handler{q: q}
}

type createJobRequest struct {
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	MaxRetries     int             `json:"max_retries"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
}

// CreateJob handles POST /jobs
func (h *Handler) CreateJob(w http.ResponseWriter, r *http.Request) {
	var req createJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Type == "" {
		respondError(w, http.StatusBadRequest, "type is required")
		return
	}
	if req.MaxRetries < 0 {
		req.MaxRetries = 3
	}

	job := &queue.Job{
		ID:             uuid.New().String(),
		Type:           req.Type,
		Payload:        req.Payload,
		MaxRetries:     req.MaxRetries,
		IdempotencyKey: req.IdempotencyKey,
	}

	if err := h.q.Enqueue(r.Context(), job); err != nil {
		if errors.Is(err, queue.ErrDuplicateJob) {
			respondJSON(w, http.StatusConflict, map[string]string{
				"error": "duplicate job",
			})
			return
		}
		respondError(w, http.StatusInternalServerError, "failed to enqueue job")
		return
	}

	respondJSON(w, http.StatusCreated, map[string]string{
		"id":     job.ID,
		"status": string(queue.StatusPending),
	})
}

// GetJob handles GET /jobs/{id}
func (h *Handler) GetJob(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")
	if jobID == "" {
		respondError(w, http.StatusBadRequest, "job ID required")
		return
	}
	job, err := h.q.GetJob(r.Context(), jobID)
	if err != nil {
		if errors.Is(err, queue.ErrJobNotFound) {
			respondError(w, http.StatusNotFound, "job not found")
			return
		}
		respondError(w, http.StatusInternalServerError, "failed to get job")
		return
	}
	respondJSON(w, http.StatusOK, job)
}

// GetStats handles GET /queues/stats
func (h *Handler) GetStats(w http.ResponseWriter, r *http.Request) {
	pending, processing, dead := h.q.Stats()
	respondJSON(w, http.StatusOK, map[string]int{
		"pending":    pending,
		"processing": processing,
		"dead":       dead,
	})
}

// Health handles GET /healthz — always returns 200 if the process is alive.
func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ok"}`))
}

func respondJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func respondError(w http.ResponseWriter, status int, msg string) {
	respondJSON(w, status, map[string]string{"error": msg})
}
