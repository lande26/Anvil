package api

import "net/http"

// NewRouter registers all Anvil HTTP routes on a new ServeMux.
// Uses Go 1.22+ method+path pattern matching.
func NewRouter(h *Handler) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", h.CreateJob)
	mux.HandleFunc("GET /jobs/{id}", h.GetJob)
	mux.HandleFunc("GET /queues/stats", h.GetStats)
	mux.HandleFunc("GET /healthz", h.Health)
	return mux
}
