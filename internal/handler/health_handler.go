package handler

import (
	"context"
	"database/sql"
	"net/http"
	"sync"
	"time"
)

// RedisClient is the minimal Redis interface the health check needs.
type RedisClient interface {
	Ping(ctx context.Context) error
}

// ESClient is the minimal Elasticsearch interface the health check needs.
type ESClient interface {
	Ping() error
}

// HealthHandler exposes liveness and readiness probes for Kubernetes.
type HealthHandler struct {
	db    *sql.DB
	redis RedisClient
	es    ESClient
}

// NewHealthHandler creates a HealthHandler with all required dependencies.
func NewHealthHandler(db *sql.DB, redis RedisClient, es ESClient) *HealthHandler {
	return &HealthHandler{db: db, redis: redis, es: es}
}

// livenessResponse is the fixed body for the liveness probe.
type livenessResponse struct {
	Status  string `json:"status"`
	Service string `json:"service"`
}

// readinessResponse includes per-dependency check results.
type readinessResponse struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks"`
}

// Liveness handles GET /health
// Always returns 200 OK as long as the process is running.
// Kubernetes restarts the pod if this endpoint stops responding.
func (h *HealthHandler) Liveness(w http.ResponseWriter, r *http.Request) {
	respondJSON(w, http.StatusOK, livenessResponse{
		Status:  "ok",
		Service: "jobcrawler",
	})
}

// Readiness handles GET /ready
// Checks all downstream dependencies concurrently within a 3 s budget.
// Returns 503 if any dependency is unhealthy — Kubernetes will stop
// routing traffic to the pod until it recovers.
func (h *HealthHandler) Readiness(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	checks := map[string]string{
		"database":      "ok",
		"redis":         "ok",
		"elasticsearch": "ok",
	}
	var mu sync.Mutex
	var wg sync.WaitGroup

	setUnhealthy := func(name string) {
		mu.Lock()
		checks[name] = "unhealthy"
		mu.Unlock()
	}

	// ── Database ──────────────────────────────────────────────────────────────
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := h.db.PingContext(ctx); err != nil {
			setUnhealthy("database")
		}
	}()

	// ── Redis ─────────────────────────────────────────────────────────────────
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := h.redis.Ping(ctx); err != nil {
			setUnhealthy("redis")
		}
	}()

	// ── Elasticsearch ─────────────────────────────────────────────────────────
	// Skip the probe when ES isn't wired — the service can run without it
	// during early phases where search is served straight from Postgres.
	if h.es == nil {
		delete(checks, "elasticsearch")
	} else {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := h.es.Ping(); err != nil {
				setUnhealthy("elasticsearch")
			}
		}()
	}

	wg.Wait()

	// Determine overall status
	status := "ready"
	httpStatus := http.StatusOK
	for _, v := range checks {
		if v == "unhealthy" {
			status = "degraded"
			httpStatus = http.StatusServiceUnavailable
			break
		}
	}

	respondJSON(w, httpStatus, readinessResponse{
		Status: status,
		Checks: checks,
	})
}