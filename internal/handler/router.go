package handler

import (
	"database/sql"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"

	custommiddleware "github.com/applytude/jobcrawler/internal/handler/middleware"
	"github.com/applytude/jobcrawler/internal/service"
)

// Deps holds every dependency the router needs to build all handlers.
type Deps struct {
	JobService          service.JobService
	DB                  *sql.DB
	Redis               RedisClient
	ES                  ESClient
	// RateLimitMiddleware is the per-IP sliding-window limiter (Phase 06).
	// If nil, rate limiting is skipped — useful in tests and early phases.
	RateLimitMiddleware func(http.Handler) http.Handler
}

// NewRouter builds and returns a fully configured chi.Router.
//
// Middleware order (request flows top → bottom, response bottom → top):
//  1. RequestID   — must be first; all subsequent middleware use the ID
//  2. RealIP      — rewrite RemoteAddr from X-Forwarded-For before logging
//  3. Tracing     — start span early so it wraps Logger and all handlers
//  4. Logger      — logs after response using responseWriter wrapper
//  5. Metrics     — records duration using its own responseWriter wrapper
//  6. Recoverer   — catches panics below this point
//  7. CORS        — must handle preflight before auth/rate-limit checks
//  8. Timeout     — hard deadline for the full handler execution
//  9. RateLimit   — applied last so health/metrics probes bypass it
func NewRouter(deps Deps) http.Handler {
	r := chi.NewRouter()

	tracer := otel.Tracer("jobcrawler/http")
	_ = tracer // used inside TracingMiddleware via otel.Tracer

	// ── Global middleware ─────────────────────────────────────────────────────
	r.Use(custommiddleware.RequestID)
	r.Use(chimiddleware.RealIP)
	r.Use(custommiddleware.TracingMiddleware)  // Phase 07: OTel span per request
	r.Use(custommiddleware.Logger)
	r.Use(custommiddleware.MetricsMiddleware)  // Phase 07: Prometheus duration histogram
	r.Use(chimiddleware.Recoverer)
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodOptions},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-Request-ID", "traceparent", "tracestate"},
		ExposedHeaders:   []string{"X-Request-ID", "X-Trace-ID"},
		AllowCredentials: false,
		MaxAge:           300,
	}))
	r.Use(chimiddleware.Timeout(60 * time.Second))

	// Rate limiting — only applied when wired (nil-safe).
	if deps.RateLimitMiddleware != nil {
		r.Use(deps.RateLimitMiddleware)
	}

	// ── Observability endpoints — exempt from rate limiting ───────────────────
	health := NewHealthHandler(deps.DB, deps.Redis, deps.ES)
	r.Get("/health", health.Liveness)
	r.Get("/ready", health.Readiness)
	r.Handle("/metrics", promhttp.Handler()) // Prometheus scrape endpoint

	// ── API v1 ────────────────────────────────────────────────────────────────
	r.Route("/api/v1", func(r chi.Router) {
		jobs := NewJobHandler(deps.JobService)
		r.Route("/jobs", func(r chi.Router) {
			r.Get("/", jobs.Search)
			r.Get("/stats", jobs.GetStats) // before /{id} — chi matches in order
			r.Get("/{id}", jobs.GetByID)
		})
	})

	return r
}