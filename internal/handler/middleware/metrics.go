package middleware

import (
	"net/http"
	"strconv"
	"time"

	"github.com/applytude/jobcrawler/pkg/metrics"
)

// MetricsMiddleware records HTTP request duration for every request that
// passes through the router. It uses the responseWriter wrapper (already
// defined in logger.go) to capture the status code written by the handler.
//
// Labels observed:
//   - method:      HTTP verb (GET, POST, …)
//   - path:        raw URL path — NOT the chi route template.
//     Using chi.RouteContext(r.Context()).RoutePattern() would give cleaner
//     cardinality but requires a chi import in this package. Raw path is
//     acceptable for our limited endpoint set.
//   - status_code: HTTP response status as a string ("200", "404", etc.)
//
// Placement in middleware chain:
//   MetricsMiddleware must come AFTER Logger (so the responseWriter wrapper
//   is already set up) but BEFORE the actual handler. In practice it wraps
//   its own responseWriter independently — no coupling to Logger's wrapper.
func MetricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		wrapped := newResponseWriter(w)

		next.ServeHTTP(wrapped, r)

		duration := time.Since(start).Seconds()
		statusStr := strconv.Itoa(wrapped.statusCode)

		metrics.HTTPRequestDuration.
			WithLabelValues(r.Method, r.URL.Path, statusStr).
			Observe(duration)
	})
}