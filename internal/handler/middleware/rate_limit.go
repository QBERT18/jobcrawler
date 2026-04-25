package middleware

import (
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"time"

	pkgredis "github.com/applytude/jobcrawler/pkg/redis"
)

// rateLimitResponse is the JSON body returned when a request is rejected.
type rateLimitResponse struct {
	Error             string `json:"error"`
	RetryAfterSeconds int    `json:"retry_after_seconds"`
}

// RateLimitMiddleware returns a middleware that enforces a sliding-window
// rate limit per client IP address.
//
// Parameters:
//   - limiter: the Redis-backed RateLimiter
//   - limit:   maximum requests allowed per window
//   - window:  duration of the sliding window
//
// On rejection (HTTP 429):
//   - Sets the Retry-After header (RFC 7231 §7.1.3) to the window in seconds.
//   - Writes a structured JSON error body.
//   - Does NOT call next — the handler never runs.
//
// IP resolution:
//   Tries X-Forwarded-For first (set by load balancers and API gateways),
//   falls back to X-Real-IP, then to r.RemoteAddr. This correctly identifies
//   clients even when the API runs behind a reverse proxy.
func RateLimitMiddleware(limiter *pkgredis.RateLimiter, limit int64, window time.Duration) func(http.Handler) http.Handler {
	retryAfterSeconds := int(window.Seconds())
	retryAfterStr := strconv.Itoa(retryAfterSeconds)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ip := realIP(r)
			key := pkgredis.KeyRateLimitAPI(ip)

			allowed, err := limiter.Allow(r.Context(), key, limit, window)
			if err != nil {
				// Redis is down — fail open (allow the request).
				// Failing closed would make Redis a hard dependency of the API,
				// causing total outages whenever Redis has a blip.
				next.ServeHTTP(w, r)
				return
			}

			if !allowed {
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
				w.Header().Set("Retry-After", retryAfterStr)
				w.Header().Set("X-RateLimit-Limit", strconv.FormatInt(limit, 10))
				w.WriteHeader(http.StatusTooManyRequests)

				_ = json.NewEncoder(w).Encode(rateLimitResponse{
					Error:             "rate limit exceeded",
					RetryAfterSeconds: retryAfterSeconds,
				})
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// realIP extracts the client IP from the request, respecting proxy headers.
func realIP(r *http.Request) string {
	// X-Forwarded-For may be a comma-separated list: "client, proxy1, proxy2"
	// The first entry is the original client.
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// net.ParseIP validates each candidate
		for _, part := range splitCSV(xff) {
			if ip := net.ParseIP(part); ip != nil {
				return ip.String()
			}
		}
	}

	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		if ip := net.ParseIP(xri); ip != nil {
			return ip.String()
		}
	}

	// Fall back to RemoteAddr — strip the port suffix.
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// splitCSV splits a comma-separated header value and trims each part.
func splitCSV(s string) []string {
	var parts []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == ',' {
			part := trim(s[start:i])
			if part != "" {
				parts = append(parts, part)
			}
			start = i + 1
		}
	}
	if part := trim(s[start:]); part != "" {
		parts = append(parts, part)
	}
	return parts
}

func trim(s string) string {
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t') {
		s = s[1:]
	}
	for len(s) > 0 && (s[len(s)-1] == ' ' || s[len(s)-1] == '\t') {
		s = s[:len(s)-1]
	}
	return s
}