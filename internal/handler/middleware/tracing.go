package middleware

import (
	"log/slog"
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"

	pkglogger "github.com/applytude/jobcrawler/pkg/logger"
	"github.com/applytude/jobcrawler/pkg/tracing"
)

// TracingMiddleware integrates OpenTelemetry distributed tracing into the
// HTTP middleware chain.
//
// What it does per request:
//  1. Extracts an incoming W3C TraceContext from "traceparent" / "tracestate"
//     headers — if present, the new span becomes a child of the caller's trace.
//  2. Starts a new server span named "http.request".
//  3. Adds the span to the request context — downstream handlers call
//     trace.SpanFromContext(ctx) to create child spans.
//  4. Enriches the per-request slog.Logger with trace_id and span_id attributes
//     so every log line within this request is automatically correlated.
//  5. Sets http.status_code on the span after the handler returns.
//  6. Ends the span — this triggers export via the BatchSpanProcessor.
//
// Placement: must come AFTER RequestID (so the request ID is in context)
// and BEFORE the handler (so the span wraps the full handler execution).
func TracingMiddleware(next http.Handler) http.Handler {
	tracer := otel.Tracer("jobcrawler/http")
	propagator := otel.GetTextMapPropagator()

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// ── 1. Extract incoming trace context ─────────────────────────────────
		// If the API Gateway or an upstream service set "traceparent", this
		// makes our span a child of their trace — enabling end-to-end traces
		// that span multiple services.
		ctx := propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))

		// ── 2. Start server span ──────────────────────────────────────────────
		spanName := r.Method + " " + r.URL.Path
		ctx, span := tracer.Start(ctx, spanName,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				semconv.HTTPRequestMethodOriginal(r.Method),
				semconv.URLPath(r.URL.Path),
				semconv.URLScheme(scheme(r)),
				attribute.String("http.host", r.Host),
				attribute.String("http.user_agent", r.UserAgent()),
				attribute.String("request_id", GetRequestID(ctx)),
			),
		)
		defer span.End()

		// ── 3. Enrich logger with trace/span IDs ──────────────────────────────
		// Any call to logger.FromContext(ctx).InfoContext(ctx, ...) within this
		// request will automatically include trace_id and span_id.
		traceID := tracing.TraceIDFromContext(ctx)
		spanID := tracing.SpanIDFromContext(ctx)

		requestLogger := pkglogger.FromContext(ctx).With(
			slog.String(pkglogger.TraceIDKey, traceID),
			slog.String(pkglogger.SpanIDKey, spanID),
		)
		ctx = pkglogger.WithLogger(ctx, requestLogger)

		// ── 4. Inject span context into response headers ──────────────────────
		// Allows clients to correlate their request with our trace.
		if traceID != "" {
			w.Header().Set("X-Trace-ID", traceID)
		}

		// ── 5. Call handler ───────────────────────────────────────────────────
		wrapped := newResponseWriter(w)
		next.ServeHTTP(wrapped, r.WithContext(ctx))

		// ── 6. Record HTTP status on span ─────────────────────────────────────
		span.SetAttributes(
			semconv.HTTPResponseStatusCode(wrapped.statusCode),
		)

		// Mark spans for 5xx responses as errors so they appear in error dashboards.
		if wrapped.statusCode >= 500 {
			span.SetStatus(
				codes.Error,
				http.StatusText(wrapped.statusCode),
			)
		}
	})
}

// scheme returns "https" or "http" based on the request's TLS state.
func scheme(r *http.Request) string {
	if r.TLS != nil {
		return "https"
	}
	if proto := r.Header.Get("X-Forwarded-Proto"); proto != "" {
		return proto
	}
	return "http"
}