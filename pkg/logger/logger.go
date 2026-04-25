package logger

import (
	"context"
	"log/slog"
	"os"
)

// traceIDKey is the slog attribute key used to embed trace IDs in every log line.
// Keeping it as a typed constant prevents key collisions across packages.
const TraceIDKey = "trace_id"

// spanIDKey is the slog attribute key for the current span ID.
const SpanIDKey = "span_id"

// NewLogger constructs a *slog.Logger appropriate for the given environment
// and registers it as the global default via slog.SetDefault.
//
// Design choice: two output formats, one interface.
//   - production: JSON — machine-parseable by Loki, Datadog, Elasticsearch.
//     No colour, no human formatting — every field is a typed key-value pair.
//   - development: Text — human-readable, colour-friendly in terminals.
//     Debug level enabled so all log lines are visible locally.
//
// Callers never need to know which handler is active — they always call
// slog.InfoContext, slog.ErrorContext, etc. with the context, and the
// trace_id attribute injected by TracingMiddleware appears automatically.
func NewLogger(env string) *slog.Logger {
	var handler slog.Handler

	if env == "production" {
		handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level:     slog.LevelInfo,
			AddSource: false, // source paths leak internal structure; disable in prod
		})
	} else {
		handler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level:     slog.LevelDebug,
			AddSource: true, // show file:line in dev for faster debugging
		})
	}

	log := slog.New(handler)
	slog.SetDefault(log)
	return log
}

// contextKey is the package-private type for context values stored by this package.
type contextKey struct{ name string }

var loggerKey = contextKey{"logger"}

// WithLogger stores log in ctx so handlers and middleware can retrieve it
// with FromContext — carrying per-request fields (trace_id, request_id, etc.)
// without passing the logger explicitly through every function call.
func WithLogger(ctx context.Context, log *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey, log)
}

// FromContext retrieves the logger stored by WithLogger.
// Falls back to slog.Default() if no logger is in ctx — ensures callers
// always get a working logger even if middleware wasn't applied.
func FromContext(ctx context.Context) *slog.Logger {
	if log, ok := ctx.Value(loggerKey).(*slog.Logger); ok && log != nil {
		return log
	}
	return slog.Default()
}