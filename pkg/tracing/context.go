package tracing

import (
	"context"

	"go.opentelemetry.io/otel/trace"
)

// TraceIDFromContext extracts the trace ID from the active span stored in ctx.
// Returns an empty string if ctx carries no span or if the span context is invalid.
//
// Usage: inject the result into slog attributes so every log line within a
// request includes the trace ID — enabling log ↔ trace correlation in Grafana.
//
//	logger.InfoContext(ctx, "job processed",
//	    slog.String("trace_id", tracing.TraceIDFromContext(ctx)),
//	)
func TraceIDFromContext(ctx context.Context) string {
	spanCtx := trace.SpanFromContext(ctx).SpanContext()
	if !spanCtx.IsValid() {
		return ""
	}
	return spanCtx.TraceID().String()
}

// SpanIDFromContext extracts the span ID from the active span in ctx.
// Returns an empty string if no valid span is present.
func SpanIDFromContext(ctx context.Context) string {
	spanCtx := trace.SpanFromContext(ctx).SpanContext()
	if !spanCtx.IsValid() {
		return ""
	}
	return spanCtx.SpanID().String()
}

// SpanFromContext is a convenience re-export of trace.SpanFromContext.
// Callers import only this package instead of the OTel SDK directly,
// making it easier to swap the tracing backend in the future.
func SpanFromContext(ctx context.Context) trace.Span {
	return trace.SpanFromContext(ctx)
}

// StartSpan creates a child span within ctx using the global TracerProvider.
// Returns the child context (containing the new span) and the span itself.
// The caller must call span.End() — defer span.End() is the idiomatic pattern.
//
// Example:
//
//	ctx, span := tracing.StartSpan(ctx, "repo.GetByID")
//	defer span.End()
func StartSpan(ctx context.Context, spanName string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	tracer := trace.SpanFromContext(ctx).TracerProvider().Tracer("jobcrawler")
	return tracer.Start(ctx, spanName, opts...)
}