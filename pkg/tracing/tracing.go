package tracing

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// InitTracer configures the global OpenTelemetry TracerProvider and
// TextMapPropagator, then returns a shutdown function the caller must invoke
// before process exit to flush any buffered spans.
//
// Architecture decision: OTLP HTTP exporter → Tempo (or any OTLP-compatible backend)
//   OTLP is the vendor-neutral wire format — the same binary works with
//   Jaeger, Tempo, Datadog, Honeycomb, and any other OTel-compatible backend.
//   The only thing that changes between backends is otlpEndpoint.
//
// BatchSpanProcessor:
//   Spans are buffered in memory and exported in batches rather than one
//   at a time. This decouples the application's hot path from the exporter's
//   network latency — a slow Tempo instance doesn't slow down HTTP handlers.
//
// W3C TraceContext propagation:
//   otel.SetTextMapPropagator(propagation.TraceContext{}) ensures that
//   the standard "traceparent" HTTP header is used for cross-service
//   context propagation. This allows traces to span the API → Kafka →
//   Processor pipeline with a single continuous trace ID.
func InitTracer(ctx context.Context, serviceName, version, otlpEndpoint string) (func(context.Context) error, error) {
	// ── OTLP HTTP exporter ────────────────────────────────────────────────────
	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(otlpEndpoint),
		otlptracehttp.WithInsecure(), // TLS handled by the infrastructure layer (K8s service mesh)
		otlptracehttp.WithTimeout(5*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("create OTLP exporter for %s: %w", otlpEndpoint, err)
	}

	// ── Resource attributes ───────────────────────────────────────────────────
	// Resource attributes describe the entity producing telemetry — they appear
	// on every span. service.name is the most important: it groups all spans
	// from this process in the Tempo/Jaeger UI.
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(version),
		),
		resource.WithHost(),          // adds host.name
		resource.WithProcess(),       // adds process.pid, process.runtime.*
		resource.WithOSDescription(), // adds os.description
	)
	if err != nil {
		return nil, fmt.Errorf("create OTel resource: %w", err)
	}

	// ── TracerProvider ────────────────────────────────────────────────────────
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter,
			sdktrace.WithBatchTimeout(5*time.Second),
			sdktrace.WithMaxExportBatchSize(512),
		),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()), // 100% sampling — tune in production
	)

	// Register as the global provider — all otel.Tracer() calls resolve to this.
	otel.SetTracerProvider(tp)

	// W3C TraceContext: propagates "traceparent" and "tracestate" headers.
	// Baggage propagator is included for future per-request metadata passing.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	// Return a shutdown function that flushes buffered spans before exit.
	shutdown := func(ctx context.Context) error {
		if err := tp.Shutdown(ctx); err != nil {
			return fmt.Errorf("shutdown tracer provider: %w", err)
		}
		return nil
	}

	return shutdown, nil
}