package tracing

import (
	"context"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
)

// kafkaHeaderCarrier adapts []kafka.Header to the TextMapCarrier interface
// required by the OTel propagator. This allows the W3C TraceContext propagator
// to read and write Kafka message headers as if they were HTTP headers.
//
// Why a custom carrier?
//   OTel's propagation.HeaderCarrier only works with http.Header (map[string][]string).
//   Kafka headers are []kafka.Header (a flat slice of key-value pairs).
//   We need an adapter that implements Get/Set/Keys against the Kafka slice.
type kafkaHeaderCarrier struct {
	headers *[]kafka.Header
}

// Get returns the first value for the given key, or "" if not found.
// Key comparison is case-sensitive — Kafka header keys are not normalised
// the way HTTP headers are, so both sides must use consistent casing.
func (c kafkaHeaderCarrier) Get(key string) string {
	for _, h := range *c.headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// Set adds or replaces the header for key. If the key already exists,
// it is overwritten (Kafka doesn't support multi-value headers in practice).
func (c kafkaHeaderCarrier) Set(key, value string) {
	for i, h := range *c.headers {
		if h.Key == key {
			(*c.headers)[i].Value = []byte(value)
			return
		}
	}
	*c.headers = append(*c.headers, kafka.Header{
		Key:   key,
		Value: []byte(value),
	})
}

// Keys returns all header key names — required by the TextMapCarrier interface.
func (c kafkaHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(*c.headers))
	for _, h := range *c.headers {
		keys = append(keys, h.Key)
	}
	return keys
}

// InjectToHeaders serialises the active span context from ctx into the
// Kafka message headers slice using the global W3C TraceContext propagator.
//
// After calling this, the message headers will contain "traceparent" (and
// optionally "tracestate") so the consuming service can reconstruct the trace.
//
// Usage in Producer.Publish:
//
//	headers := make([]kafka.Header, 0)
//	tracing.InjectToHeaders(ctx, &headers)
//	msg := kafka.Message{..., Headers: headers}
func InjectToHeaders(ctx context.Context, headers *[]kafka.Header) {
	otel.GetTextMapPropagator().Inject(ctx, kafkaHeaderCarrier{headers: headers})
}

// ExtractFromHeaders reconstructs the trace context from Kafka message headers
// and returns a new context containing the remote span context.
//
// The returned context should be used for all processing of the message —
// any spans started from it will appear as children of the producer's trace,
// creating a continuous trace across the Kafka boundary.
//
// Usage in consumer loop:
//
//	ctx = tracing.ExtractFromHeaders(ctx, msg.Headers)
//	ctx, span := tracer.Start(ctx, "processor.processMessage")
//	defer span.End()
func ExtractFromHeaders(ctx context.Context, headers []kafka.Header) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, kafkaHeaderCarrier{headers: &headers})
}