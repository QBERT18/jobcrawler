package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// KafkaProducer is the interface crawlers, processors, and the scheduler use
// to publish messages. Keeping it as an interface allows easy mock in tests.
type KafkaProducer interface {
	Publish(ctx context.Context, topic, key string, payload any) error
	PublishBatch(ctx context.Context, topic string, messages []Message) error
	Close() error
}

// Message is a single item in a batch publish call.
type Message struct {
	Key     string
	Payload any
	Headers map[string]string // optional additional headers
}

// traceIDKey is the context key used to propagate trace IDs through the call chain.
type traceIDKey struct{}

// ContextWithTraceID stores a trace ID in ctx so it is picked up by Publish.
func ContextWithTraceID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, traceIDKey{}, id)
}

// traceIDFromContext extracts the trace ID stored by ContextWithTraceID.
// Returns an empty string if none is set.
func traceIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(traceIDKey{}).(string)
	return id
}

// Producer wraps kafka.Writer with typed, context-aware publish helpers.
type Producer struct {
	writer *kafka.Writer
}

// NewProducer creates a Producer connected to the given brokers.
//
// Configuration rationale:
//   - LeastBytes balancer: routes to the partition with the least buffered data,
//     giving better throughput distribution than RoundRobin for variable payloads.
//   - RequireAll: all in-sync replicas must acknowledge before Publish returns.
//     Higher latency, zero message loss.
//   - Async: false — callers know immediately whether publishing succeeded.
//     A failed publish can be routed to the DLQ rather than silently lost.
//   - MaxAttempts: 3 — retry transient broker errors before surfacing the error.
//   - WriteTimeout: 10s — prevents Publish from blocking indefinitely on a
//     partitioned Kafka cluster.
func NewProducer(brokers []string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: kafka.RequireAll,
			Async:        false,
			MaxAttempts:  3,
			WriteTimeout: 10 * time.Second,
		},
	}
}

// Publish serialises payload as JSON and writes a single message to topic.
// The trace ID from ctx is injected as a "trace-id" Kafka Header so it
// can be extracted and logged by the consuming service.
func (p *Producer) Publish(ctx context.Context, topic, key string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("kafka publish: marshal payload: %w", err)
	}

	msg := kafka.Message{
		Topic:   topic,
		Key:     []byte(key),
		Value:   data,
		Headers: buildHeaders(ctx, nil),
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("kafka publish to %s: %w", topic, err)
	}
	return nil
}

// PublishBatch writes multiple messages to topic in a single WriteMessages call.
// One network round trip regardless of batch size — used by the Scheduler to
// enqueue dozens of crawl tasks efficiently.
func (p *Producer) PublishBatch(ctx context.Context, topic string, messages []Message) error {
	kmsgs := make([]kafka.Message, 0, len(messages))

	for _, m := range messages {
		data, err := json.Marshal(m.Payload)
		if err != nil {
			return fmt.Errorf("kafka batch: marshal payload for key %q: %w", m.Key, err)
		}
		kmsgs = append(kmsgs, kafka.Message{
			Topic:   topic,
			Key:     []byte(m.Key),
			Value:   data,
			Headers: buildHeaders(ctx, m.Headers),
		})
	}

	if err := p.writer.WriteMessages(ctx, kmsgs...); err != nil {
		return fmt.Errorf("kafka batch write to %s (%d msgs): %w", topic, len(kmsgs), err)
	}
	return nil
}

// Close flushes any buffered messages and closes the underlying writer.
func (p *Producer) Close() error {
	return p.writer.Close()
}

// buildHeaders constructs the Kafka header slice from context trace ID
// and any additional headers provided by the caller.
func buildHeaders(ctx context.Context, extra map[string]string) []kafka.Header {
	var headers []kafka.Header

	if id := traceIDFromContext(ctx); id != "" {
		headers = append(headers, kafka.Header{
			Key:   "trace-id",
			Value: []byte(id),
		})
	}

	for k, v := range extra {
		headers = append(headers, kafka.Header{
			Key:   k,
			Value: []byte(v),
		})
	}
	return headers
}

// HeaderValue extracts the value of a named header from a kafka.Message.
// Returns an empty string if the header is not present.
func HeaderValue(msg kafka.Message, key string) string {
	for _, h := range msg.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}