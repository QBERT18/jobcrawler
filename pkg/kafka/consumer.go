package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// KafkaReader is the interface workers use to consume messages.
// The narrow interface makes it trivial to inject a fake in tests.
type KafkaReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// Consumer wraps kafka.Reader with sensible defaults.
type Consumer struct {
	reader *kafka.Reader
}

// NewConsumer creates a Consumer subscribed to topic with the given groupID.
//
// Configuration rationale:
//   - MinBytes/MaxBytes: balance between latency (fetch when ≥10KB is ready)
//     and memory usage (never buffer more than 10MB per fetch).
//   - MaxWait: 1s — don't wait longer than 1 second for MinBytes to fill.
//     Keeps processing latency low during quiet periods.
//   - StartOffset: FirstOffset — new consumer groups start from the beginning
//     of the topic, ensuring no messages are skipped on first deployment.
func NewConsumer(brokers []string, topic, groupID string) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:     brokers,
			Topic:       topic,
			GroupID:     groupID,
			MinBytes:    10e3,           // 10 KB
			MaxBytes:    10e6,           // 10 MB
			MaxWait:     time.Second,
			StartOffset: kafka.FirstOffset,
			// CommitInterval 0 means we commit manually — no auto-commit.
			// This ensures offsets only advance after successful processing.
			CommitInterval: 0,
		}),
	}
}

// FetchMessage blocks until a message is available or ctx is cancelled.
func (c *Consumer) FetchMessage(ctx context.Context) (kafka.Message, error) {
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return kafka.Message{}, fmt.Errorf("fetch message from %s: %w", c.reader.Config().Topic, err)
	}
	return msg, nil
}

// CommitMessages advances the consumer group offset past the given messages.
// Call this only after the message has been fully processed (or sent to DLQ).
func (c *Consumer) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	if err := c.reader.CommitMessages(ctx, msgs...); err != nil {
		return fmt.Errorf("commit messages on %s: %w", c.reader.Config().Topic, err)
	}
	return nil
}

// Close closes the underlying reader and releases resources.
func (c *Consumer) Close() error {
	return c.reader.Close()
}