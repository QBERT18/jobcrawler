package processor

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	jobkafka "github.com/applytude/jobcrawler/pkg/kafka"
	"github.com/segmentio/kafka-go"
)

// FailedMessage is the envelope written to jobs.failed.
// It preserves the original payload so the message can be replayed
// once the root cause is fixed — without re-crawling the source.
type FailedMessage struct {
	OriginalTopic string    `json:"original_topic"`
	OriginalKey   string    `json:"original_key"`
	Payload       string    `json:"payload"`       // original message value as string
	Error         string    `json:"error"`         // human-readable error description
	Attempts      int       `json:"attempts"`      // number of processing attempts
	FailedAt      time.Time `json:"failed_at"`
}

// PublishToDLQ wraps originalMsg with error metadata and publishes it to
// the jobs.failed Dead Letter Queue topic.
//
// Design contract:
//   - This function NEVER returns an error to the caller. If the DLQ publish
//     itself fails, the failure is logged. The caller must still commit the
//     original Kafka offset to avoid an infinite retry loop.
//   - The original message key is preserved so DLQ consumers can filter by
//     source (e.g. process all failed STEPSTONE messages together).
func PublishToDLQ(
	ctx context.Context,
	producer jobkafka.KafkaProducer,
	originalMsg kafka.Message,
	processingErr error,
) {
	envelope := FailedMessage{
		OriginalTopic: originalMsg.Topic,
		OriginalKey:   string(originalMsg.Key),
		Payload:       string(originalMsg.Value),
		Error:         processingErr.Error(),
		Attempts:      1,
		FailedAt:      time.Now().UTC(),
	}

	// Use the original message key so DLQ is partitioned by source.
	key := string(originalMsg.Key)
	if key == "" {
		key = fmt.Sprintf("unknown-%d", originalMsg.Offset)
	}

	if err := producer.Publish(ctx, jobkafka.TopicJobsFailed, key, envelope); err != nil {
		// DLQ publish failed — log and move on. The original message will
		// be committed by the caller to unblock the consumer.
		slog.ErrorContext(ctx, "CRITICAL: failed to publish to DLQ — message will be lost",
			slog.String("original_topic", originalMsg.Topic),
			slog.Int64("original_offset", originalMsg.Offset),
			slog.String("original_key", string(originalMsg.Key)),
			slog.String("dlq_error", err.Error()),
			slog.String("processing_error", processingErr.Error()),
		)
		return
	}

	slog.WarnContext(ctx, "message sent to DLQ",
		slog.String("topic", jobkafka.TopicJobsFailed),
		slog.String("original_topic", originalMsg.Topic),
		slog.Int64("original_offset", originalMsg.Offset),
		slog.String("error", processingErr.Error()),
	)
}