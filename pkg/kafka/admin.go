package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/segmentio/kafka-go"
)

// topicSpec describes the desired configuration for a single Kafka topic.
type topicSpec struct {
	name              string
	numPartitions     int
	replicationFactor int
}

// requiredTopics is the canonical list of topics the JobCrawler system needs.
// Partition counts are sized for expected throughput:
//   - crawl.queue: 4 partitions (one per source with room to grow)
//   - jobs.raw: 8 partitions (high-throughput HTML payloads)
//   - jobs.processed: 4 partitions (normalised, consumed by alert service)
//   - jobs.failed: 1 partition (DLQ — low volume, ordering preserved)
var requiredTopics = []topicSpec{
	{TopicCrawlQueue, 4, 1},
	{TopicJobsRaw, 8, 1},
	{TopicJobsProcessed, 4, 1},
	{TopicJobsFailed, 1, 1},
}

// CreateTopics ensures all required topics exist, creating any that are missing.
// Safe to call on every startup — already-existing topics are skipped.
// Uses the first broker in the list to communicate with the cluster.
func CreateTopics(brokers []string) error {
	if len(brokers) == 0 {
		return fmt.Errorf("create topics: no brokers provided")
	}

	conn, err := dialLeader(brokers[0])
	if err != nil {
		return fmt.Errorf("create topics: dial broker %s: %w", brokers[0], err)
	}
	defer conn.Close()

	// Fetch existing topics so we only create missing ones.
	partitions, err := conn.ReadPartitions()
	if err != nil {
		return fmt.Errorf("create topics: read partitions: %w", err)
	}

	existing := make(map[string]bool, len(partitions))
	for _, p := range partitions {
		existing[p.Topic] = true
	}

	var toCreate []kafka.TopicConfig
	for _, spec := range requiredTopics {
		if existing[spec.name] {
			slog.Info("kafka topic already exists — skipping", slog.String("topic", spec.name))
			continue
		}
		toCreate = append(toCreate, kafka.TopicConfig{
			Topic:             spec.name,
			NumPartitions:     spec.numPartitions,
			ReplicationFactor: spec.replicationFactor,
		})
	}

	if len(toCreate) == 0 {
		return nil
	}

	if err := conn.CreateTopics(toCreate...); err != nil {
		// Kafka returns an error even for "topic already exists" — filter it out.
		if !isTopicExistsErr(err) {
			return fmt.Errorf("create topics: %w", err)
		}
	}

	for _, tc := range toCreate {
		slog.Info("kafka topic created",
			slog.String("topic", tc.Topic),
			slog.Int("partitions", tc.NumPartitions),
			slog.Int("replication", tc.ReplicationFactor),
		)
	}

	return nil
}

// dialLeader connects to the Kafka cluster and returns a connection to the
// controller (leader) broker, which is required for admin operations.
func dialLeader(broker string) (*kafka.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := kafka.DialContext(ctx, "tcp", broker)
	if err != nil {
		return nil, err
	}

	// Navigate to the controller broker — admin ops must go to the leader.
	controller, err := conn.Controller()
	conn.Close()
	if err != nil {
		return nil, fmt.Errorf("get controller: %w", err)
	}

	leaderConn, err := kafka.DialContext(ctx, "tcp",
		net.JoinHostPort(controller.Host, fmt.Sprintf("%d", controller.Port)),
	)
	if err != nil {
		return nil, fmt.Errorf("dial controller %s:%d: %w", controller.Host, controller.Port, err)
	}

	return leaderConn, nil
}

// isTopicExistsErr returns true when the error indicates the topic already exists.
// kafka-go surfaces this as a *kafka.Error with Code == TopicAlreadyExists.
func isTopicExistsErr(err error) bool {
	var kafkaErr kafka.Error
	if errors.As(err, &kafkaErr) {
		return kafkaErr == kafka.TopicAlreadyExists
	}
	return false
}