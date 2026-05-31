package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/applytude/jobcrawler/config"
	"github.com/applytude/jobcrawler/internal/scheduler"
	jobkafka "github.com/applytude/jobcrawler/pkg/kafka"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	cfg, err := config.Load()
	if err != nil {
		logger.Error("failed to load config", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// Ensure required topics exist before the first enqueue. CreateTopics is
	// idempotent and shared with the processor binary; calling it here removes
	// the cold-start race where scheduler's initial enqueue beats whichever
	// binary happens to create the topics first.
	if err := jobkafka.CreateTopics(cfg.Kafka.Brokers); err != nil {
		logger.Error("kafka topic provisioning failed", slog.String("error", err.Error()))
		os.Exit(1)
	}

	producer := jobkafka.NewProducer(cfg.Kafka.Brokers)
	defer func() {
		if err := producer.Close(); err != nil {
			logger.Error("kafka producer close error", slog.String("error", err.Error()))
		}
	}()

	sched := scheduler.New(producer, cfg.Crawler, logger)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logger.Info("scheduler starting",
		slog.Any("brokers", cfg.Kafka.Brokers),
	)

	sched.Start(ctx) // blocks until ctx is cancelled

	logger.Info("scheduler shut down cleanly")
}