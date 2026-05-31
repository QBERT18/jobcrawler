package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/applytude/jobcrawler/config"
	"github.com/applytude/jobcrawler/internal/crawler"
	"github.com/applytude/jobcrawler/internal/janitor"
	"github.com/applytude/jobcrawler/internal/processor"
	"github.com/applytude/jobcrawler/internal/repository/postgres"
	"github.com/applytude/jobcrawler/pkg/database"
	jobkafka "github.com/applytude/jobcrawler/pkg/kafka"
	"github.com/redis/go-redis/v9"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// ── Config ────────────────────────────────────────────────────────────────
	cfg, err := config.Load()
	if err != nil {
		logger.Error("config load failed", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// ── PostgreSQL ────────────────────────────────────────────────────────────
	db, err := database.NewDB(cfg.Database)
	if err != nil {
		logger.Error("db connect failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer db.Close()

	// Run migrations on every startup — idempotent, safe in production.
	if err := database.RunMigrations(cfg.Database.DSN, "./migrations"); err != nil {
		logger.Error("migrations failed", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// ── Redis ─────────────────────────────────────────────────────────────────
	redisClient := redis.NewClient(&redis.Options{
		Addr:         cfg.Redis.Addr,
		Password:     cfg.Redis.Password,
		DB:           cfg.Redis.DB,
		PoolSize:     cfg.Redis.PoolSize,
		DialTimeout:  cfg.Redis.DialTimeout,
		ReadTimeout:  cfg.Redis.ReadTimeout,
	})
	defer redisClient.Close()

	// ── Kafka topic provisioning ──────────────────────────────────────────────
	// Auto-creation races the first publish for jobs.processed; provision
	// explicitly so the producer's metadata cache never sees a negative result.
	if err := jobkafka.CreateTopics(cfg.Kafka.Brokers); err != nil {
		logger.Error("kafka topic provisioning failed", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// ── Kafka ─────────────────────────────────────────────────────────────────
	consumer := jobkafka.NewConsumer(
		cfg.Kafka.Brokers,
		jobkafka.TopicJobsRaw,
		cfg.Kafka.GroupID+"-processor",
	)
	defer consumer.Close()

	producer := jobkafka.NewProducer(cfg.Kafka.Brokers)
	defer producer.Close()

	// ── Dependencies ──────────────────────────────────────────────────────────
	jobRepo    := postgres.NewJobRepo(db)
	dedup      := processor.NewDeduplicator(redisClient)
	normalizer := processor.NewNormalizer()
	registry   := crawler.NewRegistry()

	// ── Worker ────────────────────────────────────────────────────────────────
	worker := processor.NewProcessorWorker(
		consumer,
		producer,
		jobRepo,
		dedup,
		normalizer,
		registry,
		nil,
		cfg.Processor.MaxTotalJobs,
		logger,
	)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Retention cleanup cron — runs alongside the consumer loop, sharing the
	// processor's DB connection. Keeps the jobs table bounded over time.
	go janitor.New(jobRepo, cfg.Processor, logger).Start(ctx)

	logger.Info("processor worker starting",
		slog.Any("brokers", cfg.Kafka.Brokers),
		slog.String("group", cfg.Kafka.GroupID+"-processor"),
	)

	worker.Start(ctx)

	logger.Info("processor worker stopped cleanly")
}