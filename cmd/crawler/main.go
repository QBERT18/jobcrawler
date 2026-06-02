package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/applytude/jobcrawler/config"
	"github.com/applytude/jobcrawler/internal/crawler"
	"github.com/applytude/jobcrawler/pkg/httputil"
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

	// ── Kafka topic provisioning ──────────────────────────────────────────────
	// Idempotent: every binary that talks to Kafka calls this so cold-start
	// ordering doesn't matter. Avoids "Unknown Topic Or Partition" races on
	// fresh stacks where this binary may start before the processor.
	if err := jobkafka.CreateTopics(cfg.Kafka.Brokers); err != nil {
		logger.Error("kafka topic provisioning failed", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// ── Kafka ─────────────────────────────────────────────────────────────────
	consumer := jobkafka.NewConsumer(
		cfg.Kafka.Brokers,
		jobkafka.TopicCrawlQueue,
		cfg.Kafka.GroupID+"-crawler",
	)
	defer func() {
		if err := consumer.Close(); err != nil {
			logger.Error("kafka consumer close", slog.String("error", err.Error()))
		}
	}()

	producer := jobkafka.NewProducer(cfg.Kafka.Brokers)
	defer func() {
		if err := producer.Close(); err != nil {
			logger.Error("kafka producer close", slog.String("error", err.Error()))
		}
	}()

	// ── HTTP Client ───────────────────────────────────────────────────────────
	userAgents := cfg.Crawler.UserAgents
	if len(userAgents) == 0 {
		userAgents = config.DefaultUserAgents
	}
	httpClient := httputil.NewCrawlerClient(userAgents)

	// ── Source Registry ───────────────────────────────────────────────────────
	registry := crawler.NewRegistry()

	// ── Redis (rate limiter + stats) — wired in Phase 06 ─────────────────────
	// redisClient := pkgredis.NewClient(cfg.Redis)
	// defer redisClient.Close()
	// limiter := pkgredis.NewRateLimiter(redisClient)

	// Temporary no-op implementations until Phase 06 wires Redis.
	limiter := &noopRateLimiter{}
	stats := &noopStatsUpdater{}

	// ── Worker ────────────────────────────────────────────────────────────────
	worker := crawler.NewCrawlerWorker(
		consumer,
		producer,
		httpClient,
		registry,
		limiter,
		stats,
		cfg.Crawler.MaxListingFanout,
		logger,
	)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logger.Info("crawler worker starting",
		slog.Any("brokers", cfg.Kafka.Brokers),
		slog.String("group", cfg.Kafka.GroupID+"-crawler"),
	)

	worker.Start(ctx)

	logger.Info("crawler worker stopped cleanly")
}

// ── Temporary no-op stubs — replaced when Redis is wired in Phase 06 ─────────

type noopRateLimiter struct{}

func (n *noopRateLimiter) Allow(_ context.Context, _ string, _ int64, _ time.Duration) (bool, error) {
	return true, nil
}

type noopStatsUpdater struct{}

func (n *noopStatsUpdater) Set(_ context.Context, _ string, _ any, _ time.Duration) error {
	return nil
}
func (n *noopStatsUpdater) Incr(_ context.Context, _ string) error { return nil }
func (n *noopStatsUpdater) HSet(_ context.Context, _, _ string, _ any) error {
	return nil
}