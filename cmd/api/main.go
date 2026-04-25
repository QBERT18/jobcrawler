package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/applytude/jobcrawler/config"
	"github.com/applytude/jobcrawler/internal/handler"
	custommiddleware "github.com/applytude/jobcrawler/internal/handler/middleware"
	"github.com/applytude/jobcrawler/internal/repository/postgres"
	"github.com/applytude/jobcrawler/internal/service"
	"github.com/applytude/jobcrawler/pkg/database"
	"github.com/applytude/jobcrawler/pkg/logger"
	pkgredis "github.com/applytude/jobcrawler/pkg/redis"
	"github.com/applytude/jobcrawler/pkg/shutdown"
	"github.com/applytude/jobcrawler/pkg/tracing"
	redis "github.com/redis/go-redis/v9"
)

// redisHealthAdapter adapts *redis.Client to handler.RedisClient.
// go-redis's Ping returns *StatusCmd; the health handler only needs the error.
type redisHealthAdapter struct{ *redis.Client }

func (a redisHealthAdapter) Ping(ctx context.Context) error {
	return a.Client.Ping(ctx).Err()
}

// ready is the global readiness flag.
// Set to 0 (not ready) before shutdown begins so /ready returns 503
// and Kubernetes stops routing traffic before resources are closed.
// Using atomic to avoid data races between the shutdown goroutine and
// the health handler goroutine.
var ready atomic.Bool

func main() {
	// ── Logger ────────────────────────────────────────────────────────────────
	// Must be the very first thing — all subsequent init steps log errors.
	env := os.Getenv("ENV")
	if env == "" {
		env = "development"
	}
	log := logger.NewLogger(env)

	// ── Config ────────────────────────────────────────────────────────────────
	cfg, err := config.Load()
	if err != nil {
		log.Error("config load failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
	log.Info("config loaded",
		slog.String("env", env),
		slog.Int("port", cfg.Server.Port),
	)

	// ── Tracing ───────────────────────────────────────────────────────────────
	otlpEndpoint := os.Getenv("OTLP_ENDPOINT")
	if otlpEndpoint == "" {
		otlpEndpoint = "localhost:4318"
	}

	tracerShutdown, err := tracing.InitTracer(
		context.Background(),
		"jobcrawler-api",
		"1.0.0",
		otlpEndpoint,
	)
	if err != nil {
		// Tracing failure is non-fatal — app runs without it.
		log.Warn("tracer init failed — running without tracing",
			slog.String("error", err.Error()),
		)
		tracerShutdown = func(_ context.Context) error { return nil }
	}

	// ── PostgreSQL — with retry ───────────────────────────────────────────────
	db, err := connectWithRetry(log, "postgres", 10, 2*time.Second, func() error {
		var dbErr error
		db, dbErr := database.NewDB(cfg.Database)
		if dbErr != nil {
			return dbErr
		}
		_ = db
		return nil
	})
	if err != nil {
		log.Error("postgres connect failed after retries", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// Re-open to get the actual *sql.DB value (connectWithRetry is generic).
	db2, err := database.NewDB(cfg.Database)
	if err != nil {
		log.Error("postgres open failed", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// ── DB Migrations ─────────────────────────────────────────────────────────
	if err := database.RunMigrations(cfg.Database.DSN, "./migrations"); err != nil {
		log.Error("db migrations failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
	log.Info("database migrations applied")

	// ── Redis — with retry ────────────────────────────────────────────────────
	_, err = connectWithRetry(log, "redis", 10, 2*time.Second, func() error {
		_, err := pkgredis.NewClient(cfg.Redis)
		return err
	})
	if err != nil {
		log.Error("redis connect failed after retries", slog.String("error", err.Error()))
		os.Exit(1)
	}
	redisClient, err := pkgredis.NewClient(cfg.Redis)
	if err != nil {
		log.Error("redis open failed", slog.String("error", err.Error()))
		os.Exit(1)
	}

	cache := pkgredis.NewCache(redisClient)
	rateLimiter := pkgredis.NewRateLimiter(redisClient)

	// ── Services & Handlers ───────────────────────────────────────────────────
	// Phase 1: Postgres-backed search; ES integration is a later phase.
	jobRepo := postgres.NewJobRepo(db2)
	baseJobSvc := service.NewPostgresJobService(jobRepo)
	cachedJobSvc := service.NewCachedJobService(baseJobSvc, cache, log)

	apiRateLimit := custommiddleware.RateLimitMiddleware(rateLimiter, 100, time.Minute)

	// ── Mark service ready ────────────────────────────────────────────────────
	// All dependencies are confirmed healthy — start accepting traffic.
	ready.Store(true)

	// ── Router ────────────────────────────────────────────────────────────────
	router := handler.NewRouter(handler.Deps{
		JobService:          cachedJobSvc,
		DB:                  db2,
		Redis:               redisHealthAdapter{redisClient},
		RateLimitMiddleware: apiRateLimit,
	})

	// ── HTTP Server ───────────────────────────────────────────────────────────
	srv := &http.Server{
		Addr:         fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port),
		Handler:      router,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
		IdleTimeout:  cfg.Server.IdleTimeout,
	}

	go func() {
		log.Info("api server listening", slog.String("addr", srv.Addr))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server error", slog.String("error", err.Error()))
			os.Exit(1)
		}
	}()

	// ── Graceful shutdown registration ────────────────────────────────────────
	gs := shutdown.New()

	// Step 1: Mark not-ready IMMEDIATELY on SIGTERM so Kubernetes stops
	// routing traffic before we start closing connections.
	// This runs synchronously before the fan-out hooks.
	gs.Register("readiness-flag", func(_ context.Context) error {
		ready.Store(false)
		log.Info("readiness flag set to false — pod will be removed from load balancer")
		// Brief sleep to allow in-flight requests to complete after
		// Kubernetes updates the Endpoints object (typically 1-2 seconds).
		time.Sleep(2 * time.Second)
		return nil
	})

	// Step 2: Stop the HTTP server (waits for in-flight requests).
	gs.Register("http-server", func(ctx context.Context) error {
		return srv.Shutdown(ctx)
	})

	// Step 3: Flush spans to the tracing backend.
	gs.Register("tracer", tracerShutdown)

	// Step 4: Close database connections.
	gs.Register("postgres", func(_ context.Context) error {
		return db2.Close()
	})

	// Step 5: Close Redis connections.
	gs.Register("redis", func(_ context.Context) error {
		return redisClient.Close()
	})

	_ = db

	// ── Block until signal ────────────────────────────────────────────────────
	gs.ListenAndShutdown(context.Background(), 30*time.Second)

	log.Info("process exiting cleanly")
}

// connectWithRetry calls connect up to maxAttempts times with delay between
// attempts. Returns the last error if all attempts fail.
// Logs each attempt so the startup sequence is visible in the container logs.
func connectWithRetry(
	log *slog.Logger,
	name string,
	maxAttempts int,
	delay time.Duration,
	connect func() error,
) (struct{}, error) {
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := connect()
		if err == nil {
			log.Info("dependency connected",
				slog.String("name", name),
				slog.Int("attempt", attempt),
			)
			return struct{}{}, nil
		}

		log.Warn("dependency not ready — retrying",
			slog.String("name", name),
			slog.Int("attempt", attempt),
			slog.Int("max_attempts", maxAttempts),
			slog.String("error", err.Error()),
			slog.Duration("retry_in", delay),
		)

		if attempt < maxAttempts {
			time.Sleep(delay)
		}
	}

	return struct{}{}, fmt.Errorf("%s: failed after %d attempts", name, maxAttempts)
}