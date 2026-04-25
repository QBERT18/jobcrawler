package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/applytude/jobcrawler/config"
	"github.com/redis/go-redis/v9"
)

// NewClient creates a go-redis client, configures the connection pool,
// and verifies connectivity with a Ping before returning.
//
// Pool configuration rationale:
//   - PoolSize: controls max simultaneous connections. Set to match the
//     number of goroutines that may call Redis concurrently (e.g. HTTP workers).
//   - MinIdleConns: keeps 5 connections warm so the first requests after
//     a quiet period don't incur connection setup latency.
//   - DialTimeout: how long to wait when establishing a new connection.
//     5s is generous — fail fast if Redis is unreachable at startup.
//   - ReadTimeout/WriteTimeout: per-operation deadlines. Prevents a slow
//     Redis from cascading into a hung HTTP handler.
func NewClient(cfg config.RedisConfig) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     cfg.PoolSize,
		MinIdleConns: 5,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.ReadTimeout, // symmetric with read
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("redis ping failed — check REDIS_ADDR in .env: %w", err)
	}

	return client, nil
}