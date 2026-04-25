package database

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/applytude/jobcrawler/config"
	_ "github.com/lib/pq" // PostgreSQL driver — imported for side effects
)

// NewDB opens a PostgreSQL connection pool, configures it from cfg,
// and verifies connectivity with a ping. Returns a ready-to-use *sql.DB.
func NewDB(cfg config.DatabaseConfig) (*sql.DB, error) {
	db, err := sql.Open("postgres", cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}

	// Connection pool settings — prevent connection exhaustion under load.
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	db.SetConnMaxIdleTime(5 * time.Minute)

	// Verify the DSN is reachable before proceeding.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping db: %w — check DATABASE_DSN in .env", err)
	}

	return db, nil
}