package config

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Load reads configuration from a .env file (if present) and from real
// environment variables. Real ENV vars always override .env values.
// Returns a validated Config or a descriptive error.
func Load() (*Config, error) {
	v := viper.New()

	// ── Defaults ──────────────────────────────────────────────────────────────
	v.SetDefault("SERVER_HOST", "0.0.0.0")
	v.SetDefault("SERVER_PORT", 8080)
	v.SetDefault("SERVER_READ_TIMEOUT", 15*time.Second)
	v.SetDefault("SERVER_WRITE_TIMEOUT", 15*time.Second)
	v.SetDefault("SERVER_IDLE_TIMEOUT", 60*time.Second)

	v.SetDefault("DATABASE_MAX_OPEN_CONNS", 25)
	v.SetDefault("DATABASE_MAX_IDLE_CONNS", 10)
	v.SetDefault("DATABASE_CONN_MAX_LIFETIME", 5*time.Minute)

	v.SetDefault("REDIS_DB", 0)
	v.SetDefault("REDIS_POOL_SIZE", 10)
	v.SetDefault("REDIS_DIAL_TIMEOUT", 5*time.Second)
	v.SetDefault("REDIS_READ_TIMEOUT", 3*time.Second)

	v.SetDefault("KAFKA_GROUP_ID", "jobcrawler")
	v.SetDefault("KAFKA_REQUIRED_ACKS", -1) // RequireAll

	v.SetDefault("ES_INDEX_NAME", "jobs")

	v.SetDefault("CRAWLER_RATE_LIMIT_RPS", 0.5)
	v.SetDefault("CRAWLER_MAX_RETRIES", 3)

	v.SetDefault("MAX_TOTAL_JOBS", 0) // 0 = unlimited
	v.SetDefault("CLEANUP_ENABLED", true)
	v.SetDefault("CLEANUP_SCHEDULE", "0 3 * * *") // daily 03:00
	v.SetDefault("CLEANUP_RETENTION_DAYS", 30)

	// ── .env file (optional, never fatal if missing) ──────────────────────────
	v.SetConfigFile(".env")
	v.SetConfigType("env")
	if err := v.ReadInConfig(); err != nil {
		var notFound viper.ConfigFileNotFoundError
		// A missing .env is fine — we rely on real ENV vars in production.
		if !errors.As(err, &notFound) && !strings.Contains(err.Error(), "no such file") {
			return nil, fmt.Errorf("reading .env file: %w", err)
		}
	}

	// ── Real ENV vars override .env ───────────────────────────────────────────
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// AutomaticEnv only resolves keys Viper already knows about (defaults or
	// explicit binds). Required keys without defaults must be bound explicitly
	// so the app can run purely from ENV vars (e.g. in containers, no .env).
	for _, key := range []string{
		"DATABASE_DSN",
		"REDIS_ADDR",
		"REDIS_PASSWORD",
		"KAFKA_BROKERS",
		"ES_ADDRESSES",
		"CRAWLER_USER_AGENTS",
	} {
		_ = v.BindEnv(key)
	}

	// ── Unmarshal ─────────────────────────────────────────────────────────────
	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshalling config: %w", err)
	}

	// ── Validation ────────────────────────────────────────────────────────────
	if err := validate(&cfg, v); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// validate checks that all required fields are present.
func validate(cfg *Config, v *viper.Viper) error {
	var missing []string

	if cfg.Database.DSN == "" {
		missing = append(missing, "DATABASE_DSN")
	}
	if cfg.Redis.Addr == "" {
		missing = append(missing, "REDIS_ADDR")
	}
	if len(cfg.Kafka.Brokers) == 0 || (len(cfg.Kafka.Brokers) == 1 && cfg.Kafka.Brokers[0] == "") {
		missing = append(missing, "KAFKA_BROKERS")
	}

	if len(missing) > 0 {
		return fmt.Errorf(
			"missing required environment variables: %s\n"+
				"Copy .env.example to .env and fill in the values.",
			strings.Join(missing, ", "),
		)
	}

	_ = v
	return nil
}