package config

import "time"

// Config is the root configuration for all JobCrawler services.
// Each binary loads the same config but only uses the relevant sub-structs.
type Config struct {
	Server        ServerConfig        `mapstructure:",squash"`
	Database      DatabaseConfig      `mapstructure:",squash"`
	Redis         RedisConfig         `mapstructure:",squash"`
	Kafka         KafkaConfig         `mapstructure:",squash"`
	Elasticsearch ElasticsearchConfig `mapstructure:",squash"`
	Crawler       CrawlerConfig       `mapstructure:",squash"`
	Processor     ProcessorConfig     `mapstructure:",squash"`
}

// ServerConfig holds HTTP server settings for the API binary.
type ServerConfig struct {
	Host         string        `mapstructure:"SERVER_HOST"`
	Port         int           `mapstructure:"SERVER_PORT"`
	ReadTimeout  time.Duration `mapstructure:"SERVER_READ_TIMEOUT"`
	WriteTimeout time.Duration `mapstructure:"SERVER_WRITE_TIMEOUT"`
	IdleTimeout  time.Duration `mapstructure:"SERVER_IDLE_TIMEOUT"`
}

// DatabaseConfig holds PostgreSQL connection settings.
type DatabaseConfig struct {
	DSN             string        `mapstructure:"DATABASE_DSN"`
	MaxOpenConns    int           `mapstructure:"DATABASE_MAX_OPEN_CONNS"`
	MaxIdleConns    int           `mapstructure:"DATABASE_MAX_IDLE_CONNS"`
	ConnMaxLifetime time.Duration `mapstructure:"DATABASE_CONN_MAX_LIFETIME"`
}

// RedisConfig holds Redis connection and pool settings.
type RedisConfig struct {
	Addr        string        `mapstructure:"REDIS_ADDR"`
	Password    string        `mapstructure:"REDIS_PASSWORD"`
	DB          int           `mapstructure:"REDIS_DB"`
	PoolSize    int           `mapstructure:"REDIS_POOL_SIZE"`
	DialTimeout time.Duration `mapstructure:"REDIS_DIAL_TIMEOUT"`
	ReadTimeout time.Duration `mapstructure:"REDIS_READ_TIMEOUT"`
}

// KafkaConfig holds Kafka broker and producer/consumer settings.
type KafkaConfig struct {
	Brokers      []string `mapstructure:"KAFKA_BROKERS"`
	GroupID      string   `mapstructure:"KAFKA_GROUP_ID"`
	RequiredAcks int      `mapstructure:"KAFKA_REQUIRED_ACKS"`
}

// ElasticsearchConfig holds Elasticsearch cluster settings.
type ElasticsearchConfig struct {
	Addresses []string `mapstructure:"ES_ADDRESSES"`
	IndexName string   `mapstructure:"ES_INDEX_NAME"`
}

// CrawlerConfig holds per-crawler behaviour settings.
type CrawlerConfig struct {
	RateLimitRPS float64  `mapstructure:"CRAWLER_RATE_LIMIT_RPS"`
	MaxRetries   int      `mapstructure:"CRAWLER_MAX_RETRIES"`
	UserAgents   []string `mapstructure:"CRAWLER_USER_AGENTS"`
}

// ProcessorConfig holds storage-growth limits enforced by the processor binary:
// a hard cap on total stored jobs and a retention cleanup cron. These keep the
// dataset bounded on small/self-hosted deployments.
type ProcessorConfig struct {
	// MaxTotalJobs is the hard cap on rows in the jobs table. 0 = unlimited.
	// When reached, the processor pauses inserts (soft, self-healing — inserts
	// resume once cleanup drops the count below the cap).
	MaxTotalJobs int64 `mapstructure:"MAX_TOTAL_JOBS"`
	// CleanupEnabled toggles the retention cron.
	CleanupEnabled bool `mapstructure:"CLEANUP_ENABLED"`
	// CleanupSchedule is a cron expression (robfig/cron) for the cleanup job.
	CleanupSchedule string `mapstructure:"CLEANUP_SCHEDULE"`
	// CleanupRetentionDays deletes jobs whose created_at is older than this many days.
	CleanupRetentionDays int `mapstructure:"CLEANUP_RETENTION_DAYS"`
}