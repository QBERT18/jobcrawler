# JobCrawler

A distributed job-crawling pipeline written in Go. It scrapes job postings from
sources like **StepStone** and **XING**, normalizes and deduplicates them,
indexes them in **Elasticsearch**, and exposes a REST API for querying. The
stack runs locally on Docker Compose and ships with Kubernetes manifests for
production deployment.

## Architecture

The system is split into four binaries that coordinate over Kafka and share a
PostgreSQL/Redis/Elasticsearch backend:

| Service     | Role                                                                 |
|-------------|----------------------------------------------------------------------|
| `scheduler` | Cron-driven trigger that publishes crawl jobs to Kafka.              |
| `crawler`   | Fetches & parses job listings from sources, publishes raw jobs.      |
| `processor` | Consumes raw jobs, normalizes, deduplicates, indexes in Elasticsearch. |
| `api`       | REST API (chi) for searching and retrieving jobs.                    |

Supporting infrastructure: PostgreSQL (state), Redis (rate-limit & dedup
cache), Kafka (message bus), Elasticsearch (search index), Prometheus & OTEL
(observability).

## Prerequisites

- Go **1.26+**
- Docker & Docker Compose
- `make`
- (Optional) `golang-migrate`, `golangci-lint`, `kubectl` for advanced workflows

## 1. Configure your `.env`

Copy the template and adjust as needed. The defaults work with the bundled
`docker-compose.yml`, so for local development you usually don't need to
change anything.

```bash
cp .env.example .env
```

Key variables:

| Variable                  | Purpose                                               |
|---------------------------|-------------------------------------------------------|
| `SERVER_HOST` / `SERVER_PORT` | Bind address for the API (`0.0.0.0:8080`).        |
| `DATABASE_DSN`            | PostgreSQL connection string.                         |
| `REDIS_ADDR`              | Redis host:port for caching & rate limiting.          |
| `KAFKA_BROKERS`           | Comma-separated Kafka brokers.                        |
| `ES_ADDRESSES`            | Comma-separated Elasticsearch nodes.                  |
| `ES_INDEX_NAME`           | Name of the Elasticsearch index (default `jobs`).     |
| `CRAWLER_RATE_LIMIT_RPS`  | Requests per second per source (be polite).           |
| `CRAWLER_MAX_RETRIES`     | Retry attempts on transient errors.                   |

See [`.env.example`](.env.example) for the full list.

## 2. Start the local stack

Spin up Postgres, Redis, Kafka, Elasticsearch, and all four services:

```bash
make dev
```

This is a thin wrapper around `docker compose up -d`. The first run pulls
images and may take a few minutes.

Verify everything is healthy:

```bash
make dev-ps
```

Service URLs:

- **API**           — http://localhost:8080
- **Metrics**       — http://localhost:8080/metrics
- **Elasticsearch** — http://localhost:9200
- **Kafka**         — localhost:9092
- **PostgreSQL**    — localhost:5432
- **Redis**         — localhost:6379

## 3. Run database migrations

The processor auto-applies migrations on startup. If you want to run them
manually:

```bash
make migrate
```

## 4. Use it

Hit the API:

```bash
curl 'http://localhost:8080/jobs?q=golang&location=berlin'
```

Or open the bundled UI by serving `index.html` (e.g. with `python3 -m
http.server`) — it talks directly to the local API.

## Development workflow

```bash
make build         # compile all 4 binaries into ./bin/
make build-local   # macOS-friendly local build
make test          # go test ./... -race
make test-cover    # generate coverage report (coverage.html)
make lint          # golangci-lint
make fmt           # gofmt -w -s .
make clean         # remove ./bin and coverage artifacts
```

Run a single binary against the docker-compose infra:

```bash
go run ./cmd/api
go run ./cmd/crawler
go run ./cmd/processor
go run ./cmd/scheduler
```

## Tearing down

```bash
make dev-down      # stop all containers AND delete volumes
make dev-logs      # tail compose logs
```

## Kubernetes deployment

Manifests live under [`k8s/`](k8s/). To deploy to the active cluster:

```bash
make k8s-deploy            # apply all manifests
make k8s-status            # watch pod rollout
make scale svc=crawler n=3 # scale a deployment
make logs svc=api          # tail pod logs
```

## Repo layout

```
cmd/         entrypoints for the 4 binaries
config/      viper-based configuration loading
internal/    application code (crawler, processor, scheduler, handler, ...)
pkg/         reusable infrastructure (kafka, redis, database, metrics, ...)
docker/      per-service Dockerfiles
k8s/         Kubernetes manifests
migrations/  golang-migrate SQL files
```

## Troubleshooting

- **Elasticsearch fails to start with `vm.max_map_count` errors** — bump the
  sysctl on the host: `sudo sysctl -w vm.max_map_count=262144`.
- **Kafka consumer can't connect** — ensure `KAFKA_BROKERS` matches the host
  reachable from inside the container (the compose network uses
  `kafka:9092`).
- **API returns empty results** — wait for the scheduler to fire its first
  crawl, or trigger one manually via the scheduler service.
