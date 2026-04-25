# AGENTS.md

Guidance for AI coding agents (Claude Code, Cursor, Copilot, Codex, etc.)
working in this repository. Follows the [agents.md](https://agents.md)
convention.

## Project Overview

JobCrawler is a distributed Go pipeline that crawls job postings from StepStone,
XING (and stubs for Indeed/LinkedIn), normalizes & deduplicates them through
Kafka, persists to PostgreSQL, and serves them via a chi-based REST API. Search
results are cached in Redis. Elasticsearch is wired into config but Phase-1
search is currently Postgres-backed (see [cmd/api/main.go](cmd/api/main.go),
"Phase 1: Postgres-backed search; ES integration is a later phase").

The Go module is `github.com/applytude/jobcrawler` (Go 1.26).

## Common Commands

All workflows go through the [Makefile](Makefile). Notable targets:

```bash
make build           # cross-compile all 4 binaries (CGO_ENABLED=0, parallel)
make build-local     # macOS-friendly build (no CGO_ENABLED override)
make test            # go test ./... -race -timeout 60s -count=1
make test-cover      # generate coverage.html
make lint            # golangci-lint run ./...
make fmt             # gofmt -w -s .
make dev             # docker compose up -d (full local stack)
make dev-down        # tear down + delete volumes
make dev-logs        # tail compose logs
make migrate         # run migrations against LOCAL_DSN
make k8s-deploy      # apply k8s/ manifests
make scale svc=crawler n=3
make logs svc=api    # tail k8s pod logs for one service
```

Single-test patterns (no Makefile target — use `go test` directly):

```bash
go test ./internal/processor -run TestDeduplicator -race
go test ./pkg/redis -run TestRateLimiter_Allow -v
```

Run a single binary against the docker-compose infra:

```bash
go run ./cmd/api
go run ./cmd/crawler
go run ./cmd/processor
go run ./cmd/scheduler
```

## Architecture

### Four-binary pipeline

The system is split into four independent binaries under [cmd/](cmd/) that
coordinate **only** through Kafka and shared backing stores. They never call
each other directly.

```
scheduler ──▶ crawl.queue ──▶ crawler ──▶ jobs.raw ──▶ processor ──▶ Postgres ──▶ api
                                                       │
                                                       └──▶ jobs.failed (DLQ)
```

| Binary      | Purpose                                                                |
|-------------|------------------------------------------------------------------------|
| `scheduler` | Cron-only. Publishes `CrawlTask` messages — does no crawling itself.   |
| `crawler`   | Consumes `crawl.queue`, fetches+parses listing/detail pages, publishes `RawJob`. |
| `processor` | Consumes `jobs.raw` → normalize → dedup → Postgres → alert match.      |
| `api`       | chi REST API; reads from Postgres via a Redis-cached service layer.    |

Kafka topic names live in [pkg/kafka/topics.go](pkg/kafka/topics.go) — always
import these constants, never hardcode strings.

### Source plug-in model

Every crawl source implements [`crawler.Source`](internal/crawler/source.go) —
`Name() / ParseListing() / ParseDetail()`. Sources self-register in
[internal/crawler/registry.go](internal/crawler/registry.go) via
`NewRegistry()`. **To add a new source:** implement `Source`, add a line to
`NewRegistry()`, add a constant to [internal/domain/source.go](internal/domain/source.go),
and (if it should auto-crawl) add a cron entry in
[internal/scheduler/scheduler.go](internal/scheduler/scheduler.go). The
registry returns an error for unknown sources rather than silently skipping —
this is intentional to surface scheduler/registry drift.

### Layered API service

The API uses a strict layering: `handler → service → repository`. The handler
takes a `service.JobService` interface, not a concrete type, which is why
[cmd/api/main.go](cmd/api/main.go) wraps `PostgresJobService` in
`CachedJobService` (Redis read-through cache) before injection. When adding new
read paths, decorate at this layer rather than scattering cache logic into
handlers or repositories.

### Router middleware order

Defined in [internal/handler/router.go](internal/handler/router.go) and the
ordering matters — request flows top→bottom, response bottom→top:
RequestID → RealIP → Tracing → Logger → Metrics → Recoverer → CORS → Timeout →
RateLimit. `/health`, `/ready`, and `/metrics` are mounted **before**
`/api/v1`, so they bypass rate limiting.

### Graceful shutdown contract

[cmd/api/main.go](cmd/api/main.go) registers shutdown hooks with
[pkg/shutdown](pkg/shutdown/shutdown.go) in a deliberate order: flip the
readiness flag (with a 2s sleep so K8s endpoints update) → stop HTTP server →
flush tracer → close Postgres → close Redis. Preserve this ordering when
adding new resources; the readiness-flag-first step is what prevents
in-flight 5xx responses during rolling deploys.

### Configuration

Single root `Config` struct in [config/config.go](config/config.go) loaded by
viper from env vars (or `.env` via `subosito/gotenv`). Every binary loads the
same config and only uses the sub-structs it needs (`,squash` mapstructure
tag). `.env.example` documents every supported variable.

### Domain model

[internal/domain/](internal/domain/) holds the canonical types — `Job`,
`Company`, `Location`, `JobSource`, `RemoteType`, `CrawlTask`, `Alert`. The
`Job.Fingerprint` is a SHA-256 of normalized title+company+location and is the
key the processor's `Deduplicator` uses to coalesce duplicates across sources.

## Branching

This repo uses **git-flow**. Default branch is `develop`; `main` is reserved
for production releases. Create feature branches with `git flow feature start
<name>`.

## Conventions worth knowing

- **Logging**: structured `log/slog`. Use `slog.String("error", err.Error())`
  rather than `slog.Error(err)` — see existing call sites.
- **Errors from registry**: a missing source crawler returns an error rather
  than no-op'ing — preserve this behavior.
- **Phase markers**: comments like `// Phase 06` / `// Phase 07` in router
  middleware refer to project rollout phases, not feature flags. Don't strip
  them.
- **Migrations** run automatically on API startup
  ([cmd/api/main.go](cmd/api/main.go) `database.RunMigrations`). Add new
  migrations to [migrations/](migrations/) following the
  `NNNNNN_name.up.sql` / `.down.sql` convention used by `golang-migrate`.

## Companion files

- [CLAUDE.md](CLAUDE.md) — same guidance, loaded automatically by Claude Code.
- [README.md](README.md) — human-facing setup & usage docs.
