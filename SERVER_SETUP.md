# Hosting JobCrawler on a vServer

Step-by-step guide to run JobCrawler on a self-hosted server with:

- **Public access** via ngrok (API + web frontend on one URL)
- **Push-to-deploy**: push to `main` → GitHub Actions builds images → GHCR →
  Watchtower auto-pulls and restarts on the server
- **Bounded storage**: a hard job cap + nightly retention cleanup

The server and local dev share **one** [docker-compose.yml](docker-compose.yml),
split by Docker profile. The `prod` profile runs the server stack — Postgres,
Redis, Zookeeper, Kafka, the 4 app services, ngrok and Watchtower. The
observability UIs (Elasticsearch, Kibana, RedisInsight) live in the `dev`
profile and are **not** started on the server — search is Postgres-backed in
Phase 1. Everything env-specific (restart policy, log level, image tag, Kafka
retention, …) is driven by the server's `.env`.

> Replace `qbert18` in image paths below if your GitHub owner differs. GHCR image
> names are always lowercase.

---

## 1. Prerequisites (on the vServer)

Ubuntu/Debian. Install Docker Engine + the compose plugin:

```bash
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker "$USER"   # then log out/in so the group applies
docker version && docker compose version
```

Minimum sizing: ~2 GB RAM (Kafka+Postgres are the heavy parts; mem limits are set
in the compose file). 2 vCPU and ~10 GB disk is comfortable.

---

## 2. GitHub side (one-time)

1. Push this repo to GitHub. The workflow [.github/workflows/build-and-push.yml](.github/workflows/build-and-push.yml)
   triggers on every push to `main` and on manual dispatch.
2. Merge/push to `main` once. In the repo's **Actions** tab confirm the
   "Build and push images" run succeeds (4 images: api, crawler, processor, scheduler).
3. The images appear under your profile **Packages** as
   `ghcr.io/qbert18/jobcrawler-<service>`.
4. **Access for the server — make the packages public (recommended):**
   open each package's settings → **Change visibility → Public**. Then neither the
   server nor Watchtower needs any registry login — the prod compose mounts no docker
   credentials.
   - *Private alternative:* create a GitHub **Personal Access Token** (classic) with
     `read:packages` and `docker login ghcr.io -u qbert18` on the server, then add a
     credentials mount to the `watchtower` service —
     `${HOME}/.docker:/config:ro` plus env `DOCKER_CONFIG=/config`. (Do **not**
     bind-mount the single `config.json` file — if it's missing Docker creates it as
     a directory and breaks the docker CLI.)

> `main` is the only long-lived branch and the default. Deploys happen when you
> push to `main`.

---

## 3. Bootstrap the server

```bash
git clone https://github.com/QBERT18/jobcrawler.git
cd jobcrawler
cp .env.example .env
```

Edit `.env` and set at least:

```ini
NGROK_AUTHTOKEN=<from https://dashboard.ngrok.com/get-started/your-authtoken>
POSTGRES_PASSWORD=<a strong password>

# Production overrides (the compose defaults are dev-friendly — these flip it to
# server behaviour). Without these the stack would run with restart=no,
# ENV=development, debug logging and try to reach a non-existent Elasticsearch.
RESTART_POLICY=unless-stopped  # auto-restart containers (dev default: no)
APP_ENV=production
LOG_LEVEL=info                 # dev default: debug
KAFKA_LOG_RETENTION_HOURS=6    # trim the transient queue to save disk (dev: 24)
ES_ADDRESSES=                  # empty: Postgres-backed search, no ES on the server
IMAGE_TAG=latest               # GHCR tag Watchtower tracks

# Storage limits (tune to your disk)
MAX_TOTAL_JOBS=50000          # 0 = unlimited; processor pauses inserts at this count
CLEANUP_ENABLED=true
CLEANUP_SCHEDULE=0 3 * * *     # nightly 03:00
CLEANUP_RETENTION_DAYS=30      # delete jobs older than 30 days
```

---

## 4. Launch

One [docker-compose.yml](docker-compose.yml) drives both environments via Docker
profiles — pick the matching command.

### ▶ Server (this guide)

Pull prebuilt images from GHCR + start ngrok and Watchtower:

```bash
docker compose --profile prod --env-file .env up -d
```

### ▶ Local development

Build the app images from source + start the observability UIs:

```bash
docker compose --profile dev up -d --build
```

### 🔄 Apply compose changes to a running stack

After editing `docker-compose.yml` or `.env`, re-run `up -d` — Compose
recreates **only** the services whose config changed. To force every container
to be recreated regardless, add `--force-recreate`:

```bash
# Recreate only what changed (normal case)
docker compose --profile prod --env-file .env up -d

# Force-recreate every container (picks up any change, e.g. env tweaks)
docker compose --profile prod --env-file .env up -d --force-recreate
```

> Use the same `--profile dev` / `--profile prod` you launched with. Add
> `--build` on dev if you also changed app source.

Then check status:

```bash
docker compose --profile prod ps
```

The `--profile prod` flag starts the core services plus ngrok and Watchtower,
and (no `--build`) pulls the prebuilt app images from GHCR. Migrations run
automatically on api/processor startup.

---

## 5. Get the public URL

```bash
docker logs jobcrawler-ngrok | grep -o 'url=https://[^ ]*' | tail -1
# or just read the full log:
docker logs jobcrawler-ngrok
```

> The ngrok web inspector is published on host port 4040 — browse requests at
> `http://localhost:4040` (SSH-tunnel from your laptop with
> `ssh -L 4040:localhost:4040 user@server` if the server is remote). If a
> host-run ngrok already owns 4040, change the host side of the mapping in
> [docker-compose.yml](docker-compose.yml) (e.g. `"4041:4040"`).
>
> The ngrok service runs with `--pooling-enabled`, so if a previous session is
> still online on the same endpoint it load-balances instead of failing with
> `ERR_NGROK_334`.

You'll get an `https://<random>.ngrok-free.app` URL.

---

## 6. Verify

- Open the ngrok URL in a browser → the **job browser UI** loads and lists jobs
  (it may be empty until the first crawl completes; the scheduler enqueues
  immediately on startup, then on its cron cadence).
- API health: `curl https://<your-url>.ngrok-free.app/health` → `200`.
- API directly: `curl "https://<your-url>.ngrok-free.app/api/v1/jobs?per_page=5"`.

---

## 7. Auto-update flow (push-to-deploy)

```
push to main → GitHub Actions builds 4 images → pushed to GHCR :latest
            → Watchtower (polls every 60s) detects new :latest
            → pulls + recreates the 4 app containers → done
```

Only the 4 app services carry the `com.centurylinklabs.watchtower.enable=true`
label, so infra (Postgres/Redis/Kafka) is never auto-restarted. Watch it work:

```bash
docker logs -f jobcrawler-watchtower
```

---

## 8. Operations

**How the limits behave**
- **Hard cap (`MAX_TOTAL_JOBS`)**: when the jobs table reaches the cap, the
  processor logs `job cap reached — skipping insert` and stops inserting. It keeps
  draining Kafka (no backlog). It's *self-healing*: once the nightly cleanup drops
  the count below the cap, inserts resume automatically (within ~60s). `0` disables.
- **Retention (`CLEANUP_*`)**: the processor runs a cron that deletes jobs older
  than `CLEANUP_RETENTION_DAYS`. Disable with `CLEANUP_ENABLED=false`.
- Kafka topics self-trim via `KAFKA_LOG_RETENTION_HOURS=6` in the prod compose.

**Change limits later**

```bash
nano .env
docker compose --profile prod --env-file .env up -d jobcrawler-processor
```

**Logs**

```bash
docker compose --profile prod logs -f jobcrawler-processor
docker compose --profile prod logs -f jobcrawler-crawler
```

**Manual cleanup / inspect counts** (psql in the Postgres container)

```bash
docker exec -it jobcrawler-postgres psql -U jobcrawler -d jobcrawler \
  -c "SELECT count(*) FROM jobs;"
docker exec -it jobcrawler-postgres psql -U jobcrawler -d jobcrawler \
  -c "DELETE FROM jobs WHERE created_at < NOW() - INTERVAL '7 days';"
```

**Reach the observability UIs (not exposed publicly)** — they live in the `dev`
profile and aren't started on the server. If you ever start one
(`docker compose --profile dev up -d redisinsight`), SSH-tunnel to it from your
laptop, e.g. `ssh -L 5540:localhost:5540 user@server`.

**Stop / tear down**

```bash
docker compose --profile prod down            # stop, keep data
docker compose --profile prod down -v         # stop + delete volumes (DESTROYS DB)
```
