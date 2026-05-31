# Hosting JobCrawler on a vServer

Step-by-step guide to run JobCrawler on a self-hosted server with:

- **Public access** via ngrok (API + web frontend on one URL)
- **Push-to-deploy**: push to `main` → GitHub Actions builds images → GHCR →
  Watchtower auto-pulls and restarts on the server
- **Bounded storage**: a hard job cap + nightly retention cleanup

The server runs a trimmed stack ([docker-compose.prod.yml](docker-compose.prod.yml)):
Postgres, Redis, Zookeeper, Kafka, the 4 app services, ngrok and Watchtower.
Elasticsearch and the observability UIs (Kibana, Kafka-UI, RedisInsight) are **not**
run on the server — search is Postgres-backed in Phase 1.

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
4. **Access for the server:**
   - *Easiest:* open each package's settings → **Change visibility → Public**. No
     login needed on the server.
   - *Private:* create a GitHub **Personal Access Token** (classic) with
     `read:packages`, then on the server run:
     ```bash
     echo "<TOKEN>" | docker login ghcr.io -u qbert18 --password-stdin
     ```
     This writes `~/.docker/config.json`, which the Watchtower service mounts to
     pull updates.

> The repo is git-flow: `develop` is the default branch, `main` is for releases.
> Deploys happen when you push/merge to `main`.

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

# Storage limits (tune to your disk)
MAX_TOTAL_JOBS=50000          # 0 = unlimited; processor pauses inserts at this count
CLEANUP_ENABLED=true
CLEANUP_SCHEDULE=0 3 * * *     # nightly 03:00
CLEANUP_RETENTION_DAYS=30      # delete jobs older than 30 days
```

---

## 4. Launch

```bash
docker compose -f docker-compose.prod.yml --env-file .env up -d
docker compose -f docker-compose.prod.yml ps
```

Migrations run automatically on api/processor startup.

---

## 5. Get the public URL

```bash
# Either read the ngrok inspector API…
curl -s http://127.0.0.1:4040/api/tunnels | grep -o '"public_url":"[^"]*' | head -1
# …or check the logs:
docker logs jobcrawler-ngrok
```

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
docker compose -f docker-compose.prod.yml --env-file .env up -d jobcrawler-processor
```

**Logs**

```bash
docker compose -f docker-compose.prod.yml logs -f jobcrawler-processor
docker compose -f docker-compose.prod.yml logs -f jobcrawler-crawler
```

**Manual cleanup / inspect counts** (psql in the Postgres container)

```bash
docker exec -it jobcrawler-postgres psql -U jobcrawler -d jobcrawler \
  -c "SELECT count(*) FROM jobs;"
docker exec -it jobcrawler-postgres psql -U jobcrawler -d jobcrawler \
  -c "DELETE FROM jobs WHERE created_at < NOW() - INTERVAL '7 days';"
```

**Reach the observability UIs (not exposed publicly)** — SSH-tunnel from your laptop
if you ever start them in the dev stack, e.g. `ssh -L 8081:localhost:8081 user@server`.

**Stop / tear down**

```bash
docker compose -f docker-compose.prod.yml down            # stop, keep data
docker compose -f docker-compose.prod.yml down -v         # stop + delete volumes (DESTROYS DB)
```
