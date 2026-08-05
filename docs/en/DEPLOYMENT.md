# Deployment Guide (dev / prod)

After pushing to `dev` / `main`, CI automatically: **Build image → Push to GHCR → SSH to server and sync the compose file → `pull && up -d --wait` → retag the local `dev-latest` / `latest` image and prune images older than 7 days**. On deployment failure, CI dumps the status and log tails of all 5 services — no manual SSH log digging needed.

CI **only builds and deploys**; it does not generate secrets or config. `.env` and `{dev,prod}-config.yaml` are prepared and maintained by a human on the server.

> dev and prod are independent: different branches, image tags, servers, and compose projects. See comparison table at the end.

---

## 1. Architecture

```
mariadb  ─┐
redis   ──┼─ app(8000)
          ├─ worker (celery -Q default)
          └─ beat   (celery scheduler)
```

`enable_embedded_celery: false` — worker/beat each run in their own container.

- **dev**: mariadb/redis ports exposed (`13306`/`16379`) for external access
- **prod**: mariadb/redis bind to loopback `127.0.0.1` only; external access requires SSH tunnel

dev container panel: <https://testpanel.neonexus.top/panel>

prod container panel: <https://panel.bionet.pro/login>

---

## 2. One-Time Server Preparation

Commands are structurally identical for dev / prod; only names differ. Run once on each machine.

```bash
# Set environment variable first (modify based on actual environment)
export ENV_NAME=dev   # or prod

# 1) Create directories
sudo mkdir -p /opt/kawaru-${ENV_NAME}/data/{mariadb,redis,logs/{app,worker,beat}}
cd /opt/kawaru-${ENV_NAME}

# 2) Hand over top-level to SSH user (CD uses this user to scp compose files)
sudo chown -R "$USER":"$USER" /opt/kawaru-${ENV_NAME}

# 3) app/worker/beat run as appuser (uid 1000)
sudo chown -R 1000:1000 /opt/kawaru-${ENV_NAME}/data/logs

# 4) Place config files
#    .env must contain CONFIG_PATH=/app/config/${ENV_NAME}-config.yaml
#    Config file named ${ENV_NAME}-config.yaml
```

**Directory structure example:**

```
/opt/kawaru-${ENV_NAME}/
├── .env                    # Environment variables, contains CONFIG_PATH
├── ${ENV_NAME}-config.yaml # Application config
├── docker-compose.${ENV_NAME}.yml  # Auto-synced by CD
└── data/
    ├── mariadb/
    ├── redis/
    └── logs/
        ├── app/
        ├── worker/
        └── beat/
```

After this, every push to `dev` / `main` triggers a rolling upgrade. Database, Redis, and log data remain on the host; business files remain in OSS.

---

## 3. Persistent Storage

| Content | Container Path | Host Path |
|---------|---------------|-----------|
| MariaDB | `/var/lib/mysql` | `data/mariadb` |
| Redis (AOF) | `/data` | `data/redis` |
| Logs (per service) | `/app/kawaru_logs` | `data/logs/{app,worker,beat}` |

Logs are split by service to avoid `RotatingFileHandler` conflicts from concurrent writes.

Application containers do not mount a business-file directory. Uploaded ZIPs, extracted imZML/ibd files, images, download archives, and processing Zarr outputs are all stored in OSS.

---

## 4. OSS and FC Prerequisites

The service is OSS-only. Complete this checklist before the first deployment:

- In `{dev,prod}-config.yaml`, set the OSS `region_id`, `bucket_name`, the four object roots (`oss_zip_storage_root`, `oss_packup_zip_storage_root`, `oss_origin_storage_root`, `oss_processed_storage_root`), and all six FC endpoints/timeouts.
- In `.env`, set `OSS_ACCESS_KEY_ID`, `OSS_ACCESS_KEY_SECRET`, `OSS_ROLE_ARN`, all six `OSS_FC_*_TOKEN` values (packup, algorithm, TIC, feature, raw-convert, clustering), and `FC_CALLBACK_TOKEN`.
- Grant the long-lived OSS identity only the permissions the backend needs (including assuming the configured role and managing/tagging its objects). Configure the RAM role so the application can issue the generated path-scoped upload/download STS policies to clients.
- Configure the OSS upload event to invoke the extraction FC function. It must write imZML/ibd under the configured origin root and call `/callback/oss_upload_callback` with the same `FC_CALLBACK_TOKEN` in `X-FC-Token`.
- Configure every other FC function to call its matching `/callback/*` route with `X-FC-Token`. The callback URL must be reachable from FC, and app/worker containers must have outbound access to OSS and all FC endpoints.
- Configure an OSS lifecycle rule to remove abandoned objects that never receive the `persistent=true` tag. Do not expire tagged source/extracted objects with that rule.

Default object placement is controlled by configuration rather than host directories:

| Content | OSS location |
|---------|--------------|
| Uploaded ZIP | `oss_zip_storage_root` |
| FC-created download ZIP | `oss_packup_zip_storage_root` |
| Extracted imZML/ibd | `oss_origin_storage_root` |
| Algorithm/raw-convert/clustering Zarr | `oss_processed_storage_root` |
| TIC/feature images | `images/file_{file_id}/` |

> A healthy `/docs` endpoint only proves that the API and its backing services started. Before production traffic, run one end-to-end smoke test: preflight → STS upload → extraction callback → RAW signed download. Also verify one processing callback and one packup callback if those features are enabled for clients.

---

## 5. Common Operations

**Dev proxy note:** For dev, enable proxy before triggering tasks and disable after; then restart containers. Prod doesn't need this.
See: <https://github.com/nelvko/clash-for-linux-install>

```bash
# Set environment variables first (modify based on actual environment)
export ENV_NAME=dev   # or prod
export APP_IMAGE=ghcr.io/neonexusx/kawaru:dev-latest   # dev
# export APP_IMAGE=ghcr.io/neonexusx/kawaru:latest     # prod

cd /opt/kawaru-${ENV_NAME}

# View config / passwords
cat .env

# View admin password (printed on first startup, not repeated on restart)
sudo grep -rE "create admin user|user already exists" /opt/kawaru-${ENV_NAME}/data/logs/app/

# View logs (change service name to app / beat / mariadb / redis)
docker compose -f docker-compose.${ENV_NAME}.yml -p kawaru-${ENV_NAME} logs -f worker

# Rebuild after config change
docker compose -f docker-compose.${ENV_NAME}.yml -p kawaru-${ENV_NAME} up -d

# Verify worker connected to broker (one-time check, don't add to healthcheck)
docker compose -f docker-compose.${ENV_NAME}.yml -p kawaru-${ENV_NAME} exec -T worker \
  celery -A celery_runtime.entrypoint:celery_app inspect ping --timeout 30
```

**External DB/Redis access:**

```bash
# dev: connect directly to server IP
mysql -h <HOST> -P 13306 -uroot -p
redis-cli -h <HOST> -p 16379 -a <REDIS_PASSWORD> ping

# prod: SSH tunnel first, then connect locally
ssh -L 13306:127.0.0.1:13306 -L 16379:127.0.0.1:16379 <PROD_SSH_USER>@<HOST>
mysql -h 127.0.0.1 -P 13306 -uroot -p
redis-cli -h 127.0.0.1 -p 16379 -a <REDIS_PASSWORD> ping
# DB/Redis passwords are in .env
```

---

## 6. Healthcheck

Only `app` + `mariadb` + `redis` have healthchecks. worker/beat do not (beat doesn't respond to ping; worker ping cold-starts the entire app and times out). Both are kept alive by `restart: unless-stopped`.

---

## 7. GitHub Secrets

| Name | Purpose |
|------|---------|
| `DEV_SSH_HOST/USER/KEY` | Dev server |
| `PROD_SSH_HOST/USER/KEY` | Prod server (separate) |
| `GHCR_PULL_USER/TOKEN` | Pull private images from GHCR (shared) |

Business credentials (DB/Redis/SMTP/OSS/FC) are in the server's `.env`, **not in GitHub Secrets**.

---

## 8. Dev / Prod Comparison

| Dimension | Dev | Prod |
|-----------|-----|------|
| Trigger branch | `dev` | `main` |
| Workflow | [dev-cd.yml](../../.github/workflows/dev-cd.yml) | [prod-cd.yml](../../.github/workflows/prod-cd.yml) |
| Image tag | `dev-latest` + `dev-<sha>` | `latest` + `sha-<sha>` |
| Server secrets | `DEV_SSH_*` | `PROD_SSH_*` |
| Concurrency | `cancel-in-progress: true` | `cancel-in-progress: false` |
