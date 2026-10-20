# Deployment Guide (dev / prod)

After pushing to `dev` / `main`, CD automatically: **Build image → Push to ACR → SSH to server and sync the compose file → Backup database (prod) → Start base services → Run migrate container upgrade → `up -d --wait` application containers → retag the local `dev-latest` / `latest` image and prune images older than 7 days**. On deployment failure, CD dumps the status and log tails of all 6 services (including migrate) — no manual SSH log digging needed.

> **Images live in Alibaba Cloud ACR (Hong Kong), not GHCR**: the private repository is `crpi-y6a776c9l4k3agmh.cn-hongkong.personal.cr.aliyuncs.com/kawaru/kawaru`. Use the public domain on GitHub runners and on the dev server (Hangzhou); on the prod server (Hong Kong) use the `-vpc` intranet domain. Docker authenticates per domain, so log in with the same domain you pull from. ACR Personal Edition rejects OCI attestation manifests, so the workflows pin `provenance: false` + `sbom: false`.

CD **only builds and deploys**; it does not generate secrets or config. `.env` and `{dev,prod}-config.yaml` are prepared and maintained by a human on the server.

> dev and prod are independent: different branches, image tags, servers, and compose projects. See comparison table at the end.

---

## 1. Architecture (Celery Split Mode + Dedicated Migrate Container)

```
mariadb  ─┐
redis   ──┼─ migrate (one-shot upgrade) ──> app(8000)
          ├─ worker (celery -Q default)
          └─ beat   (celery scheduler)
```

- `enable_embedded_celery: false` — worker/beat each run in their own container.
- Schema migrations are run by a dedicated one-shot `migrate` container; `app`, `worker`, and `beat` explicitly depend on `migrate` completing successfully (`condition: service_completed_successfully`).
- Port: app `8000`

**dev vs prod key differences:**

| Item | Dev | Prod |
|------|-----|------|
| mariadb/redis ports | Exposed on `0.0.0.0:13306/16379` (for external testing) | Bound to loopback `127.0.0.1:13306/16379` only (not exposed to internet) |
| External access | Direct server IP | Requires SSH tunnel |

dev container panel: <https://testpanel.neonexus.top/panel>

prod container panel: <https://panel.bionet.pro/login>

---

## 2. One-Time Server Preparation

Commands are structurally identical for dev / prod; only names differ. Run once on each machine.

```bash
# Set environment variable first (modify based on actual environment)
export ENV_NAME=dev   # or prod

# 1) Create directories (prod automatically stores database snapshots in data/backups)
sudo mkdir -p /opt/kawaru-${ENV_NAME}/data/{mariadb,redis,backups,logs/{app,worker,beat}}
cd /opt/kawaru-${ENV_NAME}

# 2) Hand over top-level to SSH user (CD uses this user to scp compose files)
sudo chown -R "$USER":"$USER" /opt/kawaru-${ENV_NAME}

# 3) app/worker/beat run as appuser (uid 1000)
sudo chown -R 1000:1000 /opt/kawaru-${ENV_NAME}/data/logs

# 4) Place config files
#    .env must contain CONFIG_PATH=/app/config/${ENV_NAME}-config.yaml
#    Config file named ${ENV_NAME}-config.yaml
```

Container configuration must use Compose service addresses: `database.host=mariadb`, `database.port=3306`, `database.db_name=kawaru`, `redis.host=redis`, and `redis.port=6379`; keep `celery.enable_embedded_celery=false`. Host mappings `13306` / `16379` are only for access from outside the Compose network and must not be used by application containers.

**Directory structure example:**

```
/opt/kawaru-${ENV_NAME}/
├── .env                    # Environment variables, contains CONFIG_PATH
├── ${ENV_NAME}-config.yaml # Application config
├── docker-compose.${ENV_NAME}.yml  # Auto-synced by CD
└── data/
    ├── mariadb/
    ├── redis/
    ├── backups/            # prod automated pre-deployment DB backups
    └── logs/
        ├── app/
        ├── worker/
        └── beat/
```

After this, every push to `dev` / `main` pulls the new image and recreates the corresponding application containers. Database, Redis, and log data remain on the host; business files remain in OSS.

The server `.env` must cover both Compose and application startup: `DATABASE_PASSWORD`, `REDIS_PASSWORD`, `SECRET_KEY`, `SMTP_PASSWORD`, `CONFIG_PATH`, `REGION_PATH`, `LOG_DIR`, `LOG_LEVEL`, OSS credentials, FC invocation tokens, and `FC_CALLBACK_TOKEN`. `APP_IMAGE` is deliberately not stored in `.env`: CD injects the commit-specific image through the SSH environment, which takes precedence over `.env`. Before running Compose manually you must `export APP_IMAGE` as shown in section 5 — otherwise the `${APP_IMAGE:?}` check aborts immediately instead of silently using a wrong image.

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

- In `{dev,prod}-config.yaml`, set the OSS `region_id`, `bucket_name`, the object roots (`oss_zip_storage_root`, `oss_origin_storage_root`, `oss_processed_storage_root`), and the FC endpoints/timeouts.
- In `.env`, set `OSS_ACCESS_KEY_ID`, `OSS_ACCESS_KEY_SECRET`, `OSS_ROLE_ARN`, all five `OSS_FC_*_TOKEN` values (algorithm, TIC, feature, raw-convert, clustering), and `FC_CALLBACK_TOKEN`.
- Grant the long-lived OSS identity only the permissions the backend needs (including assuming the configured role and managing/tagging its objects). Configure the RAM role so the application can issue the generated path-scoped upload/download STS policies to clients.
- Configure the OSS upload event to invoke the extraction FC function. Uploaded keys use `{oss_zip_storage_root}/{file_verify_code}/{file_id}.zip`; FC must parse `file_id` from the file name, write imZML/ibd below `{oss_origin_storage_root}/{file_verify_code}/{file_id}/`, and call `/callback/oss_upload_callback` with `file_id` and the same `FC_CALLBACK_TOKEN` in `X-FC-Token`.
- Configure every other FC function to call its matching `/callback/*` route with `X-FC-Token`. Feature/UMAP results use `/callback/umap_callback`, TIC uses `/callback/tic_callback`, and `image_path` must be the preview directory key ending in `/` (e.g. `images/{public_id}/`), not a full object key. The callback URL must be reachable from FC, and app/worker containers must have outbound access to OSS, STS, and all FC endpoints (the app process issues STS credentials directly).
- Configure an OSS lifecycle rule to remove abandoned objects that never receive the `persistent=true` tag. Do not expire tagged source/extracted objects with that rule.

Default object placement is controlled by configuration rather than host directories:

| Content | OSS location |
|---------|--------------|
| Uploaded ZIP | `{oss_zip_storage_root}/{file_verify_code}/{file_id}.zip` |
| Extracted imZML/ibd | `{oss_origin_storage_root}/{file_verify_code}/{file_id}/{original_name}.imzML/.ibd` |
| Algorithm/raw-convert Zarr | `{oss_processed_storage_root}/run_{run_id}.zarr/` |
| Clustering result | Written back into the run's Zarr; no second output directory |
| TIC/UMAP preview | `images/{public_id}/` directory (the FC callback supplies this directory key ending in `/`; TIC writes `preview.jpg`, UMAP writes `preview.jpg`/`preview_2.jpg`/`preview_3.jpg`) |

> A healthy `/docs` endpoint only proves that the API and its backing services started. Before production traffic, run one end-to-end smoke test: preflight → STS upload → extraction callback → RAW signed download. Also verify one relay download and one processing callback if those features are enabled for clients.

---

## 5. Common Operations

**Dev proxy note:** For dev, enable proxy before triggering tasks and disable after; then restart containers. Prod doesn't need this.
See: <https://github.com/nelvko/clash-for-linux-install>

```bash
# Set environment variables first (modify based on actual environment)
export ENV_NAME=dev   # or prod
export APP_IMAGE=crpi-y6a776c9l4k3agmh.cn-hongkong.personal.cr.aliyuncs.com/kawaru/kawaru:dev-latest    # dev
# export APP_IMAGE=crpi-y6a776c9l4k3agmh-vpc.cn-hongkong.personal.cr.aliyuncs.com/kawaru/kawaru:latest  # prod

cd /opt/kawaru-${ENV_NAME}

# View config / passwords
cat .env

# View admin password (printed on first startup, not repeated on restart)
sudo grep -rE "create admin user|user already exists" /opt/kawaru-${ENV_NAME}/data/logs/app/

# View logs (change service name to app / beat / mariadb / redis / migrate)
docker compose -f docker-compose.${ENV_NAME}.yml -p kawaru-${ENV_NAME} logs -f worker

# Force application-container recreation after changing .env / ${ENV_NAME}-config.yaml
docker compose -f docker-compose.${ENV_NAME}.yml -p kawaru-${ENV_NAME} up -d --force-recreate app worker beat

# Verify worker connected to broker (one-time check, don't add to healthcheck)
docker compose -f docker-compose.${ENV_NAME}.yml -p kawaru-${ENV_NAME} exec -T worker \
  celery -A celery_runtime.entrypoint:celery_app inspect ping --timeout 30

# Manually run database migration and verification (see [Database Migration Guide](../zh/数据库迁移指南.md))
docker compose -f docker-compose.${ENV_NAME}.yml -p kawaru-${ENV_NAME} run --rm migrate \
  python -m database.migration upgrade
docker compose -f docker-compose.${ENV_NAME}.yml -p kawaru-${ENV_NAME} run --rm migrate \
  python -m database.migration verify
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
| `ACR_USERNAME/ACR_PASSWORD` | ACR login for image push (CI) and for the post-SSH pull (dev / prod shared) |
| `OSS_ACCESS_KEY_ID/SECRET` and `OSS_ROLE_ARN` | CI integration-test access to the test bucket and STS role |
| `SMTP_PASSWORD` | CI concurrent-signup and password-reset tests |

Dev/prod runtime credentials for DB, Redis, OSS, FC, and SMTP stay in each server's `.env`. The OSS/SMTP values in GitHub Secrets are only for CI test resources. See [secrets.example.yaml](../../secrets.example.yaml) for the complete checklist.

---

## 8. Dev / Prod Comparison

| Dimension | Dev | Prod |
|-----------|-----|------|
| Trigger branch | `dev` | `main` |
| Workflow | [dev-cd.yml](../../.github/workflows/dev-cd.yml) | [prod-cd.yml](../../.github/workflows/prod-cd.yml) |
| Image tag | `dev-latest` + `dev-<sha>` | `latest` + `sha-<sha>` |
| Image registry | ACR Hong Kong `kawaru/kawaru`, pull via public domain | Same, pull via `-vpc` intranet domain |
| Server secrets | `DEV_SSH_*` | `PROD_SSH_*` |
| Concurrency | `cancel-in-progress: false` | `cancel-in-progress: false` |
