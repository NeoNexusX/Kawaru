# Kawaru

Backend Server for SpatialXomics

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

**[English](README.md)** | **[中文](README_zh.md)**

---

Kawaru is a FastAPI-based backend server project that provides backend support for SpatialXomics. Built with Python, it supports file management, experiment data processing, user authentication, and more.

## Requirements

- Python 3.12+
- [uv](https://github.com/astral-sh/uv) package manager
- Reachable MariaDB and Redis instances (local install or Docker)
- An Aliyun OSS bucket, a RAM role that can issue scoped STS credentials, and the configured Function Compute (FC) functions

## Quick Start

### 1. Clone the Project

```bash
git clone https://github.com/NeoNexusX/Kawaru.git
cd Kawaru
```

### 2. Install Dependencies

The project uses `uv` for dependency management. First install `uv` (if not already installed):

```bash
# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
```

Then sync the dependency environment:

```bash
uv sync
```

Activate the virtual environment:

```bash
# macOS/Linux
source .venv/bin/activate

# Windows
.venv\Scripts\activate
```

### 3. Prepare MariaDB and Redis

The application connects to MariaDB and Redis at startup. If you don't have local instances, spin up throwaway ones with Docker:

```bash
docker run -d --name kawaru-mariadb -p 13306:3306 \
  -e MARIADB_ROOT_PASSWORD=your_db_password \
  -e MARIADB_DATABASE=kawaru mariadb:11.4

docker run -d --name kawaru-redis -p 16379:6379 \
  redis:8.4-alpine redis-server --requirepass your_redis_password
```

Connection settings (host / port / db name) live in the `database:` / `redis:` sections of `config.yaml`; passwords go in `.env` (next step).

### 4. Configure Environment Variables

```bash
# Copy the example file
cp .env_example .env
```

Core runtime variables are checked at startup, and OSS initialization/callback registration require their own credentials. Feature-specific FC tokens are consumed when the corresponding task runs, so a healthy startup does not prove every FC integration is configured. Key entries:

| Variable | Purpose |
|----------|---------|
| `DATABASE_PASSWORD` | MariaDB password (connects as root) |
| `REDIS_PASSWORD` | Redis password |
| `SECRET_KEY` | Stable JWT signing key. Set it explicitly outside the disposable test stack; otherwise a random key is generated on every process start and existing tokens become invalid after restart. |
| `SMTP_PASSWORD` | SMTP login password (server / port / account / sender are configured in `config.yaml` → `email:`) |
| `OSS_ACCESS_KEY_ID` / `OSS_ACCESS_KEY_SECRET` / `OSS_ROLE_ARN` | Aliyun OSS credentials and the RAM role used to issue scoped upload/download STS credentials |
| `OSS_FC_ALGORITHM_TOKEN` / `OSS_FC_TIC_TOKEN` / `OSS_FC_FEATURE_TOKEN` / `OSS_FC_RAW_CONVERT_TOKEN` / `OSS_FC_CLUSTERING_TOKEN` | Tokens used by the worker to invoke the configured FC functions |
| `FC_CALLBACK_TOKEN` | Shared token required in the `X-FC-Token` header of every `/callback/*` request |
| `CONFIG_PATH` / `REGION_PATH` | Paths to the app config and country/region validation dataset |
| `LOG_DIR` / `LOG_LEVEL` | Log directory and level |
| `BASE_URL` / `TEST_USER` / `TEST_PWD` | Only used when running pytest locally (see [Contributing](docs/en/CONTRIBUTING.md)) |

### 5. Configure Server Parameters

```bash
cp config_example.yaml config.yaml
```

Edit `config.yaml`, focusing on:

- `database:` / `redis:` — point to the instances prepared in step 3. For the commands above, use database `kawaru`, host `127.0.0.1`, and ports `13306` / `16379`.
- `email:` — SMTP server, port, account, sender
- `oss:` — required bucket, region, storage roots, and FC endpoints; file storage is OSS-only

The OSS identity used by the application must be able to assume the configured RAM role and perform the required object operations; the role must support the scoped upload/download policies issued to clients. Configure an OSS event trigger for the upload ZIP extraction function and a lifecycle rule that removes abandoned temporary objects without the `persistent=true` tag while retaining tagged source and extracted objects.

### 6. Initialize / Upgrade Database Schema

The application verifies the database schema version against the code head on startup using Schema Guard. Before initial startup or after code updates, execute the database migration:

```bash
uv run python -m database.migration upgrade
```

### 7. Run the Project

```bash
uv run main.py
```

Enable auto-reload on code changes during development:

```bash
UVICORN_RELOAD=true uv run main.py
```

Or run directly with uvicorn:

```bash
uv run uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

After the server starts, visit:
- API Documentation (Swagger): `http://localhost:8000/docs`
- API Documentation (ReDoc): `http://localhost:8000/redoc`

### 8. Background Tasks (Celery)

Email delivery, file processing, and scheduled cleanup all run as Celery tasks — the API process alone does not execute them. Two ways to run them locally:

**Option A — embedded (simplest):** set `celery.enable_embedded_celery: true` in `config.yaml`; `uv run main.py` then manages worker + beat subprocesses for you.

**Option B — separate processes** (mirrors production; keep `enable_embedded_celery: false`):

```bash
# worker (consumes the default queue)
uv run celery -A celery_runtime.entrypoint:celery_app worker \
  -Q default --pool threads --concurrency 10 --without-gossip --loglevel INFO

# beat (scheduled task dispatcher)
uv run celery -A celery_runtime.beat_entrypoint:beat_app beat --loglevel info
```

> In production, worker / beat run in their own containers (split mode) — see the [Deployment Guide](docs/en/DEPLOYMENT.md).

## Current OSS Workflow

Kawaru now supports **OSS storage only**. The former local-storage and backend chunk-upload paths have been removed; clients upload ZIP files directly to OSS with short-lived STS credentials.

1. Call `POST /files/preflight` with file metadata, MD5, `storage_type=oss`, `spectrum_mode`, and `storage_mode`. The response reserves a file identified by its `public_id` (a random 16-character base62 string; the internal `file_id` is never returned); `is_reuse=true` means an existing record with the same hash was linked to the current user and no new upload is required.
2. For a new object, call `POST /files/upload` with `pre_public_id` (the preflight `public_id`) and `filename`. The backend verifies that the preflight record belongs to the current user and is still uploadable, then returns path-scoped STS credentials. The ZIP key is `{oss_zip_storage_root}/{file_verify_code}/{file_id}.zip` (the hash prefix spreads concurrent writes across OSS partitions), so `oss_path` still contains the internal `file_id`.
3. Upload the ZIP directly to OSS. The extraction FC function parses `file_id` from the key, writes the imZML/ibd pair below `{oss_origin_storage_root}/{file_verify_code}/{file_id}/`, and calls `POST /callback/oss_upload_callback` with `X-FC-Token` and `file_id`.
4. On a successful callback, the backend stores all three object keys, commits them, and then tags the ZIP, imZML, and ibd objects with `persistent=true`. `storage_mode=continuous` dispatches the feature FC task and receives its result at `/callback/umap_callback`; other modes dispatch TIC and use `/callback/tic_callback`. Both callbacks store the preview directory key ending with `/`, normally `images/{public_id}/`; clients append `preview.jpg` to the `image_path` returned in file responses.
5. Algorithm processing (`POST /processes`) and raw conversion (`POST /processes/raw-convert`) take the source file as `file_public_id`, return runs with `source_file_public_id`, and write `{oss_processed_storage_root}/run_{run_id}.zarr/`. Clustering (`POST /processes/{run_id}/clustering`) updates that same Zarr rather than creating a second output path. `GET /processes/{run_id}/zarr` returns scoped read-only STS access after the base run completes.
6. `GET /files/{public_id}/download_raw` returns signed imZML and ibd URLs; `GET /files/{public_id}/download_relay` returns backend URLs that stream the same two objects through the server. There is no ZIP download: the uploaded source ZIP is deleted by `cleanup_source_zips` after its retention period.

The configured object roots are `oss_zip_storage_root`, `oss_origin_storage_root`, and `oss_processed_storage_root`. Celery beat schedules stale-preflight cleanup, source-ZIP cleanup, failed-experiment cleanup, stale-processing-run marking, deleted-user cleanup, stale file/run deletion retries, and a heartbeat. Objects that never reach a callback and therefore never receive `persistent=true` still rely on the OSS lifecycle rule.

## Main API Areas

| Area | Main routes |
|------|-------------|
| File upload and access | `/files/preflight`, `/files/upload`, `/files/{public_id}/metadata`, `/files/{public_id}/download_raw`, `/files/{public_id}/download_raw_noauth`, `/files/{public_id}/download_relay` |
| Processing runs | `POST /processes`, `POST /processes/raw-convert`, `POST /processes/{run_id}/clustering`, `GET /processes/{run_id}/zarr`, `GET /processes/mine`, `DELETE /processes/{run_id}` |
| FC callbacks | `/callback/oss_upload_callback`, `/callback/algorithm_callback`, `/callback/tic_callback`, `/callback/umap_callback`, `/callback/clustering_callback` |

All `/callback/*` routes require `X-FC-Token`. Use Swagger at `/docs` for the complete request and response schemas.

## Project Structure

Layering direction: `routers/` → `api/` → `api_services/` / `api_helpers/` → `database/` / `models/`; background tasks live in `background/`.

| Directory | Role |
|-----------|------|
| `routers/` | FastAPI route definitions |
| `api/` | Request handlers / endpoint logic |
| `api_services/` | Business service layer |
| `api_helpers/` | Shared helpers for the api layer |
| `background/` | Celery task definitions |
| `celery_runtime/` | Celery app construction, worker / beat entrypoints, embedded launcher |
| `boot/` | Bootstrap: env / config / logging / celery / OSS initialization |
| `models/` | Pydantic / SQLModel models |
| `database/` | DB engine, Alembic migration CLI, startup Schema Guard, Redis client |
| `migrations/` | Alembic migration script history and environment configuration |
| `storage/` | OSS storage implementation |
| `constants/`, `exceptions/`, `tools/` | Constants, exception definitions, logger and utilities |
| `docker/` | Dockerfile and dev / prod / test compose stacks |
| `tests/` | End-to-end pytest suite and `tests/migrations/` migration tests |
| `.github/` | CI / CD GitHub Actions workflows |
| `docs/` | Contributing, deployment, database migration guides, and upload callback flow |

Top-level files:

| File | Purpose |
|------|---------|
| `main.py` | Application entry: FastAPI instance, router registration, lifespan (Schema Guard, create admin, embedded celery start/stop) |
| `alembic.ini` | Alembic migration tool static configuration |
| `config_example.yaml` | App config template — copy to `config.yaml` |
| `.env_example` | Environment variable template — copy to `.env` |
| `secrets.example.yaml` | GitHub Actions secrets checklist for CI and dev/prod CD |
| `pyproject.toml` / `uv.lock` | Dependency declaration and lockfile (uv) |
| `pytest.ini` | pytest configuration and marker definitions |
| `.pylintrc` | pylint config (local use only, not in CI; line length 150) |
| `.python-version` | Python version pin (3.12) |
| `.gitignore` / `.dockerignore` | git / Docker build ignore rules |
| `LICENSE` | GPLv3 license |

## Documentation

For detailed documentation, refer to the `docs/` directory:

| Document | Description |
|----------|-------------|
| [Contributing](docs/en/CONTRIBUTING.md) | Collaboration workflow and quality gates |
| [Deployment](docs/en/DEPLOYMENT.md) | dev/prod deployment guide and container orchestration |
| [Database Migration Guide (Chinese)](docs/zh/数据库迁移指南.md) | Alembic setup, migration workflows, and zero-downtime deployment |
| [Async tasks and callbacks (Chinese)](docs/zh/异步任务与回调架构.md) | Celery task flow, FC callbacks, and callback failure cleanup |

## License

This project is licensed under the GNU General Public License v3.0. See [LICENSE](LICENSE) for details.
