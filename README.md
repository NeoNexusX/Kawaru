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

## macOS Additional Dependencies

The project depends on `m2crypto`, which requires compiling C extensions from source on macOS. Additional tools are needed:

```bash
# Install SWIG and OpenSSL
brew install swig openssl@3
```

After installation, set the following environment variables when compiling `m2crypto` (required for Apple Silicon):

```bash
export LDFLAGS="-L$(brew --prefix openssl@3)/lib"
export CFLAGS="-I$(brew --prefix openssl@3)/include -D__aarch64__"
export SWIG_FEATURES="-I$(brew --prefix openssl@3)/include -cpperraswarn"
```

> **Notes:**
> - `-cpperraswarn` — Allows SWIG to downgrade `#error "Unsupported architecture"` in system headers to a warning.
> - These variables only need to be set temporarily when running `uv sync`; no need to add them to shell config files.
> - Intel Mac users do not need `-D__aarch64__`.

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
  -e MARIADB_ROOT_PASSWORD=your_db_password mariadb:11.4

docker run -d --name kawaru-redis -p 16379:6379 \
  redis:8.4-alpine redis-server --requirepass your_redis_password
```

Connection settings (host / port / db name) live in the `database:` / `redis:` sections of `config.yaml`; passwords go in `.env` (next step).

### 4. Configure Environment Variables

```bash
# Copy the example file
cp .env_example .env
```

Required variables are validated at startup (the app fails fast if any are missing). Key entries:

| Variable | Purpose |
|----------|---------|
| `DATABASE_PASSWORD` | MariaDB password (connects as root) |
| `REDIS_PASSWORD` | Redis password |
| `SMTP_PASSWORD` | SMTP login password (server / port / account / sender are configured in `config.yaml` → `email:`) |
| `OSS_ACCESS_KEY_ID` / `OSS_ACCESS_KEY_SECRET` / `OSS_ROLE_ARN` | Aliyun OSS credentials (required when `storage.allowed_storage_types` includes `oss`) |
| `OSS_FC_*_TOKEN` / `FC_CALLBACK_TOKEN` | Function Compute (FC) trigger and callback tokens |
| `CONFIG_PATH` | Path to the app config file (default `config.yaml`) |
| `LOG_DIR` / `LOG_LEVEL` | Log directory and level |
| `BASE_URL` / `TEST_USER` / `TEST_PWD` | Only used when running pytest locally (see [Contributing](docs/en/CONTRIBUTING.md)) |

### 5. Configure Server Parameters

```bash
cp config_example.yaml config.yaml
```

Edit `config.yaml`, focusing on:

- `database:` / `redis:` — point to the instances prepared in step 3
- `email:` — SMTP server, port, account, sender
- `oss:` — bucket, region, FC endpoints; if you don't use OSS, set `storage.allowed_storage_types` to `["local"]`

### 6. Run the Project

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

### 7. Background Tasks (Celery)

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
| `database/` | DB engine, table creation, Redis client |
| `storage/` | Local / OSS storage implementations |
| `constants/`, `exceptions/`, `tools/` | Constants, exception definitions, logger and utilities |
| `docker/` | Dockerfile and dev / prod / test compose stacks |
| `tests/` | End-to-end pytest suite (requires a running server, see Contributing) |
| `.github/` | CI / CD GitHub Actions workflows |
| `docs/` | Contributing and deployment guides |

Top-level files:

| File | Purpose |
|------|---------|
| `main.py` | Application entry: FastAPI instance, router registration, lifespan (create tables, create admin, embedded celery start/stop) |
| `config_example.yaml` | App config template — copy to `config.yaml` |
| `.env_example` | Environment variable template — copy to `.env` |
| `secrets.example.yaml` | GitHub Secrets checklist (fill in before running CI on your fork) |
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
| [Contributing](docs/en/CONTRIBUTING.md) | Collaboration workflow |
| [Deployment](docs/en/DEPLOYMENT.md) | dev/prod deployment guide |

## License

This project is licensed under the GNU General Public License v3.0. See [LICENSE](LICENSE) for details.
