# Contribution Guide

## Quick Start

1. Clone the repository and sync the latest `test` branch
2. `uv sync` to install dependencies
3. Create a feature branch from `test`, submit a PR when done

## Branch Strategy

```
main  ← merge commit ← dev  ← merge commit ← test  ← squash merge ← feature/*
```

| Branch | Purpose | Merge Strategy |
|--------|---------|----------------|
| `feature/*` | Personal feature branches | — |
| `test` | Integration testing | feature → test: **Squash and merge**, delete branch after merge |
| `dev` | Acceptance | test → dev: **Create a merge commit** |
| `main` | Production | dev → main: **Create a merge commit** |

test/dev/main use merge commit (not squash) to preserve intermediate commit history and avoid duplicate commits appearing in future PRs.

## Submission Process

### 1. Start a New Task

```bash
git checkout test
git pull origin test
git checkout -b feature/<topic>
```

### 2. Submit PR

- **feature → test**: Squash and merge, delete feature branch after merge
- Title follows Conventional Commits: `feat(file): support oss upload`
- Description includes key changes, impact scope, `Closes #<id>`

### 3. Sync During Collaboration

After someone's PR is merged, sync the latest test before continuing:

```bash
git fetch origin
git rebase origin/test   # recommended, keeps linear history
# or git merge origin/test
```

### 4. Merge Upward

After testing passes: test → dev → main, all via Create a merge commit PR. Pushing to `dev` / `main` triggers automatic deployment (see [Deployment Guide](DEPLOYMENT.md)).

## Commit Message Conventions

Use Conventional Commits: `feat` / `fix` / `docs` / `refactor` / `test`

## Naming Conventions

| Type | Style | Example |
|------|-------|---------|
| Files/functions/variables | `snake_case` | `user_api.py`, `create_user` |
| Classes | `PascalCase` | `UserInDB`, `UserPublic` |
| Constants | `UPPER_CASE` | `SECRET_KEY` |
| Boolean variables | `is_`/`has_` prefix | `is_active` |
| Dependency injection | `get_` prefix | `get_db` |

Model naming: `UserBase` (shared fields), `UserInDB` (database), `UserPublic` (response), `UserCreate`/`UserUpdate` (request).

All function parameters and return values must have type annotations. The runtime requires Python 3.12+; use `|` instead of `Union` for union types.

## CI and Code Checks

Pushing to `test` triggers CI ([.github/workflows/ci.yml](../../.github/workflows/ci.yml)) across four sequential stages:

1. **Migration check (`migration-check`)**: Spins up a disposable MariaDB service on the GitHub runner and executes:
   - `check-heads`: ensures a single Alembic head (prevents unmerged multi-branch migration conflicts)
   - `upgrade` + `verify` + `alembic check`: builds the schema from scratch on an empty database and asserts 100% parity between SQLModel models and DDL
   - `test-upgrade-from-previous`: tests smooth incremental upgrades from the immediate previous version
   - Executes `tests/migrations/` migration unit test suite
2. **Static checks (`static-checks`)**: `uv sync --frozen` verifies the lockfile → `compileall` syntax check → `uvx ruff check .` lint (currently non-blocking; will become mandatory once existing lint debt is cleared)
3. **Build image (`build-image`)**: builds a test image and pushes it to GHCR
4. **Integration test (`integration-test`)**: spins up the full test stack on the GitHub runner via [docker/docker-compose.test.yml](../../docker/docker-compose.test.yml) (migrate + app on 9001 + celery-worker + celery-beat + mariadb + redis), then runs `pytest -m "oss or perf_concurrent or password_reset"` inside the container (requires OSS / SMTP secrets)

Before submitting a PR, run at least:

```bash
uv run python -m database.migration check-heads
uv run python -m database.migration upgrade
uv run python -m database.migration verify
uv run alembic check
uv run pytest -c pytest.ini --confcutdir=tests/migrations tests/migrations -q
uv run python -m compileall -q .
uv run pytest --collect-only -q
uvx ruff check .
```

Run the relevant test marker as well when the required external services are available.

> pylint is not part of CI; if you run it locally, note that [.pylintrc](../../.pylintrc) sets max-line-length to 150.

## Running Tests

The tests are end-to-end (they need a running server + MariaDB + Redis + celery worker). Two ways to run them.

### Option 1: Repository Actions (recommended)

CI runs automatically after code reaches the shared `test` branch, and it can also be started manually with `workflow_dispatch` from the Actions page. Do not force-push a personal feature branch over the shared `test` branch just to run CI.

> Integration tests require four repository secrets under **Settings → Secrets and variables → Actions**: `OSS_ACCESS_KEY_ID` / `OSS_ACCESS_KEY_SECRET` / `OSS_ROLE_ARN` / `SMTP_PASSWORD` (see the complete checklist in [secrets.example.yaml](../../secrets.example.yaml)). Without them, integration testing fails fast while static checks and image building still run.

### Option 2: Run locally

The suite must connect to a **running** server (URL from `.env` `BASE_URL`, default `http://127.0.0.1:8000`):

1. Start the local stack per the README (including a celery worker, otherwise email tasks never execute)
2. Set `TEST_USER` / `TEST_PWD` in `.env`; if left empty, conftest parses the first-boot admin credentials printed in `kawaru_logs/api.log`
3. Run `uv run pytest` (filter with markers like `-m oss`; marker definitions in [pytest.ini](../../pytest.ini))

Alternatively, run the full test stack (same as CI, port 9001, destroyed afterwards):

```bash
docker compose -f docker/docker-compose.test.yml -p kawaru-test up -d --build --wait
docker compose -f docker/docker-compose.test.yml -p kawaru-test exec -u root app sh -lc 'mkdir -p /test_temp && chown -R appuser /test_temp'
docker compose -f docker/docker-compose.test.yml -p kawaru-test exec -T app python -m pytest -m "oss or perf_concurrent or password_reset" -q
docker compose -f docker/docker-compose.test.yml -p kawaru-test down -v
```

External dependencies per marker:

- `oss` uses the real bucket and covers preflight → scoped STS → direct OSS upload → wait for event-triggered FC extraction → simulated authenticated upload callback → RAW signed download with MD5 verification → cleanup. It requires OSS credentials, the configured bucket event, and a working extraction FC function. The test simulates the callback after extraction instead of waiting for FC to call the API itself.
- `password_reset` needs a working `SMTP_PASSWORD`. `perf_concurrent` uses the real email path by default, but `PERF_TEST_VERIFY_CODE` can supply the registration code directly.

The current OSS test does not validate automatic callback delivery, TIC/UMAP callback output, ZIP packup, algorithm/raw-convert/clustering runs, failure/retry behavior, or multi-user races. Add focused coverage when changing any of those paths; a green `oss` marker alone does not prove the entire OSS/FC deployment is operational.

## PR Review Checklist

- Model modifications include matching Alembic migration scripts and pass single-head and migration tests
- Naming conventions, module boundaries, and interfaces are consistent
- No obvious performance/memory issues
- Error handling is in place, boundary cases covered
- Documentation updated accordingly
- Tests pass and cover critical logic
