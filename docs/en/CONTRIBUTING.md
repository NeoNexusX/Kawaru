# Contribution Guide

## Quick Start

1. Fork the repository, sync the latest `test` branch from upstream on GitHub
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
# After Sync fork on GitHub:
git checkout test
git pull origin test
git checkout -b feature/<topic>
```

### 2. Submit PR

- **feature → test**: Squash and merge, delete feature branch after merge
- Title follows Conventional Commits: `feat(file): support chunked upload`
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

All function parameters and return values must have type annotations. Use Python 3.10+ syntax (`|` instead of `Union`).

## CI and Code Checks

Pushing to `test` triggers CI ([.github/workflows/ci.yml](../../.github/workflows/ci.yml)) in three stages:

1. **Static checks** (GitHub runner): `uv sync --frozen` verifies the lockfile → `compileall` syntax check → `uvx ruff check .` lint (currently non-blocking; will become mandatory once existing lint debt is cleared)
2. **Build image**: builds a test image and pushes it to GHCR
3. **Integration test**: spins up the full test stack on the GitHub runner via [docker/docker-compose.test.yml](../../docker/docker-compose.test.yml) (app on 9001 + celery-worker + celery-beat + mariadb + redis), then runs `pytest -m "oss or perf_concurrent or password_reset"` inside the container (requires OSS / SMTP secrets)

Before submitting a PR, run at least:

```bash
uvx ruff check .    # lint, same as CI
```

> pylint is not part of CI; if you run it locally, note that [.pylintrc](../../.pylintrc) sets max-line-length to 150.

## Running Tests

The tests are end-to-end (they need a running server + MariaDB + Redis + celery worker). Two ways to run them.

### Option 1: Actions on your own repo's `test` branch (recommended)

Pushing your feature branch to the `test` branch of **your own fork** triggers the exact same CI as upstream — no local environment needed; watch the results on your repo's Actions page:

```bash
# Push the feature branch to your fork's test branch (origin = your fork)
git push origin feature/<topic>:test
```

> The integration test requires four secrets in your repo's **Settings → Secrets and variables → Actions**: `OSS_ACCESS_KEY_ID` / `OSS_ACCESS_KEY_SECRET` / `OSS_ROLE_ARN` / `SMTP_PASSWORD` (see [secrets.example.yaml](../../secrets.example.yaml) for what each value is). Without them the integration-test stage fails fast (static checks and the image build still run; the image is pushed to GHCR under your own account).

### Option 2: Run locally

The suite must connect to a **running** server (URL from `.env` `BASE_URL`, default `http://127.0.0.1:8000`):

1. Start the local stack per the README (including a celery worker, otherwise email tasks never execute)
2. Set `TEST_USER` / `TEST_PWD` in `.env`; if left empty, conftest parses the first-boot admin credentials printed in `kawaru_logs/api.log`
3. Run `uv run pytest` (filter with markers like `-m oss`; marker definitions in [pytest.ini](../../pytest.ini))

Alternatively, run the full test stack (same as CI, port 9001, destroyed afterwards):

```bash
docker compose -f docker/docker-compose.test.yml -p kawaru-test up -d --build --wait
docker compose -f docker/docker-compose.test.yml -p kawaru-test exec -u root app sh -lc 'mkdir -p /test_temp && chown -R appuser /test_temp'
docker compose -f docker/docker-compose.test.yml -p kawaru-test exec -T app python -m pytest -m "oss or perf_concurrent" -q
docker compose -f docker/docker-compose.test.yml -p kawaru-test down -v
```

> External dependencies per marker: `oss` needs OSS credentials (`OSS_ACCESS_KEY_ID` etc.); `perf_concurrent` / `password_reset` need a working `SMTP_PASSWORD` (registration / reset go through the real email path).

## PR Review Checklist

- Naming conventions, module boundaries, and interfaces are consistent
- No obvious performance/memory issues
- Error handling is in place, boundary cases covered
- Documentation updated accordingly
- Tests pass and cover critical logic
