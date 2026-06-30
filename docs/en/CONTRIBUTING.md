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

After testing passes: test → dev → main, all via Create a merge commit PR.

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

## Code Checks

```bash
uv run pylint api/ models/ database/   # max line length 130
uv run pytest                           # run tests
```

## PR Review Checklist

- Naming conventions, module boundaries, and interfaces are consistent
- No obvious performance/memory issues
- Error handling is in place, boundary cases covered
- Documentation updated accordingly
- Tests pass and cover critical logic
