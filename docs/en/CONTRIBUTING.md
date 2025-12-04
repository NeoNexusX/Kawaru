# Contribution Guide

Welcome to contribute to MassFlow-Kawaru! This document provides a unified process and quality standards for multi-person collaboration, ensuring orderly division of labor, code stability, and controllable releases.

## Quick Start

- After forking the repository, first switch to the remote `dev` branch and create your working branch (see branch naming rules below).

- Install recommended Cursor/Trae/VSCode extensions locally (see README and `.vscode/extensions.json`). PyCharm users can skip this.

- Install dependencies and development environment:

  ```bash
  # Ensure uv is installed
  uv sync
  ```

- Follow naming conventions and code style (`../zh/命名规范.md`, `.pylintrc`).

- After completing the task, submit a PR, self-check according to the review checklist, and request a review.

## Branch Strategy

- `dev`: Main development branch, used for daily development work, merged after review by designated personnel.
  - Feature: `feature/<topic>` (e.g., `feature/imzml-import`)
- `main`: Protected branch, only PRs reviewed by administrators are merged; kept stable and releasable.
- Merge Strategy: Prioritize squash merge to keep commit history clean; rebase to clean up commit records when necessary.

## Submission Process (Issue → Branch → PR)

1. Claim Issue: Before starting a new issue, delete the previous repository [including local and online content].
2. Development and Self-check: Follow code standards, run local checks, and write necessary tests [you can submit a PR first and write test code as requested by the reviewer].
3. Submit PR:
   - Concise Title (Recommended to follow Conventional Commits): e.g., `feat(file): ...`
   - Complete Description: Key changes, impact scope, test points (`Closes #<id>`). AI generation is recommended.
   - Request Review: Assign module owner or code owner.
4. Review and Merge: AI preliminary review → Fix comment items → Owner review → Meet review checklist → Merge to `dev`.

## Commit Message Conventions

- Recommended to use Conventional Commits:
  - `feat: <description>` New feature
  - `fix: <description>` Bug fix
  - `docs: <description>` Documentation update
  - `refactor: <description>` Refactoring without affecting function
  - `test: <description>` Test related
- Example: `feat(data-manager): support split/merge write modes`

## Code Standards and Naming

- Follow `../zh/命名规范.md`:
  - Class: `PascalCase`, domain abbreviations keep uppercase (e.g., `MSIDataManager`)
  - Function/Variable/File: `snake_case` (e.g., `load_full_data_from_file`)
  - Property: Private `_meta_*` and public `meta_*` property mapping (via `@property`)
- Unified Style and Static Check:
  - Run `pylint` (follow `.pylintrc`), `black`, `isort`

## Editors and Tools

- Cursor/Trae/VSCode:
  - Install recommended extensions (Python, Pylance, Pylint)
  - Workspace `.vscode/extensions.json` will automatically prompt for installation
- Optional: Enable Format on Save and configure Black/isort consistency.

## Testing and Quality Assurance

- Test Scope:

  - Unit Tests: Module methods and critical paths (load, write, filter, visualize)
  - Integration Tests: End-to-end path from data file to processing output (lightweight data)

- Run Tests:

  ```bash
  uv run pytest
  ```

- Local Check (Recommended as a pre-commit step):

  - No errors, 10 runs passed

## PR Review Checklist

- Naming style and module boundaries are consistent, interfaces and data conventions meet specifications
- No obvious performance and memory issues; avoid unnecessary copy/allocation
- Assertions and error handling are in place, boundary cases are covered
- Documentation updates (README/Chinese docs/examples) are in place
- Passed local and CI checks; new/updated tests cover critical logic
- Change description is clear

## Contact

- If you have questions or support needs, please submit an Issue in the repository; or @owner in the PR for discussion.
