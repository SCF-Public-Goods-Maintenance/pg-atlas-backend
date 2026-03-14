# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PG Atlas Backend — ingestion pipeline, storage layer, metric computation, and REST API for the SCF Public Goods dependency graph. Built with FastAPI, SQLAlchemy 2.x (async), PostgreSQL, and Procrastinate job queue. Licensed MPL-2.0.

## Commands

All commands use `uv run` (requires Python 3.14+ and [uv](https://docs.astral.sh/uv/)):

```sh
uv sync                                    # install deps
uv run python -m pg_atlas --reload          # dev server (localhost:8000)
uv run pytest                               # all tests
uv run pytest tests/test_health.py          # single file
uv run pytest -k test_name                  # single test
uv run ruff check .                         # lint
uv run ruff format --check .                # format check
uv run mypy pg_atlas/                       # type check
uv run alembic upgrade heads                # apply migrations
uv run alembic revision --autogenerate -m "msg"  # new migration
docker compose up --build                   # full stack (postgres + api)
```

## Architecture

- **pg_atlas/routers/** — FastAPI route handlers (health, ingestion)
- **pg_atlas/ingestion/** — SBOM (SPDX 2.3 JSON) validation, parsing, and persistence pipeline
- **pg_atlas/auth/** — GitHub OIDC token verification (PyJWT + JWKS)
- **pg_atlas/db_models/** — SQLAlchemy ORM with joined-table inheritance (RepoVertex → Repo/ExternalRepo), custom HexBinary column type, PostgreSQL ENUMs
- **pg_atlas/crawlers/** — Registry crawlers (Pub.dev, Packagist) with abstract base providing retry/rate-limit logic
- **pg_atlas/deps_dev/** — gRPC wrapper for deps.dev Insights API (generated protobuf stubs in `lib/`, excluded from linting)
- **pg_atlas/procrastinate/** — PostgreSQL-backed job queue tasks and worker; queues: "opengrants", "package-deps"
- **pg_atlas/migrations/** — Alembic migrations; version locations include both `migrations/versions` and `procrastinate/versions`

### Key Patterns

- **Async-first**: all I/O uses async/await (FastAPI, SQLAlchemy asyncpg, httpx, procrastinate)
- **FastAPI Depends()**: OIDC verification and DB sessions injected via dependency overrides
- **SBOM pipeline**: receive → validate SPDX → store raw artifact → parse → persist ORM → mark processed; deduplication via SHA-256 content hash
- **Per-package transaction boundaries** in crawlers for isolation
- **Database optional**: ingestion gracefully degrades without DB (logs instead of persisting)

## Testing

- pytest with `asyncio_mode = "auto"` — all async tests run automatically
- DB integration tests require `PG_ATLAS_DATABASE_URL` env var (skipped otherwise)
- Test fixtures in `tests/conftest.py`: `authenticated_client` (OIDC mocked), `async_client` (real auth), `db_session`
- Sample SBOM documents in `tests/data_fixtures/`

## Configuration

All settings prefixed `PG_ATLAS_` (see `pg_atlas/config.py`). Key vars:
- `PG_ATLAS_API_URL` — required; OIDC audience validation
- `PG_ATLAS_DATABASE_URL` — PostgreSQL DSN (asyncpg driver appended automatically)
- `PG_ATLAS_ARTIFACT_STORE_PATH` — raw SBOM storage (default: `./artifact_store`)

## Conventions

- **Conventional Commits** required (enforced by pre-commit hook). Releases managed by release-please.
- Ruff lint rules: E, F, I; line length 127. `pg_atlas/deps_dev/lib` excluded from linting/mypy.
- mypy strict mode (`disallow_untyped_defs`, `warn_return_any`).

## Agentic Workflow

Specialist agents are defined in `.claude/agents/`. Invoke them via the `/agents` command or the Agent tool. Each agent has a clear lane; hand off between agents in the sequence below for non-trivial work.

### Agent Registry

| Agent | Model | When to Invoke |
|-------|-------|---------------|
| `systems-architect` | Opus 4.6 | **First, always.** Repo recon, architecture mapping, call-graph tracing, scoping PRs, identifying risks and invariants before any code is written. |
| `metrics-ecologist` | Opus 4.6 | Any task touching metric definition, graph analytics, ecological framing, or manuscript alignment (CONTRIBUTION_COMPASS, CELL_PATTERNS_MANUSCRIPT). Invoke before `backend-implementer` for metric work. |
| `backend-implementer` | Sonnet 4.6 | Translating a scoped plan into code: FastAPI endpoints, SQLAlchemy models, Procrastinate tasks, ingestion logic, crawlers, migrations. |
| `verification-engineer` | Sonnet 4.6 | Proving a change works: test design, invariant validation, failure debugging, migration safety, PR readiness. |
| `governance-writer` | Sonnet 4.6 | Packaging work for others: PR descriptions, ADRs, manuscript Methods text, funder-facing summaries. |

### Standard Delegation Sequence

```
systems-architect → [metrics-ecologist] → backend-implementer → verification-engineer → governance-writer
```

The `metrics-ecologist` step is required when the task involves any metric, graph algorithm, or manuscript-method alignment. Skip it only for pure infrastructure work (endpoint plumbing, crawler fixes, migration, config).

### Model Rationale

- **Opus 4.6** for `systems-architect` and `metrics-ecologist`: these roles require multi-hop reasoning across complex architectures and scientific rigor for governance-grade claims. The quality ceiling here directly affects what gets built and what gets published.
- **Sonnet 4.6** for `backend-implementer`, `verification-engineer`, and `governance-writer`: these roles execute against a well-defined plan. Reliable, efficient output matters more than exploratory depth.

### Quality Gate (non-negotiable before any PR)

```sh
uv run ruff check .
uv run ruff format --check .
uv run mypy pg_atlas/
uv run pytest
```

All four must pass. The pre-commit hook enforces Conventional Commits on every commit.
