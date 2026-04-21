# Global Instructions

## Architecture Documentation

When architectural context is needed, do not guess. Instead, use your GitHub tools to explore the
`SCF-Public-Goods-Maintenance/scf-public-goods-maintenance.github.io` repository. Start by listing
the `docs/` directory, then read only the .md files relevant to the current task.

## Docs

Always document your work. When the output is code, write clear docstrings for each function. If it
is not obvious where to document your work, create a new .md file.

## Tests

Whenever possible, write test cases to validate your work. Do not hesitate to write unit tests. If
you need to write a larger integration test or GitHub workflow, ask for user input first.

## Git & Version Control

Never run `git add`, `git stage`, `git commit`, `git push`, or any equivalent (including GitHub MCP
`push_files` / `create_or_update_file` to the repo) without **explicit user approval**. Prepare
changes in the working tree, summarize what is ready, and wait for the user to review before any
commit is created.

## Known Issues and PR Context

As mandatory preparation for any task, use your GitHub tools to list all open _and_ closed issues
for the current repo. Read the open and closed issues in full with all their comments when they are
relevant for the current task. Memorize the entire issue list so you can read full issues during
task implementation, as they become relevant.

Work is always done on feature branches. If the current branch is `main`, WARN the user. Check if
the feature branch is associated with a PR: read the full PR including its comments to understand
the input from team members. Do not assume your PR context is up-to-date; after changes have been
pulled from the upstream/remote, use your GitHub tools again to read the current PR and its
comments.

---

# PG Atlas Backend — Project-Specific Instructions

## Deliverable Naming

Work is organised into deliverables labelled A1, A2, A3 … (from the proposal in
`SCF-Public-Goods-Maintenance/scf-public-goods-maintenance.github.io`). Current scope is defined by
whichever is being built; stubs for later deliverables are marked `# TODO A<n>:` in code.

## Tooling

- **uv** for package management. Always use `uv run` commands; never activate the venv manually.
- **ruff** for lint and format (`line-length = 127`, selects E, F, I). Ruff 0.15.2 with
  `requires-python = ">=3.14"` (i.e. `target-version = "py314"`) intentionally rewrites `except (A, B):`
  → `except A, B:` per PEP 758.
- **mypy** in strict mode (`disallow_untyped_defs`, `explicit_package_bases`, `ignore_missing_imports`).
- **basedpyright** in strict mode. Run diagnostics and fix problems for every file you touch. Strict
  really is strict, also for test files. Check for `types-*` packages on PyPI. Write missing `.pyi`
  stubs. Wrap fetched JSON in msgspec or Pydantic models. Never cast. Only suppress warnings if you have
  exhausted these options.
- **pytest-asyncio** with `asyncio_mode = "auto"` — no `@pytest.mark.asyncio` on individual tests.
- Translate all `pytest` commands to the `runTests` tool equivalent, and run them with the tool.
  Only fall back to the terminal after trying `runTests` on the test you must run.
- Run the full check suite before considering work done:
  ```sh
  rg '%[sdf]' pg_atlas/  # always use f-strings
  uv run pytest -v
  uv run ruff check .
  uv run ruff format --check .
  uv run mypy pg_atlas/
  # run basedpyright last; it is the strictest QA in the check suite.
  uv run basedpyright $(git diff --name-only -- '*.py')
  ```
  Prepend `PG_ATLAS_API_URL=https://test.pg-atlas.example` if a test errors because the env var is
  not set. This is rare.

## Project Layout

- Flat `pg_atlas/` package — no `src/` layout, no `__init__.py` files (namespace packages).
- Test fixtures in `tests/**/data_fixtures/`.
- Database migrations in `pg_atlas/migrations/` (Alembic, async engine, 'atlas' branch).

## Code Style

These rules MUST be enforced manually. No ruff rules are available to enforce them.

- Breathing space:
  - Multi-line docstrings leave the first line blank after `"""` — the summary sentence begins on the
    second line. Single-line docstrings stay on one line.
  - Always include a blank line (may contain only a comment or closing brackets) when exiting a
    nested block (e.g. `try`/`except`, `for`, `if`) to separate the block's internal logic from the
    subsequent code at a lower indentation level.
  - Insert a blank line before a final `return` or terminal `raise` statement at the end of a
    function or logical section.
  - Use blank lines between adjacent control structures (like an `if` block followed by a `for`
    loop).
- Always use `f"hey {agent}!"` f-strings instead of older string interpolation.
- Style normalization scope: apply breathing-space and interpolation to authored app/test code;
  do not mass-normalize migration/revision scripts (`pg_atlas/migrations/versions/`,
  `pg_atlas/procrastinate/versions/`) unless specifically fixing a functional defect.
- Exception handling: be precise — do not use bare `except Exception` to catch expected errors;
  name the specific exception types (e.g. `except PyJWKClientError, OSError`).
- Fail fast over silent fallbacks: if a required config value is missing, raise `ValueError` at
  import/startup rather than falling back to a placeholder that silently misbehaves later.
- Conventional Commits for all commit messages. release-please handles changelog and version bumps.

## GitHub Actions

- When you add a non-trivial step to a workflow (e.g. a metrics materialization or data-processing command), tee its output to a log file, parse the most pertinent fields, and incorporate them into the job summary so viewers can see at a glance what ran and what the outcome was.
- **Never use shell `tee` (`cmd 2>&1 | tee file`)**; it swallows the exit code and makes failing steps appear green. Use `--tee=<file>` flags backed by `pg_atlas.instruments.tee.run_with_tee` instead.
- The `gh` CLI is available as a fallback when the GitHub MCP gives a 403.
- The SBOM action (`SCF-Public-Goods-Maintenance/pg-atlas-sbom-action`) is used by this repo too —
  it runs in CI on push to main.

## Current Deployment State

This is a compact list. Keep it compact.
When you start work on a new deliverable, read `.github/agent-handoff/implementation-notes.md`.
When you have validated that a deliverable has been completed, update `implementation-notes.md`.

- **A1 complete**: CI green, DO App Platform live at
  `https://api.pgatlas.xyz` (`basic-xxs`, region `ams3`).
- **A2 complete**: PostgreSQL schema (`pg_atlas/db_models/`), Alembic migrations,
  artifact storage (`pg_atlas/storage/artifacts.py`). Hosted DB (`pg-atlas-dev`, PG 18)
  lives on DO App Platform; `entrypoint.sh` runs `alembic upgrade head` at startup.
- **A3 complete**: SBOM ingestion (`POST /ingest/sbom`, OIDC auth, SPDX 2.3 parsing, 202 Accepted),
  health endpoint (`GET /health`). SBOM write path — `pg_atlas/ingestion/persist.py` implements
  full end-to-end persistence.
- **A5 complete**: Reference Graph Bootstrapping — Procrastinate + GitHub Actions crawl of
  OpenGrants, GitHub, and deps.dev. `pg_atlas/procrastinate/` sub-package. deps.dev gRPC wrapper
  via grpclib (generated code in `pg_atlas/deps_dev/lib/`). GitHub Actions workflows:
  `bootstrap.yml` (weekly, log-based summary), `sync-depsdev-proto.yml` (daily).
- **A6 complete**: Active Subgraph Projection (`pg_atlas/metrics/active_subgraph.py`) fully
  implemented and validated. Registry Crawlers incorporated in graph bootstrapper.
- **A7 complete**: Git Log Parser is integrated with Procrastinate (`gitlog` queue), with batch deferral,
  per-attempt `GitLogArtifact` audit rows, terminal-failure private marking, and API read endpoints for
  gitlog artifact list/detail.
- **A8 complete**: SBOM post-validation processing now runs in a dedicated Procrastinate `sbom`
  queue, with parser and semantic-hash hot-path optimizations (`msgspec` +
  `JsonLikeDictParser`) and canonical unwrapped SPDX artifact storage for new submissions.
- **A10 complete**: Direct registry crawling now supports `NPM`, `CARGO`, and `PYPI` alongside
  `DART` and `COMPOSER`; bootstrap writes per-package download snapshots into
  `Repo.repo_metadata["adoption_downloads_by_purl"]` for later adoption materialization.

## Keeping These Instructions Current

After completing a todo list for a session, update the sections above with any new conventions
decisions, or patterns that would help future sessions collaborate smoothly. Clarify what you can.
Remove anything that was superseded.

NO ADDITIONS AFTER THIS SECTION
