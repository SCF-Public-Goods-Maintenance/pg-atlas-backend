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

As mandatory preparation for any task, use your GitHub tools to inspect all open _and_ closed issues for the current repo. Read all open issues and their comments in full. Read the closed issues in full only when they are relevant for the current task.

Work is always done on feature branches. If the current branch is `main`, WARN the user. Check if the
feature branch is associated with a PR: read the full PR including its comments to understand the input from team members. Do not assume your PR context is up-to-date; after changes have been pulled from the upstream/remote, use your GitHub tools again to read the current PR and its comments.

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
- **pylance** for IDE-integrated quality control. If Pylance tools are available: run diagnostics and
  fix problems for every file you touch.
- **pytest-asyncio** with `asyncio_mode = "auto"` — no `@pytest.mark.asyncio` on individual tests.
- Run the full check suite before considering work done:
  ```sh
  PG_ATLAS_API_URL=https://test.pg-atlas.example uv run pytest -v
  uv run ruff check .
  uv run ruff format --check .
  uv run mypy pg_atlas/
  ```

## Project Layout

- Flat `pg_atlas/` package — no `src/` layout, no `__init__.py` files (namespace packages).
- Test fixtures in `tests/data_fixtures/`.
- Database migrations in `pg_atlas/migrations/` (Alembic, async engine).

## Code Style

- Breathing space:
  - Multi-line docstrings open with a blank line after `"""` — the summary sentence begins on the
    second line. Single-line docstrings stay on one line.
  - Always include a blank line (may contain only a comment or closing brackets) when exiting a nested
    block (e.g. `try`/`except`, `for`, `if`) to separate the block's internal logic from the subsequent
    code at a lower indentation level.
  - Insert a blank line before a final `return` or terminal `raise` statement at the end of a function
    or logical section.
  - Use blank lines between adjacent control structures (like an `if` block followed by a `for` loop).
- Prefer `f"hey {agent}!"` format strings over older string interpolation.
- Style normalization scope: apply breathing-space and interpolation to authored app/test code;
  do not mass-normalize migration/revision scripts (`pg_atlas/migrations/versions/`,
  `pg_atlas/procrastinate/versions/`) unless specifically fixing a functional defect.
- Exception handling: be precise — do not use bare `except Exception` to catch expected errors;
  name the specific exception types (e.g. `except PyJWKClientError, OSError`).
- Fail fast over silent fallbacks: if a required config value is missing, raise `ValueError` at
  import/startup rather than falling back to a placeholder that silently misbehaves later.
- Conventional Commits for all commit messages. release-please handles changelog and version bumps.

## GitHub Actions

- The `gh` CLI is available as a fallback when the GitHub MCP gives a 403.
- The SBOM action (`SCF-Public-Goods-Maintenance/pg-atlas-sbom-action`) is used by this repo too —
  it runs in CI on push to main.

## Current Deployment State

- **A1 complete**: , CI green, DO App Platform live at
  `https://pg-atlas-backend-h8gen.ondigitalocean.app` (`basic-xxs`, region `ams3`).
- **A2 complete**: PostgreSQL schema (`pg_atlas/db_models/`), Alembic migration (revision
  `f3d946ade07e`), artifact storage (`pg_atlas/storage/artifacts.py`). Schema refined in a follow-up
  session: `metadata` attribute renamed (`project_metadata`/`repo_metadata`); `latest_version` made
  required on `Repo`/`ExternalRepo`; `artifact_path` made non-nullable; enum values now use
  `values_callable=enum_values` throughout; `email_hash` stored as BYTEA via `HexBinary`.
  Hosted DB (`pg-atlas-dev`, PG 18) lives on DO App Platform; `entrypoint.sh` runs
  `alembic upgrade head` at startup.
- **A3 complete**: SBOM ingestion (`POST /ingest/sbom`, OIDC auth, SPDX 2.3 parsing, 202 Accepted),
  health endpoint (`GET /health`). SBOM write path — `pg_atlas/ingestion/persist.py` implements full end-to-end
  persistence: `Repo`/`ExternalRepo` upserts (PURL canonical IDs), bulk-replace `depends_on` edges
  (`confidence=verified_sbom`), `SbomSubmission` audit rows, artifact storage. Router uses
  `maybe_db_session` dependency (falls back to log-only when `DATABASE_URL` is unset).
- **A5 complete**: Reference Graph Bootstrapping — Procrastinate + GitHub Actions crawl of
  OpenGrants, GitHub, and deps.dev. `pg_atlas/procrastinate/` sub-package with `app.py` (App +
  PsycopgConnector), `worker.py` (CLI), `seed.py` (defers root task), `opengrants.py` (async
  HTTP client for OpenGrants API), `depsdev.py` (deps.dev gRPC wrapper via `betterproto2`),
  `upserts.py` (async SQLAlchemy upsert helpers), `tasks.py` (4 Procrastinate task definitions:
  `sync_opengrants` → `process_project` → `crawl_github_repo` → `crawl_package_deps`).
  Procrastinate schema via Alembic migration (`procrastinate_001`). deps.dev tooling
  in `pg_atlas/deps_dev/`. GitHub Actions workflows: `bootstrap.yml` (weekly), `sync-depsdev-proto.yml`
  (daily). Manual `project-git-mapping.yml` for early-round projects missing `org.stellar.communityfund.code`.
- Not complete: ingestion of SCF Impact Survey results for activity status (still blocked).
- A6 partly complete: Registry Crawlers incorporated in the graph bootstrapper. Active Subgraph Projection in review.
- A7 partly complete: Git Log Parser is done, still needs to be hooked up to Procrastinate.
- A8: the SBOM post-validation processing needs to be moved to a new Procrastinate queue.

## Keeping These Instructions Current

After completing a todo list for a session, append any new conventions, decisions, or patterns that
would help future sessions collaborate smoothly. Remove anything that was superseded. This file is
the hand-off document between sessions.

## A3 Implementation Notes

These conventions emerged during A3 and apply to all future write-path work.

### PURL canonical IDs
- GitHub repos use `pkg:github/owner/repo` (from the OIDC `repository` claim, or extracted from
  SPDX `documentNamespace`).
- Package dependencies use the PURL from `package.external_references[].locator` where
  `reference_type` contains `"purl"`, with the version suffix stripped
  (`pkg:cargo/foo@1.2.3` → `pkg:cargo/foo`). Falls back to `package.name.lower()`.
- The helper functions live in `pg_atlas/ingestion/persist.py`:
  `canonical_id_for_github_repo()` and `canonical_id_for_spdx_package()`.

### JTI upsert safety
- `_upsert_external_repo()` checks the `RepoVertex` base table first (not just `ExternalRepo`)
  before attempting an insert. This prevents `UniqueViolationError` when the same `canonical_id`
  already exists as a `Repo` or another subtype. If a base row is found, return it as-is.
- Pattern: `SELECT id FROM repo_vertices WHERE canonical_id = ?` → if found, return; else insert.

### `maybe_db_session` dependency
- Declared in `pg_atlas/db_models/session.py`.
- Yields `None` when `settings.DATABASE_URL` is empty (no DB configured).
- Yields a live `AsyncSession` otherwise.
- Test fixtures must override this dependency in `app.dependency_overrides` to yield `None`
  (using `_no_db_session` async generator) to prevent event-loop binding issues.

### Test isolation for DB integration tests
- Each test generates a unique `repository` claim via `_unique_claims()` with a `uuid4().hex[:8]`
  suffix so tests sharing SBOM fixture content don't conflict on `(content_hash, repository_claim)`.
- Exception: `test_handle_sbom_submission_github_dep_graph` uses the exact repo name from the SPDX
  fixture to trigger the self-reference check in `_upsert_external_repo()`.

## A2 Implementation Notes

These conventions emerged during A2 and apply to all future work.

### postgres:18 quirks
- Role names starting with `pg_` are disallowed as superuser names. Use `atlas` (not `pg_atlas`).
- Data directory volume mount is `/var/lib/postgresql` (not `.../data`) — PG18 layout change.
- `docker-compose.yml` uses `POSTGRES_USER: atlas`, `POSTGRES_DB: pg_atlas`.
- Local `DATABASE_URL`: `postgresql://atlas:changeme@localhost:5432/pg_atlas`.

### SQLAlchemy JTI with MappedAsDataclass
- `polymorphic_identity` must be the **enum member** (e.g. `NodeType.repo`), not the string value.
  The `Enum(NodeType)` column type causes SQLAlchemy to return enum members on SELECT, and the
  polymorphic map must match.
- `init=False` must be declared at the **attribute level** (`id: Mapped[intpk] = mapped_column(init=False)`),
  not inside an `Annotated` alias — SADeprecationWarning otherwise.
- `__mapper_args__` must not have a type annotation — SQLAlchemy stubs type it as instance var,
  `ClassVar` annotation causes mypy `[misc]` errors.
- When two relationships both write to the same FK column, add `overlaps="<other_rel_name>"` to
  silence the SQLAlchemy warning.
- **`metadata` is reserved** by `DeclarativeBase`. Use `project_metadata` / `repo_metadata` as the
  Python attribute name with `mapped_column("metadata", ...)` to keep the DB column named `metadata`.
- **MappedAsDataclass field ordering**: non-default fields must come before fields with `default=`
  or `default_factory=`. When making a nullable column required, verify it doesn't break dataclass
  ordering in the same or sub-class. Fix by reordering fields or adding `default=None` explicitly.

### Enum values vs names
- All PostgreSQL ENUM columns use `values_callable=enum_values` (from `base.py`) so Postgres stores
  the Python `.value` (e.g. `"in-dev"`) not the Python name (e.g. `"in_dev"`). This keeps DB data
  readable and resilient to Python identifier renaming.
- The shared helper is `pg_atlas.db_models.base.enum_values`; do not write per-column lambdas.

### Async test fixtures with asyncpg
- pytest-asyncio (`asyncio_mode = "auto"`) creates a new event loop **per test function** by default.
  asyncpg connections are bound to a specific event loop. Therefore, the `db_session` fixture must
  create a **fresh engine with `NullPool`** per test and dispose it afterward. Never use a pooled
  singleton engine across tests.

### Alembic + custom TypeDecorators
- Alembic autogenerate renders custom TypeDecorators using their module-qualified repr, which creates
  unimportable references in migration files. Fix: add a `render_item` hook to `migrations/env.py`
  that catches `isinstance(obj, HexBinary)` and returns `f"sa.LargeBinary(length={obj.impl.length})"`.
- The `render_item` signature must be `(str, Any, AutogenContext) -> str | Literal[False]`;
  import `AutogenContext` from `alembic.autogenerate.api`.
- PostgreSQL enum types created implicitly with tables are **not** dropped by `op.drop_table()` in
  downgrade functions. Add explicit `op.execute("DROP TYPE IF EXISTS <name>")` calls at the end of
  every `downgrade()` that creates enum columns.

### db_models package layout
```
pg_atlas/db_models/
    __init__.py          # re-exports all model classes; single import registers all on PgBase.metadata
    base.py              # PgBase, HexBinary, enum_values, all enums, intpk/canonical_id/content_hash
    project.py           # Project (standalone)
    repo_vertex.py       # RepoVertex (JTI base), Repo, ExternalRepo
    contributor.py       # Contributor (standalone)
    depends_on.py        # DependsOn edge
    contributed_to.py    # ContributedTo edge
    sbom_submission.py   # SbomSubmission audit table
    session.py           # async session manager; uses a lazy singleton factory; tests must bypass it with `NullPool`.
```

## A5 Implementation Notes

These conventions emerged during A5 implementation and apply to all future task/crawl work.

### Upsert patterns
- `upserts.py` helpers each open their own session via `_get_session_factory()` from
  `pg_atlas.db_models.session` — the factory is imported locally inside functions to avoid
  event-loop binding issues.
- `_promote_external_to_repo()` does a 3-step Core operation: delete ExternalRepo child → update
  discriminator on `repo_vertices` → insert Repo child row. Uses `metadata` (column name) not
  `repo_metadata` (Python attribute name) for the Core insert.
- `is_project_repo()` checks for a `Repo` row with a non-null `project_id`.

### Task testing patterns
- Procrastinate tasks are callable: `await task(...)` executes the underlying function.
- To mock session factory inside task functions that import it locally, patch
  `pg_atlas.db_models.session._get_session_factory` (the **source module**), not
  `pg_atlas.procrastinate.tasks._get_session_factory`.
- For recursive `defer_async` calls, use `patch.object(task, "configure", ...)` to intercept
  the Procrastinate task object's method.
- Tests that exit before DB access (e.g., `DepsDevError`) don't need the session factory mock.

### pyproject.toml tooling config
- `ruff exclude`: `pg_atlas/deps_dev/lib` (generated code).
- `ruff per-file-ignores`: `E501` for Procrastinate migration files (embedded SQL).
- `mypy exclude`: `pg_atlas/deps_dev/lib` (generated code).
- `types-PyYAML` added to dev deps for mypy stubs.

## Test Cleanup Contract (2026-03-18)

These conventions emerged during DB test-isolation hardening and apply to DB-integrating tests.

- Shared cleanup utilities live in `tests/db_cleanup.py`.
- Cleanup strategy is snapshot + diff on primary keys: capture pre-test rows, delete only rows created during the test.
- Never blanket-clear all tables in test teardown; preserve any pre-existing developer data.
- Debug controls:
  - `PG_ATLAS_TEST_BREAK_BEFORE_CLEANUP=1` triggers `breakpoint()` before teardown deletion.
  - `PG_ATLAS_TEST_SKIP_CLEANUP=1` skips teardown deletion (for interactive debugging only).
- Keep table cleanup ordering FK-safe: edge/child tables before parent tables.
