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

---

# PG Atlas Backend — Project-Specific Instructions

## Deliverable Naming

Work is organised into deliverables labelled A1, A2, A3 … (from the proposal in
`SCF-Public-Goods-Maintenance/scf-public-goods-maintenance.github.io`). Current scope is defined by
whichever is being built; stubs for later deliverables are marked `# TODO A<n>:` in code.

## Tooling

- **uv** for package management. Always use `uv run` commands; never activate the venv manually.
- **ruff** for lint and format (`line-length = 127`, selects E, F, I). Ruff 0.15.2 with `requires-python = ">=3.14"` (i.e. `target-version = "py314"`) intentionally
  rewrites `except (A, B):` → `except A, B:` per PEP 758.
- **mypy** in strict mode (`disallow_untyped_defs`, `explicit_package_bases`, `ignore_missing_imports`).
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

- Multi-line docstrings open with a blank line after `"""` — the summary sentence begins on the
  second line. Single-line docstrings stay on one line.
- Exception handling: be precise — do not use bare `except Exception` to catch expected errors;
  name the specific exception types (e.g. `except (PyJWKClientError, OSError)`).
- Fail fast over silent fallbacks: if a required config value is missing, raise `ValueError` at
  import/startup rather than falling back to a placeholder that silently misbehaves later.
- Conventional Commits for all commit messages. release-please handles changelog and version bumps.

## GitHub Actions

- The `gh` CLI is available as a fallback when the GitHub MCP gives a 403.
- The SBOM action (`SCF-Public-Goods-Maintenance/pg-atlas-sbom-action`) is used by this repo too —
  it runs in CI on push to main.

## Current Deployment State

- **A1 complete**: SBOM ingestion (`POST /ingest/sbom`, OIDC auth, SPDX 2.3 parsing, 202 Accepted),
  health endpoint (`GET /health`), CI green, DO App Platform live at
  `https://pg-atlas-backend-h8gen.ondigitalocean.app` (`basic-xxs`, region `ams3`).
- **A2 complete**: PostgreSQL schema (`pg_atlas/db_models/`), Alembic migration (revision
  `f3d946ade07e`), artifact storage (`pg_atlas/storage/artifacts.py`). Schema refined in a follow-up
  session: `metadata` attribute renamed (`project_metadata`/`repo_metadata`); `latest_version` made
  required on `Repo`/`ExternalRepo`; `artifact_path` made non-nullable; enum values now use
  `values_callable=enum_values` throughout; `email_hash` stored as BYTEA via `HexBinary`.
  See A2 notes below.
- **A3 is next**: SBOM write path — wire `SbomSubmission` rows into the ingestion queue,
  persist artifacts via `store_artifact()`, set `status` → `processed`/`failed`.
  See `devops.md` `### Future Work` for the concrete A3–A6 checklist.

## Keeping These Instructions Current

After completing a todo list for a session, append any new conventions, decisions, or patterns that
would help future sessions collaborate smoothly. Remove anything that was superseded. This file is
the hand-off document between sessions.

## A2 Implementation Notes

These conventions emerged during A2 and apply to all future work.

### postgres:18 quirks
- Role names starting with `pg_` are disallowed as superuser names. Use `atlas` (not `pg_atlas`).
- Data directory volume mount is `/var/lib/postgresql` (not `.../data`) — PG18 layout change.
- `docker-compose.yml` uses `POSTGRES_USER: atlas`, `POSTGRES_DB: pg_atlas`.
- Local `DATABASE_URL`: `postgresql+asyncpg://atlas:changeme@localhost:5432/pg_atlas`.

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
```
`pg_atlas/db/base.py` is a re-export shim for backward compatibility.
`pg_atlas/db/session.py` uses a lazy singleton factory; tests must bypass it with `NullPool`.
