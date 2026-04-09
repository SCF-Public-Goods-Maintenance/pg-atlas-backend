# Implementation Notes

Human + agent co-authored conventions that are good to be aware of.

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

- `upsert_external_repo()` (in `vertex_ops.py`) checks the `RepoVertex` base table first. If the
  canonical_id belongs to a `Repo`, it **raises `ValueError`** (programming error — bootstrap flow
  never hits this). If the canonical_id belongs to an `ExternalRepo`, it updates and returns it.
  Only inserts if no base row exists.
- SBOM ingestion (`persist.py`) catches `ValueError` from `upsert_external_repo()` and falls back
  to `get_vertex()` — a dependency in the SBOM can legitimately match an existing `Repo`.

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

```txt
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

- `upserts.py` helpers each open their own session via `get_session_factory()` from
  `pg_atlas.db_models.session` — the factory is imported locally inside functions to avoid
  event-loop binding issues.
- `_promote_external_to_repo()` does a 3-step Core operation: delete ExternalRepo child → update
  discriminator on `repo_vertices` → insert Repo child row. Uses `metadata` (column name) not
  `repo_metadata` (Python attribute name) for the Core insert.
- `absorb_external_repo()` merges an ExternalRepo into an existing Repo by re-pointing all
  DependsOn edges and deleting the ExternalRepo vertex. Uses SQLAlchemy Core for edge manipulation
  to handle composite-PK conflict detection.
- `find_repo_by_release_purl()` uses JSONB containment (`@>`) on `repos.releases` to look up
  a Repo by a package PURL from its releases list. GIN index `ix_repos_releases_gin` makes this fast.

### Vertex model invariants

- **Repo.canonical_id** = always `pkg:github/owner/repo`; ExternalRepo.canonical_id = always
  `pkg:{ecosystem}/name`.
- Crawlers create ExternalRepo when no Project association is known; Repo only through project link.
- `is_project_repo()` was removed — deps.dev requirements use ecosystem PURLs, never `pkg:github/`.
  Use `find_repo_by_release_purl()` to check if a package PURL maps to an existing Repo.

### deps.dev channel reuse

- `depsdev_session()` yields a reusable `InsightsStub` backed by a single TLS channel. All 4 public
  functions (`get_package`, `get_requirements`, `get_project_batch`, `get_project_package_versions`)
  accept an optional `stub` keyword argument for channel reuse. Without it, an ephemeral channel
  is created per call (backwards compatible).

### Task testing patterns

- Procrastinate tasks are callable: `await task(...)` executes the underlying function.
- To mock session factory inside task functions, patch at the **import site**:
  `pg_atlas.procrastinate.tasks.get_session_factory` (not the source module
  `pg_atlas.db_models.session.get_session_factory`).
- For recursive `defer_async` calls, use `patch.object(task, "configure", ...)` to intercept
  the Procrastinate task object's method.
- Tests that exit before DB access (e.g., `DepsDevError`) don't need the session factory mock.

### pyproject.toml tooling config

- `ruff exclude`: `pg_atlas/deps_dev/lib` (generated code).
- `ruff per-file-ignores`: `E501` for Procrastinate migration files (embedded SQL).
- `mypy exclude`: `pg_atlas/deps_dev/lib` (generated code).
- `types-PyYAML` added to dev deps for type stubs.

## A8 Implementation Notes

These conventions emerged during A8 implementation (PR #26) and apply to artifact storage and background processing.

### Artifact Storage (Filebase)

- Durable artifact storage uses Filebase's S3-compatible API.
- The `ARTIFACT_S3_ENDPOINT` requires `ARTIFACT_S3_BUCKET`, `FILEBASE_ACCESS_KEY`, and `FILEBASE_SECRET_KEY`. Pydantic models validate this implicitly.
- Uploads happen using `boto3` (via `aiobotocore`) via S3 APIs `put_object`.
- The durable unique ID (CID) of an artifact is returned in the `x-amz-meta-cid` header from both `put_object` and `head_object`.
- Retrieval path differs from S3: reading Filebase objects via standard S3 API `get_object` by CID returns `NoSuchKey`. Instead, all reads (both worker processing and public API) use the IPFS gateway endpoint `https://ipfs.filebase.io/ipfs/<cid>`.
- The `artifact_path` stored in the DB is always the CID string itself.

### Parser/latency optimization (A8 follow-up)

- `pg_atlas/ingestion/spdx.py` no longer uses stdlib `json` or Pydantic for SBOM decode/hash extraction.
- Raw SPDX bytes are decoded exactly once in `parse_and_validate_spdx()` using `msgspec` (`msgspec.json.Decoder`).
- SPDX validation uses `spdx_tools.spdx.parser.jsonlikedict.JsonLikeDictParser` directly (no temp files).
- `ParsedSbom` now carries:
  - `document`
  - `package_count`
  - `unwrapped_bytes` (canonical SPDX JSON with no GitHub `{"sbom": ...}` envelope)
  - `semantic_hash`
- `compute_sbom_semantic_hash(raw)` now delegates to `parse_and_validate_spdx(raw)` and falls back to raw SHA-256 when parsing fails.

### Artifact write format

- New ingested artifacts are persisted as **unwrapped SPDX JSON** bytes (inner document only).
- Legacy stored enveloped artifacts remain supported: worker parsing still accepts `{"sbom": ...}` payloads.
- On validation failure where envelope decoding succeeds, failed-submission artifacts are still stored in unwrapped form for consistency.
