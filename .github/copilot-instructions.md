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

---

# PG Atlas Backend — Project-Specific Instructions

## Deliverable Naming

Work is organised into deliverables labelled A1, A2, A3 … (from the proposal in
`SCF-Public-Goods-Maintenance/scf-public-goods-maintenance.github.io`). Current scope is defined by
whichever is being built; stubs for later deliverables are marked `# TODO A<n>:` in code.

## Tooling

- **uv** for package management. Always use `uv run` commands; never activate the venv manually.
- **ruff** for lint and format (`line-length = 127`, selects E, F, I).
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

## Git & Version Control

Never run `git add`, `git stage`, `git commit`, `git push`, or any equivalent (including GitHub MCP
`push_files` / `create_or_update_file` to the repo) without **explicit user approval**. Prepare
changes in the working tree, summarize what is ready, and wait for the user to review before any
commit is created.

## Keeping These Instructions Current

After completing a todo list for a session, append any new conventions, decisions, or patterns that
would help future sessions collaborate smoothly. Remove anything that was superseded. This file is
the hand-off document between sessions.
