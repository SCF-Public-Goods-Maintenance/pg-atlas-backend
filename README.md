# PG Atlas Backend

Backend for [PG Atlas](https://scf-public-goods-maintenance.github.io/pg-atlas) — the metrics
backbone for the SCF Public Goods dependency graph. Provides the ingestion pipeline, storage layer,
metric computation engine, and REST API.

Built as free open-source software under the Mozilla Public License 2.0.

## Architecture

See the
[PG Atlas architecture documentation](https://scf-public-goods-maintenance.github.io/pg-atlas) for
design decisions. Key documents:

- [Ingestion](https://scf-public-goods-maintenance.github.io/pg-atlas/ingestion)
- [Storage](https://scf-public-goods-maintenance.github.io/pg-atlas/storage)
- [API](https://scf-public-goods-maintenance.github.io/pg-atlas/api)

## Local Development

[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/SCF-Public-Goods-Maintenance/pg-atlas-backend/main.svg)](https://results.pre-commit.ci/latest/github/SCF-Public-Goods-Maintenance/pg-atlas-backend/main)

### Prerequisites

- Python 3.14+
- [uv](https://docs.astral.sh/uv/) (`pip install uv` or `brew install uv`)

### Setup

```sh
git clone https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend.git
cd pg-atlas-backend

# Install all dependencies (including dev tools) into a managed venv
uv sync

# Install pre-commit hooks (pre-commit and commit-msg hook types)
uv run pre-commit install --install-hooks --hook-type pre-commit --hook-type commit-msg

# Copy the example env file and fill in required values
cp .env.example .env
```

Edit `.env` and set at minimum:

```dotenv
PG_ATLAS_API_URL=http://localhost:8000
```

Create DB schema and apply revisions:

```sh
uv run alembic upgrade heads
```

### Running the API

```sh
uv run python -m pg_atlas --reload
# or equivalently:
uv run uvicorn pg_atlas.main:app --reload
```

The API will be available at <http://localhost:8000>. Interactive docs at
<http://localhost:8000/docs>.

### Running with Docker Compose

```sh
docker compose up --build
```

### Running Tests

```sh
uv run pytest
```

DB integration tests use selective cleanup (snapshot + delete created rows only) so
existing local development data is preserved.

Debug toggles for DB test teardown:

- `PG_ATLAS_TEST_BREAK_BEFORE_CLEANUP=1`: triggers `breakpoint()` immediately before cleanup.
- `PG_ATLAS_TEST_SKIP_CLEANUP=1`: skips cleanup deletion for the current run.

When `PG_ATLAS_TEST_SKIP_CLEANUP=1` is enabled, reset it before normal runs to
restore test isolation guarantees.

Lint and type checks:

```sh
uv run ruff check .
uv run ruff format --check .
uv run mypy pg_atlas/
```

## Environment Variables

All settings are prefixed with `PG_ATLAS_`. See [pg_atlas/config.py](pg_atlas/config.py) for the full
list and documentation.

| Variable                          | Required      | Default | Description                                                 |
| --------------------------------- | ------------- | ------- | ----------------------------------------------------------- |
| `PG_ATLAS_API_URL`                | Yes           | —       | Canonical URL of this API instance. Used as OIDC audience.  |
| `PG_ATLAS_DATABASE_URL`           | Yes           | `""`    | PostgreSQL DSN / connection string (`postgresql://...`).    |
| `PG_ATLAS_OPENGRANTS_KEY`         | No            | `""`    | OpenGrants API key for increased rate limits.               |
| `PG_ATLAS_LOG_LEVEL`              | No            | `INFO`  | Python log level (`DEBUG`, `INFO`, `WARNING`, `ERROR`).     |
| `PG_ATLAS_JWKS_CACHE_TTL_SECONDS` | No            | `3600`  | How long to cache GitHub's JWKS in memory (seconds).        |

## Submitting an SBOM

Project teams submit SBOMs by adding the
[pg-atlas-sbom-action](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-sbom-action) to their
CI workflow:

```yaml
jobs:
  sbom:
    runs-on: ubuntu-latest
    permissions:
      contents: read # for GitHub Dependency Graph API
      id-token: write # for OIDC authentication to PG Atlas
    steps:
      - uses: SCF-Public-Goods-Maintenance/pg-atlas-sbom-action@<full-commit-hash>
```

The action fetches the repo's SPDX 2.3 dependency graph from GitHub's Dependency Graph API and
submits it to `POST /ingest/sbom`, authenticated via a short-lived GitHub OIDC token. No secrets need
to be configured in the calling repository.

## Conventional Commits

Commits must follow [Conventional Commits](https://www.conventionalcommits.org/).
Releases and `CHANGELOG.md` are managed automatically by
[release-please](https://github.com/googleapis/release-please) on every push to `main`.
See the [cheatsheet](https://gist.github.com/qoomon/5dfcdf8eec66a051ecd85625518cfd13)
on how to write _good_ commit messages.

## License

[Mozilla Public License 2.0](LICENSE). Contributions welcome — see [CONTRIBUTING.md](CONTRIBUTING.md)
(to be added).
