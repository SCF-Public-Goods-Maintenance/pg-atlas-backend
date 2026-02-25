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

### Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) (`pip install uv` or `brew install uv`)

### Setup

```sh
git clone https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend.git
cd pg-atlas-backend

# Install all dependencies (including dev tools) into a managed venv
uv sync

# Copy the example env file and fill in required values
cp .env.example .env
```

Edit `.env` and set at minimum:

```dotenv
PG_ATLAS_API_URL=http://localhost:8000
```

### Running the API

```sh
uv run python -m pg_atlas
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
| `PG_ATLAS_DATABASE_URL`           | No (until A2) | `""`    | Async SQLAlchemy database URL (`postgresql+asyncpg://...`). |
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

## License

[Mozilla Public License 2.0](LICENSE). Contributions welcome — see [CONTRIBUTING.md](CONTRIBUTING.md)
(to be added).
