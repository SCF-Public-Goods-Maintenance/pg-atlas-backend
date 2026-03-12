"""
Application settings loaded from environment variables or a .env file.

All settings are prefixed with PG_ATLAS_ in the environment. A .env file at
the project root is automatically loaded in development (at least with VS Code).

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from pathlib import Path

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    PG Atlas application settings.

    Required environment variables:
        PG_ATLAS_API_URL: The canonical URL of this API instance. Used as the
            OIDC audience when verifying GitHub OIDC tokens from the SBOM action.
            Must exactly match the ``audience`` value the action was configured with.

    Optional environment variables:
        PG_ATLAS_DATABASE_URL: PostgreSQL DSN / connection string
            (``postgresql://user:pass@host/db``). An empty string disables
            the database session factory; the server will start but any endpoint that
            calls ``get_db_session()`` will raise at runtime.
        PG_ATLAS_ARTIFACT_STORE_PATH: Filesystem path where raw SBOM bytes are written
            (local dev). Defaults to ``./artifact_store`` (relative to the working
            directory). In production, set this to the container-local mount point of
            the Storacha-backed volume.
        PG_ATLAS_LOG_LEVEL: Python log level string (DEBUG, INFO, WARNING, ERROR).
            Defaults to INFO.
        PG_ATLAS_JWKS_CACHE_TTL_SECONDS: How long to cache GitHub's JWKS response
            in memory. Defaults to 3600 (1 hour). GitHub rotates keys infrequently.
    """

    model_config = SettingsConfigDict(
        env_prefix="PG_ATLAS_",
        env_file=".env",
        env_file_encoding="utf-8",
    )

    API_URL: str = "http://localhost:8000"
    DATABASE_URL: str = ""
    OPENGRANTS_KEY: str = ""

    @field_validator("DATABASE_URL", mode="before")
    @classmethod
    def coerce_async_driver(cls, db_url: str) -> str:
        """
        Rewrite ``postgres://`` / ``postgresql://`` → ``postgresql+asyncpg://``.

        DigitalOcean App Platform injects managed-database connection strings in the
        plain ``postgresql://`` form.  SQLAlchemy needs the driver qualifier so that
        it selects asyncpg as the DBAPI.  Non-empty values that already contain
        ``+asyncpg`` are returned unchanged. Strips query parameters.
        """
        for prefix in ("postgres://", "postgresql://"):
            if db_url.startswith(prefix):
                rewritten_url = "postgresql+asyncpg://" + db_url[len(prefix) :]
                # also strip any query params that may not be supported
                return rewritten_url.partition("?")[0]

        return db_url

    ARTIFACT_STORE_PATH: Path = Path("./artifact_store")
    LOG_LEVEL: str = "INFO"
    JWKS_CACHE_TTL_SECONDS: int = 3600

    # --- Crawler settings ---
    CRAWLER_RATE_LIMIT: float = 1.0
    CRAWLER_MAX_RETRIES: int = 3
    CRAWLER_TIMEOUT: float = 30.0

    # --- Git log parser settings ---
    GITLOG_SINCE_MONTHS: int = 24
    GITLOG_CLONE_DIR: str = "/tmp/pg-atlas-clones"
    GITLOG_CLONE_TIMEOUT: float = 120.0
    GITLOG_CLONE_DELAY: float = 1.0


# Module-level singleton — import this throughout the application.
settings = Settings()
