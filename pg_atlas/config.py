"""
Application settings loaded from environment variables or a .env file.

All settings are prefixed with PG_ATLAS_ in the environment. A .env file at
the project root is automatically loaded in development (at least with VS Code).

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from pathlib import Path

from pydantic import Field, HttpUrl, field_validator, model_validator
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
        PG_ATLAS_ARTIFACT_STORE_PATH: Filesystem path where artifacts are written
            (local dev). Defaults to ``./artifact_store`` (relative to the working
            directory).
        PG_ATLAS_IPFS_GATEWAY_URL: Optional IPFS Gateway URL, used for artifact reads.
        PG_ATLAS_ARTIFACT_S3_ENDPOINT: Optional Filebase S3-compatible endpoint URL.
            When set, raw artifacts are uploaded to Filebase and the stored
            ``artifact_path`` becomes the returned CID instead of a local relative path.
        PG_ATLAS_ARTIFACT_S3_BUCKET: Filebase bucket used for artifact uploads.
        PG_ATLAS_FILEBASE_ACCESS_KEY: Filebase S3 access key.
        PG_ATLAS_FILEBASE_SECRET_KEY: Filebase S3 secret key.
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
    IPFS_GATEWAY_URL: HttpUrl | None = None
    ARTIFACT_S3_ENDPOINT: HttpUrl | None = None
    ARTIFACT_S3_BUCKET: str = "pga-ingested-artifacts"
    FILEBASE_ACCESS_KEY: str | None = None
    FILEBASE_SECRET_KEY: str | None = None
    LOG_LEVEL: str = "INFO"
    JWKS_CACHE_TTL_SECONDS: int = 3600

    # --- Crawler settings ---
    CRAWLER_RATE_LIMIT: float = 1.0
    CRAWLER_MAX_RETRIES: int = 3
    CRAWLER_TIMEOUT: float = 30.0

    # --- GitHub dependents crawler ---
    # Scheduling fails closed: ENABLED=false schedules nothing; ENABLED=true
    # schedules only repositories on the allowlist (comma-separated owner/repo,
    # case-insensitive); the explicit value "*" schedules all eligible repos.
    # Changing either setting does not cancel already-queued jobs. Explicit
    # CLI runs bypass both. Caps bound the scrape per repository: pages per
    # listing, dependents overall, and packages walked for multi-package
    # repositories.
    GITHUB_DEPENDENTS_ENABLED: bool = False
    GITHUB_DEPENDENTS_ALLOWLIST: str = ""
    GITHUB_DEPENDENTS_PAGES_CAP: int = Field(default=20, gt=0)
    GITHUB_DEPENDENTS_ENTRY_CAP: int = Field(default=500, gt=0)
    GITHUB_DEPENDENTS_PACKAGES_CAP: int = Field(default=25, gt=0)

    # --- Maintenance metric ---
    # Scheduling fails closed: ENABLED=false schedules nothing; ENABLED=true
    # schedules only repositories on the allowlist (comma-separated
    # owner/repo, case-insensitive); the explicit value "*" schedules all
    # eligible repos. The collector task re-checks both settings at execution
    # time, so disabling stops already-queued work; gated materialization
    # re-checks the enable flag. Explicit CLI runs bypass the gate.
    MAINTENANCE_METRIC_ENABLED: bool = False
    MAINTENANCE_METRIC_ALLOWLIST: str = ""
    #: Repos whose issue tracking is declared to live outside GitHub
    #: (comma-separated owner/repo). Issue signals become not-applicable.
    MAINTENANCE_EXTERNAL_TRACKER_REPOS: str = ""
    #: Repos that hold a funded project's contributions but that the project
    #: does not control or maintain as a whole (comma-separated owner/repo).
    #: Repo-level signals there describe the hosting organization, not the
    #: funded work: declared host repos are never scheduled for collection,
    #: their profiles render every signal not-applicable, and they join no
    #: percentile pool.
    MAINTENANCE_HOST_REPOS: str = ""
    #: Per-repo declared maintainer logins (comma-separated
    #: "owner/repo=login1|login2"). Listed logins qualify as maintainers for
    #: that repo regardless of the author association GitHub reports, closing
    #: the blind spot where private org membership hides MEMBER. Empty leaves
    #: the association heuristic as the only rule.
    MAINTENANCE_DECLARED_MAINTAINERS: str = ""
    #: Repos that deploy from the default branch and use GitHub Releases as a
    #: changelog (comma-separated owner/repo). Their profiles report
    #: release_ships_code=false in the release-cadence block; ranking is
    #: unchanged.
    MAINTENANCE_DEPLOY_ON_PUSH_REPOS: str = ""
    #: Trailing window for the responsiveness cohorts and the commit count.
    #: 90 days is approximately one funding quarter, aligning a quarter-end
    #: run with the funded period.
    MAINTENANCE_WINDOW_DAYS: int = Field(default=90, gt=0)
    #: Days a community issue or PR may wait for its first maintainer
    #: response and still count as responded within the interval.
    MAINTENANCE_RESPONSE_INTERVAL_DAYS: int = Field(default=7, gt=0)
    #: Release cadence: the median gap runs over the last N shipping events;
    #: fewer distinct dates than the minimum leave the median gap unavailable.
    MAINTENANCE_CADENCE_LAST_N_EVENTS: int = Field(default=10, gt=1)
    MAINTENANCE_CADENCE_MIN_EVENTS: int = Field(default=3, gt=1)
    #: Cohort membership rules, recorded in the signal payloads as
    #: measurement parameters: prereleases as shipping events in the GitHub
    #: Releases fallback, draft PRs in the community PR cohort.
    MAINTENANCE_INCLUDE_PRERELEASES: bool = True
    MAINTENANCE_INCLUDE_DRAFT_PRS: bool = True
    #: Per-repo caps for the GitHub collection: listing pages per item type,
    #: total API requests, and wall-clock seconds. A capped repo is recorded
    #: incomplete for the affected signals, never silently mixed into results.
    MAINTENANCE_ITEM_PAGE_CAP: int = Field(default=10, gt=0)
    MAINTENANCE_REQUEST_CAP: int = Field(default=120, gt=0)
    MAINTENANCE_TIME_CAP_SECONDS: float = Field(default=180.0, gt=0)
    #: Longest single wait for an advertised rate-limit reset; a reset
    #: further away records the affected signals incomplete (rate-limited).
    MAINTENANCE_RATE_LIMIT_MAX_WAIT_SECONDS: float = Field(default=120.0, ge=0)
    #: Freshness bounds: collected issue/PR signals and git-log artifacts older
    #: than these are context only, never percentile-ranked.
    MAINTENANCE_SIGNALS_MAX_AGE_DAYS: int = Field(default=14, gt=0)
    MAINTENANCE_GITLOG_MAX_AGE_DAYS: int = Field(default=21, gt=0)

    # --- Git log parser settings ---
    GITLOG_SINCE_MONTHS: int = 24
    GITLOG_CLONE_DIR: str = "/tmp/pg-atlas-clones"
    GITLOG_CLONE_TIMEOUT: float = 120.0
    GITLOG_CLONE_DELAY: float = 0.0
    GITLOG_BATCH_SIZE: int = 20
    GITLOG_DORMANCY_K_RUNS: int = 9
    GITLOG_RATE_LIMIT_MAX_RETRIES: int = 4
    GITLOG_RATE_LIMIT_INITIAL_BACKOFF_SECONDS: float = 2.0
    GITLOG_RATE_LIMIT_MAX_BACKOFF_SECONDS: float = 60.0

    @model_validator(mode="after")
    def validate_filebase_settings(self) -> Settings:
        """
        Require Filebase credentials when remote artifact storage is enabled.
        """

        if self.ARTIFACT_S3_ENDPOINT is None:
            return self

        missing: list[str] = []
        for name, value in (
            ("ARTIFACT_S3_BUCKET", self.ARTIFACT_S3_BUCKET),
            ("FILEBASE_ACCESS_KEY", self.FILEBASE_ACCESS_KEY),
            ("FILEBASE_SECRET_KEY", self.FILEBASE_SECRET_KEY),
        ):
            if not value:
                missing.append(name)

        if missing:
            joined = ", ".join(missing)
            raise ValueError(
                f"Filebase artifact storage requires the following settings when ARTIFACT_S3_ENDPOINT is set: {joined}"
            )

        return self


# Module-level singleton — import this throughout the application.
settings = Settings()
