"""Application settings loaded from environment variables or a .env file.

All settings are prefixed with PG_ATLAS_ in the environment. A .env file at
the project root is automatically loaded in development.

Author: SCF Public Goods Maintenance <https://github.com/SCF-Public-Goods-Maintenance>
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """PG Atlas application settings.

    Required environment variables:
        PG_ATLAS_API_URL: The canonical URL of this API instance. Used as the
            OIDC audience when verifying GitHub OIDC tokens from the SBOM action.
            Must exactly match the `audience` value the action was configured with.

    Optional environment variables:
        PG_ATLAS_DATABASE_URL: Async SQLAlchemy database URL
            (e.g. postgresql+asyncpg://user:pass@host/db). Not required until A2.
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

    API_URL: str
    DATABASE_URL: str = ""
    LOG_LEVEL: str = "INFO"
    JWKS_CACHE_TTL_SECONDS: int = 3600


# Module-level singleton — import this throughout the application.
# pydantic-settings reads API_URL from the environment; mypy cannot see that at
# type-check time, so the required-field call-arg error is suppressed here.
settings = Settings()  # type: ignore[call-arg]
