"""
Async SQLAlchemy engine and session factory for PG Atlas.

This module is not imported by the app in A3 \u2014 the ingestion webhook and health
endpoint do not access the database yet. It will be wired up in A2 when the
PostgreSQL schema is introduced.

Importing this module requires ``PG_ATLAS_DATABASE_URL`` to be set; a
``ValueError`` is raised at import time if it is empty so that a misconfigured
deployment fails fast rather than producing a cryptic connection error on first
use.

Usage (in FastAPI route handlers, once A2 is complete):

    async with AsyncSessionLocal() as session:
        result = await session.scalars(select(Repo))

Author: SCF Public Goods Maintenance <https://github.com/SCF-Public-Goods-Maintenance>
"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from pg_atlas.config import settings

# Engine and session factory are module-level singletons, created at import time.
# DATABASE_URL is not required in A3 — the ingestion webhook does not persist data
# yet. Once A2 is wired up, an empty DATABASE_URL will raise a ValueError here
# at startup rather than producing a confusing connection error later.
if not settings.DATABASE_URL:
    raise ValueError(
        "PG_ATLAS_DATABASE_URL is not configured. "
        "Set it to a postgresql+asyncpg:// URL before starting the server with a database."
    )

_engine = create_async_engine(
    settings.DATABASE_URL,
    echo=settings.LOG_LEVEL == "DEBUG",
    pool_pre_ping=True,
)

AsyncSessionLocal: async_sessionmaker[AsyncSession] = async_sessionmaker(
    _engine,
    expire_on_commit=False,
)
