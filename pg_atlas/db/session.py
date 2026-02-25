"""Async SQLAlchemy engine and session factory for PG Atlas.

The engine and session factory are created lazily on first use so that the
application starts up even when PG_ATLAS_DATABASE_URL is not yet configured
(the database is not required for the ingestion webhook or health endpoint in A3).

Usage (in FastAPI route handlers, once A2 is complete):

    async with AsyncSessionLocal() as session:
        result = await session.scalars(select(Repo))

Author: SCF Public Goods Maintenance <https://github.com/SCF-Public-Goods-Maintenance>
"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from pg_atlas.config import settings

# Engine and session factory are module-level singletons, created at import time.
# They will raise a configuration error on first connection attempt if
# PG_ATLAS_DATABASE_URL is empty — which is expected until A2.
_engine = create_async_engine(
    settings.DATABASE_URL or "postgresql+asyncpg://localhost/pg_atlas",
    echo=settings.LOG_LEVEL == "DEBUG",
    pool_pre_ping=True,
)

AsyncSessionLocal: async_sessionmaker[AsyncSession] = async_sessionmaker(
    _engine,
    expire_on_commit=False,
)
