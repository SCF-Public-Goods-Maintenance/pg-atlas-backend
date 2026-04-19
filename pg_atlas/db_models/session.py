"""
Async SQLAlchemy engine and session factory for PG Atlas.

The engine and session factory are lazily initialised on first use so that this
module can be imported freely without requiring ``PG_ATLAS_DATABASE_URL`` to be set.
A ``ValueError`` is raised at the point of first use if ``DATABASE_URL`` is empty,
so misconfigured deployments still fail fast — just not at import time.

Typical usage in FastAPI route handlers (via dependency injection)::

    from fastapi import Depends
    from sqlalchemy.ext.asyncio import AsyncSession
    from pg_atlas.db.session import get_db_session

    @router.get("/example")
    async def example(session: AsyncSession = Depends(get_db_session)):
        result = await session.scalars(select(Repo))

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from collections.abc import AsyncGenerator

import msgspec
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from pg_atlas.config import settings

# ---------------------------------------------------------------------------
# Lazy singletons — created on first call to _get_session_factory()
# ---------------------------------------------------------------------------


_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    """
    Return the async session factory, creating the engine on first call.

    Raises ``ValueError`` if ``PG_ATLAS_DATABASE_URL`` is not configured.
    """
    global _engine, _session_factory
    if _session_factory is None:
        if not settings.DATABASE_URL:
            raise ValueError(
                "PG_ATLAS_DATABASE_URL is not configured. Set it to a postgresql:// DSN before using database sessions."
            )

        decoder = msgspec.json.Decoder()
        _engine = create_async_engine(
            settings.DATABASE_URL,
            echo=settings.LOG_LEVEL == "DEBUG",
            pool_pre_ping=True,
            json_deserializer=decoder.decode,
        )
        _session_factory = async_sessionmaker(_engine, expire_on_commit=False)
    return _session_factory


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """
    FastAPI dependency that yields a database session per request.

    Opens a new ``AsyncSession`` from the shared connection pool and closes it
    (returning the connection to the pool) after the request completes. Use via
    ``Depends(get_db_session)`` in route handlers that need database access.

    Raises ``ValueError`` at first use if ``PG_ATLAS_DATABASE_URL`` is not set.
    """
    async with get_session_factory()() as session:
        yield session


async def maybe_db_session() -> AsyncGenerator[AsyncSession | None, None]:
    """
    FastAPI dependency that yields a live session when the database is configured
    or ``None`` when it is not.

    Use this in endpoints that must remain functional even without a database
    (e.g. in CI or during local development without Docker).  When
    ``PG_ATLAS_DATABASE_URL`` is empty the dependency yields ``None``; calling
    code should fall back to stub / logging-only behaviour.
    """
    if not settings.DATABASE_URL:
        yield None
        return
    async with get_session_factory()() as session:
        yield session
