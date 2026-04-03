"""
Shared router utilities — pagination dependency and DB session guard.

Centralises cross-cutting concerns that were previously duplicated across every
router module.  Import ``require_session`` and ``PaginationParams`` from here
instead of defining per-router copies.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

# Re-export ``maybe_db_session`` so callers that genuinely need the
# ``AsyncSession | None`` path (e.g. ingestion's POST) can import from one place.
__all__ = ["DbSession", "PaginationParams", "maybe_db_session", "require_session"]

from typing import Annotated

from fastapi import Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from pg_atlas.db_models.session import maybe_db_session


def require_session(
    session: Annotated[AsyncSession | None, Depends(maybe_db_session)],
) -> AsyncSession:
    """
    Raise HTTP 503 if the database session is unavailable.

    Used as a FastAPI dependency so that every DB-backed endpoint fails fast
    with a clear message when ``PG_ATLAS_DATABASE_URL`` is not configured.
    """

    if session is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database is not configured.",
        )

    return session


DbSession = Annotated[AsyncSession, Depends(require_session)]
"""
Annotated dependency that resolves to a live ``AsyncSession``.

Combines ``maybe_db_session`` with the ``require_session`` guard in a single
type alias.  Use as a parameter annotation on any endpoint that needs the DB::

    async def list_things(db: DbSession) -> ...:
"""


class PaginationParams:
    """
    Reusable pagination dependency for list endpoints.

    Inject via ``Depends(PaginationParams)`` to get validated ``limit``
    and ``offset`` from query parameters::

        async def list_things(
            db: DbSession,
            pagination: PaginationParams = Depends(),
        ) -> PaginatedResponse[ThingSummary]:
            ...
            total = ...
            rows = query.limit(pagination.limit).offset(pagination.offset)
            return PaginatedResponse(
                items=...,
                total=total,
                limit=pagination.limit,
                offset=pagination.offset,
            )
    """

    def __init__(
        self,
        limit: Annotated[int, Query(ge=1, le=200, description="Maximum number of items to return")] = 50,
        offset: Annotated[int, Query(ge=0, description="Number of items to skip")] = 0,
    ) -> None:
        self.limit = limit
        self.offset = offset
