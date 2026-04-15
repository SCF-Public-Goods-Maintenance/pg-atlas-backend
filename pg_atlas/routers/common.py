"""
Shared router utilities — pagination dependency, DB session guard, and sort parsing.

Centralises cross-cutting concerns that were previously duplicated across every
router module.  Import ``require_session`` and ``PaginationParams`` from here
instead of defining per-router copies.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

# Re-export ``maybe_db_session`` so callers that genuinely need the
# ``AsyncSession | None`` path (e.g. ingestion's POST) can import from one place.
__all__ = ["DbSession", "PaginationParams", "maybe_db_session", "parse_sort_params", "require_session"]

from typing import Annotated, Any

from fastapi import Depends, HTTPException, Query, status
from sqlalchemy import UnaryExpression, asc, case, desc, nulls_last
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import ColumnProperty, InstrumentedAttribute

from pg_atlas.db_models.base import ActivityStatus
from pg_atlas.db_models.session import maybe_db_session

# Canonical lifecycle ordering for activity_status sorts.
# Lower values sort first in ascending order.
_ACTIVITY_STATUS_ORDER = {
    ActivityStatus.live: 0,
    ActivityStatus.in_dev: 1,
    ActivityStatus.discontinued: 2,
    ActivityStatus.non_responsive: 3,
}


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


def parse_sort_params(
    sort: str | None,
    allowed_fields: dict[str, InstrumentedAttribute[Any]],
    tiebreaker: InstrumentedAttribute[Any],
) -> list[UnaryExpression[Any]]:
    """
    Parse a comma-separated ``field:direction`` sort string into ORDER BY clauses.

    Parameters
    ----------
    sort:
        Raw query-string value, e.g. ``"criticality_score:desc,display_name:asc"``.
        ``None`` or empty string → fall back to ``tiebreaker ASC`` only.
    allowed_fields:
        Mapping of field name → SQLAlchemy column attribute.  Only these fields
        are accepted; anything else raises HTTP 422.
    tiebreaker:
        Column appended as the final ORDER BY clause to guarantee deterministic
        pagination (typically ``Model.canonical_id``).

    Returns
    -------
    list[UnaryExpression]
        Ready-to-use ORDER BY clauses for ``Select.order_by(*clauses)``.

    Special handling
    ~~~~~~~~~~~~~~~~
    - **``activity_status``**: uses a ``CASE`` expression so that lifecycle
      stages sort in a meaningful order (``live`` > ``in-dev`` > ``discontinued``
      > ``non-responsive``) rather than alphabetical.
    - **Nullable numeric columns**: wrapped with ``NULLS LAST`` so that rows
      with ``NULL`` metric values sort to the bottom regardless of direction.
    """
    if not sort:
        return [asc(tiebreaker)]

    clauses: list[UnaryExpression[Any]] = []

    for part in sort.split(","):
        part = part.strip()
        if not part:
            continue

        tokens = part.split(":")
        field_name = tokens[0].strip()
        direction = tokens[1].strip().lower() if len(tokens) > 1 else "asc"

        if field_name not in allowed_fields:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Invalid sort field: '{field_name}'. Allowed: {sorted(allowed_fields.keys())}",
            )

        if direction not in ("asc", "desc"):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Invalid sort direction: '{direction}'. Must be 'asc' or 'desc'.",
            )

        col = allowed_fields[field_name]
        dir_fn = asc if direction == "asc" else desc

        # activity_status: use CASE-based ordering instead of raw enum text.
        if field_name == "activity_status":
            status_expr = case(
                _ACTIVITY_STATUS_ORDER,
                value=col,
                else_=99,
            )
            clauses.append(dir_fn(status_expr))
            continue

        # Nullable metric columns: NULLS LAST to prevent nulls burying real data.
        col_prop = getattr(col, "property", None)
        if isinstance(col_prop, ColumnProperty) and col_prop.columns:
            col_obj = col_prop.columns[0]
            col_type = getattr(col_obj.type, "python_type", None)
            is_nullable = getattr(col_obj, "nullable", True)
        else:
            col_type = None
            is_nullable = True
            
        if is_nullable and col_type in (int, float):
            clauses.append(nulls_last(dir_fn(col)))
        else:
            clauses.append(dir_fn(col))

    # Always append canonical_id as tiebreaker for deterministic pagination.
    clauses.append(asc(tiebreaker))

    return clauses


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
