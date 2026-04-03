"""
Metadata router — ecosystem-wide summary statistics.

Provides a single ``GET /metadata`` endpoint that returns aggregate counts
across all graph entities.  This is the lightest-weight endpoint and a good
health/readiness signal for the frontend.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from pg_atlas.db_models.base import ActivityStatus
from pg_atlas.db_models.contributed_to import ContributedTo
from pg_atlas.db_models.depends_on import DependsOn
from pg_atlas.db_models.project import Project
from pg_atlas.db_models.repo_vertex import ExternalRepo, Repo
from pg_atlas.db_models.session import maybe_db_session
from pg_atlas.routers.models import MetadataResponse
from pg_atlas.routers.tags import Graph, Source

router = APIRouter()


def _require_session(session: AsyncSession | None) -> AsyncSession:
    """Raise HTTP 503 if the database session is unavailable."""

    if session is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database is not configured.",
        )

    return session


@router.get(
    "/metadata",
    response_model=MetadataResponse,
    summary="Ecosystem summary statistics",
    tags=[Graph.metadata, Source.opengrants, Source.deps_dev, Source.github, Source.pg_atlas],
)
async def get_metadata(
    session: Annotated[AsyncSession | None, Depends(maybe_db_session)],
) -> MetadataResponse:
    """
    Return aggregate counts across all graph entities.

    Counts are computed on-the-fly via simple ``COUNT(*)`` queries.  No
    caching is applied in this dev version — queries hit the DB directly.
    """
    db = _require_session(session)

    (
        total_projects,
        active_projects,
        total_repos,
        total_external,
        total_deps,
        total_contribs,
        last_updated,
    ) = await _run_aggregate_queries(db)

    return MetadataResponse(
        total_projects=total_projects,
        active_projects=active_projects,
        total_repos=total_repos,
        total_external_repos=total_external,
        total_dependency_edges=total_deps,
        total_contributor_edges=total_contribs,
        last_updated=last_updated,
    )


async def _run_aggregate_queries(
    db: AsyncSession,
) -> tuple[int, int, int, int, int, int, datetime.datetime | None]:
    """
    Execute all aggregate counts in parallel and return the raw values.

    Returned as a tuple for easy unpacking in the caller.
    """
    results = await db.execute(
        select(
            select(func.count()).select_from(Project).scalar_subquery().label("total_projects"),
            select(func.count())
            .select_from(Project)
            .where(Project.activity_status.in_([ActivityStatus.live, ActivityStatus.in_dev]))
            .scalar_subquery()
            .label("active_projects"),
            select(func.count()).select_from(Repo).scalar_subquery().label("total_repos"),
            select(func.count()).select_from(ExternalRepo).scalar_subquery().label("total_external"),
            select(func.count()).select_from(DependsOn).scalar_subquery().label("total_deps"),
            select(func.count()).select_from(ContributedTo).scalar_subquery().label("total_contribs"),
            select(func.max(Project.updated_at)).scalar_subquery().label("last_updated"),
        )
    )
    row = results.one()

    return (
        row.total_projects,
        row.active_projects,
        row.total_repos,
        row.total_external,
        row.total_deps,
        row.total_contribs,
        row.last_updated,
    )
