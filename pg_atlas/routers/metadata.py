"""
Metadata router — ecosystem-wide summary statistics.

Provides a single ``GET /metadata`` endpoint that returns aggregate counts
across all graph entities. Powers the headline metrics strip on our dashboard.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import datetime as dt

from fastapi import APIRouter
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from pg_atlas.db_models.base import ActivityStatus
from pg_atlas.db_models.contributed_to import ContributedTo
from pg_atlas.db_models.depends_on import DependsOn
from pg_atlas.db_models.project import Project
from pg_atlas.db_models.repo_vertex import ExternalRepo, Repo
from pg_atlas.routers.common import DbSession
from pg_atlas.routers.models import MetadataResponse
from pg_atlas.routers.tags import Graph, Source

router = APIRouter()


@router.get(
    "/metadata",
    response_model=MetadataResponse,
    summary="Ecosystem summary statistics",
    tags=[Graph.metadata, Source.opengrants, Source.deps_dev, Source.github, Source.pg_atlas],
)
async def get_metadata(
    db: DbSession,
) -> MetadataResponse:
    """
    Return aggregate counts across all graph entities.

    Counts are computed on-the-fly via simple ``COUNT(*)`` queries.  No
    caching is applied in this dev version — queries hit the DB directly.
    """
    return await _run_aggregate_queries(db)


async def _run_aggregate_queries(
    db: AsyncSession,
) -> MetadataResponse:
    """
    First get total counts, then use last_updated to calculate rolling time windows.
    We use contributions as a proxy for the graph freshness. It is a good middle
    between bootstrap runs (e.g. Project) and frequent SBOM submissions.

    The cutoff dates for contribution stats depend on data freshness rather than
    the current request time. This is easier to cache, and I don't want headline
    metrics to disappear if I'm working on a dev DB that is infrequently updated.
    """
    total_counts = await db.execute(
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
            select(func.max(ContributedTo.last_commit_date)).scalar_subquery().label("last_updated"),
        )
    )
    totals_row = total_counts.one()

    cutoff_30d = totals_row.last_updated - dt.timedelta(days=30)
    cutoff_90d = totals_row.last_updated - dt.timedelta(days=90)

    rolling_counts = await db.execute(
        select(
            select(func.count(func.distinct(ContributedTo.contributor_id)))
            .where(ContributedTo.last_commit_date >= cutoff_30d)
            .scalar_subquery()
            .label("active_contributors_30d"),
            select(func.count(func.distinct(ContributedTo.contributor_id)))
            .where(ContributedTo.last_commit_date >= cutoff_90d)
            .scalar_subquery()
            .label("active_contributors_90d"),
            select(func.count(func.distinct(ContributedTo.repo_id)))
            .where(ContributedTo.last_commit_date >= cutoff_90d)
            .scalar_subquery()
            .label("active_repos_90d"),
        )
    )
    window_row = rolling_counts.one()

    return MetadataResponse(
        total_projects=totals_row.total_projects,
        active_projects=totals_row.active_projects,
        total_repos=totals_row.total_repos,
        total_external_repos=totals_row.total_external,
        total_dependency_edges=totals_row.total_deps,
        total_contributor_edges=totals_row.total_contribs,
        last_updated=totals_row.last_updated,
        active_contributors_30d=window_row.active_contributors_30d,
        active_contributors_90d=window_row.active_contributors_90d,
        active_repos_90d=window_row.active_repos_90d,
    )
