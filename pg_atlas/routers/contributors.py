"""
Contributors router — individual contributor detail with per-repo activity.

All endpoints are read-only and unauthenticated.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from pg_atlas.db_models.contributor import Contributor
from pg_atlas.db_models.session import maybe_db_session
from pg_atlas.routers.models import ContributionEntry, ContributorDetailResponse
from pg_atlas.routers.tags import Graph, Source

router = APIRouter()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _require_session(session: AsyncSession | None) -> AsyncSession:
    """Raise HTTP 503 if the database session is unavailable."""

    if session is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database is not configured.",
        )

    return session


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get(
    "/contributors/{contributor_id}",
    response_model=ContributorDetailResponse,
    summary="Contributor detail",
    tags=[Graph.contributors, Graph.contributor_graph, Source.github],
)
async def get_contributor(
    contributor_id: int,
    session: Annotated[AsyncSession | None, Depends(maybe_db_session)],
) -> ContributorDetailResponse:
    """
    Full detail for a single contributor with aggregated statistics and
    per-repo commit activity.

    The ``total_commits`` field sums commits across all repos.
    ``first_contribution`` / ``last_contribution`` are the earliest and latest
    commit dates across all repos.
    """
    db = _require_session(session)

    result = await db.execute(select(Contributor).where(Contributor.id == contributor_id))
    contributor = result.scalar_one_or_none()

    if contributor is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Contributor {contributor_id} not found.",
        )

    # Build per-repo entries from the eager-loaded contribution_edges.
    entries: list[ContributionEntry] = []
    total_commits = 0
    first_contribution = None
    last_contribution = None

    for edge in contributor.contribution_edges:
        total_commits += edge.number_of_commits

        if first_contribution is None or edge.first_commit_date < first_contribution:
            first_contribution = edge.first_commit_date

        if last_contribution is None or edge.last_commit_date > last_contribution:
            last_contribution = edge.last_commit_date

        # The repo relationship is eager-loaded; project_id → project is also
        # eager-loaded on Repo.
        repo = edge.repo
        entries.append(
            ContributionEntry(
                repo_canonical_id=repo.canonical_id,
                repo_display_name=repo.display_name,
                project_canonical_id=repo.project.canonical_id if repo.project else None,
                number_of_commits=edge.number_of_commits,
                first_commit_date=edge.first_commit_date,
                last_commit_date=edge.last_commit_date,
            )
        )

    return ContributorDetailResponse(
        id=contributor.id,
        name=contributor.name,
        email_hash=contributor.email_hash,
        total_repos=len(entries),
        total_commits=total_commits,
        first_contribution=first_contribution,
        last_contribution=last_contribution,
        repos=entries,
    )
