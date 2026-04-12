"""
Gitlog audit router.

Provides read-only endpoints for gitlog processing attempts and stored raw gitlog
artifacts.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, select

from pg_atlas.db_models.gitlog_artifact import GitLogArtifact
from pg_atlas.db_models.repo_vertex import Repo
from pg_atlas.routers.common import DbSession, PaginationParams
from pg_atlas.routers.models import GitLogArtifactDetailResponse, GitLogArtifactSummary, PaginatedResponse
from pg_atlas.routers.tags import Graph, Source
from pg_atlas.storage.artifacts import read_artifact

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get(
    "/gitlog",
    response_model=PaginatedResponse[GitLogArtifactSummary],
    summary="List gitlog processing attempts",
    tags=[Graph.contributor_graph, Source.github, Source.pg_atlas],
)
async def list_gitlog_artifacts(
    db: DbSession,
    pagination: Annotated[PaginationParams, Depends()],
    repo: Annotated[str | None, Query(description="Filter by repo canonical_id (exact match)")] = None,
) -> PaginatedResponse[GitLogArtifactSummary]:
    """Paginated list of gitlog processing attempts with optional repo filter."""

    base = select(GitLogArtifact, Repo).join(Repo, Repo.id == GitLogArtifact.repo_id)
    if repo is not None:
        base = base.where(Repo.canonical_id == repo)

    total = (await db.execute(select(func.count()).select_from(base.subquery()))).scalar_one()
    rows = (
        await db.execute(base.order_by(GitLogArtifact.submitted_at.desc()).limit(pagination.limit).offset(pagination.offset))
    ).all()

    items: list[GitLogArtifactSummary] = []
    for artifact, repo_row in rows:
        items.append(
            GitLogArtifactSummary(
                id=artifact.id,
                repo_id=artifact.repo_id,
                repo_canonical_id=repo_row.canonical_id,
                repo_display_name=repo_row.display_name,
                artifact_path=artifact.artifact_path,
                status=artifact.status,
                error_detail=artifact.error_detail,
                since_months=artifact.since_months,
                submitted_at=artifact.submitted_at,
                processed_at=artifact.processed_at,
            )
        )

    return PaginatedResponse[GitLogArtifactSummary](
        items=items,
        total=total,
        limit=pagination.limit,
        offset=pagination.offset,
    )


@router.get(
    "/gitlog/{artifact_id}",
    response_model=GitLogArtifactDetailResponse,
    summary="Get gitlog processing attempt detail",
    tags=[Graph.contributor_graph, Source.github, Source.pg_atlas],
)
async def get_gitlog_artifact(
    artifact_id: int,
    db: DbSession,
) -> GitLogArtifactDetailResponse:
    """Return one gitlog attempt record with its raw artifact content when available."""
    # consider making this a streaming response if the artifacts become large and latency suffers

    row = (
        await db.execute(
            select(GitLogArtifact, Repo).join(Repo, Repo.id == GitLogArtifact.repo_id).where(GitLogArtifact.id == artifact_id)
        )
    ).one_or_none()

    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Gitlog artifact {artifact_id} not found.",
        )

    artifact, repo_row = row
    raw_artifact: str | None = None
    if artifact.artifact_path is not None:
        try:
            raw_artifact = (await read_artifact(artifact.artifact_path)).decode("utf-8")
        except FileNotFoundError:
            logger.warning(f"Gitlog artifact file not found: {artifact.artifact_path}")
        except OSError, UnicodeDecodeError:
            logger.exception(f"Failed to read gitlog artifact: {artifact.artifact_path}")

    return GitLogArtifactDetailResponse(
        id=artifact.id,
        repo_id=artifact.repo_id,
        repo_canonical_id=repo_row.canonical_id,
        repo_display_name=repo_row.display_name,
        artifact_path=artifact.artifact_path,
        status=artifact.status,
        error_detail=artifact.error_detail,
        since_months=artifact.since_months,
        submitted_at=artifact.submitted_at,
        processed_at=artifact.processed_at,
        raw_artifact=raw_artifact,
    )
