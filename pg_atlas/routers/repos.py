"""
Repos router — list, detail, and dependency endpoints for repositories.

All endpoints are read-only and unauthenticated.  Repos use ``{canonical_id:path}``
path parameters because PURLs contain slashes (e.g. ``pkg:github/stellar/sdk``).

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from typing import Annotated
from urllib.parse import quote

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from pg_atlas.db_models.base import RepoVertexType
from pg_atlas.db_models.contributed_to import ContributedTo
from pg_atlas.db_models.contributor import Contributor
from pg_atlas.db_models.depends_on import DependsOn
from pg_atlas.db_models.repo_vertex import ExternalRepo, Repo, RepoVertex
from pg_atlas.db_models.vertex_ops import POLY_LOAD
from pg_atlas.routers.common import DbSession, PaginationParams
from pg_atlas.routers.models import (
    ContributorSummary,
    DepCounts,
    PaginatedResponse,
    ProjectSummary,
    RepoContributorSummary,
    RepoDependency,
    RepoDetailResponse,
    RepoSummary,
)
from pg_atlas.routers.tags import Graph, Source

router = APIRouter()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _re_encode_purl(canonical_id: str) -> str:
    """

    Re-apply percent-encoding that Starlette strips from ``{path}`` params.

    PURL canonical IDs stored in the database retain percent-encoding from
    upstream sources (e.g. ``pkg:npm/%40tailwindcss/postcss``).  Starlette
    auto-decodes path parameters (``%40`` → ``@``), which breaks DB lookups.
    Re-encoding with ``safe="/:"`` restores the original form while keeping
    PURL structural separators intact.
    """

    return quote(canonical_id, safe="/:")


async def _get_repo_or_404(db: AsyncSession, canonical_id: str) -> Repo:
    """
    Fetch a Repo by canonical_id or raise 404.

    Uses the JTI polymorphic loader and verifies the result is a ``Repo``
    (not ``ExternalRepo``).  Tries the raw path parameter first, then the
    re-encoded form to handle Starlette's automatic percent-decoding.
    """
    # Try decoded value first (most canonical_ids have no percent-encoding).
    result = await db.execute(select(RepoVertex).where(RepoVertex.canonical_id == canonical_id).options(POLY_LOAD))
    vertex = result.scalar_one_or_none()

    # Fall back to re-encoded form for PURLs with percent-encoded chars (e.g. npm scoped packages).
    if vertex is None:
        encoded = _re_encode_purl(canonical_id)

        if encoded != canonical_id:
            result = await db.execute(select(RepoVertex).where(RepoVertex.canonical_id == encoded).options(POLY_LOAD))
            vertex = result.scalar_one_or_none()

    if vertex is None or not isinstance(vertex, Repo):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Repo '{canonical_id}' not found.",
        )

    return vertex


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get(
    "/repos",
    response_model=PaginatedResponse[RepoSummary],
    summary="List repos",
    tags=[Graph.repos, Source.github, Source.deps_dev],
)
async def list_repos(
    db: DbSession,
    pagination: Annotated[PaginationParams, Depends()],
    project_id: int | None = None,
    search: Annotated[str | None, Query(max_length=256)] = None,
) -> PaginatedResponse[RepoSummary]:
    """
    Paginated list of in-ecosystem repos with optional filters.

    - **project_id**: filter to repos belonging to a specific project.
    - **search**: case-insensitive substring match on `display_name`.
    - Results are ordered by `canonical_id` for deterministic pagination.
    """
    base = select(Repo)

    if project_id is not None:
        base = base.where(Repo.project_id == project_id)

    if search is not None:
        base = base.where(Repo.display_name.ilike(f"%{search}%"))

    count_result = await db.execute(select(func.count()).select_from(base.subquery()))
    total = count_result.scalar_one()

    rows_result = await db.execute(base.order_by(Repo.canonical_id).limit(pagination.limit).offset(pagination.offset))
    repos = rows_result.scalars().all()

    return PaginatedResponse[RepoSummary](
        items=[RepoSummary.model_validate(r) for r in repos],
        total=total,
        limit=pagination.limit,
        offset=pagination.offset,
    )


# NOTE: depends-on and has-dependents routes are registered BEFORE the
# catch-all detail route because {canonical_id:path} is greedy and would
# otherwise swallow the /depends-on or /has-dependents suffix.


@router.get(
    "/repos/{canonical_id:path}/depends-on",
    response_model=list[RepoDependency],
    summary="Direct dependencies of a repo",
    tags=[Graph.dependency_graph, Source.deps_dev, Source.pg_atlas],
)
async def get_repo_depends_on(
    canonical_id: str,
    db: DbSession,
) -> list[RepoDependency]:
    """
    List of direct dependencies (outgoing edges) for a given repo.

    Each entry includes the target vertex's canonical ID, display name,
    type (``repo`` or ``external-repo``), version range, and confidence level.
    """
    repo = await _get_repo_or_404(db, canonical_id)

    return await _dep_edges(db, repo.id, direction="outgoing")


@router.get(
    "/repos/{canonical_id:path}/has-dependents",
    response_model=list[RepoDependency],
    summary="Repos that depend on this repo",
    tags=[Graph.dependency_graph, Source.deps_dev, Source.pg_atlas],
)
async def get_repo_has_dependents(
    canonical_id: str,
    db: DbSession,
) -> list[RepoDependency]:
    """
    List of direct dependents (incoming edges) for a given repo.

    Each entry includes the source vertex's canonical ID, display name,
    type, version range, and confidence level.
    """
    repo = await _get_repo_or_404(db, canonical_id)

    return await _dep_edges(db, repo.id, direction="incoming")


@router.get(
    "/repos/{canonical_id:path}/contributors",
    response_model=PaginatedResponse[RepoContributorSummary],
    summary="Contributors for a repo",
    tags=[Graph.repos, Graph.contributors, Graph.contributor_graph, Source.github],
)
async def get_repo_contributors(
    canonical_id: str,
    db: DbSession,
    pagination: Annotated[PaginationParams, Depends()],
    search: Annotated[str | None, Query(max_length=256)] = None,
) -> PaginatedResponse[RepoContributorSummary]:
    """Paginated contributors for one repo with commit-count and commit-date spans."""

    repo = await _get_repo_or_404(db, canonical_id)

    base = (
        select(ContributedTo)
        .join(Contributor, Contributor.id == ContributedTo.contributor_id)
        .where(ContributedTo.repo_id == repo.id)
        .options(selectinload(ContributedTo.contributor))
    )
    if search is not None:
        base = base.where(Contributor.name.ilike(f"%{search}%"))

    total = (await db.execute(select(func.count()).select_from(base.subquery()))).scalar_one()
    edges = (
        (
            await db.execute(
                base.order_by(ContributedTo.number_of_commits.desc(), ContributedTo.contributor_id.asc())
                .limit(pagination.limit)
                .offset(pagination.offset)
            )
        )
        .scalars()
        .all()
    )

    return PaginatedResponse[RepoContributorSummary](
        items=[
            RepoContributorSummary(
                id=edge.contributor.id,
                name=edge.contributor.name,
                email_hash=str(edge.contributor.email_hash),
                number_of_commits=edge.number_of_commits,
                first_commit_date=edge.first_commit_date,
                last_commit_date=edge.last_commit_date,
            )
            for edge in edges
        ],
        total=total,
        limit=pagination.limit,
        offset=pagination.offset,
    )


@router.get(
    "/repos/{canonical_id:path}",
    response_model=RepoDetailResponse,
    summary="Repo detail",
    tags=[Graph.repos, Source.github, Source.deps_dev, Source.pg_atlas],
)
async def get_repo(
    canonical_id: str,
    db: DbSession,
) -> RepoDetailResponse:
    """
    Full detail for a single repo including parent project, contributors,
    releases, and dependency counts.
    """
    repo = await _get_repo_or_404(db, canonical_id)

    # Dependency counts by direction and target vertex type.
    outgoing = await _dep_counts(db, repo.id, direction="outgoing")
    incoming = await _dep_counts(db, repo.id, direction="incoming")

    # Contributors via eager-loaded relationship.
    contributors = [ContributorSummary.model_validate(edge.contributor) for edge in repo.contributor_edges]

    # Parent project (eager-loaded).
    parent = ProjectSummary.model_validate(repo.project) if repo.project else None

    return RepoDetailResponse(
        canonical_id=repo.canonical_id,
        display_name=repo.display_name,
        visibility=repo.visibility,
        latest_version=repo.latest_version,
        latest_commit_date=repo.latest_commit_date,
        repo_url=repo.repo_url,
        project_id=repo.project_id,
        pony_factor=repo.pony_factor,
        criticality_score=repo.criticality_score,
        adoption_downloads=repo.adoption_downloads,
        adoption_stars=repo.adoption_stars,
        adoption_forks=repo.adoption_forks,
        updated_at=repo.updated_at,
        releases=repo.releases,
        parent_project=parent,
        contributors=contributors,
        outgoing_dep_counts=outgoing,
        incoming_dep_counts=incoming,
    )


# ---------------------------------------------------------------------------
# Internal query helpers
# ---------------------------------------------------------------------------


async def _dep_counts(db: AsyncSession, repo_id: int, *, direction: str) -> DepCounts:
    """
    Count dependency edges grouped by target vertex type.

    ``direction="outgoing"`` → this repo depends on …
    ``direction="incoming"`` → … depends on this repo
    """
    if direction == "outgoing":
        fk_col = DependsOn.in_vertex_id
        target_col = DependsOn.out_vertex_id
    else:
        fk_col = DependsOn.out_vertex_id
        target_col = DependsOn.in_vertex_id

    stmt = (
        select(RepoVertex.vertex_type, func.count().label("cnt"))
        .select_from(DependsOn)
        .join(RepoVertex, RepoVertex.id == target_col)
        .where(fk_col == repo_id)
        .group_by(RepoVertex.vertex_type)
    )
    result = await db.execute(stmt)
    counts = {row.vertex_type: row.cnt for row in result.all()}

    return DepCounts(
        repos=counts.get(RepoVertexType.repo, 0),
        external_repos=counts.get(RepoVertexType.external_repo, 0),
    )


async def _dep_edges(db: AsyncSession, repo_id: int, *, direction: str) -> list[RepoDependency]:
    """
    Fetch dependency edges with target vertex info for the edge list endpoints.
    """
    if direction == "outgoing":
        fk_col = DependsOn.in_vertex_id
    else:
        fk_col = DependsOn.out_vertex_id

    # Load edges with their related nodes, applying JTI polymorphic loading
    # so that Repo/ExternalRepo subtype columns (display_name) are available.
    stmt = (
        select(DependsOn)
        .where(fk_col == repo_id)
        .options(
            selectinload(DependsOn.out_node).selectin_polymorphic([Repo, ExternalRepo]),
            selectinload(DependsOn.in_node).selectin_polymorphic([Repo, ExternalRepo]),
        )
    )
    result = await db.execute(stmt)
    edges = result.scalars().all()

    deps: list[RepoDependency] = []
    for edge in edges:
        target: RepoVertex = edge.out_node if direction == "outgoing" else edge.in_node

        # Determine display_name depending on concrete type.
        if isinstance(target, Repo | ExternalRepo):
            display_name = target.display_name
        else:
            display_name = target.canonical_id

        deps.append(
            RepoDependency(
                canonical_id=target.canonical_id,
                display_name=display_name,
                vertex_type=RepoVertexType(target.vertex_type),
                version_range=edge.version_range,
                confidence=edge.confidence,
            )
        )

    return deps
