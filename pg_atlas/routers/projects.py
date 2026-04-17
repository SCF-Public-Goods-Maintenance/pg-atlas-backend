"""
Projects router — list, detail, repos, and dependency endpoints for projects.

All endpoints are read-only and unauthenticated.  Projects are the top-level
grouping entity in PG Atlas — each project may own multiple repos.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import datetime as dt
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import InstrumentedAttribute

from pg_atlas.db_models.base import ActivityStatus, ProjectType
from pg_atlas.db_models.contributed_to import ContributedTo
from pg_atlas.db_models.contributor import Contributor
from pg_atlas.db_models.depends_on import DependsOn
from pg_atlas.db_models.project import Project
from pg_atlas.db_models.repo_vertex import Repo
from pg_atlas.routers.common import DbSession, PaginationParams, parse_sort_params
from pg_atlas.routers.models import (
    PaginatedResponse,
    ProjectContributorSummary,
    ProjectDependency,
    ProjectDetailResponse,
    ProjectMetadata,
    ProjectSummary,
    RepoSummary,
)
from pg_atlas.routers.tags import Graph, Source

router = APIRouter()

# Whitelist of sortable fields for GET /projects.
_PROJECT_SORT_FIELDS: dict[str, InstrumentedAttribute[Any]] = {
    "display_name": Project.display_name,
    "activity_status": Project.activity_status,
    "criticality_score": Project.criticality_score,
    "pony_factor": Project.pony_factor,
    "adoption_score": Project.adoption_score,
    "updated_at": Project.updated_at,
    "category": Project.category,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _get_project_or_404(db: AsyncSession, canonical_id: str) -> Project:
    """Fetch a project by canonical_id or raise 404."""

    result = await db.execute(select(Project).where(Project.canonical_id == canonical_id))
    project = result.scalar_one_or_none()

    if project is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Project '{canonical_id}' not found.",
        )

    return project


async def _active_contributors_for_project(db: AsyncSession, project_id: int) -> tuple[int, int]:
    """
    Count active contributors for a project in rolling 30d/90d windows.

    Window cutoffs are anchored to the global max ``ContributedTo.last_commit_date``
    (same freshness proxy as metadata), then filtered to repos owned by ``project_id``.
    """
    max_date_result = await db.execute(select(func.max(ContributedTo.last_commit_date)))
    last_updated = max_date_result.scalar_one_or_none()

    if last_updated is None:
        return 0, 0

    cutoff_30d = last_updated - dt.timedelta(days=30)
    cutoff_90d = last_updated - dt.timedelta(days=90)

    counts_result = await db.execute(
        select(
            select(func.count(func.distinct(ContributedTo.contributor_id)))
            .select_from(ContributedTo)
            .join(Repo, Repo.id == ContributedTo.repo_id)
            .where(
                Repo.project_id == project_id,
                ContributedTo.last_commit_date >= cutoff_30d,
            )
            .scalar_subquery()
            .label("active_contributors_30d"),
            select(func.count(func.distinct(ContributedTo.contributor_id)))
            .select_from(ContributedTo)
            .join(Repo, Repo.id == ContributedTo.repo_id)
            .where(
                Repo.project_id == project_id,
                ContributedTo.last_commit_date >= cutoff_90d,
            )
            .scalar_subquery()
            .label("active_contributors_90d"),
        )
    )
    counts_row = counts_result.one()

    return counts_row.active_contributors_30d, counts_row.active_contributors_90d


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get(
    "/projects",
    response_model=PaginatedResponse[ProjectSummary],
    summary="List projects",
    tags=[Graph.projects, Source.opengrants, Source.pg_atlas],
)
async def list_projects(
    db: DbSession,
    pagination: Annotated[PaginationParams, Depends()],
    project_type: ProjectType | None = None,
    activity_status: ActivityStatus | None = None,
    search: Annotated[str | None, Query(max_length=256)] = None,
    sort: Annotated[
        str | None,
        Query(
            max_length=512,
            description="Comma-separated field:direction pairs, e.g. 'criticality_score:desc,display_name:asc'.",
        ),
    ] = None,
    category: Annotated[
        str | None,
        Query(max_length=128, description="Filter by project category (exact match)."),
    ] = None,
) -> PaginatedResponse[ProjectSummary]:
    """
    Paginated list of SCF-funded projects with optional filters and sorting.

    - **project_type**: filter by `public-good` or `scf-project`.
    - **activity_status**: filter by lifecycle status (`live`, `in-dev`, etc.).
    - **search**: case-insensitive substring match on `display_name`.
    - **sort**: comma-separated `field:direction` pairs for server-side ordering.
    - **category**: filter by exact project category string.
    - Default order is `canonical_id` for deterministic pagination.
    """
    base = select(Project)

    if project_type is not None:
        base = base.where(Project.project_type == project_type)

    if activity_status is not None:
        base = base.where(Project.activity_status == activity_status)

    if search is not None:
        base = base.where(Project.display_name.ilike(f"%{search}%"))

    if category is not None:
        base = base.where(Project.category == category)

    count_result = await db.execute(select(func.count()).select_from(base.subquery()))
    total = count_result.scalar_one()

    order_clauses = parse_sort_params(sort, _PROJECT_SORT_FIELDS, Project.canonical_id)

    rows_result = await db.execute(base.order_by(*order_clauses).limit(pagination.limit).offset(pagination.offset))
    projects = rows_result.scalars().all()

    return PaginatedResponse[ProjectSummary](
        items=[ProjectSummary.model_validate(p) for p in projects],
        total=total,
        limit=pagination.limit,
        offset=pagination.offset,
    )


@router.get(
    "/projects/{canonical_id}",
    response_model=ProjectDetailResponse,
    summary="Project detail",
    tags=[Graph.projects, Source.opengrants, Source.pg_atlas],
)
async def get_project(
    canonical_id: str,
    db: DbSession,
) -> ProjectDetailResponse:
    """
    Full detail for a single project, including validated metadata.

    The ``metadata`` field is the normalised form of the raw JSONB column —
    unknown keys from the crawler are passed through via ``extra="allow"``.
    """
    project = await _get_project_or_404(db, canonical_id)
    active_30d, active_90d = await _active_contributors_for_project(db, project.id)

    return ProjectDetailResponse(
        canonical_id=project.canonical_id,
        display_name=project.display_name,
        project_type=project.project_type,
        activity_status=project.activity_status,
        category=project.category,
        git_owner_url=project.git_owner_url,
        pony_factor=project.pony_factor,
        criticality_score=project.criticality_score,
        adoption_score=project.adoption_score,
        updated_at=project.updated_at,
        project_id=project.id,
        active_contributors_30d=active_30d,
        active_contributors_90d=active_90d,
        metadata=ProjectMetadata.model_validate(project.project_metadata or {}),
    )


@router.get(
    "/projects/{canonical_id}/repos",
    response_model=PaginatedResponse[RepoSummary],
    summary="Repos belonging to a project",
    tags=[Graph.projects, Source.github, Source.deps_dev],
)
async def get_project_repos(
    canonical_id: str,
    db: DbSession,
    pagination: Annotated[PaginationParams, Depends()],
) -> PaginatedResponse[RepoSummary]:
    """
    Paginated list of repos that belong to the given project.

    Returns 404 if the project does not exist.
    """
    project = await _get_project_or_404(db, canonical_id)

    base = select(Repo).where(Repo.project_id == project.id)

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


@router.get(
    "/projects/{canonical_id}/contributors",
    response_model=PaginatedResponse[ProjectContributorSummary],
    summary="Contributors across a project's repos",
    tags=[Graph.projects, Graph.contributors, Graph.contributor_graph, Source.github],
)
async def get_project_contributors(
    canonical_id: str,
    db: DbSession,
    pagination: Annotated[PaginationParams, Depends()],
    search: Annotated[str | None, Query(max_length=256)] = None,
) -> PaginatedResponse[ProjectContributorSummary]:
    """Paginated contributors aggregated across all repos belonging to the project."""

    project = await _get_project_or_404(db, canonical_id)

    base = (
        select(
            Contributor.id.label("id"),
            Contributor.name.label("name"),
            Contributor.email_hash.label("email_hash"),
            func.sum(ContributedTo.number_of_commits).label("total_commits_in_project"),
        )
        .join(ContributedTo, ContributedTo.contributor_id == Contributor.id)
        .join(Repo, Repo.id == ContributedTo.repo_id)
        .where(Repo.project_id == project.id)
        .group_by(Contributor.id, Contributor.name, Contributor.email_hash)
    )

    if search is not None:
        base = base.where(Contributor.name.ilike(f"%{search}%"))

    total = (await db.execute(select(func.count()).select_from(base.subquery()))).scalar_one()
    rows = (
        await db.execute(
            base.order_by(func.sum(ContributedTo.number_of_commits).desc(), Contributor.id.asc())
            .limit(pagination.limit)
            .offset(pagination.offset)
        )
    ).all()

    return PaginatedResponse[ProjectContributorSummary](
        items=[
            ProjectContributorSummary(
                id=row.id,
                name=row.name,
                email_hash=row.email_hash,
                total_commits_in_project=row.total_commits_in_project,
            )
            for row in rows
        ],
        total=total,
        limit=pagination.limit,
        offset=pagination.offset,
    )


@router.get(
    "/projects/{canonical_id}/depends-on",
    response_model=list[ProjectDependency],
    summary="Project-level dependencies",
    tags=[Graph.dependency_graph, Source.deps_dev],
)
async def get_project_depends_on(
    canonical_id: str,
    db: DbSession,
) -> list[ProjectDependency]:
    """
    Collapsed project-level dependencies.

    Aggregates repo-level ``depends_on`` edges: for each distinct target project,
    returns the target project summary and the number of repo-level edges between
    the two projects.  Self-references and edges to external repos (which have no
    project) are excluded.
    """
    project = await _get_project_or_404(db, canonical_id)

    return await _project_level_deps(db, project, direction="outgoing")


@router.get(
    "/projects/{canonical_id}/has-dependents",
    response_model=list[ProjectDependency],
    summary="Projects that depend on this project",
    tags=[Graph.dependency_graph, Source.deps_dev],
)
async def get_project_has_dependents(
    canonical_id: str,
    db: DbSession,
) -> list[ProjectDependency]:
    """
    Collapsed project-level reverse dependencies.

    Same aggregation as ``depends-on`` but in the reverse direction: which
    other projects have repos that depend on repos of *this* project.
    """
    project = await _get_project_or_404(db, canonical_id)

    return await _project_level_deps(db, project, direction="incoming")


# ---------------------------------------------------------------------------
# Internal query helpers
# ---------------------------------------------------------------------------


async def _project_level_deps(
    db: AsyncSession,
    project: Project,
    *,
    direction: str,
) -> list[ProjectDependency]:
    """
    Aggregate repo-level edges into project-level dependency summaries.

    ``direction="outgoing"`` → repos of *project* depend on repos of *other* projects.
    ``direction="incoming"`` → repos of *other* projects depend on repos of *project*.
    """
    # Aliases for the two sides of the join.
    source_repo = Repo
    target_repo_alias = select(Repo.id, Repo.project_id).subquery("target_repo")

    if direction == "outgoing":
        # source_repo (this project) → DependsOn → target_repo (other project)
        stmt = (
            select(target_repo_alias.c.project_id, func.count().label("edge_count"))
            .select_from(DependsOn)
            .join(source_repo, source_repo.id == DependsOn.in_vertex_id)
            .join(target_repo_alias, target_repo_alias.c.id == DependsOn.out_vertex_id)
            .where(
                source_repo.project_id == project.id,
                target_repo_alias.c.project_id.isnot(None),
                target_repo_alias.c.project_id != project.id,
            )
            .group_by(target_repo_alias.c.project_id)
        )
    else:
        # target_repo (other project) → DependsOn → source_repo (this project)
        stmt = (
            select(target_repo_alias.c.project_id, func.count().label("edge_count"))
            .select_from(DependsOn)
            .join(source_repo, source_repo.id == DependsOn.out_vertex_id)
            .join(target_repo_alias, target_repo_alias.c.id == DependsOn.in_vertex_id)
            .where(
                source_repo.project_id == project.id,
                target_repo_alias.c.project_id.isnot(None),
                target_repo_alias.c.project_id != project.id,
            )
            .group_by(target_repo_alias.c.project_id)
        )

    result = await db.execute(stmt)
    rows = result.all()

    if not rows:
        return []

    # Fetch project summaries for all target project IDs in one query.
    target_ids = [row.project_id for row in rows]
    edge_counts = {row.project_id: row.edge_count for row in rows}

    projects_result = await db.execute(select(Project).where(Project.id.in_(target_ids)).order_by(Project.canonical_id))
    target_projects = projects_result.scalars().all()

    return [
        ProjectDependency(
            project=ProjectSummary.model_validate(p),
            edge_count=edge_counts[p.id],
        )
        for p in target_projects
    ]
