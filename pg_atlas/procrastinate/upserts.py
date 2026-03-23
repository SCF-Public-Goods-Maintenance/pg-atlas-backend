"""
Async SQLAlchemy upsert helpers for the A5 bootstrap pipeline.

These functions create or update ``Project``, ``Repo``, ``ExternalRepo``, and
``DependsOn`` rows from within Procrastinate tasks.  Each function opens its
own ``AsyncSession`` (via the shared session factory) and commits before
returning, so callers don't need to manage transactions.

Promotion logic: when a ``canonical_id`` that already exists as ``ExternalRepo``
needs to become a ``Repo`` (because the bootstrap crawler discovered it belongs
to an SCF project), ``promote_external_to_repo`` deletes the ``ExternalRepo``
child row and inserts a ``Repo`` child row that reuses the same
``repo_vertices.id`` PK.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import datetime
import logging
from typing import Any

from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from pg_atlas.db_models.base import (
    ActivityStatus,
    EdgeConfidence,
    ProjectType,
    Visibility,
)
from pg_atlas.db_models.depends_on import DependsOn
from pg_atlas.db_models.project import Project
from pg_atlas.db_models.repo_vertex import ExternalRepo, Repo, RepoVertex
from pg_atlas.db_models.session import get_session_factory

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Session helper
# ---------------------------------------------------------------------------


async def _session() -> AsyncSession:
    """
    Open a new ``AsyncSession`` from the shared factory.

    Procrastinate tasks run outside FastAPI's dependency injection, so we
    access the session factory directly.
    """
    factory = get_session_factory()

    return factory()


# ---------------------------------------------------------------------------
# Project upsert
# ---------------------------------------------------------------------------


async def upsert_project(
    *,
    canonical_id: str,
    display_name: str,
    project_type: ProjectType,
    activity_status: ActivityStatus,
    git_org_url: str | None = None,
    project_metadata: dict[str, Any] | None = None,
) -> int:
    """
    Insert or update a ``Project`` row and return its ``id``.

    On conflict (same ``canonical_id`` already present), the mutable columns
    are overwritten with the new values.
    """
    session = await _session()

    try:
        result = await session.execute(select(Project).where(Project.canonical_id == canonical_id))
        project = result.scalar_one_or_none()

        if project is None:
            project = Project(
                canonical_id=canonical_id,
                display_name=display_name,
                project_type=project_type,
                activity_status=activity_status,
                git_org_url=git_org_url,
                project_metadata=project_metadata,
            )
            session.add(project)
        else:
            project.display_name = display_name
            project.project_type = project_type
            project.activity_status = activity_status
            if git_org_url is not None:
                project.git_org_url = git_org_url
            if project_metadata is not None:
                project.project_metadata = project_metadata

        await session.flush()
        project_id: int = project.id
        await session.commit()

        logger.info(f"Upserted Project {canonical_id} (id={project_id})")

        return project_id

    except Exception:
        await session.rollback()

        raise

    finally:
        await session.close()


# ---------------------------------------------------------------------------
# Repo upsert
# ---------------------------------------------------------------------------


async def upsert_repo(
    *,
    canonical_id: str,
    display_name: str,
    latest_version: str,
    project_id: int | None = None,
    repo_url: str | None = None,
    latest_commit_date: datetime.datetime | None = None,
    adoption_stars: int | None = None,
    adoption_forks: int | None = None,
    releases: list[dict[str, Any]] | None = None,
    repo_metadata: dict[str, Any] | None = None,
) -> int:
    """
    Insert or update a ``Repo`` vertex and return its ``id``.

    If a ``RepoVertex`` with the same ``canonical_id`` already exists as an
    ``ExternalRepo``, it is promoted to ``Repo`` via
    ``promote_external_to_repo``.

    ``latest_commit_date`` is written only when it is greater than the
    currently stored value (or the stored value is ``None``).  This
    ensures that the most recent date wins regardless of whether it was
    set by the bootstrap crawler (``pushed_at``) or the gitlog parser.
    """
    session = await _session()

    try:
        result = await session.execute(select(RepoVertex).where(RepoVertex.canonical_id == canonical_id))
        vertex = result.scalar_one_or_none()

        if vertex is not None and isinstance(vertex, ExternalRepo):
            repo_id = await _promote_external_to_repo(
                session,
                vertex_id=vertex.id,
                display_name=display_name,
                latest_version=latest_version,
                project_id=project_id,
                repo_url=repo_url,
                latest_commit_date=latest_commit_date,
                adoption_stars=adoption_stars,
                adoption_forks=adoption_forks,
                releases=releases,
                repo_metadata=repo_metadata,
            )
            await session.commit()

            return repo_id

        if vertex is not None and isinstance(vertex, Repo):
            # Already a Repo — update mutable columns.
            vertex.display_name = display_name
            if latest_version:
                vertex.latest_version = latest_version
            if project_id is not None:
                vertex.project_id = project_id
            if repo_url is not None:
                vertex.repo_url = repo_url
            if adoption_stars is not None:
                vertex.adoption_stars = adoption_stars
            if latest_commit_date is not None and (
                vertex.latest_commit_date is None or latest_commit_date > vertex.latest_commit_date
            ):
                vertex.latest_commit_date = latest_commit_date
            if adoption_forks is not None:
                vertex.adoption_forks = adoption_forks
            if releases is not None:
                vertex.releases = releases
            if repo_metadata is not None:
                vertex.repo_metadata = repo_metadata

            await session.flush()
            repo_id = vertex.id
            await session.commit()

            return repo_id

        # New vertex — insert.
        repo = Repo(
            canonical_id=canonical_id,
            display_name=display_name,
            visibility=Visibility.public,
            latest_version=latest_version,
            project_id=project_id,
            repo_url=repo_url,
            latest_commit_date=latest_commit_date,
            adoption_stars=adoption_stars,
            adoption_forks=adoption_forks,
            releases=releases,
            repo_metadata=repo_metadata,
        )
        session.add(repo)
        await session.flush()
        repo_id = repo.id
        await session.commit()

        logger.info(f"Upserted Repo {canonical_id} (id={repo_id})")

        return repo_id

    except Exception:
        await session.rollback()

        raise

    finally:
        await session.close()


# ---------------------------------------------------------------------------
# ExternalRepo upsert
# ---------------------------------------------------------------------------


async def upsert_external_repo(
    *,
    canonical_id: str,
    display_name: str,
    latest_version: str,
    repo_url: str | None = None,
) -> int:
    """
    Insert an ``ExternalRepo`` vertex or update it if it already exists.

    If a vertex with the same ``canonical_id`` already exists as a ``Repo``
    (i.e. it was promoted earlier), the existing ``Repo`` id is returned
    without modification.
    """
    session = await _session()

    try:
        result = await session.execute(select(RepoVertex).where(RepoVertex.canonical_id == canonical_id))
        vertex = result.scalar_one_or_none()

        if vertex is not None:
            if isinstance(vertex, ExternalRepo):
                vertex.display_name = display_name
                if latest_version:
                    vertex.latest_version = latest_version
                if repo_url:
                    vertex.repo_url = repo_url

                await session.flush()

            # Repo or ExternalRepo — return its id either way.
            vertex_id: int = vertex.id
            await session.commit()

            return vertex_id

        ext = ExternalRepo(
            canonical_id=canonical_id,
            display_name=display_name,
            latest_version=latest_version,
            repo_url=repo_url,
        )
        session.add(ext)
        await session.flush()
        ext_id: int = ext.id
        await session.commit()

        return ext_id

    except Exception:
        await session.rollback()

        raise

    finally:
        await session.close()


# ---------------------------------------------------------------------------
# Promotion: ExternalRepo → Repo
# ---------------------------------------------------------------------------


async def _promote_external_to_repo(
    session: AsyncSession,
    *,
    vertex_id: int,
    display_name: str,
    latest_version: str,
    project_id: int | None,
    repo_url: str | None,
    latest_commit_date: datetime.datetime | None,
    adoption_stars: int | None,
    adoption_forks: int | None,
    releases: list[dict[str, Any]] | None,
    repo_metadata: dict[str, Any] | None,
) -> int:
    """
    Promote an ``ExternalRepo`` child row to a ``Repo`` child row.

    Keeps the same ``repo_vertices.id`` PK (and thus all existing
    ``DependsOn`` edges) intact.  Operates within the caller's session /
    transaction.

    Steps:
        1. Delete the ``external_repos`` child row.
        2. Update the discriminator on ``repo_vertices`` to ``repo``.
        3. Insert a ``repos`` child row with the same PK.
    """
    from pg_atlas.db_models.base import RepoVertexType

    # 1. Delete ExternalRepo child row.
    await session.execute(
        delete(ExternalRepo.__table__).where(ExternalRepo.__table__.c.id == vertex_id)  # type: ignore[arg-type]
    )

    # 2. Update discriminator on the base table.
    await session.execute(
        update(RepoVertex.__table__)  # type: ignore[arg-type]
        .where(RepoVertex.__table__.c.id == vertex_id)
        .values(
            vertex_type=RepoVertexType.repo.value,
        )
    )

    # 3. Insert Repo child row using Core (bypasses dataclass __init__ ordering).
    await session.execute(
        Repo.__table__.insert().values(  # type: ignore[attr-defined]
            id=vertex_id,
            display_name=display_name,
            visibility=Visibility.public.value,
            latest_version=latest_version,
            project_id=project_id,
            repo_url=repo_url,
            latest_commit_date=latest_commit_date,
            adoption_stars=adoption_stars,
            adoption_forks=adoption_forks,
            releases=releases,
            metadata=repo_metadata,
        )
    )
    await session.flush()

    logger.info(f"Promoted ExternalRepo -> Repo (vertex_id={vertex_id})")

    return vertex_id


# ---------------------------------------------------------------------------
# Check project association
# ---------------------------------------------------------------------------


async def is_project_repo(canonical_id: str) -> bool:
    """
    Return ``True`` if a vertex with *canonical_id* exists and is a ``Repo``
    linked to a ``Project``.
    """
    session = await _session()

    try:
        result = await session.execute(select(Repo.project_id).where(Repo.canonical_id == canonical_id))
        row = result.one_or_none()

        return row is not None and row[0] is not None

    finally:
        await session.close()


# ---------------------------------------------------------------------------
# DependsOn edge upsert
# ---------------------------------------------------------------------------


async def upsert_depends_on(
    *,
    in_vertex_id: int,
    out_vertex_id: int,
    version_range: str | None = None,
    confidence: EdgeConfidence = EdgeConfidence.inferred_shadow,
) -> None:
    """
    Insert a ``DependsOn`` edge if it does not already exist.

    Existing edges are left unchanged (no update on conflict) because the
    ``(in_vertex_id, out_vertex_id)`` composite PK already enforces
    uniqueness.
    """
    session = await _session()

    try:
        result = await session.execute(
            select(DependsOn).where(
                DependsOn.in_vertex_id == in_vertex_id,
                DependsOn.out_vertex_id == out_vertex_id,
            )
        )
        existing = result.scalar_one_or_none()

        if existing is not None:
            await session.close()

            return

        edge = DependsOn(
            in_vertex_id=in_vertex_id,
            out_vertex_id=out_vertex_id,
            version_range=version_range,
            confidence=confidence,
        )
        session.add(edge)
        await session.commit()

    except Exception:
        await session.rollback()

        raise

    finally:
        await session.close()


# ---------------------------------------------------------------------------
# Associate repos with a project
# ---------------------------------------------------------------------------


async def associate_repo_with_project(repo_canonical_id: str, project_id: int) -> None:
    """Set ``project_id`` on a ``Repo`` row identified by its canonical ID."""
    session = await _session()

    try:
        result = await session.execute(select(Repo).where(Repo.canonical_id == repo_canonical_id))
        repo = result.scalar_one_or_none()

        if repo is not None:
            repo.project_id = project_id
            await session.commit()

    except Exception:
        await session.rollback()

        raise

    finally:
        await session.close()
