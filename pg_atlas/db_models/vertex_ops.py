"""
Reusable async query and upsert operations for the RepoVertex JTI hierarchy.

All queries apply ``selectin_polymorphic`` so that subtype columns (from the
``repos`` and ``external_repos`` child tables) are eagerly loaded in a single
async round-trip.  Without this, accessing any subtype attribute in an
``AsyncSession`` would trigger a synchronous lazy load and raise
``MissingGreenlet``.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from collections.abc import Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectin_polymorphic

from pg_atlas.db_models.repo_vertex import ExternalRepo, Repo, RepoVertex

# Reusable loader option — tells SQLAlchemy to eagerly batch-load JTI child
# tables so that Repo/ExternalRepo attributes are available without lazy loads.
POLY_LOAD = selectin_polymorphic(RepoVertex, [Repo, ExternalRepo])


async def get_vertex(session: AsyncSession, canonical_id: str) -> RepoVertex | None:
    """Look up a single ``RepoVertex`` by ``canonical_id`` with eager subtype loading."""
    result = await session.execute(select(RepoVertex).where(RepoVertex.canonical_id == canonical_id).options(POLY_LOAD))

    return result.scalar_one_or_none()


async def get_all_vertices(session: AsyncSession) -> Sequence[RepoVertex]:
    """Load all ``RepoVertex`` rows with eager subtype loading."""

    return (await session.execute(select(RepoVertex).options(POLY_LOAD))).scalars().all()


async def upsert_external_repo(
    session: AsyncSession,
    *,
    canonical_id: str,
    display_name: str,
    latest_version: str,
    repo_url: str | None,
) -> RepoVertex:
    """
    Insert an ``ExternalRepo`` or update an existing vertex's mutable fields.

    If the ``canonical_id`` already belongs to a ``Repo`` (within-ecosystem),
    the existing row is returned unchanged — ``ExternalRepo`` never overwrites
    a ``Repo``.

    Calls ``session.flush()`` so the returned object has its ``id`` populated.
    """
    vertex = await get_vertex(session, canonical_id)

    if vertex is not None:
        if isinstance(vertex, ExternalRepo):
            vertex.display_name = display_name
            if latest_version:
                vertex.latest_version = latest_version
            if repo_url:
                vertex.repo_url = repo_url

            await session.flush()

            return vertex

        raise ValueError(f"Cannot overwrite Repo {vertex.canonical_id} (id={vertex.id}) with ExternalRepo")

    ext = ExternalRepo(
        canonical_id=canonical_id,
        display_name=display_name,
        latest_version=latest_version,
        repo_url=repo_url,
    )
    session.add(ext)
    await session.flush()

    return ext
