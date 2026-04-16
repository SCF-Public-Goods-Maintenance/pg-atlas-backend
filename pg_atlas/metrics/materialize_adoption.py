"""
Project adoption-score materialization from repo signal percentiles.

This module computes transient repo-level adoption composites from existing
stars, forks, and downloads columns, then materializes ``Project.adoption_score``
as the mean of child repo composites.

The core helper mutates the provided SQLAlchemy session but does not commit.

Usage::

    uv run python -m pg_atlas.metrics.materialize_adoption

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from pg_atlas.db_models.project import Project
from pg_atlas.db_models.repo_vertex import Repo
from pg_atlas.db_models.session import get_session_factory
from pg_atlas.metrics.adoption import (
    RepoAdoptionSignals,
    compute_project_adoption_scores,
    compute_repo_adoption_composites,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AdoptionMaterializationStats:
    """
    Summarize one adoption materialization pass.
    """

    repos_seen: int
    repo_composites_computed: int
    projects_seen: int
    projects_scored: int
    duration_seconds: float


async def materialize_adoption_scores(session: AsyncSession) -> AdoptionMaterializationStats:
    """
    Recompute and persist project adoption scores within one session.

    The session is mutated and flushed, but not committed.
    Projects with no child repo composites are materialized back to ``NULL`` so
    stale values are cleared.
    """

    started_at = time.perf_counter()

    repos = (await session.execute(select(Repo).order_by(Repo.id))).scalars().all()
    repo_snapshots = [
        RepoAdoptionSignals(
            canonical_id=repo.canonical_id,
            project_id=repo.project_id,
            adoption_downloads=repo.adoption_downloads,
            adoption_stars=repo.adoption_stars,
            adoption_forks=repo.adoption_forks,
        )
        for repo in repos
    ]
    repo_composites = compute_repo_adoption_composites(repo_snapshots)
    project_scores = compute_project_adoption_scores(repo_snapshots, repo_composites)

    projects = (await session.execute(select(Project).order_by(Project.id))).scalars().all()
    for project in projects:
        project.adoption_score = project_scores.get(project.id)

    await session.flush()

    duration_seconds = time.perf_counter() - started_at
    stats = AdoptionMaterializationStats(
        repos_seen=len(repos),
        repo_composites_computed=len(repo_composites),
        projects_seen=len(projects),
        projects_scored=len(project_scores),
        duration_seconds=duration_seconds,
    )

    logger.info(
        "materialize_adoption_scores: "
        f"repos_seen={stats.repos_seen} "
        f"repo_composites_computed={stats.repo_composites_computed} "
        f"projects_seen={stats.projects_seen} "
        f"projects_scored={stats.projects_scored} "
        f"duration_seconds={stats.duration_seconds:.3f}"
    )

    return stats


async def main() -> None:
    """
    Run one offline adoption materialization pass and commit the results.
    """

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )

    factory = get_session_factory()
    async with factory() as session:
        stats = await materialize_adoption_scores(session)
        await session.commit()

    logger.info(
        "project adoption materialization finished: "
        f"repos_seen={stats.repos_seen} "
        f"repo_composites_computed={stats.repo_composites_computed} "
        f"projects_scored={stats.projects_scored} "
        f"duration_seconds={stats.duration_seconds:.3f}"
    )


if __name__ == "__main__":
    asyncio.run(main())
