"""
Project adoption-score materialization from repo signal percentiles.

This module computes transient repo-level adoption composites from existing
stars, forks, and downloads columns, then materializes ``Project.adoption_score``
as the mean of child repo composites.

All writeback is via bulk DML (``session.execute(update(…), …)``). The session
identity map is never mutated; no ``flush()`` is issued.

Usage::

    uv run python -m pg_atlas.metrics.materialize_adoption
    uv run python -m pg_atlas.metrics.materialize_adoption --tee=adoption.log

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, NamedTuple

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from pg_atlas.db_models.project import Project
from pg_atlas.db_models.repo_vertex import Repo
from pg_atlas.db_models.session import get_session_factory
from pg_atlas.instruments.tee import run_with_tee
from pg_atlas.metrics.adoption import (
    aggregate_repo_downloads,
    compute_project_adoption_scores,
    compute_repo_adoption_composites,
    downloads_by_purl_from_metadata,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AdoptionMaterializationStats:
    """
    Summarize one adoption materialization pass.
    """

    repos_seen: int
    repo_composites_computed: int
    projects_scored: int
    duration_seconds: float


async def materialize_adoption_scores(session: AsyncSession) -> AdoptionMaterializationStats:
    """
    Recompute and persist project adoption scores within one session.

    All writes use bulk DML; the ORM identity map is untouched.
    Projects with no child repo composites are cleared to ``NULL`` via a
    ``WHERE NOT IN`` set-based update so stale values are removed.
    """

    started_at = time.perf_counter()

    class RepoAdoptionRow(NamedTuple):
        id: int
        canonical_id: str
        project_id: int | None
        adoption_downloads: int | None
        adoption_stars: int | None
        adoption_forks: int | None
        metadata: dict[str, Any] | None

    # --- raw columnar load (no ORM instances) ---
    rows = (
        await session.execute(
            select(
                Repo.id,
                Repo.canonical_id,
                Repo.project_id,
                Repo.adoption_downloads,
                Repo.adoption_stars,
                Repo.adoption_forks,
                Repo.repo_metadata,
            ).order_by(Repo.id)
        )
    ).all()

    # column lists
    ids: list[int] = []
    canonical_ids: list[str] = []
    project_ids: list[int | None] = []
    downloads: list[int | None] = []
    stars: list[int | None] = []
    forks: list[int | None] = []

    # bulk update plan
    download_updates: list[dict[str, int | None]] = []

    # --- transpose rows to columns and plan updates ---
    repo_rows = (RepoAdoptionRow(*row) for row in rows)
    for row in repo_rows:
        ids.append(row.id)
        canonical_ids.append(row.canonical_id)
        project_ids.append(row.project_id)
        by_purl: dict[str, int] | None = downloads_by_purl_from_metadata(row.metadata, repo_canonical_id=row.canonical_id)
        total_downloads: int | None = aggregate_repo_downloads(by_purl)
        downloads.append(total_downloads)
        stars.append(row.adoption_stars)
        forks.append(row.adoption_forks)

        if total_downloads != row.adoption_downloads:
            download_updates.append({"id": row.id, "adoption_downloads": total_downloads})

    # sanity check to ensure equal-length columns
    expected_len = len(rows)
    assert all(len(col) == expected_len for col in (ids, canonical_ids, project_ids, downloads, stars, forks)), (
        "Transposition failed: one or more columns do not match the input row count."
    )

    # --- bulk-persist download sums (only changed rows) ---
    if download_updates:
        await session.execute(update(Repo), download_updates)

    # --- compute composites and project scores ---
    repo_composites = compute_repo_adoption_composites(canonical_ids, downloads, stars, forks)
    project_scores = compute_project_adoption_scores(project_ids, canonical_ids, repo_composites)

    # --- bulk-update scored projects ---
    if project_scores:
        project_updates = [{"id": pid, "adoption_score": score} for pid, score in project_scores.items()]
        await session.execute(update(Project), project_updates)

    # --- null-out stale unscored projects ---
    scored_ids = set(project_scores.keys())
    if scored_ids:
        stale_stmt = (
            update(Project)
            .where(Project.id.not_in(scored_ids))
            .where(Project.adoption_score.is_not(None))
            .values(adoption_score=None)
            .execution_options(synchronize_session=False)
        )
        await session.execute(stale_stmt)
    else:
        stale_stmt = (
            update(Project)
            .where(Project.adoption_score.is_not(None))
            .values(adoption_score=None)
            .execution_options(synchronize_session=False)
        )
        await session.execute(stale_stmt)

    duration_seconds = time.perf_counter() - started_at
    stats = AdoptionMaterializationStats(
        repos_seen=len(rows),
        repo_composites_computed=len(repo_composites),
        projects_scored=len(project_scores),
        duration_seconds=duration_seconds,
    )

    logger.info(
        "materialize_adoption_scores: "
        f"repos_seen={stats.repos_seen} "
        f"repo_composites_computed={stats.repo_composites_computed} "
        f"projects_scored={stats.projects_scored} "
        f"duration_seconds={stats.duration_seconds:.3f}"
    )

    return stats


async def main() -> None:
    """
    Run one offline adoption materialization pass and commit the results.
    """

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


def _build_parser() -> argparse.ArgumentParser:
    """
    Build the CLI parser for adoption materialization.
    """

    parser = argparse.ArgumentParser(description="Materialize project adoption scores.")
    parser.add_argument(
        "--tee",
        type=Path,
        default=None,
        help="Optional path to mirror stdout/stderr logs while preserving console output.",
    )

    return parser


def entrypoint() -> None:
    """
    Parse CLI arguments and run the materialization pass.
    """

    args = _build_parser().parse_args()

    def _run() -> None:
        asyncio.run(main())

    run_with_tee(args.tee, _run)


if __name__ == "__main__":
    entrypoint()
