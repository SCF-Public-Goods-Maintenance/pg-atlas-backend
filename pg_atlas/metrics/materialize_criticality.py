"""
A9 criticality materialization for repo-resolution dependency graphs.

This module turns the A6 and A9 in-memory metric functions into
an offline persistence pass:

1. Load the repo-resolution dependency graph from PostgreSQL.
2. Project the active ecosystem (A6).
3. Compute repo/external criticality (A9).
4. Materialize dep-layer scores back onto ORM rows.
5. Aggregate project scores from child repos.

The core helper mutates the provided SQLAlchemy session but does not commit.
Callers can therefore use it from a CLI, a background task, or a rollback-only
test transaction.

Usage::

    uv run python -m pg_atlas.metrics.materialize_criticality
    uv run python -m pg_atlas.metrics.materialize_criticality --tee=criticality.log

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

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from pg_atlas.db_models.project import Project
from pg_atlas.db_models.repo_vertex import ExternalRepo, Repo
from pg_atlas.db_models.session import get_session_factory
from pg_atlas.instruments.tee import run_with_tee
from pg_atlas.metrics.active_subgraph import project_active_subgraph
from pg_atlas.metrics.criticality import compute_criticality
from pg_atlas.metrics.graph_builder import build_dependency_graph

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CriticalityMaterializationStats:
    """
    Summarize one A9 materialization pass.
    """

    dep_nodes_seen: int
    active_dep_nodes_scored: int
    repo_rows_updated: int
    external_repo_rows_updated: int
    project_rows_updated: int
    duration_seconds: float


async def materialize_criticality_scores(session: AsyncSession) -> CriticalityMaterializationStats:
    """
    Recompute and persist A9 criticality scores within one session.

    All writes use bulk DML; the ORM identity map is untouched. No ``flush()``
    is issued.

    Behavior:
    - Repo and ExternalRepo rows are always materialized to an integer score.
      Nodes absent from the active subgraph are written as ``0``.
    - Project rows are aggregated from child Repo rows only.
      Projects with no repos remain ``NULL``.
      Projects with repos but no active dependency pressure become ``0``.
    - The computation path stays strictly at repo resolution; it never uses
      ``build_full_graph()`` or project nodes in NetworkX.
    """

    started_at = time.perf_counter()

    dependency_graph = await build_dependency_graph(session)
    active_graph = project_active_subgraph(dependency_graph)
    active_scores = compute_criticality(active_graph)

    # --- columnar load: id + canonical_id only ---
    repo_rows = (await session.execute(select(Repo.id, Repo.canonical_id))).all()
    ext_rows = (await session.execute(select(ExternalRepo.id, ExternalRepo.canonical_id))).all()
    dep_nodes_seen = len(repo_rows) + len(ext_rows)

    # --- bulk update Repo criticality scores ---
    repo_updates = [{"id": rid, "criticality_score": active_scores.get(cid, 0)} for rid, cid in repo_rows]
    if repo_updates:
        await session.execute(update(Repo), repo_updates)

    # --- bulk update ExternalRepo criticality scores ---
    ext_updates = [{"id": rid, "criticality_score": active_scores.get(cid, 0)} for rid, cid in ext_rows]
    if ext_updates:
        await session.execute(update(ExternalRepo), ext_updates)

    # --- set-based Project aggregation (sum of child Repo scores) ---
    project_score_subquery = (
        select(
            Repo.project_id.label("project_id"),
            func.sum(Repo.criticality_score).label("criticality_score"),
        )
        .where(Repo.project_id.is_not(None))
        .group_by(Repo.project_id)
        .subquery()
    )

    project_update = (
        update(Project)
        .values(
            criticality_score=(
                select(project_score_subquery.c.criticality_score)
                .where(project_score_subquery.c.project_id == Project.id)
                .scalar_subquery()
            )
        )
        .execution_options(synchronize_session=False)
    )
    await session.execute(project_update)

    project_rows_updated = await session.scalar(select(func.count(Project.id))) or 0

    duration_seconds = time.perf_counter() - started_at

    stats = CriticalityMaterializationStats(
        dep_nodes_seen=dep_nodes_seen,
        active_dep_nodes_scored=len(active_scores),
        repo_rows_updated=len(repo_rows),
        external_repo_rows_updated=len(ext_rows),
        project_rows_updated=project_rows_updated,
        duration_seconds=duration_seconds,
    )

    logger.info(
        "materialize_criticality_scores: "
        f"dep_nodes_seen={stats.dep_nodes_seen} "
        f"active_dep_nodes_scored={stats.active_dep_nodes_scored} "
        f"repo_rows_updated={stats.repo_rows_updated} "
        f"external_repo_rows_updated={stats.external_repo_rows_updated} "
        f"project_rows_updated={stats.project_rows_updated} "
        f"duration_seconds={stats.duration_seconds:.3f}"
    )

    return stats


async def main() -> None:
    """
    Run one offline A9 materialization pass and commit the results.
    """

    factory = get_session_factory()
    async with factory() as session:
        stats = await materialize_criticality_scores(session)
        await session.commit()

    logger.info(
        "A9 criticality materialization finished: "
        f"dep_nodes_seen={stats.dep_nodes_seen} "
        f"active_dep_nodes_scored={stats.active_dep_nodes_scored} "
        f"duration_seconds={stats.duration_seconds:.3f}"
    )


def _build_parser() -> argparse.ArgumentParser:
    """
    Build the CLI parser for criticality materialization.
    """

    parser = argparse.ArgumentParser(description="Materialize repo/project criticality scores.")
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
