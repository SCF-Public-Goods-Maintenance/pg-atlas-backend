"""
Pony-factor materialization from git log contributor edges.

Usage::

    uv run python -m pg_atlas.metrics.materialize_pony
    uv run python -m pg_atlas.metrics.materialize_pony --latest-seed-run
    uv run python -m pg_atlas.metrics.materialize_pony --seed-run-ordinal 7

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import time
from collections import defaultdict
from dataclasses import dataclass

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from pg_atlas.db_models.contributed_to import ContributedTo
from pg_atlas.db_models.contributor import Contributor
from pg_atlas.db_models.gitlog_artifact import GitLogArtifact
from pg_atlas.db_models.project import Project
from pg_atlas.db_models.repo_vertex import Repo
from pg_atlas.db_models.session import get_session_factory
from pg_atlas.metrics.pony_factor import compute_pony_factor

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PonyFactorMaterializationStats:
    """
    Summarize one pony-factor materialization pass.
    """

    repo_rows_updated: int
    project_rows_updated: int
    resolved_seed_run_ordinal: int | None
    duration_seconds: float


def _build_parser() -> argparse.ArgumentParser:
    """
    Build the CLI parser for pony-factor materialization.
    """

    parser = argparse.ArgumentParser(description="Materialize repo/project pony factor from gitlog artifacts.")
    scope = parser.add_mutually_exclusive_group()
    scope.add_argument(
        "--latest-seed-run",
        action="store_true",
        help="Recompute only repos included in the latest gitlog seed run and their projects.",
    )
    scope.add_argument(
        "--seed-run-ordinal",
        type=int,
        help="Recompute only repos included in the given gitlog seed run ordinal and their projects.",
    )

    return parser


async def _resolve_target_repo_ids(
    session: AsyncSession,
    *,
    latest_seed_run: bool,
    seed_run_ordinal: int | None,
) -> tuple[list[int], int | None]:
    """
    Resolve repo ids for the selected materialization scope.
    """

    if latest_seed_run:
        resolved_seed_run_ordinal = await session.scalar(select(func.max(GitLogArtifact.seed_run_ordinal)))
        if resolved_seed_run_ordinal is None:
            return [], None

        stmt = (
            select(GitLogArtifact.repo_id)
            .where(GitLogArtifact.seed_run_ordinal == resolved_seed_run_ordinal)
            .distinct()
            .order_by(GitLogArtifact.repo_id)
        )
        repo_ids = [int(repo_id) for repo_id in (await session.execute(stmt)).scalars().all()]

        return repo_ids, int(resolved_seed_run_ordinal)

    if seed_run_ordinal is not None:
        stmt = (
            select(GitLogArtifact.repo_id)
            .where(GitLogArtifact.seed_run_ordinal == seed_run_ordinal)
            .distinct()
            .order_by(GitLogArtifact.repo_id)
        )
        repo_ids = [int(repo_id) for repo_id in (await session.execute(stmt)).scalars().all()]

        return repo_ids, seed_run_ordinal

    stmt = select(Repo.id).order_by(Repo.id)
    repo_ids = [int(repo_id) for repo_id in (await session.execute(stmt)).scalars().all()]

    return repo_ids, None


async def _load_repo_email_commit_counts(
    session: AsyncSession,
    repo_ids: list[int],
) -> dict[int, dict[str, int]]:
    """
    Return commit counts grouped by repo id and contributor email hash.
    """

    if not repo_ids:
        return {}

    stmt = (
        select(ContributedTo.repo_id, Contributor.email_hash, ContributedTo.number_of_commits)
        .select_from(ContributedTo)
        .join(Contributor, Contributor.id == ContributedTo.contributor_id)
        .where(ContributedTo.repo_id.in_(repo_ids))
    )
    rows = list((await session.execute(stmt)).all())

    repo_counts: defaultdict[int, defaultdict[str, int]] = defaultdict(lambda: defaultdict(int))
    for repo_id, email_hash, commit_count in rows:
        repo_counts[int(repo_id)][str(email_hash)] += int(commit_count)

    return {repo_id: dict(counts_by_email) for repo_id, counts_by_email in repo_counts.items()}


async def _load_project_repo_membership(
    session: AsyncSession,
    *,
    full_recompute: bool,
    target_repo_ids: list[int],
) -> tuple[list[Project], dict[int, list[int]]]:
    """
    Return affected projects and their child repo ids.
    """

    if full_recompute:
        projects = list((await session.execute(select(Project).order_by(Project.id))).scalars().all())
        membership_rows = (await session.execute(select(Repo.id, Repo.project_id).where(Repo.project_id.is_not(None)))).all()
    else:
        if not target_repo_ids:
            return [], {}

        project_ids = (
            (
                await session.execute(
                    select(Repo.project_id)
                    .where(Repo.id.in_(target_repo_ids))
                    .where(Repo.project_id.is_not(None))
                    .distinct()
                    .order_by(Repo.project_id)
                )
            )
            .scalars()
            .all()
        )
        affected_project_ids = [int(project_id) for project_id in project_ids if project_id is not None]
        if not affected_project_ids:
            return [], {}

        projects = list(
            (await session.execute(select(Project).where(Project.id.in_(affected_project_ids)).order_by(Project.id)))
            .scalars()
            .all()
        )
        membership_rows = (
            await session.execute(select(Repo.id, Repo.project_id).where(Repo.project_id.in_(affected_project_ids)))
        ).all()

    repos_by_project: defaultdict[int, list[int]] = defaultdict(list)
    for repo_id, project_id in membership_rows:
        if project_id is None:
            continue

        repos_by_project[int(project_id)].append(int(repo_id))

    return projects, dict(repos_by_project)


async def materialize_pony_factor_scores(
    session: AsyncSession,
    *,
    latest_seed_run: bool = False,
    seed_run_ordinal: int | None = None,
) -> PonyFactorMaterializationStats:
    """
    Recompute and persist pony factor on Repo and Project rows within one session.

    The session is mutated and flushed, but not committed.
    """

    if latest_seed_run and seed_run_ordinal is not None:
        raise ValueError("latest_seed_run and seed_run_ordinal are mutually exclusive")

    started_at = time.perf_counter()
    full_recompute = not latest_seed_run and seed_run_ordinal is None
    target_repo_ids, resolved_seed_run_ordinal = await _resolve_target_repo_ids(
        session,
        latest_seed_run=latest_seed_run,
        seed_run_ordinal=seed_run_ordinal,
    )

    target_repos = list(
        (await session.execute(select(Repo).where(Repo.id.in_(target_repo_ids)).order_by(Repo.id))).scalars().all()
    )
    repo_counts = await _load_repo_email_commit_counts(session, target_repo_ids)
    for repo in target_repos:
        counts_by_email = repo_counts.get(repo.id, {})
        repo.pony_factor = compute_pony_factor(counts_by_email.values())

    projects, repos_by_project = await _load_project_repo_membership(
        session,
        full_recompute=full_recompute,
        target_repo_ids=target_repo_ids,
    )
    project_repo_ids = sorted({repo_id for repo_ids in repos_by_project.values() for repo_id in repo_ids})
    project_repo_counts = await _load_repo_email_commit_counts(session, project_repo_ids)

    for project in projects:
        repo_ids = repos_by_project.get(project.id, [])
        if not repo_ids:
            project.pony_factor = None
            continue

        project_counts_by_email: defaultdict[str, int] = defaultdict(int)
        for repo_id in repo_ids:
            for email_hash, commit_count in project_repo_counts.get(repo_id, {}).items():
                project_counts_by_email[email_hash] += commit_count

        project.pony_factor = compute_pony_factor(project_counts_by_email.values())

    await session.flush()

    duration_seconds = time.perf_counter() - started_at
    stats = PonyFactorMaterializationStats(
        repo_rows_updated=len(target_repos),
        project_rows_updated=len(projects),
        resolved_seed_run_ordinal=resolved_seed_run_ordinal,
        duration_seconds=duration_seconds,
    )

    logger.info(
        "materialize_pony_factor_scores: "
        f"repo_rows_updated={stats.repo_rows_updated} "
        f"project_rows_updated={stats.project_rows_updated} "
        f"resolved_seed_run_ordinal={stats.resolved_seed_run_ordinal} "
        f"duration_seconds={stats.duration_seconds:.3f}"
    )

    return stats


async def main() -> None:
    """
    Run one offline pony-factor materialization pass and commit the results.
    """

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )

    args = _build_parser().parse_args()

    factory = get_session_factory()
    async with factory() as session:
        stats = await materialize_pony_factor_scores(
            session,
            latest_seed_run=args.latest_seed_run,
            seed_run_ordinal=args.seed_run_ordinal,
        )
        await session.commit()

    logger.info(
        "Pony-factor materialization finished: "
        f"repo_rows_updated={stats.repo_rows_updated} "
        f"project_rows_updated={stats.project_rows_updated} "
        f"resolved_seed_run_ordinal={stats.resolved_seed_run_ordinal} "
        f"duration_seconds={stats.duration_seconds:.3f}"
    )


if __name__ == "__main__":
    asyncio.run(main())
