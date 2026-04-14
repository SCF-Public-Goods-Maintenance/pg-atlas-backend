"""
Seed script for git log queue processing.

Selects non-private repos with a configured repo_url and defers fixed-size batch
jobs to the ``gitlog`` queue.

Usage::

    uv run python -m pg_atlas.procrastinate.seed_gitlog

Recent repos are processed each run: if a repo has a known commit within the last 30 days,
it is always scheduled. Dormant repos get a cadence in runs, derived from their dormancy
percentile within the current dormant population (using numpy). A repo is scheduled when
the current seed-run ordinal has reached or passed its next due run. That gives the
guarantee that fully dormant repos will be scheduled eventually while still letting
slightly dormant repos be processed much more often.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
from collections.abc import Iterator, Sequence

import numpy as np
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from pg_atlas.config import settings
from pg_atlas.db_models.base import SubmissionStatus, Visibility
from pg_atlas.db_models.gitlog_artifact import GitLogArtifact
from pg_atlas.db_models.repo_vertex import Repo
from pg_atlas.db_models.session import get_session_factory
from pg_atlas.procrastinate.app import app, mark_stalled_jobs_failed

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

RECENT_COMMIT_DAYS = 30


def _batched(items: Sequence[int], batch_size: int) -> Iterator[list[int]]:
    """Yield successive fixed-size batches from a list of repo IDs."""

    for start in range(0, len(items), batch_size):
        yield list(items[start : start + batch_size])


async def _next_seed_run_ordinal(session: AsyncSession) -> int:
    """Return the next global seed run ordinal."""

    max_ordinal = await session.scalar(select(func.max(GitLogArtifact.seed_run_ordinal)))
    if max_ordinal is None:
        return 1

    return int(max_ordinal) + 1


async def _load_candidate_repos(session: AsyncSession) -> list[tuple[int, dt.datetime | None]]:
    """Load public repos with URLs and their latest known commit date."""

    stmt = (
        select(Repo.id, Repo.latest_commit_date)
        .where(Repo.repo_url.isnot(None), Repo.repo_url != "")
        .where(Repo.visibility != Visibility.private)
        .order_by(Repo.id)
    )

    rows = list((await session.execute(stmt)).all())

    return [(int(repo_id), latest_commit_date) for repo_id, latest_commit_date in rows]


def _compute_dormant_cadences(
    dormant_repos: Sequence[tuple[int, dt.datetime | None]],
    *,
    now: dt.datetime,
    k_runs: int,
) -> dict[int, int]:
    """
    Map dormant repos to cadence values in the range ``[1, k_runs]``.

    Cadence is percentile-based over dormant repositories only. Less dormant
    repos are processed more frequently, fully dormant repos get cadence ``k``.
    """

    if not dormant_repos:
        return {}

    ages_days = np.empty(len(dormant_repos), dtype=np.float64)
    for index, (_, latest_commit_date) in enumerate(dormant_repos):
        if latest_commit_date is None:
            ages_days[index] = np.inf
            continue

        ages_days[index] = max(0.0, (now - latest_commit_date).total_seconds() / 86400.0)

    percentiles = np.ones(len(dormant_repos), dtype=np.float64)
    finite_indices = np.flatnonzero(np.isfinite(ages_days))
    if finite_indices.size == 1:
        percentiles[finite_indices] = 0.0
    elif finite_indices.size > 1:
        finite_ages = ages_days[finite_indices]
        ordering = np.argsort(finite_ages, kind="stable")
        finite_ranks = np.empty(ordering.size, dtype=np.int64)
        finite_ranks[ordering] = np.arange(ordering.size)
        denominator = float(ordering.size - 1)
        percentiles[finite_indices] = finite_ranks.astype(np.float64) / denominator

    cadence_values = np.ceil(1.0 + percentiles * float(k_runs - 1)).astype(np.int64)
    cadence_values = np.clip(cadence_values, 1, k_runs)

    return {repo_id: int(cadence_values[index]) for index, (repo_id, _) in enumerate(dormant_repos)}


async def _load_last_successful_seed_runs(session: AsyncSession, repo_ids: Sequence[int]) -> dict[int, int]:
    """Return latest successful seed run ordinal per repo for the given ids."""

    if not repo_ids:
        return {}

    stmt = (
        select(GitLogArtifact.repo_id, func.max(GitLogArtifact.seed_run_ordinal))
        .where(GitLogArtifact.repo_id.in_(repo_ids))
        .where(GitLogArtifact.status == SubmissionStatus.processed)
        .group_by(GitLogArtifact.repo_id)
    )
    rows = list((await session.execute(stmt)).all())

    return {int(repo_id): int(max_ordinal) for repo_id, max_ordinal in rows if max_ordinal is not None}


def _due_dormant_repo_ids(
    dormant_repo_ids: Sequence[int],
    *,
    cadences: dict[int, int],
    last_successful_seed_runs: dict[int, int],
    current_seed_run_ordinal: int,
) -> list[int]:
    """Select dormant repos that are due in the current seed run."""

    due: list[int] = []
    for repo_id in dormant_repo_ids:
        last_success = last_successful_seed_runs.get(repo_id)
        cadence = cadences[repo_id]
        if last_success is None:
            due.append(repo_id)
            continue

        if current_seed_run_ordinal - last_success >= cadence:
            due.append(repo_id)

    return due


async def seed_gitlog_batches() -> None:
    """Resolve candidate repo IDs and defer one ``process_gitlog_batch`` job per batch."""

    stalled_marked = await mark_stalled_jobs_failed(queue_name="gitlog")
    if stalled_marked > 0:
        logger.warning(f"Marked {stalled_marked} stalled jobs as failed in queue gitlog")

    session_factory = get_session_factory()
    async with session_factory() as session:
        current_seed_run_ordinal = await _next_seed_run_ordinal(session)
        candidates = await _load_candidate_repos(session)

        if not candidates:
            logger.info("No candidate repos found for gitlog seeding")

            return

        now = dt.datetime.now(dt.UTC)
        recent_cutoff = now - dt.timedelta(days=RECENT_COMMIT_DAYS)

        recent_repo_ids: list[int] = []
        dormant_repos: list[tuple[int, dt.datetime | None]] = []
        for repo_id, latest_commit_date in candidates:
            if latest_commit_date is not None and latest_commit_date >= recent_cutoff:
                recent_repo_ids.append(int(repo_id))
            else:
                dormant_repos.append((int(repo_id), latest_commit_date))

        k_runs = max(1, settings.GITLOG_DORMANCY_K_RUNS)
        cadences = _compute_dormant_cadences(dormant_repos, now=now, k_runs=k_runs)
        dormant_repo_ids = [repo_id for repo_id, _ in dormant_repos]
        last_successful_seed_runs = await _load_last_successful_seed_runs(session, dormant_repo_ids)
        due_dormant_repo_ids = _due_dormant_repo_ids(
            dormant_repo_ids,
            cadences=cadences,
            last_successful_seed_runs=last_successful_seed_runs,
            current_seed_run_ordinal=current_seed_run_ordinal,
        )

    scheduled_repo_ids = recent_repo_ids + sorted(due_dormant_repo_ids)
    if not scheduled_repo_ids:
        logger.info(
            f"No repos due for gitlog seeding: seed_run_ordinal={current_seed_run_ordinal} "
            f"candidates={len(candidates)} recent={len(recent_repo_ids)} dormant={len(dormant_repos)}"
        )

        return

    batch_size = max(1, settings.GITLOG_BATCH_SIZE)
    deferred = 0

    async with app.open_async():
        from pg_atlas.procrastinate.tasks import defer_with_lock, process_gitlog_batch

        for batch_index, batch in enumerate(_batched(scheduled_repo_ids, batch_size), start=1):
            lock = f"gitlog-seed:{current_seed_run_ordinal}:batch:{batch_index}:{batch[0]}:{batch[-1]}"
            enqueued = await defer_with_lock(
                process_gitlog_batch,
                queueing_lock=lock,
                repo_ids=batch,
                seed_run_ordinal=current_seed_run_ordinal,
            )
            if enqueued:
                deferred += 1

            logger.info(
                f"Gitlog batch {batch_index}: size={len(batch)} first_repo_id={batch[0]} "
                f"last_repo_id={batch[-1]} enqueued={enqueued}"
            )

    logger.info(
        f"Gitlog seed complete: seed_run_ordinal={current_seed_run_ordinal} "
        f"candidates={len(candidates)} scheduled={len(scheduled_repo_ids)} recent={len(recent_repo_ids)} "
        f"dormant_due={len(due_dormant_repo_ids)} dormant_deferred={len(dormant_repos) - len(due_dormant_repo_ids)} "
        f"batch_size={batch_size} batches_deferred={deferred}"
    )


if __name__ == "__main__":
    asyncio.run(seed_gitlog_batches())
