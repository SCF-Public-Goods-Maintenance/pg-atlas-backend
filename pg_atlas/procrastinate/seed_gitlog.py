"""
Seed script for git log queue processing.

Selects non-private repos with a configured repo_url and defers fixed-size batch
jobs to the ``gitlog`` queue.

Usage::

    uv run python -m pg_atlas.procrastinate.seed_gitlog

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterator, Sequence

from sqlalchemy import select

from pg_atlas.config import settings
from pg_atlas.db_models.base import Visibility
from pg_atlas.db_models.repo_vertex import Repo
from pg_atlas.db_models.session import get_session_factory
from pg_atlas.procrastinate.app import app, mark_stalled_jobs_failed

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def _batched(items: Sequence[int], batch_size: int) -> Iterator[list[int]]:
    """Yield successive fixed-size batches from a list of repo IDs."""

    for start in range(0, len(items), batch_size):
        yield list(items[start : start + batch_size])


async def seed_gitlog_batches() -> None:
    """Resolve candidate repo IDs and defer one ``process_gitlog_batch`` job per batch."""

    stalled_marked = await mark_stalled_jobs_failed(queue_name="gitlog")
    if stalled_marked > 0:
        logger.warning(f"Marked {stalled_marked} stalled jobs as failed in queue gitlog")

    session_factory = get_session_factory()
    async with session_factory() as session:
        stmt = (
            select(Repo.id)
            .where(Repo.repo_url.isnot(None), Repo.repo_url != "")
            .where(Repo.visibility != Visibility.private)
            .order_by(Repo.id)
        )
        repo_ids = (await session.scalars(stmt)).all()

    if not repo_ids:
        logger.info("No candidate repos found for gitlog seeding")

        return

    batch_size = max(1, settings.GITLOG_BATCH_SIZE)
    deferred = 0

    async with app.open_async():
        from pg_atlas.procrastinate.tasks import defer_with_lock, process_gitlog_batch

        for batch_index, batch in enumerate(_batched(repo_ids, batch_size), start=1):
            lock = f"gitlog-batch:{batch[0]}:{batch[-1]}"
            enqueued = await defer_with_lock(
                process_gitlog_batch,
                queueing_lock=lock,
                repo_ids=batch,
            )
            if enqueued:
                deferred += 1

            logger.info(
                f"Gitlog batch {batch_index}: size={len(batch)} first_repo_id={batch[0]} "
                f"last_repo_id={batch[-1]} enqueued={enqueued}"
            )

    logger.info(f"Gitlog seed complete: candidates={len(repo_ids)} batch_size={batch_size} batches_deferred={deferred}")


if __name__ == "__main__":
    asyncio.run(seed_gitlog_batches())
