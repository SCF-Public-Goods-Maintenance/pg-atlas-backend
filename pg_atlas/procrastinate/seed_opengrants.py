"""
Seed script - defers the root ``sync_opengrants`` task.

Usage::

    uv run python -m pg_atlas.procrastinate.seed_opengrants

The script opens the Procrastinate connection, defers a single root task,
and exits immediately. The actual crawling happens when a worker processes
the queue.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import argparse
import asyncio
import logging

from pg_atlas.procrastinate.app import app, mark_stalled_jobs_failed

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


async def seed(canonical_id: list[str] | None = None) -> None:
    """Defer the root ``sync_opengrants`` task with an optional project filter."""

    selected_canonical_ids = canonical_id or []

    stalled_marked = await mark_stalled_jobs_failed(queue_name="opengrants")
    if stalled_marked > 0:
        logger.warning(f"Marked {stalled_marked} stalled jobs as failed in queue opengrants")

    async with app.open_async():
        from pg_atlas.procrastinate.tasks import sync_opengrants

        job_id = await sync_opengrants.defer_async(canonical_ids=selected_canonical_ids)
        logger.info(f"Deferred sync_opengrants task: job_id={job_id}")


def _parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the OpenGrants seed command."""

    parser = argparse.ArgumentParser(description="Defer the OpenGrants bootstrap root task")
    parser.add_argument("canonical_id", nargs="*", help="optional project filter.")

    return parser.parse_args()


if __name__ == "__main__":
    # TODO: add a CLI flag for `extended_universe`
    args = _parse_args()
    asyncio.run(seed(canonical_id=args.canonical_id))
