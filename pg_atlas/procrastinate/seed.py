"""
Seed script — defers the root ``sync_opengrants`` task.

Usage::

    uv run python -m pg_atlas.procrastinate.seed

The script opens the Procrastinate connection, defers a single root task,
and exits immediately.  The actual crawling happens when a worker processes
the queue.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import asyncio
import logging

from pg_atlas.procrastinate.app import app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


async def seed() -> None:
    """Defer the root ``sync_opengrants`` task and exit."""

    async with app.open_async():
        from pg_atlas.procrastinate.tasks import sync_opengrants

        job_id = await sync_opengrants.defer_async()
        logger.info(f"Deferred sync_opengrants task: job_id={job_id}")


if __name__ == "__main__":
    # TODO: add a CLI flag for `extended_universe`
    asyncio.run(seed())
