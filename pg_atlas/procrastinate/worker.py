"""
Procrastinate worker invocation script.

Runs one or more Procrastinate workers that process tasks from a specified queue.
With ``wait=False`` (the default) the worker exits once the queue is drained,
which is required for GitHub Actions jobs to complete.

Usage::

    # Process the opengrants queue, exit when empty:
    uv run python -m pg_atlas.procrastinate.worker --queue=opengrants

    # Process package-deps queue with higher concurrency:
    uv run python -m pg_atlas.procrastinate.worker --queue=package-deps --concurrency=8

    # Block until interrupted (local development):
    uv run python -m pg_atlas.procrastinate.worker --queue=opengrants --wait

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import argparse
import asyncio
import logging

from pg_atlas.procrastinate.app import app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


async def process_queue(queue_name: str, concurrency: int, wait: bool) -> None:
    """
    Run the Procrastinate worker until the queue is drained (or interrupted).

    Args:
        queue_name: Name of the queue to process (e.g. ``"opengrants"``).
        concurrency: Number of concurrent task executions.
        wait: If ``True``, keep polling even when the queue is empty.
            If ``False``, exit once no pending tasks remain.
    """
    logger.info(
        "Starting worker: queue=%s concurrency=%d wait=%s",
        queue_name,
        concurrency,
        wait,
    )

    async with app.open_async():
        await app.run_worker_async(
            queues=[queue_name],
            concurrency=concurrency,
            wait=wait,
        )

    logger.info("Worker finished: queue=%s", queue_name)


def main() -> None:
    """Parse CLI arguments and run the worker."""

    parser = argparse.ArgumentParser(description="PG Atlas Procrastinate worker")
    parser.add_argument(
        "--queue",
        required=True,
        help="Queue name to process (e.g. opengrants, package-deps)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=4,
        help="Number of concurrent task slots (default: 4)",
    )
    parser.add_argument(
        "--wait",
        action="store_true",
        default=False,
        help="Keep polling when queue is empty (default: exit when drained)",
    )

    args = parser.parse_args()
    asyncio.run(process_queue(args.queue, args.concurrency, args.wait))


if __name__ == "__main__":
    main()
