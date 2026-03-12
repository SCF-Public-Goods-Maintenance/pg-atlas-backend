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
from collections.abc import Iterable

import psycopg

from pg_atlas.procrastinate.app import _get_database_url, app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def _queue_status_counts(queue_name: str) -> dict[str, int]:
    """

    Return per-status job counts for one queue.
    """
    dsn = _get_database_url()
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status::text, count(*)
                FROM procrastinate_jobs
                WHERE queue_name = %s
                GROUP BY status
                """,
                (queue_name,),
            )
            rows = cur.fetchall()

    counts = {"todo": 0, "doing": 0, "succeeded": 0, "failed": 0, "cancelled": 0, "aborted": 0}
    for status, count in rows:
        counts[str(status)] = int(count)

    return counts


def _count_jobs_in_statuses(queue_name: str, statuses: Iterable[str]) -> int:
    """

    Return the number of jobs in *statuses* for one queue.
    """
    status_list = list(statuses)
    if not status_list:
        return 0

    dsn = _get_database_url()
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)
                FROM procrastinate_jobs
                WHERE queue_name = %s
                    AND status = ANY(%s)
                """,
                (queue_name, status_list),
            )
            row = cur.fetchone()

    return int(row[0]) if row is not None else 0


def _recover_stale_doing_jobs(queue_name: str, stale_worker_seconds: int) -> int:
    """

    Requeue orphaned `doing` jobs for a queue after pruning stale workers.

    A `doing` job is considered orphaned when it has no associated worker row
    after stale-worker pruning.
    """
    dsn = _get_database_url()
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT worker_id FROM procrastinate_prune_stalled_workers_v1(%s)", (stale_worker_seconds,))
            pruned_rows = cur.fetchall()
            pruned_count = len(pruned_rows)
            if pruned_count > 0:
                logger.warning(
                    "Pruned %d stale workers older than %d seconds",
                    pruned_count,
                    stale_worker_seconds,
                )

            cur.execute(
                """
                UPDATE procrastinate_jobs AS jobs
                SET
                    status = 'todo'::procrastinate_job_status,
                    worker_id = NULL,
                    abort_requested = false
                WHERE
                    jobs.queue_name = %s
                    AND jobs.status = 'doing'::procrastinate_job_status
                    AND NOT EXISTS (
                        SELECT 1
                        FROM procrastinate_workers AS workers
                        WHERE workers.id = jobs.worker_id
                    )
                """,
                (queue_name,),
            )
            recovered_count = cur.rowcount

        conn.commit()

    return recovered_count


def _pending_jobs_count(queue_name: str) -> int:
    """

    Return the number of pending jobs in a queue.

    Uses the Procrastinate jobs table so the worker can decide whether another
    non-blocking pass is needed to process newly deferred tasks.
    """

    return _count_jobs_in_statuses(queue_name, ["todo"])


async def process_queue(
    queue_name: str,
    concurrency: int,
    wait: bool,
    drain_rounds: int,
    recover_stale_doing: bool,
    stale_worker_seconds: int,
) -> None:
    """

    Run the Procrastinate worker until the queue is drained (or interrupted).

    Args:
        queue_name: Name of the queue to process (e.g. ``"opengrants"``).
        concurrency: Number of concurrent task executions.
        wait: If ``True``, keep polling even when the queue is empty.
            If ``False``, exit once no pending tasks remain.
        drain_rounds: Maximum number of non-blocking drain passes when
            ``wait`` is ``False``.
        recover_stale_doing: Whether to requeue orphaned `doing` jobs before
            draining rounds.
        stale_worker_seconds: Worker heartbeat staleness threshold (seconds)
            passed to `procrastinate_prune_stalled_workers_v1`.
    """
    logger.info(
        "Starting worker: queue=%s concurrency=%d wait=%s",
        queue_name,
        concurrency,
        wait,
    )

    if wait:
        async with app.open_async():
            await app.run_worker_async(
                queues=[queue_name],
                concurrency=concurrency,
                wait=True,
            )
    else:
        if recover_stale_doing:
            recovered_jobs = _recover_stale_doing_jobs(queue_name, stale_worker_seconds)
            if recovered_jobs > 0:
                logger.warning(
                    "Recovered %d orphaned doing jobs in queue %s",
                    recovered_jobs,
                    queue_name,
                )

        for round_number in range(1, drain_rounds + 1):
            pending_before = _pending_jobs_count(queue_name)
            if pending_before == 0:
                logger.info("Queue %s is empty before round %d", queue_name, round_number)
                break

            logger.info(
                "Drain round %d/%d for queue %s (pending=%d)",
                round_number,
                drain_rounds,
                queue_name,
                pending_before,
            )

            async with app.open_async():
                await app.run_worker_async(
                    queues=[queue_name],
                    concurrency=concurrency,
                    wait=False,
                )

            pending_after = _pending_jobs_count(queue_name)
            if pending_after == 0:
                logger.info("Queue %s drained after round %d", queue_name, round_number)
                break

        else:
            logger.warning(
                "Queue %s still has pending jobs after %d rounds; consider rerunning the worker",
                queue_name,
                drain_rounds,
            )

        status_counts = _queue_status_counts(queue_name)
        logger.info(
            "Queue %s final status counts: todo=%d doing=%d succeeded=%d failed=%d cancelled=%d aborted=%d",
            queue_name,
            status_counts["todo"],
            status_counts["doing"],
            status_counts["succeeded"],
            status_counts["failed"],
            status_counts["cancelled"],
            status_counts["aborted"],
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
    parser.add_argument(
        "--drain-rounds",
        type=int,
        default=20,
        help="Maximum drain passes when --wait is not used (default: 20)",
    )
    parser.add_argument(
        "--no-recover-stale-doing",
        action="store_true",
        default=False,
        help="Disable pre-drain recovery of orphaned doing jobs",
    )
    parser.add_argument(
        "--stale-worker-seconds",
        type=int,
        default=600,
        help="Heartbeat age threshold to prune stale workers (default: 600)",
    )

    args = parser.parse_args()
    asyncio.run(
        process_queue(
            args.queue,
            args.concurrency,
            args.wait,
            args.drain_rounds,
            not args.no_recover_stale_doing,
            args.stale_worker_seconds,
        )
    )


if __name__ == "__main__":
    main()
