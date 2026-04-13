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
import io
import logging
import sys
from collections import Counter
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import TextIO

import psycopg

from pg_atlas.procrastinate.app import app, get_database_url

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


class _StreamTee(io.TextIOBase):
    """
    Mirror writes to an original stream and a tee file stream.
    """

    def __init__(self, primary: TextIO, tee_file: TextIO) -> None:
        self._primary = primary
        self._tee_file = tee_file

    def writable(self) -> bool:
        return True

    def write(self, data: str) -> int:
        written = self._primary.write(data)
        self._tee_file.write(data)

        return written

    def flush(self) -> None:
        self._primary.flush()
        self._tee_file.flush()

    def isatty(self) -> bool:
        return self._primary.isatty()

    def fileno(self) -> int:
        return self._primary.fileno()


def _run_with_optional_tee(tee_path: Path | None, run: Callable[[], None]) -> None:
    """
    Run a callback while optionally mirroring stdout/stderr to a file.

    The original streams remain active so output still appears in GitHub
    Actions logs while also being persisted to ``tee_path``.
    """
    if tee_path is None:
        run()

        return

    tee_path.parent.mkdir(parents=True, exist_ok=True)
    original_stdout = sys.stdout
    original_stderr = sys.stderr

    with tee_path.open("w", encoding="utf-8") as tee_file:
        sys.stdout = _StreamTee(original_stdout, tee_file)
        sys.stderr = _StreamTee(original_stderr, tee_file)
        try:
            run()
        finally:
            sys.stdout = original_stdout
            sys.stderr = original_stderr
            tee_file.flush()


def _queue_status_counts(queue_name: str) -> Counter[str]:
    """
    Return per-status job counts for one queue.
    """
    dsn = get_database_url()
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

    counts: Counter[str] = Counter()
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

    dsn = get_database_url()
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


async def _mark_stalled_jobs_failed(queue_name: str, stale_worker_seconds: int) -> int:
    """
    Mark stalled jobs as ``failed`` for one queue at worker startup.

    Stalled jobs are discovered via Procrastinate's heartbeat-based API and then
    finished in bulk with failure semantics equivalent to:
    ``finish_job_by_id_async(..., status=failed, delete_job=False)``.
    """
    async with app.open_async():
        stalled_jobs = await app.job_manager.get_stalled_jobs(
            queue=queue_name,
            seconds_since_heartbeat=float(stale_worker_seconds),
        )

        stalled_job_ids = [job.id for job in stalled_jobs if job.id is not None]
        if not stalled_job_ids:
            return 0

        result = await app.connector.execute_query_one_async(
            query="""
            WITH marked AS (
                UPDATE procrastinate_jobs
                SET
                    status = 'failed'::procrastinate_job_status,
                    abort_requested = false,
                    attempts = CASE status
                        WHEN 'doing'::procrastinate_job_status THEN attempts + 1
                        ELSE attempts
                    END
                WHERE
                    id = ANY(%(job_ids)s::bigint[])
                    AND status IN (
                        'todo'::procrastinate_job_status,
                        'doing'::procrastinate_job_status
                    )
                RETURNING id
            )
            SELECT count(*) AS failed_count
            FROM marked
            """,
            job_ids=stalled_job_ids,
        )

    return int(result["failed_count"])


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
        stale_worker_seconds: Worker heartbeat staleness threshold (seconds)
            used when fetching stalled jobs.
    """
    logger.info(f"Starting worker: queue={queue_name} concurrency={concurrency} wait={wait}")
    status_counts_before = _queue_status_counts(queue_name)
    failed_stalled_jobs = await _mark_stalled_jobs_failed(queue_name, stale_worker_seconds)
    if failed_stalled_jobs > 0:
        logger.warning(f"Marked {failed_stalled_jobs} stalled jobs as failed in queue {queue_name}")

    if wait:
        async with app.open_async():
            await app.run_worker_async(
                queues=[queue_name],
                concurrency=concurrency,
                wait=True,
            )
    else:
        for round_number in range(1, drain_rounds + 1):
            pending_before = _pending_jobs_count(queue_name)
            if pending_before == 0:
                logger.info(f"Queue {queue_name} is empty before round {round_number}")
                break

            logger.info(f"Drain round {round_number}/{drain_rounds} for queue {queue_name} (pending={pending_before})")

            async with app.open_async():
                await app.run_worker_async(
                    queues=[queue_name],
                    concurrency=concurrency,
                    wait=False,
                )

            pending_after = _pending_jobs_count(queue_name)
            if pending_after == 0:
                logger.info(f"Queue {queue_name} drained after round {round_number}")
                break

        else:
            logger.warning(
                f"Queue {queue_name} still has pending jobs after {drain_rounds} rounds; consider rerunning the worker"
            )

        # subtract before counts from after counts
        status_counts_after = _queue_status_counts(queue_name)
        status_counts = status_counts_after - status_counts_before

        logger.info(
            f"Queue {queue_name} final status counts: todo={status_counts['todo']} "
            f"doing={status_counts['doing']} succeeded={status_counts['succeeded']} failed={status_counts['failed']} "
            f"cancelled={status_counts['cancelled']} aborted={status_counts['aborted']}"
        )

    logger.info(f"Worker finished: queue={queue_name}")


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
        default=2,
        help="Maximum drain passes when --wait is not used (default: 2)",
    )
    parser.add_argument(
        "--stale-worker-seconds",
        type=int,
        default=600,
        help="Heartbeat age threshold to detect stalled jobs (default: 600)",
    )
    parser.add_argument(
        "--tee",
        type=Path,
        default=None,
        help="Optional path to mirror stdout/stderr logs while preserving console output",
    )

    args = parser.parse_args()

    def _run_worker() -> None:
        asyncio.run(
            process_queue(
                args.queue,
                args.concurrency,
                args.wait,
                args.drain_rounds,
                args.stale_worker_seconds,
            )
        )

    _run_with_optional_tee(args.tee, _run_worker)


if __name__ == "__main__":
    main()
