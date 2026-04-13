"""
Procrastinate application instance and connector setup.

The Procrastinate ``App`` is the central registry for task definitions and the
connection broker for PostgreSQL-backed job queuing.  It uses ``PsycopgConnector``
(psycopg 3) which coexists with the asyncpg driver used by SQLAlchemy.

The connection DSN is derived from ``PG_ATLAS_DATABASE_URL`` *before* the
SQLAlchemy ``+asyncpg`` rewrite — Procrastinate needs a plain ``postgresql://`` DSN.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import logging
import os

import procrastinate

logger = logging.getLogger(__name__)


def get_database_url() -> str:
    """
    Return a plain ``postgresql://`` DSN suitable for Procrastinate.

    Reads ``PG_ATLAS_DATABASE_URL`` directly from the environment (avoiding
    the Pydantic ``Settings`` coercion that rewrites it to ``postgresql+asyncpg://``).
    Strips any query parameters for compatibility.

    Raises:
        ValueError: If the environment variable is not set or empty.
    """
    raw_url = os.environ.get("PG_ATLAS_DATABASE_URL", "")
    if not raw_url:
        raise ValueError(
            "PG_ATLAS_DATABASE_URL must be set for the Procrastinate worker. "
            "It should be a plain postgresql:// DSN (no +asyncpg driver suffix)."
        )

    # Strip query parameters that may be present in DO App Platform DSNs.
    url = raw_url.partition("?")[0]

    # Ensure we have the plain driver prefix.
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://") :]

    return url


_dsn = get_database_url()
logger.info(f"Opening Procrastinate connection to {_dsn.split('@')[-1]}")
app = procrastinate.App(
    connector=procrastinate.PsycopgConnector(conninfo=_dsn),
    import_paths=["pg_atlas.procrastinate.tasks"],
)
"""
The Procrastinate application instance.

Task modules listed in ``import_paths`` are loaded when the app is opened,
registering all ``@app.task`` definitions.  The connector is configured
lazily: ``open_async()`` reads the DSN at that point.

Usage::

    async with app.open_async():
        await app.configure_task("sync_opengrants").defer_async()
"""


async def mark_stalled_jobs_failed(queue_name: str, stale_worker_seconds: int = 600) -> int:
    """
    Mark stalled jobs as ``failed`` for a queue.

    Stalled jobs are detected via Procrastinate's heartbeat-based stalled-jobs
    lookup and then marked failed in a single bulk update. The update mirrors
    the state transition semantics of
    ``finish_job_by_id_async(..., status=failed, delete_job=False)``.

    Args:
        queue_name: Queue to inspect for stalled ``doing`` jobs.
        stale_worker_seconds: Heartbeat age threshold used by
            ``get_stalled_jobs``.

    Returns:
        Number of jobs transitioned to ``failed``.
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
