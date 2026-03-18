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


def _get_database_url() -> str:
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


_dsn = _get_database_url()
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
