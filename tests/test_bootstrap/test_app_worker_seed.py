"""
Unit tests for ``pg_atlas.procrastinate.app``, ``worker``, and ``seed``.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import os
from unittest.mock import AsyncMock, patch

import pytest

from pg_atlas.procrastinate.app import _get_database_url

# ===================================================================
# _get_database_url
# ===================================================================


class TestGetDatabaseUrl:
    """Tests for DSN normalisation in ``_get_database_url``."""

    def test_plain_postgresql(self) -> None:
        with patch.dict(os.environ, {"PG_ATLAS_DATABASE_URL": "postgresql://atlas:pw@localhost:5432/pg_atlas"}):
            assert _get_database_url() == "postgresql://atlas:pw@localhost:5432/pg_atlas"

    def test_postgres_prefix_normalised(self) -> None:
        with patch.dict(os.environ, {"PG_ATLAS_DATABASE_URL": "postgres://atlas:pw@host/db"}):
            assert _get_database_url() == "postgresql://atlas:pw@host/db"

    def test_query_params_stripped(self) -> None:
        with patch.dict(os.environ, {"PG_ATLAS_DATABASE_URL": "postgresql://atlas:pw@host/db?sslmode=require"}):
            assert _get_database_url() == "postgresql://atlas:pw@host/db"

    def test_missing_raises(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            # Remove PG_ATLAS_DATABASE_URL if it exists.
            os.environ.pop("PG_ATLAS_DATABASE_URL", None)
            with pytest.raises(ValueError, match="PG_ATLAS_DATABASE_URL must be set"):
                _get_database_url()

    def test_empty_raises(self) -> None:
        with patch.dict(os.environ, {"PG_ATLAS_DATABASE_URL": ""}):
            with pytest.raises(ValueError, match="PG_ATLAS_DATABASE_URL must be set"):
                _get_database_url()


# ===================================================================
# worker CLI
# ===================================================================


class TestWorkerCLI:
    """Tests for ``pg_atlas.procrastinate.worker.main`` argument parsing."""

    def test_missing_queue_exits(self) -> None:
        """--queue is required."""
        from pg_atlas.procrastinate.worker import main

        with patch("sys.argv", ["worker"]):
            with pytest.raises(SystemExit):
                main()

    def test_default_concurrency(self) -> None:
        """Default concurrency is 4."""

        from pg_atlas.procrastinate.worker import main

        with (
            patch("sys.argv", ["worker", "--queue=opengrants"]),
            patch("pg_atlas.procrastinate.worker.asyncio") as mock_asyncio,
        ):
            main()

            # asyncio.run was called with process_queue(queue, concurrency, wait)
            mock_asyncio.run.assert_called_once()


# ===================================================================
# seed
# ===================================================================


class TestSeed:
    """Tests for ``pg_atlas.procrastinate.seed``."""

    async def test_seed_defers_sync_opengrants(self) -> None:
        """Verify seed() opens app and defers sync_opengrants."""
        from pg_atlas.procrastinate.seed import seed

        with patch("pg_atlas.procrastinate.seed.app") as mock_app:
            # Make open_async a context manager mock.
            mock_ctx = AsyncMock()
            mock_app.open_async.return_value = mock_ctx

            with patch("pg_atlas.procrastinate.tasks.sync_opengrants") as mock_task:
                mock_task.defer_async = AsyncMock(return_value=123)

                await seed()

                mock_task.defer_async.assert_called_once()
