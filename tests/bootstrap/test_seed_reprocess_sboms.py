"""
Unit tests for ``pg_atlas.procrastinate.seed_reprocess_sboms``.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import importlib
import os

import pytest_mock
from sqlalchemy.dialects import postgresql

from pg_atlas.db_models.base import SubmissionStatus

# We only need PG_ATLAS_DATABASE_URL during import-time collection for this module.
# Set it temporarily so the module import can proceed, then restore the original env.
_original_pg_atlas_database_url = os.environ.get("PG_ATLAS_DATABASE_URL")
if not _original_pg_atlas_database_url:
    os.environ["PG_ATLAS_DATABASE_URL"] = "postgresql://not:real@localhost:5432/relies_on_mocking"

try:
    _seed_reprocess_module = importlib.import_module("pg_atlas.procrastinate.seed_reprocess_sboms")
finally:
    if not _original_pg_atlas_database_url:
        if _original_pg_atlas_database_url is None:
            del os.environ["PG_ATLAS_DATABASE_URL"]
        else:
            os.environ["PG_ATLAS_DATABASE_URL"] = _original_pg_atlas_database_url

_N_MOST_RECENT = _seed_reprocess_module._N_MOST_RECENT
seed_reprocess_failed_sboms = _seed_reprocess_module.seed_reprocess_failed_sboms


async def test_seed_reprocess_sboms_defers_recent_failed_matches(
    mocker: pytest_mock.MockerFixture,
) -> None:
    mark_mock = mocker.patch(
        "pg_atlas.procrastinate.seed_reprocess_sboms.mark_stalled_jobs_failed",
        new=mocker.AsyncMock(return_value=0),
    )

    session = mocker.AsyncMock()
    scalar_result = mocker.Mock()
    scalar_result.all.return_value = [42, 41]
    session.scalars = mocker.AsyncMock(return_value=scalar_result)

    session_context = mocker.AsyncMock()
    session_context.__aenter__.return_value = session

    factory = mocker.Mock(return_value=session_context)
    mocker.patch(
        "pg_atlas.procrastinate.seed_reprocess_sboms.get_session_factory",
        return_value=factory,
    )

    defer_mock = mocker.AsyncMock(side_effect=[True, False])
    mocker.patch(
        "pg_atlas.procrastinate.seed_reprocess_sboms.defer_sbom_processing",
        new=defer_mock,
    )

    await seed_reprocess_failed_sboms()

    mark_mock.assert_awaited_once_with(queue_name="sbom")

    stmt = session.scalars.call_args.args[0]
    sql = str(
        stmt.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )

    assert "ORDER BY sbom_submissions.submitted_at DESC" in sql
    assert f"LIMIT {_N_MOST_RECENT}" in sql
    assert "error_detail" in sql
    assert "failed" in sql

    first_call = defer_mock.await_args_list[0]
    second_call = defer_mock.await_args_list[1]
    assert first_call.kwargs == {
        "submission_id": 42,
        "expected_status": SubmissionStatus.failed,
    }
    assert second_call.kwargs == {
        "submission_id": 41,
        "expected_status": SubmissionStatus.failed,
    }


async def test_seed_reprocess_sboms_skips_defer_when_no_matches(
    mocker: pytest_mock.MockerFixture,
) -> None:
    mark_mock = mocker.patch(
        "pg_atlas.procrastinate.seed_reprocess_sboms.mark_stalled_jobs_failed",
        new=mocker.AsyncMock(return_value=0),
    )

    session = mocker.AsyncMock()
    scalar_result = mocker.Mock()
    scalar_result.all.return_value = []
    session.scalars = mocker.AsyncMock(return_value=scalar_result)

    session_context = mocker.AsyncMock()
    session_context.__aenter__.return_value = session

    factory = mocker.Mock(return_value=session_context)
    mocker.patch(
        "pg_atlas.procrastinate.seed_reprocess_sboms.get_session_factory",
        return_value=factory,
    )

    defer_mock = mocker.AsyncMock()
    mocker.patch(
        "pg_atlas.procrastinate.seed_reprocess_sboms.defer_sbom_processing",
        new=defer_mock,
    )

    await seed_reprocess_failed_sboms()

    mark_mock.assert_awaited_once_with(queue_name="sbom")
    defer_mock.assert_not_awaited()
