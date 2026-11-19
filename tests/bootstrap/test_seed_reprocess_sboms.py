"""
Unit tests for ``pg_atlas.procrastinate.seed_reprocess_sboms``.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import importlib
import os
import sys

import pytest
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
seed_reprocess_sboms_by_id = _seed_reprocess_module.seed_reprocess_sboms_by_id
_parse_args = _seed_reprocess_module._parse_args


async def test_seed_reprocess_sboms_defers_recent_failed_matches(
    mocker: pytest_mock.MockerFixture,
) -> None:
    mark_mock = mocker.patch(
        "pg_atlas.procrastinate.seed_reprocess_sboms.mark_stalled_jobs_failed",
        new=mocker.AsyncMock(return_value=0),
    )

    session = mocker.AsyncMock()
    execute_result = mocker.Mock()
    execute_result.all.return_value = [
        (42, "test-org/test-repo-42"),
        (41, "test-org/test-repo-41"),
    ]
    session.execute = mocker.AsyncMock(return_value=execute_result)

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

    stmt = session.execute.call_args.args[0]
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
        "repository_claim": "test-org/test-repo-42",
        "expected_status": SubmissionStatus.failed,
    }
    assert second_call.kwargs == {
        "submission_id": 41,
        "repository_claim": "test-org/test-repo-41",
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
    execute_result = mocker.Mock()
    execute_result.all.return_value = []
    session.execute = mocker.AsyncMock(return_value=execute_result)

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


async def test_seed_reprocess_sboms_by_id_defers_matching_rows(
    mocker: pytest_mock.MockerFixture,
) -> None:
    mark_mock = mocker.patch(
        "pg_atlas.procrastinate.seed_reprocess_sboms.mark_stalled_jobs_failed",
        new=mocker.AsyncMock(return_value=0),
    )

    session = mocker.AsyncMock()
    execute_result = mocker.Mock()
    execute_result.all.return_value = [
        (42, "test-org/test-repo-42"),
        (99, "test-org/test-repo-99"),
    ]
    session.execute = mocker.AsyncMock(return_value=execute_result)

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

    await seed_reprocess_sboms_by_id(
        submission_ids=[42, 43],
        expected_status=SubmissionStatus.pending,
    )

    mark_mock.assert_awaited_once_with(queue_name="sbom")
    assert defer_mock.await_args_list[0].kwargs == {
        "submission_id": 42,
        "repository_claim": "test-org/test-repo-42",
        "expected_status": SubmissionStatus.pending,
    }


async def test_seed_reprocess_sboms_by_id_skips_missing_rows(
    mocker: pytest_mock.MockerFixture,
) -> None:
    mocker.patch(
        "pg_atlas.procrastinate.seed_reprocess_sboms.mark_stalled_jobs_failed",
        new=mocker.AsyncMock(return_value=0),
    )

    session = mocker.AsyncMock()
    execute_result = mocker.Mock()
    execute_result.all.return_value = []
    session.execute = mocker.AsyncMock(return_value=execute_result)

    session_context = mocker.AsyncMock()
    session_context.__aenter__.return_value = session
    mocker.patch(
        "pg_atlas.procrastinate.seed_reprocess_sboms.get_session_factory",
        return_value=mocker.Mock(return_value=session_context),
    )

    defer_mock = mocker.AsyncMock()
    mocker.patch(
        "pg_atlas.procrastinate.seed_reprocess_sboms.defer_sbom_processing",
        new=defer_mock,
    )

    await seed_reprocess_sboms_by_id([404])

    defer_mock.assert_not_awaited()


def test_parse_args_defaults_to_failed_status(
    mocker: pytest_mock.MockerFixture,
) -> None:
    mocker.patch.object(sys, "argv", ["seed_reprocess_sboms.py", "42", "43"])

    args = _parse_args()

    assert args.submission_ids == [42, 43]
    assert args.expected_status is SubmissionStatus.failed


def test_parse_args_accepts_explicit_status(
    mocker: pytest_mock.MockerFixture,
) -> None:
    mocker.patch.object(
        sys,
        "argv",
        ["seed_reprocess_sboms.py", "42", "--expected-status", "pending"],
    )

    args = _parse_args()

    assert args.submission_ids == [42]
    assert args.expected_status is SubmissionStatus.pending


def test_parse_args_rejects_non_failed_status_without_ids(
    mocker: pytest_mock.MockerFixture,
) -> None:
    mocker.patch.object(
        sys,
        "argv",
        ["seed_reprocess_sboms.py", "--expected-status", "pending"],
    )

    with pytest.raises(SystemExit, match="2"):
        _parse_args()
