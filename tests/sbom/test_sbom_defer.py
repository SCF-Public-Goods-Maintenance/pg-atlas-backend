"""
Tests for ``pg_atlas.ingestion.queue``.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

from pg_atlas.db_models.base import SubmissionStatus
from pg_atlas.ingestion.queue import defer_sbom_processing


class _FakeOpenAsyncContext:
    """
    Minimal async context manager returned by ``app.open_async()``.
    """

    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None


class _FakeApp:
    """
    Minimal Procrastinate app stub for queue handoff tests.
    """

    def open_async(self) -> _FakeOpenAsyncContext:
        return _FakeOpenAsyncContext()


async def test_defer_sbom_processing_locks_by_repository_claim(monkeypatch: object) -> None:
    """
    Queue locking must key on repository claim rather than submission id.
    """

    fake_task = object()
    defer_with_lock = AsyncMock(return_value=True)

    monkeypatch.setitem(
        sys.modules,
        "pg_atlas.procrastinate.app",
        SimpleNamespace(app=_FakeApp()),
    )
    monkeypatch.setitem(
        sys.modules,
        "pg_atlas.procrastinate.tasks",
        SimpleNamespace(
            defer_with_lock=defer_with_lock,
            process_sbom_submission=fake_task,
        ),
    )

    enqueued = await defer_sbom_processing(
        submission_id=42,
        repository_claim="test-org/test-repo",
    )

    assert enqueued is True
    defer_with_lock.assert_awaited_once_with(
        fake_task,
        queueing_lock="sbom:test-org/test-repo",
        submission_id=42,
        expected_status=SubmissionStatus.pending.value,
    )
