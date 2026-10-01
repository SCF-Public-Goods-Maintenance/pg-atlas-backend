"""
Shared helpers for metrics tests.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, Callable
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from pg_atlas.config import settings

__all__ = ["_make_flush_guard"]


def _make_flush_guard(session: AsyncSession) -> tuple[AsyncMock, Callable[[], None]]:
    """
    Patch ``session.flush`` to raise if called, and return (mock, teardown).
    """

    original_flush = session.flush
    mock = AsyncMock(side_effect=AssertionError("flush() must not be called by bulk-DML materializers"))
    session.flush = mock  # type: ignore[assignment]

    def _restore() -> None:
        session.flush = original_flush  # type: ignore[assignment]

    return mock, _restore


@pytest.fixture
def assert_no_uow() -> Callable[[AsyncSession], None]:
    """
    Return a callable that asserts no ORM Unit-of-Work mutations occurred.

    Usage in test::

        await materialize_foo(session)
        assert_no_uow(session)

    Checks:
    1. ``session.dirty`` is empty (no tracked attribute mutations).
    2. ``session.new`` is empty (no pending inserts from ORM add).
    """

    def _check(session: AsyncSession) -> None:
        assert not session.dirty, f"session.dirty is not empty: {session.dirty}"
        assert not session.new, f"session.new is not empty: {session.new}"

    return _check


@pytest.fixture
def default_parameters(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin the parameter settings so canned payloads stay deterministic."""

    monkeypatch.setattr(settings, "MAINTENANCE_WINDOW_DAYS", 180)
    monkeypatch.setattr(settings, "MAINTENANCE_RESPONSE_INTERVAL_DAYS", 7)
    monkeypatch.setattr(settings, "MAINTENANCE_ITEM_PAGE_CAP", 10)
    monkeypatch.setattr(settings, "MAINTENANCE_REQUEST_CAP", 120)
    monkeypatch.setattr(settings, "MAINTENANCE_TIME_CAP_SECONDS", 300.0)
    monkeypatch.setattr(settings, "MAINTENANCE_RATE_LIMIT_MAX_WAIT_SECONDS", 120.0)
    monkeypatch.setattr(settings, "MAINTENANCE_INCLUDE_DRAFT_PRS", True)
    monkeypatch.setattr(settings, "MAINTENANCE_EXTERNAL_TRACKER_REPOS", "")
    monkeypatch.setattr(settings, "MAINTENANCE_HOST_REPOS", "")
    monkeypatch.setattr(settings, "MAINTENANCE_DECLARED_MAINTAINERS", "")
    monkeypatch.setattr(settings, "MAINTENANCE_DEPLOY_ON_PUSH_REPOS", "")


@pytest.fixture
async def rollback_db_session(db_session: AsyncSession) -> AsyncGenerator[AsyncSession, None]:
    """Run each maintenance materialization test inside a rolled-back transaction."""

    transaction = await db_session.begin()
    try:
        yield db_session
    finally:
        if transaction.is_active:
            await transaction.rollback()
