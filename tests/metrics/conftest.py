"""
Shared helpers for metrics tests.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from collections.abc import Callable
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

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
