"""
Shared helpers for reproducing real Postgres deadlocks in DB-backed tests.

Import from here instead of re-implementing bulk-update interception or
deadlock-outcome assertions in individual test modules.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol

from sqlalchemy.engine import Result
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Executable, text


class _BulkUpdatableEntity(Protocol):
    """ORM entity shape ``spy_bulk_update_id_order`` needs: a table name and an id column."""

    __tablename__: str


async def set_short_deadlock_timeout(session: AsyncSession, timeout: str = "50ms") -> None:
    """
    Lower ``deadlock_timeout`` for this session so deadlock tests resolve quickly.

    Only call this from tests that intentionally provoke a deadlock — it must
    not be applied session-wide, since it changes how fast Postgres reports
    ordinary lock contention as a deadlock.
    """

    await session.execute(text(f"SET deadlock_timeout = '{timeout}'"))


def fail_on_deadlock(outcomes: Sequence[Any]) -> None:
    """
    Fail the test if any concurrent ``asyncio.gather(..., return_exceptions=True)``
    outcome is a Postgres "deadlock detected" error — this is the TDD "red"
    assertion: it fails today, proving the deadlock, and turns green once
    production code stops provoking it. Any other exception is re-raised
    as-is so unrelated failures are never mistaken for the deadlock bug.
    """

    for outcome in outcomes:
        if isinstance(outcome, BaseException) and "deadlock detected" not in str(outcome).lower():
            raise outcome

    deadlocks = [outcome for outcome in outcomes if isinstance(outcome, BaseException)]
    assert not deadlocks, f"expected no deadlock, but got: {deadlocks}"


def _matches_bulk_update_table(statement: object, entity: type[_BulkUpdatableEntity]) -> bool:
    """
    Return True if ``statement`` is an ORM-enabled bulk UPDATE targeting ``entity``.
    """

    target = getattr(statement, "table", None)
    return getattr(target, "name", None) == entity.__tablename__


def spy_bulk_update_id_order(session: AsyncSession, *entities: type[_BulkUpdatableEntity]) -> dict[str, list[int]]:
    """
    Record the id order of every ORM-enabled bulk UPDATE against ``entities``.

    Returns a dict keyed by table name, populated as the session executes
    ``session.execute(update(Model), [{"id": ..., ...}, ...])`` calls — the
    exact bound parameter order sent to Postgres, which is what actually
    determines row lock acquisition order (row locks are not guaranteed to be
    acquired in any particular order otherwise). Does not cover single
    correlated-subquery/``WHERE ... NOT IN`` UPDATE statements, which have no
    per-row bound parameters to inspect.
    """

    observed: dict[str, list[int]] = {entity.__tablename__: [] for entity in entities}
    original_execute = session.execute

    async def spied_execute(statement: Executable, params: list[dict[str, Any]] | None = None, **kwargs: Any) -> Result[Any]:
        if params:
            for entity in entities:
                if _matches_bulk_update_table(statement, entity):
                    observed[entity.__tablename__].extend(row["id"] for row in params if "id" in row)
                    break
        return await original_execute(statement, params, **kwargs)

    session.execute = spied_execute  # type: ignore[method-assign,assignment]
    return observed


def assert_ascending_id_order(observed: dict[str, list[int]]) -> None:
    """
    Assert every recorded bulk-UPDATE id sequence is non-decreasing.

    A non-decreasing id sequence per table is what guarantees two concurrent
    transactions touching the same rows can only contend in one direction —
    never AB-BA. An empty sequence (no bulk UPDATE observed for that table)
    trivially passes.
    """

    for table_name, ids in observed.items():
        assert ids == sorted(ids), f"{table_name} bulk UPDATE ids not in ascending order: {ids}"
