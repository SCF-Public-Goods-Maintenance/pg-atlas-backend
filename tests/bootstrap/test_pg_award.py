"""
Tests for the ``Project.pg_award`` reconciliation and the offline CLI's lookup.

The reconciliation core and ``missing_pg_award_projects`` run against the test
database in a session that never commits, so closing it rolls the work back.
The committing wrapper is tested with mocks.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import datetime as dt
from typing import Any
from uuid import uuid4

from pytest_mock import MockerFixture
from sqlalchemy import event, func, select, update
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from pg_atlas.db_models.base import ActivityStatus, ProjectType
from pg_atlas.db_models.project import Project
from pg_atlas.procrastinate.pg_award import missing_pg_award_projects
from pg_atlas.procrastinate.upserts import reconcile_pg_award, reconcile_pg_award_in_session

_SEEDED_UPDATED_AT = dt.datetime(2020, 1, 1, tzinfo=dt.UTC)


async def _seed_projects(session: AsyncSession) -> dict[str, tuple[int, str]]:
    """
    Seed one project per combination of stored flag and target membership.

    Pre-existing projects are set to ``pg_award = false`` first, so they are
    unchanged by a reconciliation whose set holds only seeded keys. Every seeded
    row carries ``_SEEDED_UPDATED_AT``. Returns ``name -> (id, canonical_id)``
    for ``set_true`` (stored false, in the set), ``set_false`` (stored true, not
    in the set), ``keep_true`` (stored true, in the set) and ``keep_false``
    (stored false, not in the set).
    """

    await session.execute(update(Project).values(pg_award=False))

    suffix = uuid4().hex[:8]
    stored_values = {"set_true": False, "set_false": True, "keep_true": True, "keep_false": False}
    projects = {
        name: Project(
            canonical_id=f"daoip-5:scf:project:pg-award-{name}-{suffix}",
            display_name=f"PG Award {name}",
            project_type=ProjectType.scf_project,
            activity_status=ActivityStatus.live,
            pg_award=stored,
        )
        for name, stored in stored_values.items()
    }
    session.add_all(projects.values())
    await session.flush()

    await session.execute(
        update(Project)
        .where(Project.id.in_([project.id for project in projects.values()]))
        .values(updated_at=_SEEDED_UPDATED_AT)
        .execution_options(synchronize_session=False)
    )

    return {name: (project.id, project.canonical_id) for name, project in projects.items()}


async def _stored_state(session: AsyncSession, project_ids: list[int]) -> dict[int, tuple[bool, dt.datetime]]:
    rows = (
        await session.execute(select(Project.id, Project.pg_award, Project.updated_at).where(Project.id.in_(project_ids)))
    ).all()

    return {project_id: (pg_award, updated_at) for project_id, pg_award, updated_at in rows}


async def test_reconcile_sets_and_clears_flags_with_one_locking_select(
    db_session: AsyncSession, db_engine: AsyncEngine
) -> None:
    seeded = await _seed_projects(db_session)
    pg_award_canonical_ids = {seeded["set_true"][1], seeded["keep_true"][1]}

    statements: list[str] = []

    def _record_statement(_conn: Any, _cursor: Any, statement: str, *_rest: Any) -> None:
        statements.append(" ".join(statement.split()))

    event.listen(db_engine.sync_engine, "before_cursor_execute", _record_statement)
    try:
        counts = await reconcile_pg_award_in_session(db_session, pg_award_canonical_ids)
    finally:
        event.remove(db_engine.sync_engine, "before_cursor_execute", _record_statement)

    state = await _stored_state(db_session, [project_id for project_id, _ in seeded.values()])
    assert state[seeded["keep_true"][0]][1] == _SEEDED_UPDATED_AT
    assert state[seeded["keep_false"][0]][1] == _SEEDED_UPDATED_AT
    assert state[seeded["set_true"][0]][1] != _SEEDED_UPDATED_AT
    assert state[seeded["set_false"][0]][1] != _SEEDED_UPDATED_AT
    assert {name: state[project_id][0] for name, (project_id, _) in seeded.items()} == {
        "set_true": True,
        "set_false": False,
        "keep_true": True,
        "keep_false": False,
    }
    assert counts == (1, 1)

    locking_selects = [statement for statement in statements if "FOR UPDATE" in statement]
    assert len(locking_selects) == 1
    locking_select = locking_selects[0]
    assert locking_select.startswith("SELECT projects.id")
    assert "WHERE projects.pg_award != (projects.canonical_id IN (" in locking_select
    assert locking_select.endswith("ORDER BY projects.id FOR UPDATE")


async def test_reconcile_with_an_empty_set_clears_every_flag(db_session: AsyncSession) -> None:
    await _seed_projects(db_session)

    counts = await reconcile_pg_award_in_session(db_session, set())

    remaining_true = (await db_session.execute(select(func.count()).select_from(Project).where(Project.pg_award))).scalar_one()
    assert remaining_true == 0
    assert counts == (0, 2)


async def test_missing_pg_award_projects_returns_sorted_keys_without_a_row(db_session: AsyncSession) -> None:
    seeded = await _seed_projects(db_session)
    suffix = uuid4().hex[:8]
    missing_zeta = f"daoip-5:scf:project:pg-award-zeta-{suffix}"
    missing_alpha = f"daoip-5:scf:project:pg-award-alpha-{suffix}"

    missing = await missing_pg_award_projects(
        db_session,
        {missing_zeta, seeded["keep_true"][1], missing_alpha, seeded["set_false"][1]},
    )

    assert missing == [missing_alpha, missing_zeta]


async def test_reconcile_pg_award_commits_and_closes_its_own_session(mocker: MockerFixture) -> None:
    session = mocker.AsyncMock(spec=AsyncSession)
    mocker.patch("pg_atlas.procrastinate.upserts._session", new=mocker.AsyncMock(return_value=session))
    core = mocker.patch(
        "pg_atlas.procrastinate.upserts.reconcile_pg_award_in_session",
        new=mocker.AsyncMock(return_value=(2, 1)),
    )
    pg_award_canonical_ids = {"daoip-5:scf:project:one", "daoip-5:scf:project:two"}

    counts = await reconcile_pg_award(pg_award_canonical_ids)

    assert counts == (2, 1)
    core.assert_awaited_once_with(session, pg_award_canonical_ids)
    session.commit.assert_awaited_once()
    session.close.assert_awaited_once()
    session.rollback.assert_not_awaited()
