"""
Unit tests for ``pg_atlas.procrastinate.app``, ``worker``, and ``seed``.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import datetime as dt
from collections import Counter

import pytest
import pytest_mock

try:
    from pg_atlas.procrastinate.app import get_database_url
except ValueError:
    pytest.skip("PG_ATLAS_DATABASE_URL intentionally not set for CI tests", allow_module_level=True)


def test_get_database_url_plain(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PG_ATLAS_DATABASE_URL", "postgresql://atlas:pw@localhost:5432/pg_atlas")
    assert get_database_url() == "postgresql://atlas:pw@localhost:5432/pg_atlas"


def test_get_database_url_normalizes_postgres(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PG_ATLAS_DATABASE_URL", "postgres://atlas:pw@host/db")
    assert get_database_url() == "postgresql://atlas:pw@host/db"


def test_get_database_url_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PG_ATLAS_DATABASE_URL", raising=False)
    with pytest.raises(ValueError, match="PG_ATLAS_DATABASE_URL must be set"):
        get_database_url()


def test_worker_main_requires_queue(monkeypatch: pytest.MonkeyPatch) -> None:
    from pg_atlas.procrastinate.worker import main

    monkeypatch.setattr("sys.argv", ["worker"])
    with pytest.raises(SystemExit):
        main()


def test_worker_main_runs(monkeypatch: pytest.MonkeyPatch, mocker: pytest_mock.MockerFixture) -> None:
    from pg_atlas.procrastinate.worker import main

    monkeypatch.setattr("sys.argv", ["worker", "--queue=opengrants"])
    run_mock = mocker.patch("pg_atlas.procrastinate.worker.asyncio.run")

    main()

    run_mock.assert_called_once()
    run_arg = run_mock.call_args.args[0]
    run_arg.close()


async def test_process_queue_marks_stalled_jobs_failed(mocker: pytest_mock.MockerFixture) -> None:
    from pg_atlas.procrastinate import worker

    mark_mock = mocker.patch(
        "pg_atlas.procrastinate.worker.mark_stalled_jobs_failed",
        new=mocker.AsyncMock(return_value=2),
    )
    mocker.patch("pg_atlas.procrastinate.worker._pending_jobs_count", side_effect=[0])
    mocker.patch(
        "pg_atlas.procrastinate.worker._queue_status_counts",
        return_value=Counter(),
    )

    await worker.process_queue(
        queue_name="opengrants",
        concurrency=4,
        wait=False,
        drain_rounds=5,
        stale_worker_seconds=600,
    )

    mark_mock.assert_awaited_once_with("opengrants", 600)


async def test_process_queue_marks_stalled_jobs_failed_with_wait(mocker: pytest_mock.MockerFixture) -> None:
    from pg_atlas.procrastinate import worker

    mark_mock = mocker.patch(
        "pg_atlas.procrastinate.worker.mark_stalled_jobs_failed",
        new=mocker.AsyncMock(return_value=0),
    )
    app_mock = mocker.patch("pg_atlas.procrastinate.worker.app")
    ctx = mocker.AsyncMock()
    app_mock.open_async.return_value = ctx
    app_mock.run_worker_async = mocker.AsyncMock()
    mocker.patch(
        "pg_atlas.procrastinate.worker._queue_status_counts",
        return_value=Counter(),
    )

    await worker.process_queue(
        queue_name="opengrants",
        concurrency=4,
        wait=True,
        drain_rounds=5,
        stale_worker_seconds=600,
    )

    mark_mock.assert_awaited_once_with("opengrants", 600)


async def test_seed_defers_sync_opengrants(mocker: pytest_mock.MockerFixture) -> None:
    from pg_atlas.procrastinate.seed_opengrants import seed

    mark_mock = mocker.patch(
        "pg_atlas.procrastinate.seed_opengrants.mark_stalled_jobs_failed",
        new=mocker.AsyncMock(return_value=0),
    )
    app_mock = mocker.patch("pg_atlas.procrastinate.seed_opengrants.app")
    ctx = mocker.AsyncMock()
    app_mock.open_async.return_value = ctx

    task_mock = mocker.patch("pg_atlas.procrastinate.tasks.sync_opengrants")
    task_mock.defer_async = mocker.AsyncMock(return_value=1)

    await seed()

    mark_mock.assert_awaited_once_with(queue_name="opengrants")
    task_mock.defer_async.assert_called_once()


async def test_seed_gitlog_defers_batches(mocker: pytest_mock.MockerFixture) -> None:
    from pg_atlas.procrastinate.seed_gitlog import seed_gitlog_batches

    mark_mock = mocker.patch(
        "pg_atlas.procrastinate.seed_gitlog.mark_stalled_jobs_failed",
        new=mocker.AsyncMock(return_value=0),
    )
    app_mock = mocker.patch("pg_atlas.procrastinate.seed_gitlog.app")
    ctx = mocker.AsyncMock()
    app_mock.open_async.return_value = ctx

    session = mocker.AsyncMock()
    session_factory = mocker.Mock()
    session_factory.return_value.__aenter__ = mocker.AsyncMock(return_value=session)
    session_factory.return_value.__aexit__ = mocker.AsyncMock(return_value=None)
    mocker.patch("pg_atlas.procrastinate.seed_gitlog.get_session_factory", return_value=session_factory)

    now = dt.datetime.now(dt.UTC)
    mocker.patch(
        "pg_atlas.procrastinate.seed_gitlog._next_seed_run_ordinal",
        new=mocker.AsyncMock(return_value=7),
    )
    mocker.patch(
        "pg_atlas.procrastinate.seed_gitlog._load_candidate_repos",
        new=mocker.AsyncMock(return_value=[(1, now), (2, now - dt.timedelta(days=250)), (3, None)]),
    )
    mocker.patch(
        "pg_atlas.procrastinate.seed_gitlog._load_last_successful_seed_runs",
        new=mocker.AsyncMock(return_value={2: 5, 3: 1}),
    )
    mocker.patch(
        "pg_atlas.procrastinate.seed_gitlog._compute_dormant_cadences",
        return_value={2: 3, 3: 9},
    )

    defer_mock = mocker.patch("pg_atlas.procrastinate.tasks.defer_with_lock", new=mocker.AsyncMock(return_value=True))

    await seed_gitlog_batches()

    mark_mock.assert_awaited_once_with(queue_name="gitlog")
    assert defer_mock.call_count == 1
    assert defer_mock.await_count == 1
    assert defer_mock.await_args_list[0].kwargs["seed_run_ordinal"] == 7
    assert defer_mock.await_args_list[0].kwargs["repo_ids"] == [1]
