"""

Unit tests for ``pg_atlas.procrastinate.app``, ``worker``, and ``seed``.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

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


async def test_process_queue_recovers_stale_doing_jobs(mocker: pytest_mock.MockerFixture) -> None:
    from pg_atlas.procrastinate import worker

    recover_mock = mocker.patch("pg_atlas.procrastinate.worker._recover_stale_doing_jobs", return_value=2)
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
        recover_stale_doing=True,
        stale_worker_seconds=600,
    )

    recover_mock.assert_called_once_with("opengrants", 600)


async def test_process_queue_skips_recovery_when_disabled(mocker: pytest_mock.MockerFixture) -> None:
    from pg_atlas.procrastinate import worker

    recover_mock = mocker.patch("pg_atlas.procrastinate.worker._recover_stale_doing_jobs")
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
        recover_stale_doing=False,
        stale_worker_seconds=600,
    )

    recover_mock.assert_not_called()


async def test_seed_defers_sync_opengrants(mocker: pytest_mock.MockerFixture) -> None:
    from pg_atlas.procrastinate.seed_opengrants import seed

    app_mock = mocker.patch("pg_atlas.procrastinate.seed_opengrants.app")
    ctx = mocker.AsyncMock()
    app_mock.open_async.return_value = ctx

    task_mock = mocker.patch("pg_atlas.procrastinate.tasks.sync_opengrants")
    task_mock.defer_async = mocker.AsyncMock(return_value=1)

    await seed()

    task_mock.defer_async.assert_called_once()
