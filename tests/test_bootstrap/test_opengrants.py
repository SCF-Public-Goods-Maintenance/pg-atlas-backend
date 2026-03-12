"""

Unit tests for ``pg_atlas.procrastinate.opengrants``.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import pytest
import pytest_mock

from pg_atlas.db_models.base import ActivityStatus
from pg_atlas.procrastinate.opengrants import (
    _activity_status_from_tranche,
    _build_project_metadata,
    _map_application,
    fetch_grant_applications,
    fetch_grant_pools,
    fetch_scf_projects,
    parse_github_url,
)

FIXTURES = Path(__file__).resolve().parent / "data_fixtures"


def _fixture(name: str) -> dict[str, Any]:
    return json.loads((FIXTURES / name).read_text())


def _response(data: dict[str, Any], status_code: int = 200) -> httpx.Response:
    return httpx.Response(status_code=status_code, json=data, request=httpx.Request("GET", "https://example.com"))


def test_parse_github_url_repo() -> None:
    org, repo = parse_github_url("https://github.com/StellarCN/py-stellar-base")
    assert org == "https://github.com/StellarCN"
    assert repo == "https://github.com/StellarCN/py-stellar-base"


def test_parse_github_url_invalid() -> None:
    org, repo = parse_github_url("https://gitlab.com/a/b")
    assert org is None
    assert repo is None


def test_activity_status_from_tranche() -> None:
    app = {"extensions": {"io.scf": {"io.scf.trancheCompletionPercent": 100}}}
    assert _activity_status_from_tranche(app) == ActivityStatus.live


async def test_fetch_grant_pools(mocker: pytest_mock.MockerFixture) -> None:
    fixture = _fixture("opengrants_pools.json")
    client = mocker.AsyncMock(spec=httpx.AsyncClient)
    client.get = mocker.AsyncMock(return_value=_response(fixture))

    pools = await fetch_grant_pools(client)

    assert len(pools) > 0


async def test_fetch_grant_applications(mocker: pytest_mock.MockerFixture) -> None:
    fixture = _fixture("opengrants_applications_round1.json")
    client = mocker.AsyncMock(spec=httpx.AsyncClient)
    client.get = mocker.AsyncMock(return_value=_response(fixture))

    apps = await fetch_grant_applications(client, "daoip-5:scf:grantPool:scf_#1")

    assert len(apps) > 0


def test_map_application_extracts_urls(opengrants_round30_apps: list[dict[str, Any]]) -> None:
    project = _map_application(opengrants_round30_apps[0], [])
    assert project.git_org_url is not None


def test_build_project_metadata(opengrants_round30_apps: list[dict[str, Any]]) -> None:
    meta = _build_project_metadata(opengrants_round30_apps[0], [{"round": "R", "title": "T"}])
    assert "scf_submissions" in meta


async def test_fetch_scf_projects_dedup_and_retry(
    mocker: pytest_mock.MockerFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pools_fixture = _fixture("opengrants_pools.json")
    r1_fixture = _fixture("opengrants_applications_round1.json")
    r30_fixture = _fixture("opengrants_applications_round30.json")
    rate_limited = _response({}, status_code=429)

    client = mocker.AsyncMock(spec=httpx.AsyncClient)
    client.get = mocker.AsyncMock(
        side_effect=[
            rate_limited,
            _response(pools_fixture),
            _response(r1_fixture),
            _response(r30_fixture),
            _response({"data": [], "pagination": {"hasNext": False}}),
        ]
    )
    monkeypatch.setattr("pg_atlas.procrastinate.opengrants.INITIAL_BACKOFF_S", 0.01)

    projects = await fetch_scf_projects(client)

    assert len(projects) >= 1
