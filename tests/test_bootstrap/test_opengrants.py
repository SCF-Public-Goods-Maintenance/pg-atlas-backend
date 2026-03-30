"""

Unit tests for ``pg_atlas.procrastinate.opengrants``.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any

import httpx
import pytest
import pytest_mock

from pg_atlas.db_models.base import ActivityStatus
from pg_atlas.procrastinate.opengrants import (
    _activity_status_from_integration_status,
    _activity_status_from_tranche,
    _build_project_metadata,
    _check_project_completion,
    _extract_github_from_socials,
    _map_application,
    _merge_project_and_applications,
    _retry_delay_from_headers,
    fetch_grant_applications,
    fetch_grant_pools,
    fetch_scf_projects,
    parse_github_url,
)

FIXTURES = Path(__file__).resolve().parent / "data_fixtures"


def _fixture(name: str) -> dict[str, Any]:
    path = FIXTURES / name
    if name.endswith(".gz"):
        with gzip.open(path, "rt", encoding="utf-8") as f:
            return json.load(f)  # type: ignore[no-any-return]

    return json.loads(path.read_text())  # type: ignore[no-any-return]


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


# ---------------------------------------------------------------------------
# _activity_status_from_tranche
# ---------------------------------------------------------------------------


def test_activity_status_from_tranche() -> None:
    app: dict[str, Any] = {
        "extensions": {
            "org.stellar.communityfund": {
                "org.stellar.communityfund.trancheCompletionPercent": 100,
            },
        },
    }
    assert _activity_status_from_tranche(app) == ActivityStatus.live


def test_activity_status_from_tranche_missing() -> None:
    assert _activity_status_from_tranche({}) == ActivityStatus.in_dev


# ---------------------------------------------------------------------------
# _activity_status_from_integration_status
# ---------------------------------------------------------------------------


def test_activity_status_from_integration_status_mainnet() -> None:
    assert _activity_status_from_integration_status("Mainnet") == ActivityStatus.live


def test_activity_status_from_integration_status_abandoned() -> None:
    assert _activity_status_from_integration_status("Abandoned") == ActivityStatus.discontinued


def test_activity_status_from_integration_status_development() -> None:
    assert _activity_status_from_integration_status("Development") == ActivityStatus.in_dev


def test_activity_status_from_integration_status_unknown() -> None:
    assert _activity_status_from_integration_status("Unknown") == ActivityStatus.non_responsive


def test_activity_status_from_integration_status_empty() -> None:
    assert _activity_status_from_integration_status(None) == ActivityStatus.in_dev
    assert _activity_status_from_integration_status("") == ActivityStatus.in_dev


# ---------------------------------------------------------------------------
# _extract_github_from_socials
# ---------------------------------------------------------------------------


def test_extract_github_from_socials_found() -> None:
    socials: list[dict[str, Any]] = [
        {"name": "X", "value": "https://x.com/example"},
        {"name": "GitHub", "value": "https://github.com/stellar/go"},
    ]
    org, repo = _extract_github_from_socials(socials)
    assert org == "https://github.com/stellar"
    assert repo == "https://github.com/stellar/go"


def test_extract_github_from_socials_none() -> None:
    socials: list[dict[str, Any]] = [{"name": "X", "value": "https://x.com/example"}]
    org, repo = _extract_github_from_socials(socials)
    assert org is None
    assert repo is None


# ---------------------------------------------------------------------------
# _check_project_completion
# ---------------------------------------------------------------------------


def test_check_project_completion_warns_on_incomplete(caplog: pytest.LogCaptureFixture) -> None:
    _check_project_completion("proj:test", ActivityStatus.live, 10000.0, 5000.0)
    assert "project_completion=0.50" in caplog.text


def test_check_project_completion_no_warn_when_not_live(caplog: pytest.LogCaptureFixture) -> None:
    _check_project_completion("proj:test", ActivityStatus.in_dev, 10000.0, 5000.0)
    assert caplog.text == ""


# ---------------------------------------------------------------------------
# _retry_delay_from_headers
# ---------------------------------------------------------------------------


def test_retry_delay_from_reset_header() -> None:
    headers = httpx.Headers({"x-ratelimit-reset": "2099-01-01T00:00:00.000Z"})
    delay = _retry_delay_from_headers(headers, fallback=5.0)
    assert delay > 100  # Far-future timestamp


def test_retry_delay_from_retry_after() -> None:
    headers = httpx.Headers({"Retry-After": "3"})
    delay = _retry_delay_from_headers(headers, fallback=5.0)
    assert delay == 3.0


def test_retry_delay_fallback() -> None:
    headers = httpx.Headers({})
    delay = _retry_delay_from_headers(headers, fallback=7.0)
    assert delay == 7.0


# ---------------------------------------------------------------------------
# fetch_grant_pools / fetch_grant_applications
# ---------------------------------------------------------------------------


@pytest.mark.slow
async def test_fetch_grant_pools(mocker: pytest_mock.MockerFixture) -> None:
    fixture = _fixture("opengrants_pools.json.gz")
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


# ---------------------------------------------------------------------------
# _map_application
# ---------------------------------------------------------------------------


@pytest.mark.slow
def test_map_application_extracts_urls(opengrants_round30_apps: list[dict[str, Any]]) -> None:
    project = _map_application(opengrants_round30_apps[0], [])
    assert project.git_org_url is not None


# ---------------------------------------------------------------------------
# _build_project_metadata
# ---------------------------------------------------------------------------


@pytest.mark.slow
def test_build_project_metadata(opengrants_round30_apps: list[dict[str, Any]]) -> None:
    meta = _build_project_metadata(
        project=None,
        latest_app=opengrants_round30_apps[0],
        scf_submissions=[{"round": "R", "title": "T"}],
    )
    assert "scf_submissions" in meta


def test_build_project_metadata_project_only() -> None:
    project: dict[str, Any] = {
        "description": "A test project",
        "socials": [{"name": "GitHub", "value": "https://github.com/acme"}],
        "extensions": {
            "org.stellar.communityfund": {
                "org.stellar.communityfund.totalAwardedUSD": 50000,
                "org.stellar.communityfund.totalPaidUSD": 50000,
                "org.stellar.communityfund.openSource": True,
            },
        },
    }
    meta = _build_project_metadata(project=project, latest_app=None, scf_submissions=[])

    assert meta["description"] == "A test project"
    assert meta["total_awarded_usd"] == 50000
    assert meta["open_source"] is True


# ---------------------------------------------------------------------------
# _merge_project_and_applications
# ---------------------------------------------------------------------------


def test_merge_project_and_applications() -> None:
    project: dict[str, Any] = {
        "id": "daoip-5:scf:project:test",
        "name": "Test Project",
        "socials": [{"name": "GitHub", "value": "https://github.com/test-org/test-repo"}],
        "extensions": {
            "org.stellar.communityfund": {
                "org.stellar.communityfund.integrationStatus": "Mainnet",
                "org.stellar.communityfund.category": "Developer Tooling",
                "org.stellar.communityfund.totalAwardedUSD": 25000,
                "org.stellar.communityfund.totalPaidUSD": 25000,
            },
        },
    }
    app: dict[str, Any] = {
        "extensions": {
            "org.stellar.communityfund": {
                "org.stellar.communityfund.code": "https://github.com/test-org/test-repo",
                "org.stellar.communityfund.oneSentenceDescription": "A test project description",
            },
        },
    }
    result = _merge_project_and_applications(project, app, [{"round": "R30", "title": "Test"}])

    assert result.canonical_id == "daoip-5:scf:project:test"
    assert result.display_name == "Test Project"
    assert result.activity_status == ActivityStatus.live
    assert result.git_org_url == "https://github.com/test-org"
    assert result.git_repo_url == "https://github.com/test-org/test-repo"
    assert result.category == "Developer Tooling"
    assert result.project_metadata["description"] == "A test project description"


def test_merge_project_no_app_uses_socials_for_github() -> None:
    project: dict[str, Any] = {
        "id": "daoip-5:scf:project:social-only",
        "name": "Socials Only",
        "socials": [{"name": "GitHub", "value": "https://github.com/stellar/go"}],
        "extensions": {
            "org.stellar.communityfund": {
                "org.stellar.communityfund.integrationStatus": "Development",
            },
        },
    }
    result = _merge_project_and_applications(project, None, [])

    assert result.git_org_url == "https://github.com/stellar"
    assert result.git_repo_url == "https://github.com/stellar/go"
    assert result.activity_status == ActivityStatus.in_dev


# ---------------------------------------------------------------------------
# fetch_scf_projects (integration: mock HTTP layer)
# ---------------------------------------------------------------------------


@pytest.mark.slow
async def test_fetch_scf_projects_merges_projects_and_apps(
    mocker: pytest_mock.MockerFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    projects_fixture = _fixture("opengrants_projects.json.gz")
    pools_fixture = _fixture("opengrants_pools.json.gz")
    r1_fixture = _fixture("opengrants_applications_round1.json")
    rate_limited = _response({}, status_code=429)

    client = mocker.AsyncMock(spec=httpx.AsyncClient)
    client.get = mocker.AsyncMock(
        side_effect=[
            # fetch_opengrants_projects call
            _response(projects_fixture),
            # fetch_grant_pools call
            rate_limited,
            _response(pools_fixture),
            # fetch_grant_applications for each pool — we only provide round 1
            _response(r1_fixture),
            *[_response({"data": [], "pagination": {"hasNext": False}}) for _ in range(47)],
        ],
    )
    monkeypatch.setattr("pg_atlas.procrastinate.opengrants.INITIAL_BACKOFF_S", 0.01)

    results = await fetch_scf_projects(client)

    assert len(results) >= 1
    # Projects from the /projects endpoint should be present
    canonical_ids = {p.canonical_id for p in results}
    assert any(cid.startswith("daoip-5:scf:project:") for cid in canonical_ids)
    # At least some should have category set
    categories = {p.category for p in results if p.category}
    assert len(categories) > 0
