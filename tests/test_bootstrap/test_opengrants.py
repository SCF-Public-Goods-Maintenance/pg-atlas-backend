"""
Unit tests for ``pg_atlas.procrastinate.opengrants``.

Tests cover GitHub URL parsing, extension field extraction, activity status
derivation, application mapping, deduplication, and the full HTTP pipeline
(with mocked httpx responses).

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

import httpx

from pg_atlas.db_models.base import ActivityStatus
from pg_atlas.procrastinate.opengrants import (
    _activity_status_from_tranche,
    _build_project_metadata,
    _get_ext,
    _map_application,
    fetch_grant_applications,
    fetch_grant_pools,
    fetch_scf_projects,
    parse_github_url,
)

FIXTURES = Path(__file__).resolve().parent.parent / "data_fixtures"


def _fixture(name: str) -> dict[str, Any]:

    return json.loads((FIXTURES / name).read_text())


# ===================================================================
# parse_github_url
# ===================================================================


class TestParseGithubUrl:
    """Tests for ``parse_github_url``."""

    def test_org_and_repo(self) -> None:
        org, repo = parse_github_url("https://github.com/StellarCN/py-stellar-base")
        assert org == "https://github.com/StellarCN"
        assert repo == "https://github.com/StellarCN/py-stellar-base"

    def test_org_only(self) -> None:
        org, repo = parse_github_url("https://github.com/devasignhq")
        assert org == "https://github.com/devasignhq"
        assert repo is None

    def test_trailing_slash(self) -> None:
        org, repo = parse_github_url("https://github.com/nicholasgasior/galaxy-ramp/")
        assert org == "https://github.com/nicholasgasior"
        assert repo == "https://github.com/nicholasgasior/galaxy-ramp"

    def test_dot_git_suffix(self) -> None:
        org, repo = parse_github_url("https://github.com/StellarCN/py-stellar-base.git")
        assert org == "https://github.com/StellarCN"
        assert repo == "https://github.com/StellarCN/py-stellar-base"

    def test_http_scheme(self) -> None:
        org, repo = parse_github_url("http://github.com/foo/bar")
        assert org == "https://github.com/foo"
        assert repo == "https://github.com/foo/bar"

    def test_invalid_url_returns_none(self) -> None:
        org, repo = parse_github_url("https://gitlab.com/foo/bar")
        assert org is None
        assert repo is None

    def test_empty_string(self) -> None:
        org, repo = parse_github_url("")
        assert org is None
        assert repo is None

    def test_whitespace_stripped(self) -> None:
        org, repo = parse_github_url("  https://github.com/org/repo  ")
        assert org == "https://github.com/org"
        assert repo == "https://github.com/org/repo"

    def test_org_trailing_slash(self) -> None:
        org, repo = parse_github_url("https://github.com/coindisco/")
        assert org == "https://github.com/coindisco"
        assert repo is None


# ===================================================================
# _get_ext helper
# ===================================================================


class TestGetExt:
    """Tests for ``_get_ext``."""

    def test_existing_key(self, opengrants_round30_apps: list[dict[str, Any]]) -> None:
        app_data = opengrants_round30_apps[0]
        assert _get_ext(app_data, "io.scf.code") == "https://github.com/nicholasgasior/galaxy-ramp"

    def test_missing_key(self, opengrants_round1_apps: list[dict[str, Any]]) -> None:
        app_data = opengrants_round1_apps[0]
        assert _get_ext(app_data, "io.scf.code") is None

    def test_missing_extensions(self) -> None:
        assert _get_ext({}, "io.scf.code") is None

    def test_missing_io_scf_namespace(self) -> None:
        assert _get_ext({"extensions": {}}, "io.scf.code") is None


# ===================================================================
# _activity_status_from_tranche
# ===================================================================


class TestActivityStatusFromTranche:
    """Tests for ``_activity_status_from_tranche``."""

    def test_zero_returns_live(self, opengrants_round1_apps: list[dict[str, Any]]) -> None:
        assert _activity_status_from_tranche(opengrants_round1_apps[0]) == ActivityStatus.live

    def test_hundred_returns_in_dev(self, opengrants_round30_apps: list[dict[str, Any]]) -> None:
        assert _activity_status_from_tranche(opengrants_round30_apps[0]) == ActivityStatus.in_dev

    def test_fifty_returns_live(self, opengrants_round30_apps: list[dict[str, Any]]) -> None:
        assert _activity_status_from_tranche(opengrants_round30_apps[1]) == ActivityStatus.live

    def test_missing_returns_live(self) -> None:
        assert _activity_status_from_tranche({}) == ActivityStatus.live

    def test_non_numeric_returns_live(self) -> None:
        app_data: dict[str, Any] = {"extensions": {"io.scf": {"io.scf.trancheCompletionPercent": "not-a-number"}}}
        assert _activity_status_from_tranche(app_data) == ActivityStatus.live


# ===================================================================
# _build_project_metadata
# ===================================================================


class TestBuildProjectMetadata:
    """Tests for ``_build_project_metadata``."""

    def test_extracts_description(self, opengrants_round1_apps: list[dict[str, Any]]) -> None:
        meta = _build_project_metadata(opengrants_round1_apps[0], [])
        assert "description" in meta
        assert "Py-stellar-sdk" in meta["description"]

    def test_includes_scf_submissions(self) -> None:
        submissions = [{"round": "SCF #1", "title": "test"}]
        meta = _build_project_metadata({}, submissions)
        assert meta["scf_submissions"] == submissions

    def test_full_metadata_round30(self, opengrants_round30_apps: list[dict[str, Any]]) -> None:
        meta = _build_project_metadata(opengrants_round30_apps[0], [])
        assert meta["description"] == "Galaxy Ramp offers an onramp for any token on Stellar."
        assert meta["website"] == "https://galaxyramp.io"
        assert meta["x_profile"] == "@GalaxyRampXLM"
        assert meta["scf_category"] == "Applications"

    def test_tech_architecture(self, opengrants_round30_apps: list[dict[str, Any]]) -> None:
        meta = _build_project_metadata(opengrants_round30_apps[1], [])
        assert meta["technical_architecture"] == "FastAPI + PostgreSQL + Soroban smart contracts"


# ===================================================================
# _map_application
# ===================================================================


class TestMapApplication:
    """Tests for ``_map_application``."""

    def test_project_with_code_url(self, opengrants_round30_apps: list[dict[str, Any]]) -> None:
        proj = _map_application(opengrants_round30_apps[0], [])
        assert proj.canonical_id == "daoip-5:scf:project:coindisco"
        assert proj.display_name == "Coindisco"
        assert proj.git_org_url == "https://github.com/nicholasgasior"
        assert proj.git_repo_url == "https://github.com/nicholasgasior/galaxy-ramp"
        assert proj.activity_status == ActivityStatus.in_dev

    def test_project_without_code_url(self, opengrants_round1_apps: list[dict[str, Any]]) -> None:
        proj = _map_application(opengrants_round1_apps[0], [])
        assert proj.canonical_id == "daoip-5:scf:project:python_stellar_sdk"
        assert proj.display_name == "Python Stellar SDK"
        assert proj.git_org_url is None
        assert proj.git_repo_url is None

    def test_org_only_url(self, opengrants_round30_apps: list[dict[str, Any]]) -> None:
        proj = _map_application(opengrants_round30_apps[1], [])
        assert proj.git_org_url == "https://github.com/devasignhq"
        assert proj.git_repo_url is None

    def test_scf_submissions_preserved(self) -> None:
        app_data: dict[str, Any] = {
            "projectId": "test:proj",
            "projectName": "Test",
            "extensions": {"io.scf": {"io.scf.project": "Test"}},
        }
        submissions = [{"round": "SCF #1", "title": "Test"}]
        proj = _map_application(app_data, submissions)
        assert proj.project_metadata["scf_submissions"] == submissions


# ===================================================================
# HTTP pipeline (mocked)
# ===================================================================


def _mock_response(data: dict[str, Any]) -> httpx.Response:
    """Build an httpx.Response from JSON data."""

    return httpx.Response(
        status_code=200,
        json=data,
        request=httpx.Request("GET", "https://example.com"),
    )


class TestFetchGrantPools:
    """Tests for ``fetch_grant_pools``."""

    async def test_returns_pools(self) -> None:
        fixture = _fixture("opengrants_pools.json")
        client = AsyncMock(spec=httpx.AsyncClient)
        client.get = AsyncMock(return_value=_mock_response(fixture))

        pools = await fetch_grant_pools(client)

        assert len(pools) == 3
        assert pools[0]["id"] == "daoip-5:scf:grantPool:scf_#1"


class TestFetchGrantApplications:
    """Tests for ``fetch_grant_applications``."""

    async def test_returns_applications(self) -> None:
        fixture = _fixture("opengrants_applications_round1.json")
        client = AsyncMock(spec=httpx.AsyncClient)
        client.get = AsyncMock(return_value=_mock_response(fixture))

        apps = await fetch_grant_applications(client, "daoip-5:scf:grantPool:scf_#1")

        assert len(apps) == 2
        assert apps[0]["projectName"] == "Stellar Python SDK"

    async def test_empty_round(self) -> None:
        fixture = _fixture("opengrants_applications_round39.json")
        client = AsyncMock(spec=httpx.AsyncClient)
        client.get = AsyncMock(return_value=_mock_response(fixture))

        apps = await fetch_grant_applications(client, "daoip-5:scf:grantPool:scf_#39")

        assert apps == []


class TestFetchScfProjects:
    """Tests for ``fetch_scf_projects`` end-to-end with mocked HTTP."""

    async def test_deduplication_across_rounds(self) -> None:
        """If a project appears in multiple rounds, keep only the latest."""
        pools_fixture = _fixture("opengrants_pools.json")
        r1_fixture = _fixture("opengrants_applications_round1.json")
        r30_fixture = _fixture("opengrants_applications_round30.json")
        r39_fixture = _fixture("opengrants_applications_round39.json")

        responses = [
            _mock_response(pools_fixture),
            _mock_response(r1_fixture),
            _mock_response(r30_fixture),
            _mock_response(r39_fixture),
        ]

        client = AsyncMock(spec=httpx.AsyncClient)
        client.get = AsyncMock(side_effect=responses)

        projects = await fetch_scf_projects(client)

        # Round 1 has 2 unique projects, round 30 has 2 unique projects = 4 total.
        assert len(projects) == 4

        # Verify that python_stellar_sdk is present.
        by_id = {p.canonical_id: p for p in projects}
        assert "daoip-5:scf:project:python_stellar_sdk" in by_id

    async def test_round30_project_has_github_url(self) -> None:
        """Projects from rounds with io.scf.code get org/repo URLs."""
        pools_fixture = {"data": [{"id": "pool30", "name": "SCF #30"}], "pagination": {"hasNext": False}}
        r30_fixture = _fixture("opengrants_applications_round30.json")

        client = AsyncMock(spec=httpx.AsyncClient)
        client.get = AsyncMock(side_effect=[_mock_response(pools_fixture), _mock_response(r30_fixture)])

        projects = await fetch_scf_projects(client)
        coindisco = next(p for p in projects if "coindisco" in p.canonical_id)

        assert coindisco.git_org_url == "https://github.com/nicholasgasior"
        assert coindisco.git_repo_url == "https://github.com/nicholasgasior/galaxy-ramp"

    async def test_scf_submissions_accumulated(self) -> None:
        """When the same project appears in two rounds, both submissions are listed."""
        pools_fixture = {
            "data": [{"id": "p1", "name": "R1"}, {"id": "p2", "name": "R2"}],
            "pagination": {"hasNext": False},
        }
        app_r1: dict[str, Any] = {
            "projectId": "proj:dup",
            "projectName": "Title R1",
            "extensions": {"io.scf": {"io.scf.project": "Dup", "io.scf.round": "R1"}},
        }
        app_r2: dict[str, Any] = {
            "projectId": "proj:dup",
            "projectName": "Title R2",
            "extensions": {"io.scf": {"io.scf.project": "Dup Updated", "io.scf.round": "R2"}},
        }
        r1_resp = {"data": [app_r1], "pagination": {"hasNext": False}}
        r2_resp = {"data": [app_r2], "pagination": {"hasNext": False}}

        client = AsyncMock(spec=httpx.AsyncClient)
        client.get = AsyncMock(side_effect=[_mock_response(pools_fixture), _mock_response(r1_resp), _mock_response(r2_resp)])

        projects = await fetch_scf_projects(client)

        assert len(projects) == 1
        proj = projects[0]
        assert proj.display_name == "Dup Updated"
        assert len(proj.project_metadata["scf_submissions"]) == 2
        rounds = [s["round"] for s in proj.project_metadata["scf_submissions"]]
        assert "R1" in rounds
        assert "R2" in rounds


class TestRetryBehavior:
    """Tests for the back-off / retry logic."""

    async def test_429_retried(self) -> None:
        """Verify HTTP 429 is retried and ultimately succeeds."""
        fixture = _fixture("opengrants_pools.json")
        rate_limited = httpx.Response(
            status_code=429,
            request=httpx.Request("GET", "https://example.com"),
        )
        ok = _mock_response(fixture)

        client = AsyncMock(spec=httpx.AsyncClient)
        client.get = AsyncMock(side_effect=[rate_limited, ok])

        with patch("pg_atlas.procrastinate.opengrants.INITIAL_BACKOFF_S", 0.01):
            pools = await fetch_grant_pools(client)

        assert len(pools) == 3
