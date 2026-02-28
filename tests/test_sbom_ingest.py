"""
Tests for POST /ingest/sbom.

Covers the following scenarios:
1. Missing Authorization header → 401.
2. Invalid/unverifiable JWT → 403.
3. Valid token + malformed SPDX body → 422 with error detail.
4. Valid token + well-formed SPDX 2.3 body → 202 with repository and package_count.
5. Valid token + GitHub Dependency Graph API response (unwrapped SPDX) → 202.
6. Valid token + GitHub Dependency Graph API response ({"sbom":…} envelope) → 202.

Scenarios 5 and 6 use a fixture captured from the real GitHub Dependency Graph API
(SCF-Public-Goods-Maintenance/pg-atlas-sbom-action). Scenario 6 reproduces the
envelope that the GitHub API returns and that the pg-atlas-sbom-action forwards
unchanged.

Authentication is tested against the real OIDC dependency (async_client).
SBOM validation tests use an overridden dependency (authenticated_client) so
that the token format doesn't interfere with testing the validation logic.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from httpx import AsyncClient
from jwt import PyJWKClientError

FIXTURES = Path(__file__).parent / "data_fixtures"


async def test_missing_auth_header_returns_401(async_client: AsyncClient) -> None:
    """POST /ingest/sbom without Authorization header should return 401."""
    response = await async_client.post(
        "/ingest/sbom",
        content=b"{}",
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 401


async def test_invalid_jwt_returns_403(async_client: AsyncClient, mocker: Any) -> None:
    """
    POST /ingest/sbom with an unverifiable JWT should return 403.

    Patches _get_jwks_client so that key selection fails without making any
    real network requests to GitHub's OIDC endpoint.
    """
    mock_jwks_client = mocker.MagicMock()
    mock_jwks_client.get_signing_key_from_jwt.side_effect = PyJWKClientError("Key not found in JWKS")
    mocker.patch("pg_atlas.auth.oidc._get_jwks_client", return_value=mock_jwks_client)

    response = await async_client.post(
        "/ingest/sbom",
        content=b"{}",
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer not.a.valid.jwt",
        },
    )
    assert response.status_code == 403


async def test_invalid_spdx_returns_422(authenticated_client: AsyncClient) -> None:
    """
    POST /ingest/sbom with an invalid SPDX document should return 422.

    The response body should include a structured error with messages from the
    SPDX parser so that callers can diagnose the problem.
    """
    invalid_sbom = (FIXTURES / "invalid.spdx.json").read_bytes()
    response = await authenticated_client.post(
        "/ingest/sbom",
        content=invalid_sbom,
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 422
    body = response.json()
    assert "error" in body["detail"]


async def test_valid_sbom_returns_202(
    authenticated_client: AsyncClient,
    mock_oidc_claims: dict[str, Any],
) -> None:
    """
    POST /ingest/sbom with a valid SPDX 2.3 document should return 202.

    The response body should contain the submitting repository identity (from
    the mocked OIDC claims) and the number of packages in the document.
    """
    valid_sbom = (FIXTURES / "valid.spdx.json").read_bytes()
    response = await authenticated_client.post(
        "/ingest/sbom",
        content=valid_sbom,
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 202
    body = response.json()
    assert body["repository"] == mock_oidc_claims["repository"]
    assert body["package_count"] == 2  # two packages in valid.spdx.json
    assert body["message"] == "queued"


async def test_github_dep_graph_sbom_returns_202(
    authenticated_client: AsyncClient,
    mock_oidc_claims: dict[str, Any],
) -> None:
    """
    POST /ingest/sbom with a real GitHub Dependency Graph SBOM (unwrapped) → 202.

    Uses a fixture captured from the GitHub Dependency Graph API for
    SCF-Public-Goods-Maintenance/pg-atlas-sbom-action. This format includes
    externalRefs with PACKAGE-MANAGER category and purl referenceType.
    """
    sbom = (FIXTURES / "github_dep_graph.spdx.json").read_bytes()
    response = await authenticated_client.post(
        "/ingest/sbom",
        content=sbom,
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 202
    body = response.json()
    assert body["repository"] == mock_oidc_claims["repository"]
    assert body["package_count"] == 2  # two packages in github_dep_graph.spdx.json
    assert body["message"] == "queued"


async def test_github_api_envelope_sbom_returns_202(
    authenticated_client: AsyncClient,
    mock_oidc_claims: dict[str, Any],
) -> None:
    """
    POST /ingest/sbom with a GitHub API response envelope ({"sbom": {...}}) → 202.

    The GitHub Dependency Graph API wraps the SPDX document in a {"sbom": …}
    envelope. The pg-atlas-sbom-action submits the raw API response, so the
    API must transparently unwrap this envelope before parsing.
    """
    inner = json.loads((FIXTURES / "github_dep_graph.spdx.json").read_bytes())
    wrapped = json.dumps({"sbom": inner}).encode()
    response = await authenticated_client.post(
        "/ingest/sbom",
        content=wrapped,
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 202
    body = response.json()
    assert body["repository"] == mock_oidc_claims["repository"]
    assert body["package_count"] == 2
    assert body["message"] == "queued"
