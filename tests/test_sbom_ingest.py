"""Tests for POST /ingest/sbom.

Covers four scenarios:
1. Missing Authorization header → 401.
2. Invalid/unverifiable JWT → 403.
3. Valid token + malformed SPDX body → 422 with error detail.
4. Valid token + well-formed SPDX 2.3 body → 202 with repository and package_count.

Authentication is tested against the real OIDC dependency (async_client).
SBOM validation tests use an overridden dependency (authenticated_client) so
that the token format doesn't interfere with testing the validation logic.

Author: SCF Public Goods Maintenance <https://github.com/SCF-Public-Goods-Maintenance>
"""

from __future__ import annotations

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
    """POST /ingest/sbom with an unverifiable JWT should return 403.

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
    """POST /ingest/sbom with an invalid SPDX document should return 422.

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
    """POST /ingest/sbom with a valid SPDX 2.3 document should return 202.

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
