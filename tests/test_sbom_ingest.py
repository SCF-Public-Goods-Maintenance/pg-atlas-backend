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
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from jwt import PyJWKClientError
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from pg_atlas.auth.oidc import verify_github_oidc_token
from pg_atlas.db_models.base import SubmissionStatus
from pg_atlas.db_models.sbom_submission import SbomSubmission
from pg_atlas.db_models.session import maybe_db_session
from pg_atlas.main import app
from tests.conftest import MOCK_OIDC_CLAIMS, get_test_database_url
from tests.db_cleanup import DB_MODELS_TABLE_SPECS, capture_snapshot, cleanup_created_rows

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


@pytest.fixture
async def authenticated_db_client() -> AsyncGenerator[tuple[AsyncClient, AsyncSession], None]:
    """
    Authenticated HTTP client backed by a real test database session.

    The route handler gets live DB sessions through ``maybe_db_session`` while
    OIDC verification is overridden to return the fixed test claims.
    """

    database_url = get_test_database_url()
    if not database_url:
        pytest.skip("PG_ATLAS_DATABASE_URL / PG_ATLAS_TEST_DATABASE_URL not set")

    engine = create_async_engine(database_url, poolclass=NullPool)
    factory: async_sessionmaker[AsyncSession] = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    async def _db_override() -> AsyncGenerator[AsyncSession, None]:
        async with factory() as session:
            yield session

    app.dependency_overrides[verify_github_oidc_token] = lambda: MOCK_OIDC_CLAIMS.copy()
    app.dependency_overrides[maybe_db_session] = _db_override

    async with factory() as seed_session:
        try:
            async with AsyncClient(
                transport=ASGITransport(app=app),
                base_url="http://test",
            ) as client:
                yield client, seed_session
        finally:
            app.dependency_overrides.clear()

    await engine.dispose()


@pytest.fixture
async def cleanup_db_rows_for_authenticated_client_tests(
    authenticated_db_client: tuple[AsyncClient, AsyncSession],
) -> AsyncGenerator[None, None]:
    """
    Remove only rows created by DB-backed SBOM ingest tests.
    """

    _, seed_session = authenticated_db_client
    snapshot = await capture_snapshot(seed_session, DB_MODELS_TABLE_SPECS)
    yield
    await cleanup_created_rows(seed_session, DB_MODELS_TABLE_SPECS, snapshot)


async def test_valid_sbom_with_db_queues_processing(
    authenticated_db_client: tuple[AsyncClient, AsyncSession],
    cleanup_db_rows_for_authenticated_client_tests: None,
    mocker: Any,
) -> None:
    """
    POST /ingest/sbom with DB configured creates a pending submission and defers one job.
    """

    client, session = authenticated_db_client
    defer_mock = mocker.AsyncMock()
    mocker.patch("pg_atlas.ingestion.persist.defer_sbom_processing", new=defer_mock)

    valid_sbom = (FIXTURES / "valid.spdx.json").read_bytes()
    response = await client.post(
        "/ingest/sbom",
        content=valid_sbom,
        headers={"Content-Type": "application/json"},
    )

    assert response.status_code == 202
    body = response.json()
    assert body["repository"] == MOCK_OIDC_CLAIMS["repository"]
    assert body["package_count"] == 2
    assert body["message"] == "queued"

    submissions = (
        (
            await session.execute(
                select(SbomSubmission).where(SbomSubmission.repository_claim == MOCK_OIDC_CLAIMS["repository"])
            )
        )
        .scalars()
        .all()
    )

    assert len(submissions) == 1
    submission = submissions[0]
    assert submission.status == SubmissionStatus.pending
    defer_mock.assert_awaited_once_with(submission.id)


async def test_duplicate_sbom_does_not_enqueue_again(
    authenticated_db_client: tuple[AsyncClient, AsyncSession],
    cleanup_db_rows_for_authenticated_client_tests: None,
    mocker: Any,
) -> None:
    """
    Duplicate SBOM submissions keep the existing duplicate-skip behavior and do not re-enqueue.
    """

    client, session = authenticated_db_client
    defer_mock = mocker.AsyncMock()
    mocker.patch("pg_atlas.ingestion.persist.defer_sbom_processing", new=defer_mock)

    valid_sbom = (FIXTURES / "valid.spdx.json").read_bytes()

    first = await client.post(
        "/ingest/sbom",
        content=valid_sbom,
        headers={"Content-Type": "application/json"},
    )
    second = await client.post(
        "/ingest/sbom",
        content=valid_sbom,
        headers={"Content-Type": "application/json"},
    )

    assert first.status_code == 202
    assert second.status_code == 202
    assert second.json()["message"] == "duplicate skipped"
    assert defer_mock.await_count == 1

    count = (
        (
            await session.execute(
                select(SbomSubmission).where(SbomSubmission.repository_claim == MOCK_OIDC_CLAIMS["repository"])
            )
        )
        .scalars()
        .all()
    )
    assert len(count) == 2


async def test_invalid_spdx_with_db_records_failed_submission(
    authenticated_db_client: tuple[AsyncClient, AsyncSession],
    cleanup_db_rows_for_authenticated_client_tests: None,
) -> None:
    """
    Invalid SPDX uploads still record a failed submission when DB storage is enabled.
    """

    client, session = authenticated_db_client
    invalid_sbom = (FIXTURES / "invalid.spdx.json").read_bytes()

    pre_submission_count = (await session.execute(select(func.count()).select_from(SbomSubmission))).scalar_one()

    response = await client.post(
        "/ingest/sbom",
        content=invalid_sbom,
        headers={"Content-Type": "application/json"},
    )

    assert response.status_code == 422

    submissions = (await session.execute(select(SbomSubmission).order_by(SbomSubmission.id.desc()))).scalars().all()
    assert len(submissions) == 1 + pre_submission_count
    added_submission = submissions[0]
    assert added_submission.status == SubmissionStatus.failed


async def test_valid_sbom_returns_503_when_queue_defer_fails(
    authenticated_db_client: tuple[AsyncClient, AsyncSession],
    cleanup_db_rows_for_authenticated_client_tests: None,
    mocker: Any,
) -> None:
    """
    Queueing failures after validation must mark the submission failed and return 503.
    """

    client, session = authenticated_db_client
    mocker.patch(
        "pg_atlas.ingestion.persist.defer_sbom_processing",
        new=mocker.AsyncMock(side_effect=RuntimeError("queue unavailable")),
    )

    pre_submission_count = (await session.execute(select(func.count()).select_from(SbomSubmission))).scalar_one()

    valid_sbom = (FIXTURES / "valid.spdx.json").read_bytes()
    response = await client.post(
        "/ingest/sbom",
        content=valid_sbom,
        headers={"Content-Type": "application/json"},
    )

    assert response.status_code == 503

    submissions = (await session.execute(select(SbomSubmission).order_by(SbomSubmission.id.desc()))).scalars().all()
    assert len(submissions) == 1 + pre_submission_count
    added_submission = submissions[0]
    assert added_submission.status == SubmissionStatus.failed
    assert added_submission.error_detail is not None
    assert "queue unavailable" in added_submission.error_detail


async def test_no_db_fallback_does_not_enqueue_processing(
    authenticated_client: AsyncClient,
    mocker: Any,
) -> None:
    """
    The no-DB fallback remains logging-only and never attempts to defer queue work.
    """

    defer_mock = mocker.AsyncMock()
    mocker.patch("pg_atlas.ingestion.persist.defer_sbom_processing", new=defer_mock)

    valid_sbom = (FIXTURES / "valid.spdx.json").read_bytes()
    response = await authenticated_client.post(
        "/ingest/sbom",
        content=valid_sbom,
        headers={"Content-Type": "application/json"},
    )

    assert response.status_code == 202
    defer_mock.assert_not_awaited()
