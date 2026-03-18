"""
Shared pytest fixtures for PG Atlas backend tests.

Fixtures defined here are available to all test modules without explicit import.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

# Override the required PG_ATLAS_API_URL setting before the app is imported,
# so that Settings() can be instantiated without a .env file in CI.
import os
from collections.abc import AsyncGenerator, Generator
from typing import Any

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

os.environ.setdefault("PG_ATLAS_API_URL", "https://test.pg-atlas.example")

from pg_atlas.auth.oidc import verify_github_oidc_token
from pg_atlas.config import Settings, settings
from pg_atlas.db_models.session import maybe_db_session
from pg_atlas.main import app

TEST_API_URL = "https://test.pg-atlas.example"

# Claims dict returned by the mock OIDC dependency — represents a valid
# submission from a fictional test repository.
MOCK_OIDC_CLAIMS: dict[str, Any] = {
    "repository": "test-org/test-repo",
    "actor": "test-user",
    "iss": "https://token.actions.githubusercontent.com",
    "aud": TEST_API_URL,
}


def get_test_database_url() -> str | None:
    """
    Return the database URL used by DB integration tests.

    Test-specific configuration takes precedence so contributors can run tests
    against an isolated database without touching their local development data.
    """

    test_database_url = os.environ.get("PG_ATLAS_TEST_DATABASE_URL")
    if test_database_url:
        return Settings.coerce_async_driver(test_database_url)

    return settings.DATABASE_URL or None


@pytest.fixture
def mock_oidc_claims() -> dict[str, Any]:
    """Return the fixed OIDC claims dict used by the mocked OIDC dependency.

    Use this fixture in tests that want to inspect the claims values that flow
    into queue_sbom (e.g. to assert ``repository`` appears in the response).
    """
    return MOCK_OIDC_CLAIMS.copy()


@pytest.fixture
def app_with_mock_oidc() -> Generator[FastAPI, None, None]:
    """FastAPI app instance with the OIDC dependency overridden to return MOCK_OIDC_CLAIMS.

    Also overrides ``maybe_db_session`` to yield ``None``, preventing the
    shared pooled engine from being used across different pytest-asyncio event
    loops (which would raise *Future attached to a different loop*).

    Restores the original dependencies after the test. Use this fixture for
    tests that don't care about authentication and want to focus on SBOM
    validation or response shapes.
    """

    async def _no_db_session() -> AsyncGenerator[None, None]:
        yield None

    app.dependency_overrides[verify_github_oidc_token] = lambda: MOCK_OIDC_CLAIMS
    app.dependency_overrides[maybe_db_session] = _no_db_session
    yield app
    app.dependency_overrides.clear()


@pytest.fixture
async def async_client() -> AsyncGenerator[AsyncClient, None]:
    """Async HTTP client wired to the real FastAPI app (no OIDC override).

    Use this fixture for tests that exercise the authentication layer
    (missing/invalid tokens).
    """
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        yield client


@pytest.fixture
async def authenticated_client(
    app_with_mock_oidc: FastAPI,
) -> AsyncGenerator[AsyncClient, None]:
    """Async HTTP client wired to the FastAPI app with OIDC dep overridden.

    Use this fixture for tests that assume authentication succeeded and want to
    test SBOM validation or downstream processing.
    """
    async with AsyncClient(
        transport=ASGITransport(app=app_with_mock_oidc),
        base_url="http://test",
    ) as client:
        yield client


# ---------------------------------------------------------------------------
# Database fixtures (skipped when PG_ATLAS_DATABASE_URL is not configured)
# ---------------------------------------------------------------------------


@pytest.fixture
async def db_session() -> AsyncGenerator[Any, None]:
    """
    Real ``AsyncSession`` against the configured PostgreSQL database.

    Skipped automatically when ``PG_ATLAS_DATABASE_URL`` is not set (e.g. in CI
    without a database service).  Set the variable before running to enable
    database integration tests.

    Each test gets a **fresh** engine (with ``NullPool``) so that asyncpg
    connections are never shared across event loops.  pytest-asyncio creates a
    new event loop per test function by default; a pooled engine would attempt
    to reuse connections from a previous loop and raise
    ``RuntimeError: Future attached to a different loop``.
    """
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
    from sqlalchemy.pool import NullPool

    database_url = get_test_database_url()
    if not database_url:
        pytest.skip("PG_ATLAS_DATABASE_URL / PG_ATLAS_TEST_DATABASE_URL not set; skipping database integration test")

    engine = create_async_engine(database_url, poolclass=NullPool)
    try:
        async_session: async_sessionmaker[AsyncSession] = async_sessionmaker(
            engine, class_=AsyncSession, expire_on_commit=False
        )
        async with async_session() as session:
            yield session
    finally:
        await engine.dispose()
