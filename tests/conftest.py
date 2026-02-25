"""
Shared pytest fixtures for PG Atlas backend tests.

Fixtures defined here are available to all test modules without explicit import.

Author: SCF Public Goods Maintenance <https://github.com/SCF-Public-Goods-Maintenance>
"""

from __future__ import annotations

# Override the required PG_ATLAS_API_URL setting before the app is imported,
# so that Settings() can be instantiated without a .env file in CI.
import os
from collections.abc import AsyncGenerator
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient

os.environ.setdefault("PG_ATLAS_API_URL", "https://test.pg-atlas.example")

from pg_atlas.auth.oidc import verify_github_oidc_token
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


@pytest.fixture
def mock_oidc_claims() -> dict[str, Any]:
    """Return the fixed OIDC claims dict used by the mocked OIDC dependency.

    Use this fixture in tests that want to inspect the claims values that flow
    into queue_sbom (e.g. to assert ``repository`` appears in the response).
    """
    return MOCK_OIDC_CLAIMS.copy()


@pytest.fixture
def app_with_mock_oidc():
    """FastAPI app instance with the OIDC dependency overridden to return MOCK_OIDC_CLAIMS.

    Restores the original dependency after the test. Use this fixture for tests
    that don't care about authentication and want to focus on SBOM validation or
    response shapes.
    """
    app.dependency_overrides[verify_github_oidc_token] = lambda: MOCK_OIDC_CLAIMS
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
async def authenticated_client(app_with_mock_oidc) -> AsyncGenerator[AsyncClient, None]:
    """Async HTTP client wired to the FastAPI app with OIDC dep overridden.

    Use this fixture for tests that assume authentication succeeded and want to
    test SBOM validation or downstream processing.
    """
    async with AsyncClient(
        transport=ASGITransport(app=app_with_mock_oidc),
        base_url="http://test",
    ) as client:
        yield client
