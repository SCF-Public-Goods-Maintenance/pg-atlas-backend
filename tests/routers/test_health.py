"""
Tests for GET /health readiness reporting.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

import pytest
from httpx import AsyncClient
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from pg_atlas.config import settings
from pg_atlas.routers import health as health_router


async def test_health_returns_200(async_client: AsyncClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """GET /health should return 200 OK when the DB is not configured."""

    monkeypatch.setattr(settings, "DATABASE_URL", "")
    response = await async_client.get("/health")
    assert response.status_code == 200


async def test_health_response_shape(async_client: AsyncClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """GET /health should return the readiness payload shape."""

    monkeypatch.setattr(settings, "DATABASE_URL", "")
    monkeypatch.setattr(settings, "ARTIFACT_S3_ENDPOINT", None)
    response = await async_client.get("/health")
    body = response.json()

    assert body["status"] == "live"
    assert isinstance(body["version"], str)
    assert body["components"] == {
        "artifact_store": "local",
        "database": "not-configured",
        "schema_version": None,
    }


async def test_health_returns_ready_when_database_is_configured(
    async_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Configured databases should move /health from live to ready."""

    async def read_schema_version() -> str:
        return "66ac36af6383"

    monkeypatch.setattr(settings, "DATABASE_URL", "postgresql+asyncpg://configured")
    monkeypatch.setattr(settings, "ARTIFACT_S3_ENDPOINT", "https://s3.filebase.com")
    monkeypatch.setattr(health_router, "_read_schema_version", read_schema_version)

    response = await async_client.get("/health")
    body = response.json()

    assert response.status_code == 200
    assert body == {
        "status": "ready",
        "version": body["version"],
        "components": {
            "artifact_store": "IPFS",
            "database": "ready",
            "schema_version": "66ac36af6383",
        },
    }


async def test_health_returns_503_when_database_check_fails(
    async_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Configured-database failures should become readiness 503 responses."""

    async def read_schema_version() -> str | None:
        raise SQLAlchemyError("database unavailable")

    monkeypatch.setattr(settings, "DATABASE_URL", "postgresql+asyncpg://configured")
    monkeypatch.setattr(health_router, "_read_schema_version", read_schema_version)

    response = await async_client.get("/health")

    assert response.status_code == 503
    assert response.json() == {"detail": "database unavailable"}


def test_select_app_schema_version_ignores_procrastinate_revision() -> None:
    """The readiness response should hide the Procrastinate migration branch."""

    assert health_router._select_app_schema_version(["procrastinate_001", "66ac36af6383"]) == "66ac36af6383"


def test_select_app_schema_version_rejects_multiple_application_revisions() -> None:
    """Multiple application revisions should fail readiness explicitly."""

    with pytest.raises(ValueError, match="multiple non-Procrastinate"):
        health_router._select_app_schema_version(["66ac36af6383", "deadbeef1234", "procrastinate_001"])


async def test_schema_version_from_session_reads_database_revision(
    db_session: AsyncSession,
) -> None:
    """A real database session should expose the application Alembic revision."""

    schema_version = await health_router._schema_version_from_session(db_session)

    assert isinstance(schema_version, str)
    assert schema_version != "procrastinate_001"


async def test_openapi_describes_health_readiness_schema(async_client: AsyncClient) -> None:
    """The OpenAPI document should describe the nested readiness payload."""

    response = await async_client.get("/openapi.json")
    openapi = response.json()
    schemas = openapi["components"]["schemas"]
    health_schema = schemas["HealthResponse"]
    components_ref = health_schema["properties"]["components"]["$ref"]
    components_schema = schemas[components_ref.rsplit("/", maxsplit=1)[-1]]

    assert set(health_schema["properties"]["status"]["enum"]) == {"live", "ready"}
    assert set(components_schema["properties"]["artifact_store"]["enum"]) == {"local", "IPFS"}
    assert set(components_schema["properties"]["database"]["enum"]) == {
        "ready",
        "not-configured",
    }
    assert any(option.get("type") == "string" for option in components_schema["properties"]["schema_version"]["anyOf"])
