"""
Smoke tests for OpenAPI schema generation and serving.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from httpx import AsyncClient

from pg_atlas.main import app


async def test_openapi_schema_can_be_generated() -> None:
    """App-level OpenAPI generation should succeed and include expected top-level keys."""

    schema = app.openapi()

    assert isinstance(schema, dict)
    assert schema.get("openapi")
    assert isinstance(schema.get("paths"), dict)


async def test_openapi_json_endpoint_serves_schema(async_client: AsyncClient) -> None:
    """The OpenAPI document should be available from the canonical /openapi.json route."""

    response = await async_client.get("/openapi.json")

    assert response.status_code == 200
    payload = response.json()
    assert payload.get("openapi")
    assert isinstance(payload.get("paths"), dict)
