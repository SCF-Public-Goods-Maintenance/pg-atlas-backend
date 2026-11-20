"""
Smoke tests for OpenAPI schema generation and serving.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from httpx2 import AsyncClient

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


async def test_openapi_declares_route_specific_error_responses() -> None:
    """Route-specific 401, 403, 404, 422, 503, and 307 responses should be documented."""

    spec = app.openapi()
    paths = spec["paths"]

    health_responses = paths["/health"]["get"]["responses"]
    assert "503" in health_responses

    ingest_responses = paths["/ingest/sbom"]["post"]["responses"]
    assert {"401", "403", "422", "503"}.issubset(ingest_responses)
    assert ingest_responses["401"]["headers"]["WWW-Authenticate"]["schema"]["type"] == "string"

    contributor_responses = paths["/contributors/{contributor_id}"]["get"]["responses"]
    assert "404" in contributor_responses

    project_responses = paths["/projects/{canonical_id}"]["get"]["responses"]
    assert "404" in project_responses

    repo_responses = paths["/repos/{canonical_id}"]["get"]["responses"]
    assert {"307", "404"}.issubset(repo_responses)
    assert repo_responses["307"]["headers"]["Location"]["schema"]["type"] == "string"


async def test_openapi_distinguishes_builtin_and_spdx_validation_errors() -> None:
    """Sort 422s should use FastAPI's builtin schema while SPDX 422s should use their custom model."""

    spec = app.openapi()
    schemas = spec["components"]["schemas"]

    assert "HTTPValidationError" in schemas
    assert "ValidationError" in schemas
    assert "SpdxValidationFailed" in schemas

    project_responses = spec["paths"]["/projects"]["get"]["responses"]
    project_422_schema = project_responses["422"]["content"]["application/json"]["schema"]
    assert project_422_schema["$ref"] == "#/components/schemas/HTTPValidationError"

    ingest_responses = spec["paths"]["/ingest/sbom"]["post"]["responses"]
    ingest_422_schema = ingest_responses["422"]["content"]["application/json"]["schema"]
    assert ingest_422_schema["$ref"] == "#/components/schemas/SpdxValidationFailed"


async def test_openapi_does_not_add_dependency_503_globally() -> None:
    """The require_session 503 should not be attached to ordinary routes."""

    spec = app.openapi()
    paths = spec["paths"]

    assert "503" not in paths["/projects"]["get"]["responses"]
    assert "503" not in paths["/contributors/{contributor_id}"]["get"]["responses"]


async def test_openapi_declares_429_for_every_route() -> None:
    """Every public operation should document its middleware rate-limit response."""

    spec = app.openapi()

    for _path, path_item in spec["paths"].items():
        for method, operation in path_item.items():
            if method not in {"get", "post", "put", "patch", "delete"}:
                continue

            response = operation["responses"]["429"]
            assert response["headers"]["Retry-After"]["schema"]["type"] == "integer"
            schema = response["content"]["application/json"]["schema"]
            assert schema["$ref"] == "#/components/schemas/HTTPErrorResponse"
