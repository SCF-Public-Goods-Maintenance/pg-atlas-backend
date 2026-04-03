"""
Tests for GET /metadata endpoint.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from typing import Any

import pytest
from httpx import AsyncClient


async def test_metadata_db_unavailable_returns_503(no_db_client: AsyncClient) -> None:
    """GET /metadata returns 503 when no database is configured."""

    resp = await no_db_client.get("/metadata")
    assert resp.status_code == 503


@pytest.mark.skipif(
    not pytest.importorskip("asyncpg", reason="DB driver"),
    reason="requires asyncpg",
)
async def test_metadata_returns_counts(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """GET /metadata returns all expected count fields with correct types."""

    client, _ = seeded_client
    resp = await client.get("/metadata")
    assert resp.status_code == 200

    data = resp.json()
    assert isinstance(data["total_projects"], int)
    assert isinstance(data["active_projects"], int)
    assert isinstance(data["total_repos"], int)
    assert isinstance(data["total_external_repos"], int)
    assert isinstance(data["total_dependency_edges"], int)
    assert isinstance(data["total_contributor_edges"], int)

    # Seeded data has 2 projects (1 active), 3 repos, 1 external, 3 dep edges, 1 contrib edge.
    # But other test data may exist in the DB — assert at-least counts.
    assert data["total_projects"] >= 2
    assert data["active_projects"] >= 1
    assert data["total_repos"] >= 3
    assert data["total_external_repos"] >= 1
    assert data["total_dependency_edges"] >= 3
    assert data["total_contributor_edges"] >= 1
