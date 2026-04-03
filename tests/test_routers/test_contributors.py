"""
Tests for /contributors endpoints.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from typing import Any

import pytest
from httpx import AsyncClient

_SKIP_NO_DB = pytest.mark.skipif(
    not pytest.importorskip("asyncpg", reason="DB driver"),
    reason="requires asyncpg",
)


# ---------------------------------------------------------------------------
# No-DB tests
# ---------------------------------------------------------------------------


async def test_contributors_db_unavailable_returns_503(no_db_client: AsyncClient) -> None:
    """GET /contributors/{id} returns 503 when no database is configured."""

    resp = await no_db_client.get("/contributors/1")
    assert resp.status_code == 503


# ---------------------------------------------------------------------------
# DB integration tests
# ---------------------------------------------------------------------------


@_SKIP_NO_DB
async def test_get_contributor_detail(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """GET /contributors/{id} returns full contributor detail."""

    client, seed = seeded_client
    cid = seed["contributor"].id
    resp = await client.get(f"/contributors/{cid}")
    assert resp.status_code == 200

    data = resp.json()
    assert data["id"] == cid
    assert data["name"] == "Test Contributor"
    assert len(data["email_hash"]) == 64


@_SKIP_NO_DB
async def test_get_contributor_includes_repo_activity(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """GET /contributors/{id} includes per-repo commit activity."""

    client, seed = seeded_client
    cid = seed["contributor"].id
    resp = await client.get(f"/contributors/{cid}")
    assert resp.status_code == 200

    data = resp.json()
    assert data["total_repos"] >= 1
    assert data["total_commits"] >= 15
    assert data["first_contribution"] is not None
    assert data["last_contribution"] is not None
    assert len(data["repos"]) >= 1

    repo_entry = data["repos"][0]
    assert repo_entry["repo_canonical_id"] == seed["repo_a1"].canonical_id
    assert repo_entry["number_of_commits"] == 15


@_SKIP_NO_DB
async def test_get_contributor_not_found_returns_404(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """GET /contributors/{id} returns 404 for unknown contributor."""

    client, _ = seeded_client
    resp = await client.get("/contributors/999999")
    assert resp.status_code == 404
