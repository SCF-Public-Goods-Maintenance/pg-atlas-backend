"""
Tests for /gitlog endpoints.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from typing import Any

from httpx import AsyncClient


async def test_gitlog_db_unavailable_returns_503(no_db_client: AsyncClient) -> None:
    """Gitlog endpoints return 503 when no database is configured."""

    list_resp = await no_db_client.get("/gitlog")
    detail_resp = await no_db_client.get("/gitlog/1")
    assert list_resp.status_code == 503
    assert detail_resp.status_code == 503


async def test_list_gitlog_artifacts(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """GET /gitlog returns seeded audit rows with pagination envelope."""

    client, _ = seeded_client
    resp = await client.get("/gitlog")
    assert resp.status_code == 200

    body = resp.json()
    assert body["total"] >= 1
    assert len(body["items"]) >= 1
    item = body["items"][0]
    assert "repo_canonical_id" in item
    assert "status" in item


async def test_list_gitlog_artifacts_filter_by_repo(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """GET /gitlog?repo filters by exact repo canonical ID."""

    client, seed = seeded_client
    repo_canonical_id = seed["repo_a1"].canonical_id
    resp = await client.get("/gitlog", params={"repo": repo_canonical_id})
    assert resp.status_code == 200

    body = resp.json()
    assert body["total"] >= 1
    assert all(item["repo_canonical_id"] == repo_canonical_id for item in body["items"])


async def test_get_gitlog_artifact_detail(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """GET /gitlog/{id} returns one gitlog audit row."""

    client, seed = seeded_client
    artifact_id = seed["gitlog_artifact"].id
    resp = await client.get(f"/gitlog/{artifact_id}")
    assert resp.status_code == 200

    body = resp.json()
    assert body["id"] == artifact_id
    assert body["error_detail"] == "seeded test failure"


async def test_get_gitlog_artifact_not_found(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """GET /gitlog/{id} returns 404 for unknown artifact ID."""

    client, _ = seeded_client
    resp = await client.get("/gitlog/999999")
    assert resp.status_code == 404
