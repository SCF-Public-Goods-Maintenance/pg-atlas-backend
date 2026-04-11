"""
Tests for /projects endpoints.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from typing import Any

from httpx import AsyncClient

# ---------------------------------------------------------------------------
# No-DB tests (always run)
# ---------------------------------------------------------------------------


async def test_projects_db_unavailable_returns_503(no_db_client: AsyncClient) -> None:
    """All project endpoints return 503 when no database is configured."""

    for path in ["/projects", "/projects/foo", "/projects/foo/repos", "/projects/foo/contributors"]:
        resp = await no_db_client.get(path)
        assert resp.status_code == 503, f"{path} should return 503"


# ---------------------------------------------------------------------------
# DB integration tests (skipped without database)
# ---------------------------------------------------------------------------


async def test_list_projects_pagination(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """GET /projects returns paginated results."""

    client, _ = seeded_client
    resp = await client.get("/projects", params={"limit": 1, "offset": 0})
    assert resp.status_code == 200

    data = resp.json()
    assert len(data["items"]) == 1
    assert data["total"] >= 2
    assert data["limit"] == 1
    assert data["offset"] == 0


async def test_list_projects_filter_by_type(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """GET /projects?project_type=scf-project filters correctly."""

    client, _ = seeded_client
    resp = await client.get("/projects", params={"project_type": "scf-project"})
    assert resp.status_code == 200

    data = resp.json()
    for item in data["items"]:
        assert item["project_type"] == "scf-project"


async def test_list_projects_filter_by_activity_status(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """GET /projects?activity_status=live returns only live projects."""

    client, _ = seeded_client
    resp = await client.get("/projects", params={"activity_status": "live"})
    assert resp.status_code == 200

    data = resp.json()
    for item in data["items"]:
        assert item["activity_status"] == "live"


async def test_list_projects_search(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """GET /projects?search=alpha finds the Alpha project."""

    client, _ = seeded_client
    resp = await client.get("/projects", params={"search": "Alpha"})
    assert resp.status_code == 200

    data = resp.json()
    assert data["total"] >= 1
    names = [item["display_name"] for item in data["items"]]
    assert any("Alpha" in n for n in names)


async def test_list_projects_ordered_by_canonical_id(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """GET /projects returns items in a consistent order (DB collation)."""

    client, seed = seeded_client

    # Search for seeded projects to confirm they exist.
    a_id = seed["project_a"].canonical_id
    resp_a = await client.get("/projects", params={"search": seed["project_a"].display_name})
    assert resp_a.status_code == 200
    assert any(p["canonical_id"] == a_id for p in resp_a.json()["items"])

    # Request a full page and verify no duplicate IDs (deterministic pagination).
    resp = await client.get("/projects", params={"limit": 50})
    assert resp.status_code == 200

    data = resp.json()
    ids = [item["canonical_id"] for item in data["items"]]
    assert len(ids) == len(set(ids))


async def test_get_project_detail_includes_metadata(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """GET /projects/{canonical_id} includes validated metadata."""

    client, seed = seeded_client
    cid = seed["project_a"].canonical_id
    resp = await client.get(f"/projects/{cid}")
    assert resp.status_code == 200

    data = resp.json()
    assert data["canonical_id"] == cid
    assert "metadata" in data
    meta = data["metadata"]
    assert isinstance(meta["scf_submissions"], list)
    assert len(meta["scf_submissions"]) == 1
    assert meta["scf_submissions"][0]["round"] == "SCF-1"
    assert meta["description"] == "Test alpha project"


async def test_get_project_detail_normalizes_metadata(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """Project without metadata gets empty defaults via ProjectMetadata."""

    client, seed = seeded_client
    cid = seed["project_b"].canonical_id
    resp = await client.get(f"/projects/{cid}")
    assert resp.status_code == 200

    meta = resp.json()["metadata"]
    assert meta["scf_submissions"] == []
    assert meta["description"] is None


async def test_get_project_not_found_with_db(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """GET /projects/{canonical_id} returns 404 for unknown ID."""

    client, _ = seeded_client
    resp = await client.get("/projects/nonexistent:project")
    assert resp.status_code == 404


async def test_get_project_repos(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """GET /projects/{canonical_id}/repos returns the project's repos."""

    client, seed = seeded_client
    cid = seed["project_a"].canonical_id
    resp = await client.get(f"/projects/{cid}/repos")
    assert resp.status_code == 200

    data = resp.json()
    assert data["total"] >= 2
    repo_ids = {item["canonical_id"] for item in data["items"]}
    assert seed["repo_a1"].canonical_id in repo_ids
    assert seed["repo_a2"].canonical_id in repo_ids


async def test_get_project_contributors(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """GET /projects/{canonical_id}/contributors returns deduplicated contributor totals."""

    client, seed = seeded_client
    cid = seed["project_a"].canonical_id
    resp = await client.get(f"/projects/{cid}/contributors")
    assert resp.status_code == 200

    body = resp.json()
    assert body["total"] >= 1
    contributor = body["items"][0]
    assert contributor["total_commits_in_project"] >= 1


async def test_get_project_contributors_search(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """GET /projects/{canonical_id}/contributors?search filters by contributor name."""

    client, seed = seeded_client
    cid = seed["project_a"].canonical_id
    resp = await client.get(f"/projects/{cid}/contributors", params={"search": "Test Contributor"})
    assert resp.status_code == 200

    body = resp.json()
    assert body["total"] >= 1
    assert all("Test Contributor" in item["name"] for item in body["items"])


async def test_get_project_depends_on(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """
    GET /projects/{canonical_id}/depends-on returns collapsed project-level deps.

    Alpha's repo_a1 depends on Beta's repo_b1 → Alpha depends on Beta.
    """
    client, seed = seeded_client
    cid = seed["project_a"].canonical_id
    resp = await client.get(f"/projects/{cid}/depends-on")
    assert resp.status_code == 200

    data = resp.json()
    assert len(data) >= 1
    target_ids = [d["project"]["canonical_id"] for d in data]
    assert seed["project_b"].canonical_id in target_ids


async def test_get_project_has_dependents(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """
    GET /projects/{canonical_id}/has-dependents for Alpha should include Beta.

    Beta's repo_b1 depends on Alpha's repo_a2 → Beta depends on Alpha.
    """
    client, seed = seeded_client
    cid = seed["project_a"].canonical_id
    resp = await client.get(f"/projects/{cid}/has-dependents")
    assert resp.status_code == 200

    data = resp.json()
    assert len(data) >= 1
    source_ids = [d["project"]["canonical_id"] for d in data]
    assert seed["project_b"].canonical_id in source_ids


async def test_project_depends_on_excludes_self_refs(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """Project-level deps exclude edges where both repos belong to the same project."""

    client, seed = seeded_client
    cid = seed["project_a"].canonical_id
    resp = await client.get(f"/projects/{cid}/depends-on")
    assert resp.status_code == 200

    for dep in resp.json():
        assert dep["project"]["canonical_id"] != cid
