"""
Tests for /repos endpoints.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from typing import Any

from httpx import AsyncClient

# ---------------------------------------------------------------------------
# No-DB tests
# ---------------------------------------------------------------------------


async def test_repos_db_unavailable_returns_503(no_db_client: AsyncClient) -> None:
    """Repo endpoints return 503 when no database is configured."""

    for path in ["/repos", "/repos/pkg:github/test/repo", "/repos/pkg:github/test/repo/contributors"]:
        resp = await no_db_client.get(path)
        assert resp.status_code == 503, f"{path} should return 503"


# ---------------------------------------------------------------------------
# DB integration tests
# ---------------------------------------------------------------------------


async def test_list_repos_pagination(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """GET /repos returns paginated results."""

    client, _ = seeded_client
    resp = await client.get("/repos", params={"limit": 1})
    assert resp.status_code == 200

    data = resp.json()
    assert len(data["items"]) == 1
    assert data["total"] >= 3


async def test_list_repos_filter_by_project_id(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """GET /repos?project_id=N filters to repos belonging to that project."""

    client, seed = seeded_client
    pid = seed["project_a"].id
    resp = await client.get("/repos", params={"project_id": pid})
    assert resp.status_code == 200

    data = resp.json()
    for item in data["items"]:
        assert item["project_id"] == pid


async def test_list_repos_search(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """GET /repos?search=repo-a1 finds the seeded repo."""

    client, _ = seeded_client
    resp = await client.get("/repos", params={"search": "repo-a1"})
    assert resp.status_code == 200

    data = resp.json()
    assert data["total"] >= 1
    assert any("repo-a1" in r["display_name"] for r in data["items"])


async def test_get_repo_detail_with_parent_project(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """GET /repos/{canonical_id} includes parent_project when present."""

    client, seed = seeded_client
    cid = seed["repo_a1"].canonical_id
    resp = await client.get(f"/repos/{cid}")
    assert resp.status_code == 200

    data = resp.json()
    assert data["canonical_id"] == cid
    assert data["parent_project"] is not None
    assert data["parent_project"]["canonical_id"] == seed["project_a"].canonical_id
    assert data["active_contributors_30d"] == 1
    assert data["active_contributors_90d"] == 1


async def test_get_repo_detail_includes_contributors(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """GET /repos/{canonical_id} includes contributors list."""

    client, seed = seeded_client
    cid = seed["repo_a1"].canonical_id
    resp = await client.get(f"/repos/{cid}")
    assert resp.status_code == 200

    contribs = resp.json()["contributors"]
    assert len(contribs) >= 1
    assert any(c["name"] == "Test Contributor" for c in contribs)


async def test_get_repo_detail_no_contributions_returns_zero_activity(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """Repo detail returns zero active contributor counts when no contribution edges exist."""

    client, seed = seeded_client
    cid = seed["repo_b1"].canonical_id
    resp = await client.get(f"/repos/{cid}")
    assert resp.status_code == 200

    data = resp.json()
    assert data["active_contributors_30d"] == 0
    assert data["active_contributors_90d"] == 0


async def test_get_repo_detail_uses_global_max_date_as_activity_anchor(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """Repo detail uses global max commit date as rolling-window anchor, not request time."""

    client, seed = seeded_client
    cid = seed["repo_a2"].canonical_id
    resp = await client.get(f"/repos/{cid}")
    assert resp.status_code == 200

    data = resp.json()
    assert data["active_contributors_30d"] == 0
    assert data["active_contributors_90d"] == 1


async def test_get_repo_detail_includes_dep_counts(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """GET /repos/{canonical_id} includes outgoing/incoming dep counts."""

    client, seed = seeded_client
    cid = seed["repo_a1"].canonical_id
    resp = await client.get(f"/repos/{cid}")
    assert resp.status_code == 200

    data = resp.json()
    # repo_a1 depends on repo_b1 (1 repo) + ext_repo (1 external)
    assert data["outgoing_dep_counts"]["repos"] >= 1
    assert data["outgoing_dep_counts"]["external_repos"] >= 1


async def test_get_repo_not_found_returns_404(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """GET /repos/{canonical_id} returns 404 for unknown ID."""

    client, _ = seeded_client
    resp = await client.get("/repos/pkg:github/nonexistent/repo")
    assert resp.status_code == 404


async def test_get_repo_depends_on(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """GET /repos/{canonical_id}/depends-on returns outgoing dependencies."""

    client, seed = seeded_client
    cid = seed["repo_a1"].canonical_id
    resp = await client.get(f"/repos/{cid}/depends-on")
    assert resp.status_code == 200

    data = resp.json()
    assert len(data) >= 2
    target_ids = {d["canonical_id"] for d in data}
    assert seed["repo_b1"].canonical_id in target_ids
    assert seed["ext_repo"].canonical_id in target_ids


async def test_get_repo_has_dependents(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """GET /repos/{canonical_id}/has-dependents returns incoming dependencies."""

    client, seed = seeded_client
    cid = seed["repo_b1"].canonical_id
    resp = await client.get(f"/repos/{cid}/has-dependents")
    assert resp.status_code == 200

    data = resp.json()
    assert len(data) >= 1
    source_ids = {d["canonical_id"] for d in data}
    assert seed["repo_a1"].canonical_id in source_ids


async def test_get_repo_contributors(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """GET /repos/{canonical_id}/contributors returns per-contributor commit stats."""

    client, seed = seeded_client
    cid = seed["repo_a1"].canonical_id
    resp = await client.get(f"/repos/{cid}/contributors")
    assert resp.status_code == 200

    body = resp.json()
    assert body["total"] >= 1
    contributor = body["items"][0]
    assert contributor["number_of_commits"] >= 1
    assert contributor["name"] == "Test Contributor"


# ---------------------------------------------------------------------------
# Sort tests
# ---------------------------------------------------------------------------


async def test_list_repos_sort_by_display_name(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """GET /repos?sort=display_name:asc returns items in alphabetical order."""

    client, seed = seeded_client
    tag = seed["repo_a1"].canonical_id.split("-")[-1]
    resp = await client.get("/repos", params={"sort": "display_name:asc", "search": tag})
    assert resp.status_code == 200

    data = resp.json()
    names = [item["display_name"] for item in data["items"]]
    assert names == sorted(names)


async def test_list_repos_sort_by_adoption_stars_desc(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """GET /repos?sort=adoption_stars:desc puts higher star counts first."""

    client, seed = seeded_client
    # Search for seeded repos belonging to project_a's org.
    tag = seed["repo_a1"].canonical_id.split("-")[-1]
    resp = await client.get("/repos", params={"sort": "adoption_stars:desc", "search": tag})
    assert resp.status_code == 200

    data = resp.json()
    stars = [item["adoption_stars"] for item in data["items"] if item["adoption_stars"] is not None]
    assert stars == sorted(stars, reverse=True)


async def test_list_repos_sort_invalid_field_returns_422(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """GET /repos?sort=nonexistent_field:asc returns 422."""

    client, _ = seeded_client
    resp = await client.get("/repos", params={"sort": "nonexistent_field:asc"})
    assert resp.status_code == 422
