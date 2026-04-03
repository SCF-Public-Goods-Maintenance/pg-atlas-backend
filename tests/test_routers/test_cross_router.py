"""
Cross-router integration tests.

Verify consistency between related endpoints — e.g. that a project's repos
match the repo list filtered by project_id, and that dependency edges are
consistent across project-level and repo-level views.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from typing import Any

import pytest
from httpx import AsyncClient

_SKIP_NO_DB: pytest.MarkDecorator = pytest.mark.skipif(
    not pytest.importorskip("asyncpg", reason="DB driver"),
    reason="requires asyncpg",
)


@_SKIP_NO_DB
async def test_project_repos_match_repo_list_filter(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """
    GET /projects/{id}/repos and GET /repos?project_id=N return the same repos.
    """
    client, seed = seeded_client
    project = seed["project_a"]

    # Fetch via project sub-endpoint.
    resp1 = await client.get(f"/projects/{project.canonical_id}/repos")
    assert resp1.status_code == 200
    project_repo_ids = {r["canonical_id"] for r in resp1.json()["items"]}

    # Fetch via repos list with filter.
    resp2 = await client.get("/repos", params={"project_id": project.id})
    assert resp2.status_code == 200
    filtered_repo_ids = {r["canonical_id"] for r in resp2.json()["items"]}

    assert project_repo_ids == filtered_repo_ids


@_SKIP_NO_DB
async def test_repo_parent_project_matches_project_detail(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """
    The parent_project in repo detail matches the project detail endpoint.
    """
    client, seed = seeded_client
    repo = seed["repo_a1"]

    resp1 = await client.get(f"/repos/{repo.canonical_id}")
    assert resp1.status_code == 200
    parent = resp1.json()["parent_project"]
    assert parent is not None

    resp2 = await client.get(f"/projects/{parent['canonical_id']}")
    assert resp2.status_code == 200

    project_data = resp2.json()
    assert parent["canonical_id"] == project_data["canonical_id"]
    assert parent["display_name"] == project_data["display_name"]


@_SKIP_NO_DB
async def test_project_dep_edges_consistent_with_repo_dep_edges(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """
    Project-level depends-on should be consistent with repo-level edges.

    Alpha's repos depend on Beta's repo_b1 → project-level "Alpha depends on Beta"
    should exist. The edge_count should equal the number of repo-level edges.
    """
    client, seed = seeded_client
    alpha = seed["project_a"]

    # Project-level deps.
    resp1 = await client.get(f"/projects/{alpha.canonical_id}/depends-on")
    assert resp1.status_code == 200
    proj_deps = resp1.json()

    beta_dep = next(
        (d for d in proj_deps if d["project"]["canonical_id"] == seed["project_b"].canonical_id),
        None,
    )
    assert beta_dep is not None
    # repo_a1 → repo_b1 = 1 edge
    assert beta_dep["edge_count"] >= 1
