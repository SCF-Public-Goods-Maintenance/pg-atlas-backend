"""
Integration tests for pg_atlas.metrics.graph_builder (requires database).

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from uuid import uuid4

import networkx as nx
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from pg_atlas.db_models import ExternalRepo, Project, Repo
from pg_atlas.db_models.base import ActivityStatus, ProjectType, Visibility
from pg_atlas.metrics.graph_builder import build_dependency_graph, build_full_graph
from tests.db_cleanup import DB_MODELS_TABLE_SPECS, capture_snapshot, cleanup_created_rows


@pytest.fixture
async def cleanup_db_rows_for_metrics_builder_tests(
    db_session: AsyncSession,
) -> AsyncGenerator[None, None]:
    """
    Remove only rows created by metrics graph-builder integration tests in this module.
    """

    snapshot = await capture_snapshot(db_session, DB_MODELS_TABLE_SPECS)
    yield
    await cleanup_created_rows(db_session, DB_MODELS_TABLE_SPECS, snapshot)


async def test_build_dependency_graph_returns_digraph(db_session: AsyncSession):
    G = await build_dependency_graph(db_session)
    assert isinstance(G, nx.DiGraph)


async def test_build_dependency_graph_has_nodes(db_session: AsyncSession):
    result = await db_session.execute(text("SELECT COUNT(*) FROM repo_vertices"))
    if result.scalar_one() == 0:
        pytest.skip("repo_vertices table is empty; skipping data-presence assertion")

    G = await build_dependency_graph(db_session)
    assert G.number_of_nodes() > 0, "Expected populated repo_vertices table"


async def test_build_dependency_graph_node_keys_are_strings(db_session: AsyncSession):
    G = await build_dependency_graph(db_session)
    for node in G.nodes():
        assert isinstance(node, str), f"Node key must be str (canonical_id), got {type(node)}"


async def test_build_dependency_graph_vertex_type_title_cased(db_session: AsyncSession):
    """DB enum values ('repo', 'external-repo') must be mapped to title-case labels."""
    G = await build_dependency_graph(db_session)
    valid_types = {"Repo", "ExternalRepo"}
    for node, data in G.nodes(data=True):
        vt = data.get("vertex_type")
        assert vt in valid_types, f"Node {node!r}: unexpected vertex_type={vt!r}"


async def test_build_dependency_graph_metadata(db_session: AsyncSession):
    G = await build_dependency_graph(db_session)
    assert G.graph.get("source") == "postgresql"
    assert "reference_date" in G.graph


async def test_build_dependency_graph_days_since_commit_type(db_session: AsyncSession):
    """days_since_commit must be int or None, never a datetime or float."""
    G = await build_dependency_graph(db_session)
    for node, data in G.nodes(data=True):
        dsc = data.get("days_since_commit")
        assert dsc is None or isinstance(dsc, int), f"days_since_commit must be int or None, got {type(dsc)} on {node!r}"


async def test_build_dependency_graph_surfaces_repo_activity_status(
    db_session: AsyncSession,
    cleanup_db_rows_for_metrics_builder_tests: None,
) -> None:
    suffix = uuid4().hex[:8]
    project = Project(
        canonical_id=f"daoip-5:stellar:project:builder-{suffix}",
        display_name=f"Builder Project {suffix}",
        project_type=ProjectType.scf_project,
        activity_status=ActivityStatus.in_dev,
    )
    db_session.add(project)
    await db_session.flush()

    repo = Repo(
        canonical_id=f"pkg:github/test/builder-repo-{suffix}",
        display_name=f"builder-repo-{suffix}",
        visibility=Visibility.public,
        latest_version="1.0.0",
        project_id=project.id,
    )
    external = ExternalRepo(
        canonical_id=f"pkg:npm/builder-ext-{suffix}",
        display_name=f"builder-ext-{suffix}",
        latest_version="1.0.0",
    )
    db_session.add_all([repo, external])
    await db_session.flush()

    G = await build_dependency_graph(db_session)

    assert G.nodes[repo.canonical_id]["activity_status"] == ActivityStatus.in_dev.value
    assert G.nodes[external.canonical_id]["activity_status"] is None


async def test_build_full_graph_has_project_nodes(db_session: AsyncSession):
    result = await db_session.execute(text("SELECT COUNT(*) FROM projects"))
    if result.scalar_one() == 0:
        pytest.skip("projects table is empty; skipping project-node assertion")

    G = await build_full_graph(db_session)
    project_nodes = [n for n, d in G.nodes(data=True) if d.get("vertex_type") == "Project"]
    assert len(project_nodes) > 0, "Expected Project nodes from projects table"


async def test_build_full_graph_has_owns_edges(db_session: AsyncSession):
    # Skip when no Repo rows have project_id set (incomplete bootstrap state).
    result = await db_session.execute(text("SELECT COUNT(*) FROM repos WHERE project_id IS NOT NULL"))
    linked_count: int = result.scalar_one()
    if linked_count == 0:
        pytest.skip("No repos with project_id in DB; bootstrap pipeline not fully linked")

    G = await build_full_graph(db_session)
    owns_edges = [(u, v) for u, v, d in G.edges(data=True) if d.get("edge_type") == "owns"]
    assert len(owns_edges) > 0, "Expected Project->Repo owns edges"
