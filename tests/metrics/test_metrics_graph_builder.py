"""
Integration tests for pg_atlas.metrics.graph_builder (requires database).

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import networkx as nx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession


async def test_build_dependency_graph_returns_digraph(db_session: AsyncSession):
    from pg_atlas.metrics.graph_builder import build_dependency_graph

    G = await build_dependency_graph(db_session)
    assert isinstance(G, nx.DiGraph)


async def test_build_dependency_graph_has_nodes(db_session: AsyncSession):
    from sqlalchemy import text

    from pg_atlas.metrics.graph_builder import build_dependency_graph

    result = await db_session.execute(text("SELECT COUNT(*) FROM repo_vertices"))
    if result.scalar_one() == 0:
        pytest.skip("repo_vertices table is empty; skipping data-presence assertion")

    G = await build_dependency_graph(db_session)
    assert G.number_of_nodes() > 0, "Expected populated repo_vertices table"


async def test_build_dependency_graph_node_keys_are_strings(db_session: AsyncSession):
    from pg_atlas.metrics.graph_builder import build_dependency_graph

    G = await build_dependency_graph(db_session)
    for node in G.nodes():
        assert isinstance(node, str), f"Node key must be str (canonical_id), got {type(node)}"


async def test_build_dependency_graph_vertex_type_title_cased(db_session: AsyncSession):
    """DB enum values ('repo', 'external-repo') must be mapped to title-case labels."""
    from pg_atlas.metrics.graph_builder import build_dependency_graph

    G = await build_dependency_graph(db_session)
    valid_types = {"Repo", "ExternalRepo"}
    for node, data in G.nodes(data=True):
        vt = data.get("vertex_type")
        assert vt in valid_types, f"Node {node!r}: unexpected vertex_type={vt!r}"


async def test_build_dependency_graph_metadata(db_session: AsyncSession):
    from pg_atlas.metrics.graph_builder import build_dependency_graph

    G = await build_dependency_graph(db_session)
    assert G.graph.get("source") == "postgresql"
    assert "reference_date" in G.graph


async def test_build_dependency_graph_days_since_commit_type(db_session: AsyncSession):
    """days_since_commit must be int or None, never a datetime or float."""
    from pg_atlas.metrics.graph_builder import build_dependency_graph

    G = await build_dependency_graph(db_session)
    for node, data in G.nodes(data=True):
        dsc = data.get("days_since_commit")
        assert dsc is None or isinstance(dsc, int), f"days_since_commit must be int or None, got {type(dsc)} on {node!r}"


async def test_build_full_graph_has_project_nodes(db_session: AsyncSession):
    from sqlalchemy import text

    from pg_atlas.metrics.graph_builder import build_full_graph

    result = await db_session.execute(text("SELECT COUNT(*) FROM projects"))
    if result.scalar_one() == 0:
        pytest.skip("projects table is empty; skipping project-node assertion")

    G = await build_full_graph(db_session)
    project_nodes = [n for n, d in G.nodes(data=True) if d.get("vertex_type") == "Project"]
    assert len(project_nodes) > 0, "Expected Project nodes from projects table"


async def test_build_full_graph_has_owns_edges(db_session: AsyncSession):
    from sqlalchemy import text

    from pg_atlas.metrics.graph_builder import build_full_graph

    # Skip when no Repo rows have project_id set (incomplete bootstrap state).
    result = await db_session.execute(text("SELECT COUNT(*) FROM repos WHERE project_id IS NOT NULL"))
    linked_count: int = result.scalar_one()
    if linked_count == 0:
        pytest.skip("No repos with project_id in DB; bootstrap pipeline not fully linked")

    G = await build_full_graph(db_session)
    owns_edges = [(u, v) for u, v, d in G.edges(data=True) if d.get("edge_type") == "owns"]
    assert len(owns_edges) > 0, "Expected Project->Repo owns edges"
