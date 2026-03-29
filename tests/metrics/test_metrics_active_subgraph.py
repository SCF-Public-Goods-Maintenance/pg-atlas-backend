"""
Unit tests for pg_atlas.metrics.active_subgraph (no database required).

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import networkx as nx

from pg_atlas.metrics.active_subgraph import project_active_subgraph
from pg_atlas.metrics.config import MetricsConfig

WINDOW = 90
CFG = MetricsConfig(activity_window_days=WINDOW)


def _g(*nodes: tuple) -> nx.DiGraph:
    """Build a DiGraph from (node_id, attr_dict) tuples."""
    G = nx.DiGraph()
    for node_id, attrs in nodes:
        G.add_node(node_id, **attrs)
    return G


# ---------------------------------------------------------------------------
# Repo retention
# ---------------------------------------------------------------------------


def test_active_repo_retained():
    G = _g(("A", {"vertex_type": "Repo", "days_since_commit": 30}))
    assert "A" in project_active_subgraph(G, CFG)


def test_dormant_repo_pruned():
    G = _g(("A", {"vertex_type": "Repo", "days_since_commit": 91}))
    assert "A" not in project_active_subgraph(G, CFG)


def test_boundary_exactly_at_window_is_active():
    """days_since_commit == window (90) -> ACTIVE (<=, not <)."""
    G = _g(("A", {"vertex_type": "Repo", "days_since_commit": WINDOW}))
    assert "A" in project_active_subgraph(G, CFG)


def test_repo_with_no_commit_date_is_dormant():
    """Repo with days_since_commit=None -> dormant (conservative; no data)."""
    G = _g(("A", {"vertex_type": "Repo", "days_since_commit": None}))
    assert "A" not in project_active_subgraph(G, CFG)


def test_archived_repo_is_dormant():
    G = _g(("A", {"vertex_type": "Repo", "days_since_commit": 5, "archived": True}))
    assert "A" not in project_active_subgraph(G, CFG)


# ---------------------------------------------------------------------------
# ExternalRepo retention
# ---------------------------------------------------------------------------


def test_external_repo_retained_when_no_commit_date():
    G = _g(("E", {"vertex_type": "ExternalRepo", "days_since_commit": None}))
    assert "E" in project_active_subgraph(G, CFG)


def test_external_repo_retained_even_if_over_window():
    """ExternalRepo with stale commit data is always retained."""
    G = _g(("E", {"vertex_type": "ExternalRepo", "days_since_commit": 200}))
    assert "E" in project_active_subgraph(G, CFG)


# ---------------------------------------------------------------------------
# Project / Contributor retention
# ---------------------------------------------------------------------------


def test_project_always_retained():
    G = _g(("P", {"vertex_type": "Project"}))
    assert "P" in project_active_subgraph(G, CFG)


def test_contributor_always_retained():
    G = _g(("C", {"vertex_type": "Contributor"}))
    assert "C" in project_active_subgraph(G, CFG)


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_empty_graph_returns_empty():
    G_active = project_active_subgraph(nx.DiGraph(), CFG)
    assert G_active.number_of_nodes() == 0
    assert G_active.number_of_edges() == 0


def test_edge_attributes_preserved():
    """Induced subgraph must carry through all original edge attributes."""
    G = nx.DiGraph()
    G.add_node("A", vertex_type="Repo", days_since_commit=10)
    G.add_node("B", vertex_type="ExternalRepo", days_since_commit=None)
    G.add_edge("A", "B", confidence="verified-sbom", version_range=">=1.0")
    G_active = project_active_subgraph(G, CFG)
    assert G_active["A"]["B"]["confidence"] == "verified-sbom"
    assert G_active["A"]["B"]["version_range"] == ">=1.0"


# ---------------------------------------------------------------------------
# Graph metadata
# ---------------------------------------------------------------------------


def test_graph_metadata_populated():
    G = _g(
        ("A", {"vertex_type": "Repo", "days_since_commit": 10}),
        ("B", {"vertex_type": "Repo", "days_since_commit": 100}),
    )
    G_active = project_active_subgraph(G, CFG)
    assert G_active.graph["active_window_days"] == WINDOW
    assert G_active.graph["nodes_retained"] == 1
    assert G_active.graph["nodes_removed"] == 1
    assert "B" in G_active.graph["dormant_nodes"]


def test_graph_metadata_dormant_nodes_list():
    G = _g(
        ("A", {"vertex_type": "Repo", "days_since_commit": 5}),
        ("B", {"vertex_type": "Repo", "days_since_commit": 95}),
        ("C", {"vertex_type": "Repo", "days_since_commit": None}),
    )
    G_active = project_active_subgraph(G, CFG)
    dormant = G_active.graph["dormant_nodes"]
    assert "B" in dormant
    assert "C" in dormant
    assert "A" not in dormant


# ---------------------------------------------------------------------------
# Immutability
# ---------------------------------------------------------------------------


def test_returns_copy_mutations_dont_affect_original():
    G = _g(("A", {"vertex_type": "Repo", "days_since_commit": 10}))
    G_active = project_active_subgraph(G, CFG)
    G_active.add_node("INJECTED")
    assert "INJECTED" not in G
