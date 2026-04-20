"""
Unit tests for pg_atlas.metrics.criticality (no database required).

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import networkx as nx

from pg_atlas.metrics.criticality import compute_criticality


def _repo() -> dict[str, str]:
    return {"vertex_type": "Repo"}


def _ext() -> dict[str, str]:
    return {"vertex_type": "ExternalRepo"}


# ---------------------------------------------------------------------------
# compute_criticality — chain invariant
# ---------------------------------------------------------------------------


def test_chain_a_depends_b_depends_c():
    """
    A -> B -> C (A depends on B, B depends on C).
    Chain invariant: C.criticality=2, B.criticality=1, A.criticality=0.
    """
    G: nx.DiGraph[str] = nx.DiGraph()
    G.add_node("A", **_repo())
    G.add_node("B", **_repo())
    G.add_node("C", **_ext())
    G.add_edge("A", "B")
    G.add_edge("B", "C")
    scores = compute_criticality(G)
    assert scores["C"] == 2
    assert scores["B"] == 1
    assert scores["A"] == 0


def test_direct_dependency_only():
    """A -> B: B.criticality=1, A.criticality=0."""
    G: nx.DiGraph[str] = nx.DiGraph()
    G.add_node("A", **_repo())
    G.add_node("B", **_ext())
    G.add_edge("A", "B")
    scores = compute_criticality(G)
    assert scores["B"] == 1
    assert scores["A"] == 0


# ---------------------------------------------------------------------------
# compute_criticality — topology variants
# ---------------------------------------------------------------------------


def test_star_hub_criticality():
    """Hub depended on by n leaves -> hub.criticality = n."""
    G: nx.DiGraph[str] = nx.DiGraph()
    G.add_node("hub", **_ext())
    leaves = [f"leaf_{i}" for i in range(4)]
    for leaf in leaves:
        G.add_node(leaf, **_repo())
        G.add_edge(leaf, "hub")
    scores = compute_criticality(G)
    assert scores["hub"] == 4
    for leaf in leaves:
        assert scores[leaf] == 0


def test_disconnected_components_scored_independently():
    G: nx.DiGraph[str] = nx.DiGraph()
    G.add_node("A", **_repo())
    G.add_node("B", **_ext())
    G.add_node("X", **_repo())
    G.add_node("Y", **_ext())
    G.add_edge("A", "B")
    G.add_edge("X", "Y")
    scores = compute_criticality(G)
    assert scores["B"] == 1
    assert scores["Y"] == 1
    assert scores["A"] == 0
    assert scores["X"] == 0


def test_isolated_node_has_zero_criticality():
    G: nx.DiGraph[str] = nx.DiGraph()
    G.add_node("alone", **_ext())
    scores = compute_criticality(G)
    assert scores["alone"] == 0


# ---------------------------------------------------------------------------
# compute_criticality — active flag filter
# ---------------------------------------------------------------------------


def test_all_graph_members_count_toward_criticality():
    """All nodes in the active subgraph count toward criticality — graph membership is sufficient."""
    G: nx.DiGraph[str] = nx.DiGraph()
    G.add_node("dep_a", **_repo())
    G.add_node("dep_b", **_repo())
    G.add_node("lib", **_ext())
    G.add_edge("dep_a", "lib")
    G.add_edge("dep_b", "lib")
    scores = compute_criticality(G)
    assert scores["lib"] == 2  # both dependents count


# ---------------------------------------------------------------------------
# compute_criticality — edge cases
# ---------------------------------------------------------------------------


def test_empty_graph_returns_empty():
    assert compute_criticality(nx.DiGraph()) == {}


def test_no_dep_layer_nodes_returns_empty():
    """Graphs with only Project/Contributor nodes have no dep-layer nodes."""
    G: nx.DiGraph[str] = nx.DiGraph()
    G.add_node("P", vertex_type="Project")
    G.add_node("C", vertex_type="Contributor")
    assert compute_criticality(G) == {}
