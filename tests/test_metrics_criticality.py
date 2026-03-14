# SPDX-FileCopyrightText: 2026 PG Atlas contributors
# SPDX-License-Identifier: MPL-2.0
"""Unit tests for pg_atlas.metrics.criticality (no database required)."""

from __future__ import annotations

import networkx as nx

from pg_atlas.metrics.criticality import compute_criticality, compute_percentile_ranks


def _repo(active: bool = True) -> dict:
    return {"vertex_type": "Repo", "active": active}


def _ext(active: bool = True) -> dict:
    return {"vertex_type": "ExternalRepo", "active": active}


# ---------------------------------------------------------------------------
# compute_criticality — chain invariant
# ---------------------------------------------------------------------------


def test_chain_a_depends_b_depends_c():
    """
    A -> B -> C (A depends on B, B depends on C).
    Chain invariant: C.criticality=2, B.criticality=1, A.criticality=0.
    """
    G = nx.DiGraph()
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
    G = nx.DiGraph()
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
    G = nx.DiGraph()
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
    G = nx.DiGraph()
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
    G = nx.DiGraph()
    G.add_node("alone", **_ext())
    scores = compute_criticality(G)
    assert scores["alone"] == 0


# ---------------------------------------------------------------------------
# compute_criticality — active flag filter
# ---------------------------------------------------------------------------


def test_inactive_dependents_not_counted():
    """Nodes with active=False must not inflate criticality scores."""
    G = nx.DiGraph()
    G.add_node("active_dep", **_repo(active=True))
    G.add_node("dormant_dep", **_repo(active=False))
    G.add_node("lib", **_ext(active=True))
    G.add_edge("active_dep", "lib")
    G.add_edge("dormant_dep", "lib")
    scores = compute_criticality(G)
    # Only active_dep should count; dormant_dep is filtered out
    assert scores["lib"] == 1


# ---------------------------------------------------------------------------
# compute_criticality — edge cases
# ---------------------------------------------------------------------------


def test_empty_graph_returns_empty():
    assert compute_criticality(nx.DiGraph()) == {}


def test_no_dep_layer_nodes_returns_empty():
    """Graphs with only Project/Contributor nodes have no dep-layer nodes."""
    G = nx.DiGraph()
    G.add_node("P", vertex_type="Project", active=True)
    G.add_node("C", vertex_type="Contributor", active=True)
    assert compute_criticality(G) == {}


# ---------------------------------------------------------------------------
# compute_percentile_ranks
# ---------------------------------------------------------------------------


def test_percentile_min_is_zero():
    pcts = compute_percentile_ranks({"A": 0, "B": 5, "C": 10})
    assert pcts["A"] == 0.0


def test_percentile_max_less_than_100():
    pcts = compute_percentile_ranks({"A": 0, "B": 5, "C": 10})
    assert all(v < 100.0 for v in pcts.values())


def test_percentile_single_element_is_zero():
    pcts = compute_percentile_ranks({"X": 42})
    assert pcts["X"] == 0.0


def test_percentile_all_same_is_zero():
    pcts = compute_percentile_ranks({"A": 5, "B": 5, "C": 5})
    assert all(v == 0.0 for v in pcts.values())


def test_percentile_ascending_for_distinct_scores():
    pcts = compute_percentile_ranks({"low": 1, "mid": 5, "high": 10})
    assert pcts["low"] < pcts["mid"] < pcts["high"]


def test_percentile_values_in_range():
    scores = {str(i): i for i in range(10)}
    pcts = compute_percentile_ranks(scores)
    assert all(0.0 <= v < 100.0 for v in pcts.values())


def test_percentile_empty_returns_empty():
    assert compute_percentile_ranks({}) == {}
