"""
Unit tests for pg_atlas.metrics.active_subgraph.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import networkx as nx
import pytest

from pg_atlas.metrics.active_subgraph import project_active_subgraph


def _repo(activity_status: str | None = None, **overrides: object) -> dict[str, object]:
    """Return repo-node attributes for pure graph tests."""
    attrs: dict[str, object] = {"vertex_type": "Repo", "activity_status": activity_status}
    attrs.update(overrides)

    return attrs


def _ext(**overrides: object) -> dict[str, object]:
    """Return external-repo node attributes for pure graph tests."""
    attrs: dict[str, object] = {"vertex_type": "ExternalRepo", "activity_status": None}
    attrs.update(overrides)

    return attrs


def _project(**overrides: object) -> dict[str, object]:
    """Return project-node attributes for weird-input tests."""
    attrs: dict[str, object] = {"vertex_type": "Project"}
    attrs.update(overrides)

    return attrs


def _contributor(**overrides: object) -> dict[str, object]:
    """Return contributor-node attributes for weird-input tests."""
    attrs: dict[str, object] = {"vertex_type": "Contributor"}
    attrs.update(overrides)

    return attrs


def test_mixed_active_and_inactive_leaves_keep_only_reachable_component():
    G: nx.DiGraph[str] = nx.DiGraph()
    G.add_node("leaf_live", **_repo("live"))
    G.add_node("shared_dep", **_repo("discontinued"))
    G.add_node("leaf_dead", **_repo("discontinued"))
    G.add_node("dead_dep", **_ext())
    G.add_edge("leaf_live", "shared_dep")
    G.add_edge("leaf_dead", "dead_dep")

    G_active = project_active_subgraph(G)

    assert set(G_active.nodes()) == {"leaf_live", "shared_dep"}
    assert set(G_active.edges()) == {("leaf_live", "shared_dep")}


def test_active_leaf_retains_inactive_dependencies():
    G: nx.DiGraph[str] = nx.DiGraph()
    G.add_node("leaf_live", **_repo("live"))
    G.add_node("inactive_internal", **_repo("discontinued"))
    G.add_node("external_dep", **_ext())
    G.add_edge("leaf_live", "inactive_internal")
    G.add_edge("inactive_internal", "external_dep")

    G_active = project_active_subgraph(G)

    assert set(G_active.nodes()) == {"leaf_live", "inactive_internal", "external_dep"}
    assert set(G_active.edges()) == {
        ("leaf_live", "inactive_internal"),
        ("inactive_internal", "external_dep"),
    }


@pytest.mark.parametrize("middle_count", [1, 2, 3])
def test_active_path_retains_inactive_middle_repos(middle_count: int):
    G: nx.DiGraph[str] = nx.DiGraph()
    G.add_node("leaf_live", **_repo("live"))
    previous = "leaf_live"

    for idx in range(middle_count):
        middle = f"middle_{idx}"
        G.add_node(middle, **_repo("discontinued"))
        G.add_edge(previous, middle)
        previous = middle

    G.add_node("tail_live", **_repo("live"))
    G.add_edge(previous, "tail_live")

    G_active = project_active_subgraph(G)

    expected_nodes = {"leaf_live", "tail_live"} | {f"middle_{idx}" for idx in range(middle_count)}
    assert set(G_active.nodes()) == expected_nodes


def test_disconnected_components_only_keep_active_leaf_component():
    G: nx.DiGraph[str] = nx.DiGraph()
    G.add_node("leaf_live", **_repo("in-dev"))
    G.add_node("live_dep", **_ext())
    G.add_node("leaf_dead", **_repo("non-responsive"))
    G.add_node("dead_dep", **_repo("live"))
    G.add_edge("leaf_live", "live_dep")
    G.add_edge("leaf_dead", "dead_dep")

    G_active = project_active_subgraph(G)

    assert set(G_active.nodes()) == {"leaf_live", "live_dep"}
    assert set(G_active.edges()) == {("leaf_live", "live_dep")}


def test_no_active_leaves_returns_empty_dep_layer_subgraph():
    G: nx.DiGraph[str] = nx.DiGraph()
    G.add_node("leaf_dead", **_repo("discontinued"))
    G.add_node("inactive_dep", **_repo("non-responsive"))
    G.add_node("external_dep", **_ext())
    G.add_edge("leaf_dead", "inactive_dep")
    G.add_edge("inactive_dep", "external_dep")

    G_active = project_active_subgraph(G)

    assert G_active.number_of_nodes() == 0
    assert G_active.number_of_edges() == 0


@pytest.mark.parametrize("activity_status", [None, "mystery"])
def test_repo_with_missing_or_unknown_activity_status_does_not_seed(activity_status: str | None):
    G: nx.DiGraph[str] = nx.DiGraph()
    G.add_node("repo", **_repo(activity_status))

    G_active = project_active_subgraph(G)

    assert set(G_active.nodes()) == set()


def test_non_dependency_nodes_are_ignored():
    G: nx.DiGraph[str] = nx.DiGraph()
    G.add_node("leaf_live", **_repo("live"))
    G.add_node("dep", **_ext())
    G.add_node("project", **_project())
    G.add_node("contributor", **_contributor())
    G.add_edge("project", "leaf_live")
    G.add_edge("leaf_live", "dep")
    G.add_edge("contributor", "leaf_live")

    G_active = project_active_subgraph(G)

    assert set(G_active.nodes()) == {"leaf_live", "dep"}
    assert set(G_active.edges()) == {("leaf_live", "dep")}


def test_external_repo_with_bogus_activity_status_does_not_seed():
    G: nx.DiGraph[str] = nx.DiGraph()
    G.add_node("external", **_ext(activity_status="live"))

    G_active = project_active_subgraph(G)

    assert G_active.number_of_nodes() == 0


def test_reachable_cycle_is_retained_without_infinite_traversal():
    G: nx.DiGraph[str] = nx.DiGraph()
    G.add_node("leaf_live", **_repo("live"))
    G.add_node("cycle_repo", **_repo("discontinued"))
    G.add_node("cycle_ext", **_ext())
    G.add_edge("leaf_live", "cycle_repo")
    G.add_edge("cycle_repo", "cycle_ext")
    G.add_edge("cycle_ext", "cycle_repo")

    G_active = project_active_subgraph(G)

    assert set(G_active.nodes()) == {"leaf_live", "cycle_repo", "cycle_ext"}
    assert set(G_active.edges()) == {
        ("leaf_live", "cycle_repo"),
        ("cycle_repo", "cycle_ext"),
        ("cycle_ext", "cycle_repo"),
    }


def test_edge_attributes_preserved():
    G: nx.DiGraph[str] = nx.DiGraph()
    G.add_node("leaf_live", **_repo("live"))
    G.add_node("dep", **_ext())
    G.add_edge("leaf_live", "dep", confidence="verified-sbom", version_range=">=1.0")

    G_active = project_active_subgraph(G)

    assert G_active["leaf_live"]["dep"]["confidence"] == "verified-sbom"
    assert G_active["leaf_live"]["dep"]["version_range"] == ">=1.0"


def test_returns_copy_mutations_dont_affect_original():
    G: nx.DiGraph[str] = nx.DiGraph()
    G.add_node("leaf_live", **_repo("live"))

    G_active = project_active_subgraph(G)
    G_active.add_node("injected")

    assert "injected" not in G
