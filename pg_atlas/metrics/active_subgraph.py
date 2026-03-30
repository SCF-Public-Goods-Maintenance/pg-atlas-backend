"""
A6: Active Subgraph Projection.

Projects the repo-level dependency graph onto the active ecosystem: repo nodes
that are themselves active leaves, plus every dependency reachable upstream
from those leaves.

Ecological framing: trace energy flow from living leaves into the supporting
substrate beneath them. A dependency remains relevant if any active leaf still
relies on it, even if that dependency's own project is no longer marked live.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import logging

import networkx as nx

logger = logging.getLogger(__name__)

ACTIVE_LEAF_STATUSES = {"live", "in-dev"}
DEP_LAYER_VERTEX_TYPES = {"Repo", "ExternalRepo"}


def project_active_subgraph(G: nx.DiGraph) -> nx.DiGraph:
    """
    Return the dep-layer subgraph reachable from active repo leaves.

    Algorithm (O(V + E)):
        1. Restrict to dep-layer nodes only: Repo + ExternalRepo.
        2. Seed active leaves: Repo nodes whose provisional `activity_status`
           is `live` or `in-dev`, and whose in-degree is zero in the
           dependency graph.
        3. Traverse upstream by following original outgoing `depends_on` edges
           (dependent -> dependency). Do not reverse the graph.
        4. Return the induced subgraph over all reached dep-layer nodes.

    Graph metadata on returned graph:
        active_leaf_nodes (list[str]): canonical_ids used as traversal seeds
        nodes_retained (int): count of dep-layer nodes in the returned graph
        nodes_removed (int): count of dep-layer nodes excluded from the result

    Returns:
        nx.DiGraph: induced dep-layer subgraph copy. Mutations do not affect
        the original graph.

    Notes:
        - Input should be the repo dependency graph from `build_dependency_graph`.
        - Repo-level `activity_status` is consumed as-is from the parent
          project's current provisional status materialization.
    """
    dep_nodes = {node for node, data in G.nodes(data=True) if data.get("vertex_type") in DEP_LAYER_VERTEX_TYPES}
    G_dep = G.subgraph(dep_nodes)

    active_leaves = sorted(
        node
        for node, data in G_dep.nodes(data=True)
        if data.get("vertex_type") == "Repo"
        and data.get("activity_status") in ACTIVE_LEAF_STATUSES
        and G_dep.in_degree(node) == 0
    )

    active_nodes = set(active_leaves)
    for leaf in active_leaves:
        active_nodes.update(nx.descendants(G_dep, leaf))

    G_active: nx.DiGraph = G_dep.subgraph(active_nodes).copy()

    G_active.graph.update(
        active_leaf_nodes=active_leaves,
        nodes_retained=len(active_nodes),
        nodes_removed=G_dep.number_of_nodes() - len(active_nodes),
    )

    logger.info(
        "project_active_subgraph: %d active leaves, %d retained, %d removed",
        len(active_leaves),
        len(active_nodes),
        G_dep.number_of_nodes() - len(active_nodes),
    )

    return G_active
