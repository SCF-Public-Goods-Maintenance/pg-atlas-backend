"""
A9: Transitive Criticality.

Measures structural indispensability: how many ecosystem packages transitively
depend on a given package. High criticality = high ecosystem value built upon
this package — many packages in the portfolio depend on it, directly or
transitively.

Ecological framing: a keystone species that the rest of the food web is built
upon — not modelled by its removal impact, but by how much biomass is stacked
on top of it.

Algorithm: BFS on the reversed dependency subgraph.
The dependency graph has edges pointing toward dependencies (A -> B means
"A depends on B"). To count dependents, we reverse the graph and run BFS
from each package — the set of nodes reachable from P in the reversed graph
is exactly the set of packages that transitively depend on P.

Chain invariant: for A -> B -> C (A depends on B, B depends on C):
    C.criticality = 2, B.criticality = 1, A.criticality = 0

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import logging

import networkx as nx

logger = logging.getLogger(__name__)


def compute_criticality(G_active: nx.DiGraph[str]) -> dict[str, int]:
    """
    Count transitive active dependents for every dep-layer node via BFS.

    Algorithm (A9):
        1. Filter dep-layer nodes: vertex_type in {"Repo", "ExternalRepo"}.
        2. Build dep subgraph (induced over dep-layer nodes; all edges included
           when input is from build_dependency_graph).
        3. Reverse: G_rev edges flow from depended-upon package to its dependents.
        4. For each dep-layer node P:
               transitive_dependents = nx.descendants(G_rev, P)
               criticality[P]        = len(transitive_dependents)
           (all nodes in the active subgraph are active by graph membership)

    Input:
        Use ``build_dependency_graph`` (not ``build_full_graph``) as source — the
        dep-only graph has no contributor or ownership edges, so no edge filtering
        is necessary here.

    Complexity:
        O(V * (V + E)) worst case. In practice, power-law degree distribution
        (most nodes are leaves) makes BFS fast for the majority of nodes.

    Returns:
        dict[canonical_id, int] — 0 for nodes with no active transitive dependents.
        Empty dict if no dep-layer nodes found.
    """
    dep_nodes: set[str] = {n for n, d in G_active.nodes(data=True) if d.get("vertex_type") in ("Repo", "ExternalRepo")}

    if not dep_nodes:
        logger.warning("compute_criticality: no dep-layer nodes (Repo/ExternalRepo) found in graph")
        return {}

    # Induced subgraph on dep-layer nodes — preserves all edges between them.
    G_dep = G_active.subgraph(dep_nodes)
    # Reverse: edges now flow FROM depended-upon package TOWARD its dependents.
    G_rev = G_dep.reverse(copy=True)

    criticality: dict[str, int] = {}
    for node in dep_nodes:
        transitive_dependents = nx.descendants(G_rev, node)
        criticality[node] = len(transitive_dependents)

    nonzero = sum(1 for v in criticality.values() if v > 0)
    logger.info(
        f"compute_criticality: scored {len(criticality)} nodes; max={max(criticality.values(), default=0)}, nonzero={nonzero}"
    )
    return criticality
