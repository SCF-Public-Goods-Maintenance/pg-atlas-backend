"""
A9: Transitive Criticality.

Measures structural indispensability: how many active ecosystem packages
transitively depend on a given package. High criticality = high blast radius
if this package degrades or disappears from the ecosystem.

Ecological framing: a keystone species whose removal causes cascading trophic
collapse in downstream consumers. Criticality counts the active organisms that
depend — directly or transitively — on this one package.

Algorithm: BFS on the reversed dependency subgraph.
The dependency graph has edges pointing toward dependencies (A -> B means
"A depends on B"). To count dependents, we reverse the graph and run BFS
from each package — the set of nodes reachable from P in the reversed graph
is exactly the set of packages that transitively depend on P.

Chain invariant: for A -> B -> C (A depends on B, B depends on C):
    C.criticality = 2, B.criticality = 1, A.criticality = 0

Ported and adapted from:
    SCF_PG-Atlas/pg_atlas/metrics/criticality.py
    Author: Jay Gutierrez, PhD | SCF #41 — Building the Backbone

Production adaptations:
- Node attribute is `vertex_type` (title-cased) rather than `node_type`.
- No `edge_type` filter needed — use `build_dependency_graph` as input
  (dep-only graph; no contributor edges present).
- `active=True` flag is set by `project_active_subgraph` on all retained nodes.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import logging

import networkx as nx
import numpy as np

logger = logging.getLogger(__name__)


def compute_criticality(G_active: nx.DiGraph) -> dict[str, int]:
    """
    Count transitive active dependents for every dep-layer node via BFS.

    Algorithm (A9):
        1. Filter dep-layer nodes: vertex_type in {"Repo", "ExternalRepo"}.
        2. Build dep subgraph (induced over dep-layer nodes; all edges included
           when input is from build_dependency_graph).
        3. Reverse: G_rev edges flow from depended-upon package to its dependents.
        4. For each dep-layer node P:
               transitive_dependents = nx.descendants(G_rev, P)
               active_dependents     = {n for n in transitive_dependents
                                        if G_active.nodes[n].get("active", False)}
               criticality[P]        = len(active_dependents)

    Precondition:
        G_active must have ``active=True`` set on all nodes. This is guaranteed
        when G_active is the output of ``project_active_subgraph``. Without this
        flag, all criticality scores will be 0.

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
        active_dependents = {n for n in transitive_dependents if G_active.nodes[n].get("active", False)}
        criticality[node] = len(active_dependents)

    nonzero = sum(1 for v in criticality.values() if v > 0)
    logger.info(
        "compute_criticality: scored %d nodes; max=%d, nonzero=%d",
        len(criticality),
        max(criticality.values(), default=0),
        nonzero,
    )
    return criticality


def compute_percentile_ranks(scores: dict[str, int | float]) -> dict[str, float]:
    """
    Convert raw scores to percentile ranks within [0.0, 100.0).

    Uses numpy searchsorted (exclusive / left-side rank):
        rank        = searchsorted(sorted_scores, score)   # 0-based count of scores < this score
        percentile  = rank / n * 100.0

    Properties:
        - Minimum score  -> 0th percentile (rank = 0).
        - Maximum score  -> (n-1)/n * 100 < 100 (no node is universally top-ranked).
        - Ties           -> all tied values receive the same (lowest) percentile.
        - Single element -> 0th percentile.

    Ecological intent: avoids the illusion that any single package is
    unconditionally "top-ranked" — the ecosystem is always the reference frame.
    Consistent with scipy.stats.percentileofscore(kind="weak") minus 100 ceiling.

    Returns:
        dict[canonical_id, float] — all values in [0.0, 100.0).
        Empty dict when scores is empty.
    """
    if not scores:
        return {}

    all_scores = np.array(list(scores.values()), dtype=np.float64)
    sorted_scores = np.sort(all_scores)
    n = len(sorted_scores)

    percentiles: dict[str, float] = {}
    for node, score in scores.items():
        rank = int(np.searchsorted(sorted_scores, float(score)))
        percentiles[node] = rank / n * 100.0

    return percentiles
