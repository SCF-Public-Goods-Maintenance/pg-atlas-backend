"""
A6: Active Subgraph Projection.

Filters the full dependency graph to the ecologically active window — the
sub-population of repositories that have had observable commit activity within
the configured window. Dormant repos are excluded from metric computation but
not from the database; this is a read-only in-memory projection.

Ecological framing: like removing seasonally dormant organisms from a trophic
web analysis — they may return, but their current influence on energy flow is
negligible. Only the living, active part of the ecosystem is scored.

Ported and adapted from:
    SCF_PG-Atlas/pg_atlas/graph/active_subgraph.py
    Author: Jay Gutierrez, PhD | SCF #41 — Building the Backbone

Production adaptations:
- Input graph carries `vertex_type` (title-cased) rather than `node_type`.
- `days_since_commit` is computed by graph_builder from `latest_commit_date`.
- Returns only `nx.DiGraph` (not a tuple) — dormant set is in graph metadata.
- Dormant repos are excluded from metric computation but not from the database.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import logging

import networkx as nx

from pg_atlas.metrics.config import DEFAULT_METRICS_CONFIG, MetricsConfig

logger = logging.getLogger(__name__)


def project_active_subgraph(
    G: nx.DiGraph,
    config: MetricsConfig = DEFAULT_METRICS_CONFIG,
) -> nx.DiGraph:
    """
    Return the induced subgraph of ecologically active nodes.

    Algorithm (O(V + E)):
        1. Classify every node as active or dormant using retention rules below.
        2. Build induced subgraph over the active node set (copy preserves edge attrs).
        3. Annotate the returned graph with audit metadata.

    Retention rules by vertex_type:
        "Project":     Always retained — funding layer is not activity-filtered.
        "Contributor": Always retained — contributor risk layer stays intact.
        "ExternalRepo": Always retained — external dependencies are critical
                        infrastructure not under direct monitoring; absence of
                        commit data is expected and must not trigger dormancy.
        "Repo" with days_since_commit <= activity_window_days AND NOT archived: active.
        "Repo" with days_since_commit is None: dormant (conservative; no data =
            cannot confirm activity).
        "Repo" that is archived: dormant.
        "Repo" with days_since_commit > activity_window_days: dormant.
        Unknown vertex_type: retained conservatively with a warning.

    Graph metadata on returned graph:
        active_window_days (int): configured dormancy threshold
        nodes_retained (int): count of active nodes in returned graph
        nodes_removed (int): count of pruned dormant nodes
        dormant_nodes (list[str]): canonical_ids of pruned nodes (audit trail)

    Returns:
        nx.DiGraph: induced subgraph copy. Mutations do not affect the original G.
        All nodes in the returned subgraph are active by virtue of graph membership.
        Callers do not need to check a flag.
    """
    window: int = config.activity_window_days

    active_nodes: list[str] = []
    dormant_nodes: list[str] = []

    for node, data in G.nodes(data=True):
        vertex_type: str = data.get("vertex_type", "")

        if vertex_type in ("Project", "Contributor"):
            active_nodes.append(node)
            continue

        if vertex_type == "ExternalRepo":
            # Always retain: external deps are not subject to activity filtering.
            active_nodes.append(node)
            continue

        if vertex_type == "Repo":
            # Pragmatic reactivation heuristic — absence of commit data ≠ permanently dormant.
            days: int | None = data.get("days_since_commit")
            archived: bool = bool(data.get("archived", False))

            if days is None:
                # No commit data — conservative: treat as dormant.
                dormant_nodes.append(node)
            elif archived:
                dormant_nodes.append(node)
            elif days <= window:
                active_nodes.append(node)
            else:
                dormant_nodes.append(node)
            continue

        # Unknown vertex_type — retain conservatively and warn.
        logger.warning(
            "project_active_subgraph: unknown vertex_type=%r on node %r — retaining",
            vertex_type,
            node,
        )
        active_nodes.append(node)

    G_active: nx.DiGraph = G.subgraph(active_nodes).copy()

    G_active.graph.update(
        active_window_days=window,
        nodes_retained=len(active_nodes),
        nodes_removed=len(dormant_nodes),
        dormant_nodes=dormant_nodes,
    )

    logger.info(
        "project_active_subgraph: %d retained, %d dormant (window=%d days)",
        len(active_nodes),
        len(dormant_nodes),
        window,
    )
    return G_active
