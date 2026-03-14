"""
Async loaders that materialise PostgreSQL graph data into NetworkX DiGraph objects.

Two entry points:
- build_dependency_graph: dep-layer only (Repo/ExternalRepo nodes + DependsOn edges).
- build_full_graph: dep-layer + Project nodes linked to their repos.

Use build_dependency_graph as input to compute_criticality — it contains only
dependency edges, so no edge-type filtering is required in the criticality computation.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import datetime
import logging
from typing import Any

import networkx as nx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from pg_atlas.metrics.config import DEFAULT_METRICS_CONFIG, MetricsConfig

logger = logging.getLogger(__name__)

# DB enum value -> title-case label used as NetworkX node attribute
_VERTEX_TYPE_MAP: dict[str, str] = {
    "repo": "Repo",
    "external-repo": "ExternalRepo",
}

# Batch SQL: all repo_vertices with subtype columns via LEFT JOIN.
# Avoids N+1 — one round-trip fetches every node in the graph.
_NODES_SQL = text("""
    SELECT rv.canonical_id,
           rv.vertex_type,
           r.project_id,
           r.latest_commit_date,
           r.adoption_stars,
           r.adoption_forks
    FROM repo_vertices rv
    LEFT JOIN repos r ON r.id = rv.id
    LEFT JOIN external_repos er ON er.id = rv.id
""")

# Batch SQL: all dependency edges with canonical_id resolution.
# in_canonical_id depends on out_canonical_id.
_EDGES_SQL = text("""
    SELECT rv_in.canonical_id  AS in_canonical_id,
           rv_out.canonical_id AS out_canonical_id,
           d.confidence,
           d.version_range
    FROM depends_on d
    JOIN repo_vertices rv_in  ON rv_in.id  = d.in_vertex_id
    JOIN repo_vertices rv_out ON rv_out.id = d.out_vertex_id
""")

_PROJECTS_SQL = text("""
    SELECT p.id, p.canonical_id, p.display_name
    FROM projects p
""")


async def build_dependency_graph(
    session: AsyncSession,
    config: MetricsConfig = DEFAULT_METRICS_CONFIG,
    reference_date: datetime.date | None = None,
) -> nx.DiGraph:
    """
    Load all RepoVertex nodes and DependsOn edges from PostgreSQL into a NetworkX DiGraph.

    Node key: canonical_id (DAOIP-5 URI string).

    Node attributes:
        vertex_type (str): "Repo" | "ExternalRepo"  (title-cased from DB enum)
        project_id (int | None): FK to projects (Repo only; None for ExternalRepo)
        latest_commit_date (datetime.date | None): last known commit date (Repo only)
        days_since_commit (int | None): (reference_date - latest_commit_date).days;
            None when latest_commit_date is None
        adoption_stars (int): GitHub star count (Repo only; 0 when absent)
        adoption_forks (int): GitHub fork count (Repo only; 0 when absent)

    Edge direction: in_vertex -> out_vertex means "in_vertex depends on out_vertex".

    Edge attributes:
        confidence (str): "verified-sbom" | "inferred-shadow"
        version_range (str | None): semver constraint, if recorded

    Uses two batch SQL queries (no ORM selectinload) to avoid N+1 round-trips.
    """
    ref = reference_date or datetime.date.today()

    G: nx.DiGraph = nx.DiGraph()
    G.graph["source"] = "postgresql"
    G.graph["reference_date"] = ref.isoformat()

    rows = (await session.execute(_NODES_SQL)).mappings().all()
    for row in rows:
        canonical_id: str = row["canonical_id"]
        raw_type: str = row["vertex_type"]
        vertex_type = _VERTEX_TYPE_MAP.get(raw_type, raw_type)

        commit_dt: datetime.datetime | None = row["latest_commit_date"]
        commit_date: datetime.date | None = commit_dt.date() if commit_dt is not None else None
        days_since: int | None = (ref - commit_date).days if commit_date is not None else None

        attrs: dict[str, Any] = {
            "vertex_type": vertex_type,
            "project_id": row["project_id"],
            "latest_commit_date": commit_date,
            "days_since_commit": days_since,
            "adoption_stars": row["adoption_stars"] or 0,
            "adoption_forks": row["adoption_forks"] or 0,
        }
        G.add_node(canonical_id, **attrs)

    edge_rows = (await session.execute(_EDGES_SQL)).mappings().all()
    for row in edge_rows:
        G.add_edge(
            row["in_canonical_id"],
            row["out_canonical_id"],
            confidence=row["confidence"],
            version_range=row["version_range"],
        )

    logger.info(
        "build_dependency_graph: %d nodes, %d edges (reference_date=%s)",
        G.number_of_nodes(),
        G.number_of_edges(),
        ref.isoformat(),
    )
    return G


async def build_full_graph(
    session: AsyncSession,
    config: MetricsConfig = DEFAULT_METRICS_CONFIG,
    reference_date: datetime.date | None = None,
) -> nx.DiGraph:
    """
    Build combined graph: dependency layer + Project nodes.

    Extends build_dependency_graph with Project nodes linked to their repos
    via directed 'owns' edges (Project -> Repo).

    Note: The Contributor/ContributedTo layer is not yet populated in the DB
    (future PR). When contributor edges are added, this function should be
    extended to load them here.

    Warning: When passing this graph to compute_criticality, use
    build_dependency_graph instead to avoid contributor edges in the subgraph.
    """
    G = await build_dependency_graph(session, config, reference_date)

    project_rows = (await session.execute(_PROJECTS_SQL)).mappings().all()
    for row in project_rows:
        G.add_node(
            row["canonical_id"],
            vertex_type="Project",
            project_id=row["id"],
            display_name=row["display_name"],
        )

    # Build project_id -> project canonical_id lookup (O(n)) before linking
    proj_id_to_canonical: dict[int, str] = {}
    for p_node, p_data in G.nodes(data=True):
        if p_data.get("vertex_type") == "Project":
            pid = p_data.get("project_id")
            if pid is not None:
                proj_id_to_canonical[int(pid)] = p_node

    # Link each Repo to its Project via an ownership edge
    for node, data in G.nodes(data=True):
        if data.get("vertex_type") == "Repo":
            pid = data.get("project_id")
            if pid is not None and pid in proj_id_to_canonical:
                G.add_edge(proj_id_to_canonical[pid], node, edge_type="owns")

    logger.info(
        "build_full_graph: %d nodes, %d edges (%d projects)",
        G.number_of_nodes(),
        G.number_of_edges(),
        len(proj_id_to_canonical),
    )
    return G
