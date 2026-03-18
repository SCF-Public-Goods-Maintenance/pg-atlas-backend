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
from collections.abc import Sequence
from typing import Any

import networkx as nx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from pg_atlas.db_models.depends_on import DependsOn
from pg_atlas.db_models.project import Project
from pg_atlas.db_models.repo_vertex import ExternalRepo, Repo, RepoVertex
from pg_atlas.metrics.config import DEFAULT_METRICS_CONFIG, MetricsConfig

logger = logging.getLogger(__name__)


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

    Uses ORM queries with selectin loading (JTI auto-JOIN for vertices;
    selectin batch-loads for edges and projects). No N+1 round-trips.
    """
    ref = reference_date or datetime.date.today()

    G: nx.DiGraph = nx.DiGraph()
    G.graph["source"] = "postgresql"
    G.graph["reference_date"] = ref.isoformat()

    vertices: Sequence[RepoVertex] = (await session.execute(select(RepoVertex))).scalars().all()
    for rv in vertices:
        if isinstance(rv, Repo):
            commit_dt = rv.latest_commit_date
            commit_date = commit_dt.date() if commit_dt is not None else None
            days_since: int | None = (ref - commit_date).days if commit_date is not None else None
            project_type: str | None = rv.project.project_type.value if rv.project else None
            attrs: dict[str, Any] = {
                "vertex_type": "Repo",
                "project_id": rv.project_id,
                "project_type": project_type,
                "latest_commit_date": commit_date,
                "days_since_commit": days_since,
                "adoption_stars": rv.adoption_stars or 0,
                "adoption_forks": rv.adoption_forks or 0,
            }
        elif isinstance(rv, ExternalRepo):
            attrs = {
                "vertex_type": "ExternalRepo",
                "project_id": None,
                "project_type": None,
                "latest_commit_date": None,
                "days_since_commit": None,
                "adoption_stars": 0,
                "adoption_forks": 0,
            }
        else:
            attrs = {"vertex_type": rv.vertex_type}
        G.add_node(rv.canonical_id, **attrs)

    edges: Sequence[DependsOn] = (await session.execute(select(DependsOn))).scalars().all()
    for edge in edges:
        G.add_edge(
            edge.in_node.canonical_id,
            edge.out_node.canonical_id,
            confidence=edge.confidence.value,
            version_range=edge.version_range,
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

    projects: Sequence[Project] = (await session.execute(select(Project))).scalars().all()
    for proj in projects:
        G.add_node(
            proj.canonical_id,
            vertex_type="Project",
            project_id=proj.id,
            display_name=proj.display_name,
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
