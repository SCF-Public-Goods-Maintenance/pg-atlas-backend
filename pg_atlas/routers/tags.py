"""
OpenAPI tag enums for the PG Atlas public REST API.

Two orthogonal tag axes — ``Graph`` (which part of the dependency graph) and
``Source`` (which upstream data source) — are applied to every route via the
``tags=`` parameter.  This gives the Swagger UI meaningful grouping and tells
API consumers exactly where each piece of data comes from.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import enum


class Graph(str, enum.Enum):
    """
    Tags that describe which part of the dependency graph an endpoint touches.

    Used in ``tags=`` on route decorators so the Swagger UI groups endpoints
    by graph domain.
    """

    metadata = "Graph: Metadata"
    projects = "Graph: Projects"
    repos = "Graph: Repos"
    contributors = "Graph: Contributors"
    dependency_graph = "Graph: Dependency Graph"
    contributor_graph = "Graph: Contributor Graph"


class Source(str, enum.Enum):
    """
    Tags that indicate the upstream data source(s) an endpoint draws from.

    Helps consumers understand data provenance at a glance.
    """

    opengrants = "Source: OpenGrants"
    deps_dev = "Source: deps.dev"
    github = "Source: GitHub"
    pg_atlas = "Source: PG Atlas"


# ---------------------------------------------------------------------------
# OpenAPI tag metadata
# ---------------------------------------------------------------------------


TAGS_METADATA: list[dict[str, str]] = [
    {
        "name": Graph.metadata,
        "description": "Ecosystem-wide summary statistics and health indicators.",
    },
    {
        "name": Graph.projects,
        "description": "SCF-funded projects — list, detail, and associated repos.",
    },
    {
        "name": Graph.repos,
        "description": "Git repositories (in-ecosystem and external dependencies).",
    },
    {
        "name": Graph.contributors,
        "description": "Individual contributors and their commit activity.",
    },
    {
        "name": Graph.dependency_graph,
        "description": "Dependency and reverse-dependency edges between vertices.",
    },
    {
        "name": Graph.contributor_graph,
        "description": "Contributor-to-repository contribution edges.",
    },
    {
        "name": Source.opengrants,
        "description": "Data sourced from the SCF OpenGrants registry.",
    },
    {
        "name": Source.deps_dev,
        "description": "Data sourced from the deps.dev dependency graph.",
    },
    {
        "name": Source.github,
        "description": "Data sourced from the GitHub API (repos, contributors, git logs).",
    },
    {
        "name": Source.pg_atlas,
        "description": "Data computed or curated by PG Atlas itself (metrics, scores).",
    },
    {
        "name": "ingestion",
        "description": "Write endpoints for data submissions (OIDC-authenticated).",
    },
    {
        "name": "health",
        "description": "Operational health checks.",
    },
]
