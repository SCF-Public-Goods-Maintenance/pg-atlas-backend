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
