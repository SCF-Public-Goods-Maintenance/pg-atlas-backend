"""
FastAPI application factory for PG Atlas.

Creates and configures the FastAPI app instance, registers routers, and defines
the application lifespan (startup/shutdown hooks). Database and graph engine
initialization will be added here as later deliverables are completed.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import importlib.metadata
import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI

from pg_atlas.config import settings
from pg_atlas.routers import contributors, health, ingestion, metadata, projects, repos
from pg_atlas.routers.tags import Graph, Source

logging.basicConfig(level=settings.LOG_LEVEL)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# OpenAPI tag metadata
# ---------------------------------------------------------------------------


tags_metadata: list[dict[str, str]] = [
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


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Application lifespan manager.

    Startup and shutdown hooks live here. Database connection pool setup (A2)
    and NetworkX graph loading (A6/A8) will be added in later deliverables.
    """
    logger.info(f"PG Atlas starting up (API_URL={settings.API_URL})")
    yield
    logger.info("PG Atlas shutting down")


# ---------------------------------------------------------------------------
# App instance
# ---------------------------------------------------------------------------

try:
    _version = importlib.metadata.version("pg-atlas-backend")
except importlib.metadata.PackageNotFoundError:
    _version = "dev"

_description = """\
Public, read-only REST API exposing the **PG Atlas** dependency-graph dataset
for [Stellar Community Fund](https://communityfund.stellar.org/) public-goods
projects.

## What is PG Atlas?

PG Atlas maps the software supply chain of SCF-funded projects — repositories,
dependencies, contributors, and health metrics — into a queryable graph.  This
API is the primary interface for the
[PG Atlas Dashboard](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-frontend)
and the
[PG Atlas TypeScript SDK](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-sdk).

## Data sources

Every endpoint is tagged with the upstream source(s) its data originates from
(`OpenGrants`, `deps.dev`, `GitHub`, or `PG Atlas` for computed metrics).

## Authentication

Read endpoints are **public** — no authentication required.  Write endpoints
under `/ingest` require a GitHub Actions OIDC token.

## Versioning

This API is currently **unversioned** (development phase).  See
[api-versioning.md](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/blob/main/pg_atlas/routers/api-versioning.md)
for the future versioning strategy.
"""

app = FastAPI(
    title="PG Atlas API",
    description=_description,
    version=_version,
    license_info={"name": "MPL-2.0", "identifier": "MPL-2.0"},
    lifespan=lifespan,
    openapi_tags=tags_metadata,
    docs_url="/docs",
    redoc_url="/redoc",
)

# ---------------------------------------------------------------------------
# Router registration
# ---------------------------------------------------------------------------

app.include_router(health.router)
app.include_router(ingestion.router, prefix="/ingest")
app.include_router(metadata.router)
app.include_router(projects.router)
app.include_router(repos.router)
app.include_router(contributors.router)
