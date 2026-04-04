"""
OpenAPI schema metadata and helpers.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

import importlib.metadata
from textwrap import dedent

from fastapi.routing import APIRoute


def generate_route_id(route: APIRoute) -> str:
    if route.deprecated:
        return f"deprecated-{route.name}"
    else:
        return route.name


try:
    _version = importlib.metadata.version("pg-atlas-backend")
except importlib.metadata.PackageNotFoundError:
    _version = "dev"

VERSION = _version

DESCRIPTION = dedent(
    """\
    Permissionless REST API exposing the **PG Atlas** graph dataset with top-down
    coverage of [Stellar Community Fund](https://communityfund.stellar.org/) projects,
    and gradual bottom-up coverage of the Stellar software ecosystem.

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

    ## Contributing data

    You can add your own project's data to the Atlas by adopting the
    [SBOM Action](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-sbom-action).
    This is how we increase the visibility of the Stellar ecosystem, particularly when
    it comes to proprietary (private) codebases and non-SCF funded projects.

    If your project is already visible in the graph, its quality is still limited by
    what we can publicly crawl. By adopting the SBOM Action in all your git repositories
    that build on Stellar, you'll contribute to improving the completeness and the
    quality of the data that PG Atlas exposes.

    ## Authentication

    Read endpoints are **public** — no authentication required.  Write endpoints
    under `/ingest` require a GitHub Actions OIDC token.

    ## Versioning

    This API is currently **unversioned** (development phase).  See
    [api-versioning.md](https://github.com/SCF-Public-Goods-Maintenance/pg-atlas-backend/blob/main/pg_atlas/routers/api-versioning.md)
    for the future versioning strategy.
    """
).strip()
