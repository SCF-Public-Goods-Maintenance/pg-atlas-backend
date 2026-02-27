"""
Health check router for PG Atlas.

Provides GET /health for liveness monitoring. The response is deliberately
lightweight so that uptime monitors can call it frequently without overhead.

A ``components`` key will be added in A2 to report the health of individual
subsystems (database connection, NetworkX graph load status, etc.).

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import importlib.metadata

from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter(tags=["health"])


class HealthResponse(BaseModel):
    """Response body for GET /health."""

    status: str
    version: str


@router.get("/health", response_model=HealthResponse, summary="Liveness check")
async def health() -> HealthResponse:
    """
    Return the current health status and application version.

    This endpoint is intentionally dependency-free (no DB call) so that it
    remains responsive even when the database is unreachable. A richer check
    with named component statuses (db, graph) will be added in A2.
    """
    try:
        version = importlib.metadata.version("pg-atlas-backend")
    except importlib.metadata.PackageNotFoundError:
        version = "dev"

    return HealthResponse(status="ok", version=version)
