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
from pg_atlas.routers import health, ingestion

logging.basicConfig(level=settings.LOG_LEVEL)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Application lifespan manager.

    Startup and shutdown hooks live here. Database connection pool setup (A2)
    and NetworkX graph loading (A6/A8) will be added in later deliverables.
    """
    logger.info("PG Atlas starting up (API_URL=%s)", settings.API_URL)
    yield
    logger.info("PG Atlas shutting down")


try:
    _version = importlib.metadata.version("pg-atlas-backend")
except importlib.metadata.PackageNotFoundError:
    _version = "dev"

app = FastAPI(
    title="PG Atlas API",
    description=(
        "Public, read-only REST API exposing SCF public goods dependency graph metrics. "
        "Write endpoints are authenticated via GitHub OIDC tokens."
    ),
    version=_version,
    lifespan=lifespan,
    # Docs available at /docs (Swagger UI) and /redoc.
    docs_url="/docs",
    redoc_url="/redoc",
)

app.include_router(health.router)
app.include_router(ingestion.router, prefix="/ingest")
