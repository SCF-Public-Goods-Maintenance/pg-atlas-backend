"""
FastAPI application factory for PG Atlas.

Creates and configures the FastAPI app instance, registers routers, and defines
the application lifespan (startup/shutdown hooks). Database and graph engine
initialization will be added here as later deliverables are completed.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from pg_atlas.api_metadata import DESCRIPTION, VERSION, generate_route_id
from pg_atlas.config import settings
from pg_atlas.routers import contributors, gitlog, health, ingestion, metadata, projects, repos
from pg_atlas.routers.tags import TAGS_METADATA

logging.basicConfig(level=settings.LOG_LEVEL)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Application lifespan manager.

    Startup and shutdown hooks live here. NetworkX graph loading / refresh may be added in later
    deliverables. Graph refresh remains follow-up A8 work, not part of this PR.
    """
    logger.info(f"PG Atlas starting up (API_URL={settings.API_URL})")
    yield
    logger.info("PG Atlas shutting down")


# ---------------------------------------------------------------------------
# App instance
# ---------------------------------------------------------------------------

app = FastAPI(
    title="PG Atlas API",
    description=DESCRIPTION,
    version=VERSION,
    license_info={"name": "Mozilla Public License v2.0", "identifier": "MPL-2.0"},
    lifespan=lifespan,
    generate_unique_id_function=generate_route_id,
    openapi_tags=TAGS_METADATA,
    docs_url="/docs",
    redoc_url="/redoc",
)

# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["Content-Type", "Authorization"],
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
app.include_router(gitlog.router)
