"""
Health and readiness router for PG Atlas.

GET /health reports whether the service is merely live or fully ready. When the
database is configured, the endpoint checks the active Alembic revision and
returns HTTP 503 if readiness cannot be established.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Literal

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from pg_atlas.api_metadata import VERSION
from pg_atlas.config import settings
from pg_atlas.db_models.session import get_session_factory

router = APIRouter(tags=["health"])


class HealthComponents(BaseModel):
    """Component-level readiness details for GET /health."""

    artifact_store: Literal["local", "IPFS"]
    database: Literal["ready", "not-configured"]
    schema_version: str | None


class HealthResponse(BaseModel):
    """Response body for GET /health."""

    status: Literal["live", "ready"]
    version: str
    components: HealthComponents


def _artifact_store_status() -> Literal["local", "IPFS"]:
    """Return the configured artifact store backend."""

    if settings.ARTIFACT_S3_ENDPOINT is not None:
        return "IPFS"
    return "local"


def _select_app_schema_version(version_rows: Sequence[str]) -> str | None:
    """Select the application Alembic revision from raw alembic_version rows."""

    application_revisions = [version_row for version_row in version_rows if version_row != "procrastinate_001"]
    if len(application_revisions) > 1:
        raise ValueError("multiple non-Procrastinate revisions found in alembic_version")
    if not application_revisions:
        return None
    return application_revisions[0]


async def _schema_version_from_session(session: AsyncSession) -> str | None:
    """Read and normalize the application schema revision from one session."""

    result = await session.execute(text("SELECT version_num FROM alembic_version"))
    return _select_app_schema_version(result.scalars().all())


async def _read_schema_version() -> str | None:
    """Open a database session and read the current application schema version."""

    session_factory = get_session_factory()
    async with session_factory() as session:
        return await _schema_version_from_session(session)


@router.get("/health", response_model=HealthResponse, summary="Readiness check")
async def health() -> HealthResponse:
    """
    Return the current readiness status and application version.
    """
    artifact_store = _artifact_store_status()
    if not settings.DATABASE_URL:
        return HealthResponse(
            status="live",
            version=VERSION,
            components=HealthComponents(
                artifact_store=artifact_store,
                database="not-configured",
                schema_version=None,
            ),
        )

    try:
        schema_version = await _read_schema_version()
    except (SQLAlchemyError, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
        ) from exc

    return HealthResponse(
        status="ready",
        version=VERSION,
        components=HealthComponents(
            artifact_store=artifact_store,
            database="ready",
            schema_version=schema_version,
        ),
    )
