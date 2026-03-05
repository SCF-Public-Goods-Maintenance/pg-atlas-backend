"""
Ingestion router for PG Atlas.

Handles write endpoints that accept data submissions from project teams. All
endpoints require a valid GitHub OIDC Bearer token (see pg_atlas.auth.oidc).

Currently implemented:
    POST /ingest/sbom — Accept an SPDX 2.3 SBOM submission from the
        pg-atlas-sbom-action, validate it, persist Repo/ExternalRepo vertices
        and DependsOn edges, and store a raw artifact for auditability.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import logging
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from pg_atlas.auth.oidc import verify_github_oidc_token
from pg_atlas.db_models.session import maybe_db_session
from pg_atlas.ingestion.persist import handle_sbom_submission
from pg_atlas.ingestion.spdx import SpdxValidationError

logger = logging.getLogger(__name__)

router = APIRouter(tags=["ingestion"])


class SbomAcceptedResponse(BaseModel):
    """Response body returned on successful SBOM submission (202 Accepted)."""

    message: str
    repository: str
    package_count: int


@router.post(
    "/sbom",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=SbomAcceptedResponse,
    summary="Submit an SPDX 2.3 SBOM",
    description=(
        "Accepts an SPDX 2.3 JSON SBOM document from the pg-atlas-sbom-action. "
        "Requires a valid GitHub OIDC Bearer token with `aud` set to the PG Atlas API URL. "
        "The submitting repository is identified from the token's `repository` claim — "
        "no additional configuration is required in the calling repo beyond "
        "`permissions: id-token: write`."
    ),
)
async def ingest_sbom(
    request: Request,
    claims: Annotated[dict[str, Any], Depends(verify_github_oidc_token)],
    session: Annotated[AsyncSession | None, Depends(maybe_db_session)],
) -> SbomAcceptedResponse:
    """
    Receive, validate, and persist an SPDX 2.3 SBOM submission.

    Steps:
    1. OIDC token is verified by the ``verify_github_oidc_token`` dependency
       before this handler is invoked.
    2. ``handle_sbom_submission`` stores the raw artifact, parses the SPDX
       document, and persists Repo/ExternalRepo vertices and DependsOn edges
       within a single DB transaction.  When no database is configured it
       falls back to a logging stub so the endpoint stays functional in CI.

    Args:
        request: Raw FastAPI request — body is read directly to preserve bytes.
        claims: Decoded OIDC JWT claims injected by verify_github_oidc_token.
        session: Live DB session from ``maybe_db_session``, or ``None`` when
            ``PG_ATLAS_DATABASE_URL`` is not configured.

    Returns:
        SbomAcceptedResponse: 202 Accepted with repository identity and
            package count for confirmation.

    Raises:
        HTTPException 422: If the request body is not a valid SPDX 2.3 document.
    """
    raw_body = await request.body()

    try:
        result = await handle_sbom_submission(session, raw_body, claims)
    except SpdxValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={
                "error": exc.detail,
                "messages": exc.messages,
            },
        ) from exc

    return SbomAcceptedResponse(**result)
