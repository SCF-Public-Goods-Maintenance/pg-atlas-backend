"""Ingestion router for PG Atlas.

Handles write endpoints that accept data submissions from project teams. All
endpoints require a valid GitHub OIDC Bearer token (see pg_atlas.auth.oidc).

Currently implemented:
    POST /ingest/sbom — Accept an SPDX 2.3 SBOM submission from the
        pg-atlas-sbom-action, validate it, and enqueue for processing.

Author: SCF Public Goods Maintenance <https://github.com/SCF-Public-Goods-Maintenance>
"""

from __future__ import annotations

import logging
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel

from pg_atlas.auth.oidc import verify_github_oidc_token
from pg_atlas.ingestion.queue import queue_sbom
from pg_atlas.ingestion.spdx import SpdxValidationError, parse_and_validate_spdx

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
) -> SbomAcceptedResponse:
    """Receive, validate, and enqueue an SPDX 2.3 SBOM submission.

    Steps:
    1. OIDC token is verified by the ``verify_github_oidc_token`` dependency
       before this handler is invoked.
    2. The raw request body is read and parsed as SPDX 2.3 JSON.
    3. The validated document is passed to ``queue_sbom`` for logging and
       eventual async processing (stub in A3, Celery dispatch in A8).

    Args:
        request: Raw FastAPI request — body is read directly to preserve bytes.
        claims: Decoded OIDC JWT claims injected by verify_github_oidc_token.

    Returns:
        SbomAcceptedResponse: 202 Accepted with repository identity and
            package count for confirmation.

    Raises:
        HTTPException 422: If the request body is not a valid SPDX 2.3 document.
    """
    raw_body = await request.body()

    try:
        sbom = parse_and_validate_spdx(raw_body)
    except SpdxValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={
                "error": exc.detail,
                "messages": exc.messages,
            },
        ) from exc

    result = queue_sbom(sbom, claims)
    return SbomAcceptedResponse(**result)
