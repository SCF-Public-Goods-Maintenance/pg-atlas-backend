"""
Ingestion router for PG Atlas.

Handles write endpoints that accept data submissions from project teams.  Write
endpoints require a valid GitHub OIDC Bearer token (see ``pg_atlas.auth.oidc``).
Read endpoints are unauthenticated so submitters can verify their submissions.

Currently implemented:
    POST /ingest/sbom — Accept an SPDX 2.3 SBOM submission from the
        pg-atlas-sbom-action, validate it, store a raw artifact and audit row,
        and defer graph persistence to the background ``sbom`` queue.
    GET  /ingest/sbom — List all SBOM submissions with optional filtering and
        pagination.
    GET  /ingest/sbom/{submission_id} — Detail view for a single submission
        including the raw artifact content.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import datetime as dt
import logging
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from pg_atlas.auth.oidc import verify_github_oidc_token
from pg_atlas.db_models.base import SubmissionStatus
from pg_atlas.db_models.sbom_submission import SbomSubmission
from pg_atlas.db_models.session import maybe_db_session
from pg_atlas.ingestion.persist import SbomAcceptedResponse, SbomQueueingError, handle_sbom_submission
from pg_atlas.ingestion.spdx import SpdxValidationError
from pg_atlas.routers.common import DbSession, PaginationParams
from pg_atlas.storage.artifacts import read_artifact

logger = logging.getLogger(__name__)

router = APIRouter(tags=["ingestion"])


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class SbomSubmissionResponse(BaseModel):
    """
    Full SBOM submission record.

    Serialised from the ``SbomSubmission`` ORM model.  The ``sbom_content_hash``
    field is the SHA-256 hex digest of the raw submitted payload.
    """

    model_config = {"from_attributes": True}

    id: int
    repository_claim: str
    actor_claim: str
    sbom_content_hash: str
    artifact_path: str
    status: SubmissionStatus
    error_detail: str | None
    submitted_at: dt.datetime
    processed_at: dt.datetime | None


class SbomSubmissionDetailResponse(SbomSubmissionResponse):
    """
    Extended SBOM submission record with the raw artifact content.

    Returned by the detail endpoint.  ``raw_artifact`` contains the full JSON
    text of the stored SBOM, or ``None`` if the artifact file is missing from
    the store.
    """

    raw_artifact: str | None = None


class SbomSubmissionListResponse(BaseModel):
    """
    Paginated list of SBOM submission records.

    Returned by the list endpoint with ``total`` reflecting the count after
    any ``repository`` filter has been applied.
    """

    items: list[SbomSubmissionResponse]
    total: int
    limit: int
    offset: int


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
       document, records a ``pending`` audit row, and defers the heavy repo /
       edge persistence work to Procrastinate. When no database is configured
       it falls back to a logging stub so the endpoint stays functional in CI.

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
        HTTPException 503: If the SBOM could not be persisted due to queuing error.
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
    except SbomQueueingError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
        ) from exc

    return result


@router.get(
    "/sbom",
    response_model=SbomSubmissionListResponse,
    summary="List SBOM submissions",
    description=(
        "Returns a paginated list of SBOM submission records.  Supports optional "
        "filtering by ``repository`` claim and pagination via ``limit`` / ``offset`` "
        "query parameters.  Does not require authentication."
    ),
)
async def list_sbom_submissions(
    db: DbSession,
    pagination: Annotated[PaginationParams, Depends()],
    repository: Annotated[
        str | None,
        Query(description="Filter by repository_claim (exact match)"),
    ] = None,
) -> SbomSubmissionListResponse:
    """
    Return a paginated list of all SBOM submissions.

    Results are ordered by ``submitted_at`` descending (most recent first).
    When ``repository`` is provided, only submissions whose ``repository_claim``
    matches the supplied value are included — both in the items list and in the
    ``total`` count.
    """
    base = select(SbomSubmission)
    count_q = select(func.count()).select_from(SbomSubmission)

    if repository is not None:
        base = base.where(SbomSubmission.repository_claim == repository)
        count_q = count_q.where(SbomSubmission.repository_claim == repository)

    total = (await db.execute(count_q)).scalar_one()

    rows = (
        (await db.execute(base.order_by(SbomSubmission.submitted_at.desc()).limit(pagination.limit).offset(pagination.offset)))
        .scalars()
        .all()
    )

    return SbomSubmissionListResponse(
        items=[SbomSubmissionResponse.model_validate(row) for row in rows],
        total=total,
        limit=pagination.limit,
        offset=pagination.offset,
    )


@router.get(
    "/sbom/{submission_id}",
    response_model=SbomSubmissionDetailResponse,
    summary="Get SBOM submission detail",
    description=(
        "Returns a single SBOM submission record along with the raw artifact "
        "content read from the backing store.  If the artifact file is missing, "
        "the ``raw_artifact`` field is ``null``.  Does not require authentication."
    ),
)
async def get_sbom_submission(
    submission_id: int,
    db: DbSession,
) -> SbomSubmissionDetailResponse:
    """
    Return a single SBOM submission with its raw artifact content.

    Raises HTTP 404 if no submission with the given ``submission_id`` exists.
    The artifact file is read asynchronously via a thread-pool executor; if the
    file has been removed from the store the ``raw_artifact`` field is ``null``
    rather than raising an error.
    """
    # consider making this a streaming response if the artifacts become large and latency suffers

    row = await db.get(SbomSubmission, submission_id)
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"SBOM submission {submission_id} not found.",
        )

    # Read the raw artifact from the backing store.
    raw_artifact: str | None = None

    try:
        raw_artifact = (await read_artifact(row.artifact_path)).decode("utf-8")
    except FileNotFoundError:
        logger.warning(f"Artifact file not found: {row.artifact_path}")
    except OSError, UnicodeDecodeError:
        logger.exception(f"Error reading artifact file: {row.artifact_path}")

    detail = SbomSubmissionDetailResponse.model_validate(row)
    detail.raw_artifact = raw_artifact

    return detail
