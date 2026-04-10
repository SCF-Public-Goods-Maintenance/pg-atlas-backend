"""
Seed script - defer reprocessing for recent failed SBOM submissions.

Usage::

    uv run python -m pg_atlas.procrastinate.seed_reprocess_sboms

This script targets the most recent failed submissions that match known,
recoverable error details. Matching rows are re-enqueued to the SBOM queue
without mutating their stored status.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import asyncio
import logging

from sqlalchemy import or_, select

from pg_atlas.db_models.base import SubmissionStatus
from pg_atlas.db_models.sbom_submission import SbomSubmission
from pg_atlas.db_models.session import get_session_factory
from pg_atlas.ingestion.queue import defer_sbom_processing

_N_MOST_RECENT = 15
_ERROR_FILTER: tuple[str, ...] = ("No such file or directory",)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


async def seed_reprocess_failed_sboms() -> None:
    """
    Defer reprocessing tasks for recent failed SBOM submissions.

    Rows are filtered to ``status=failed`` and ``error_detail`` values that
    contain one of the configured ``_ERROR_FILTER`` tokens.
    """

    factory = get_session_factory()
    async with factory() as session:
        error_conditions = tuple(SbomSubmission.error_detail.ilike(f"%{token}%") for token in _ERROR_FILTER)
        stmt = (
            select(SbomSubmission.id)
            .where(SbomSubmission.status == SubmissionStatus.failed)
            .where(or_(*error_conditions))
            .order_by(SbomSubmission.submitted_at.desc())
            .limit(_N_MOST_RECENT)
        )
        submission_ids = (await session.scalars(stmt)).all()

    deferred = 0
    for submission_id in submission_ids:
        enqueued = await defer_sbom_processing(
            submission_id=submission_id,
            expected_status=SubmissionStatus.failed,
        )
        if enqueued:
            deferred += 1

    logger.info(
        "seed_reprocess_sboms completed: "
        f"selected={len(submission_ids)} deferred={deferred} "
        f"status={SubmissionStatus.failed.value}"
    )


if __name__ == "__main__":
    asyncio.run(seed_reprocess_failed_sboms())
