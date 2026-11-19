"""
Seed script - defer SBOM submissions for reprocessing.

Usage::

    uv run python -m pg_atlas.procrastinate.seed_reprocess_sboms
    uv run python -m pg_atlas.procrastinate.seed_reprocess_sboms 42 43
    uv run python -m pg_atlas.procrastinate.seed_reprocess_sboms --expected-status processed 239 295

With no submission ids, this script targets the most recent failed submissions
that match known recoverable error details. With explicit submission ids, it
defers only those matching submissions. Matching rows are re-enqueued to the SBOM
queue without mutating their stored status.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import argparse
import asyncio
import logging

from sqlalchemy import or_, select

from pg_atlas.db_models.base import SubmissionStatus
from pg_atlas.db_models.sbom_submission import SbomSubmission
from pg_atlas.db_models.session import get_session_factory
from pg_atlas.ingestion.queue import defer_sbom_processing
from pg_atlas.procrastinate.app import mark_stalled_jobs_failed

_N_MOST_RECENT = 7
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

    stalled_marked = await mark_stalled_jobs_failed(queue_name="sbom")
    if stalled_marked > 0:
        logger.warning(f"Marked {stalled_marked} stalled jobs as failed in queue sbom")

    factory = get_session_factory()
    async with factory() as session:
        error_conditions = tuple(SbomSubmission.error_detail.ilike(f"%{token}%") for token in _ERROR_FILTER)
        stmt = (
            select(SbomSubmission.id, SbomSubmission.repository_claim)
            .where(SbomSubmission.status == SubmissionStatus.failed)
            .where(or_(*error_conditions))
            .order_by(SbomSubmission.submitted_at.desc())
            .limit(_N_MOST_RECENT)
        )
        submission_rows = (await session.execute(stmt)).all()

    deferred = 0
    for submission_id, repository_claim in submission_rows:
        enqueued = await defer_sbom_processing(
            submission_id=submission_id,
            repository_claim=repository_claim,
            expected_status=SubmissionStatus.failed,
        )
        if enqueued:
            deferred += 1

    logger.info(
        "seed_reprocess_failed_sboms completed: "
        f"selected={len(submission_rows)} deferred={deferred} "
        f"status={SubmissionStatus.failed.value}"
    )


async def seed_reprocess_sboms_by_id(
    submission_ids: list[int],
    expected_status: SubmissionStatus = SubmissionStatus.failed,
) -> None:
    """
    Defer reprocessing tasks for explicitly provided SBOM submission ids.

    Args:
        submission_ids: SBOM submission primary keys to reprocess.
        expected_status: Submission status the worker must require for every
            provided id.
    """

    if not submission_ids:
        logger.info("No SBOM submission ids provided")
        return

    stalled_marked = await mark_stalled_jobs_failed(queue_name="sbom")
    if stalled_marked > 0:
        logger.warning(f"Marked {stalled_marked} stalled jobs as failed in queue sbom")

    factory = get_session_factory()
    async with factory() as session:
        stmt = select(SbomSubmission.id, SbomSubmission.repository_claim).where(SbomSubmission.id.in_(submission_ids))
        submission_rows = (await session.execute(stmt)).all()

    rows_by_id = {submission_id: repository_claim for submission_id, repository_claim in submission_rows}

    deferred = 0
    selected = 0
    for submission_id in submission_ids:
        repository_claim = rows_by_id.get(submission_id)
        if repository_claim is None:
            continue

        selected += 1
        enqueued = await defer_sbom_processing(
            submission_id=submission_id,
            repository_claim=repository_claim,
            expected_status=expected_status,
        )
        if enqueued:
            deferred += 1

    logger.info(
        "seed_reprocess_sboms_by_id completed: "
        f"requested={len(submission_ids)} selected={selected} deferred={deferred} "
        f"status={expected_status.value}"
    )


def _parse_args() -> argparse.Namespace:
    """Parse the SBOM reprocessing seed command arguments."""

    parser = argparse.ArgumentParser(description="Defer SBOM submissions for reprocessing")
    parser.add_argument(
        "submission_ids",
        nargs="*",
        type=int,
        help="SBOM submission IDs to reprocess; omit to use recent failed submissions",
    )
    parser.add_argument(
        "--expected-status",
        type=SubmissionStatus,
        choices=list(SubmissionStatus),
        default=SubmissionStatus.failed,
        help="Submission status required by the worker (default: failed)",
    )

    args = parser.parse_args()
    if not args.submission_ids and args.expected_status is not SubmissionStatus.failed:
        parser.error("--expected-status may only be changed when submission IDs are provided")

    return args


if __name__ == "__main__":
    args = _parse_args()
    if args.submission_ids:
        asyncio.run(
            seed_reprocess_sboms_by_id(
                submission_ids=args.submission_ids,
                expected_status=args.expected_status,
            )
        )
    else:
        asyncio.run(seed_reprocess_failed_sboms())
