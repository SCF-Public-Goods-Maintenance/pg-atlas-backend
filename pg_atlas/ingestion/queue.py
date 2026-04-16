"""
Async queue handoff for SBOM post-validation processing.

The request path validates SPDX submissions synchronously, then defers the
heavy repo / dependency persistence work to Procrastinate. This module is the
thin boundary between ingestion and the background worker system.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import logging

from pg_atlas.db_models.base import SubmissionStatus

logger = logging.getLogger(__name__)


async def defer_sbom_processing(
    submission_id: int,
    repository_claim: str,
    expected_status: SubmissionStatus = SubmissionStatus.pending,
) -> bool:
    """
    Defer background processing for one SBOM submission.

    Args:
        submission_id: Primary key of the ``SbomSubmission`` audit row whose
            stored artifact should be processed by the background worker.
        repository_claim: The OIDC repository claim for the submission.
        expected_status: Submission status the worker must require before
            processing this row.

    Returns:
        ``True`` when a new task was enqueued, ``False`` when an equivalent
        lock-matching task is already queued.
    """

    from pg_atlas.procrastinate.app import app
    from pg_atlas.procrastinate.tasks import defer_with_lock, process_sbom_submission

    queueing_lock = f"sbom:{repository_claim}"

    async with app.open_async():
        enqueued = await defer_with_lock(
            process_sbom_submission,
            queueing_lock=queueing_lock,
            submission_id=submission_id,
            expected_status=expected_status.value,
        )

    if enqueued:
        logger.info(
            "Deferred SBOM processing job: "
            f"submission_id={submission_id} repository_claim={repository_claim} expected_status={expected_status.value}"
        )
    else:
        logger.info(
            "Skipping duplicate SBOM defer request: "
            f"submission_id={submission_id} repository_claim={repository_claim} expected_status={expected_status.value}"
        )

    return enqueued
