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
from contextlib import AbstractAsyncContextManager
from typing import Any, cast

logger = logging.getLogger(__name__)


async def defer_sbom_processing(submission_id: int) -> None:
    """
    Defer background processing for one validated SBOM submission.

    Args:
        submission_id: Primary key of the ``SbomSubmission`` audit row whose
            stored artifact should be processed by the background worker.
    """

    from pg_atlas.procrastinate.app import app
    from pg_atlas.procrastinate.tasks import process_sbom_submission

    app_context = cast(AbstractAsyncContextManager[Any], app.open_async())
    async with app_context:
        job_id = await process_sbom_submission.defer_async(submission_id=submission_id)

    logger.info(f"Deferred SBOM processing job: submission_id={submission_id} job_id={job_id}")
