"""
Local seeder script to insert test SBOM submissions and defer them to the sbom queue.

Run this against your local database to queue tasks for worker testing.

Usage::

    uv run python -m tests.seed_sbom

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

import asyncio
import hashlib
import logging
import sys
import uuid
from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from pg_atlas.config import settings
from pg_atlas.db_models.base import SubmissionStatus
from pg_atlas.db_models.sbom_submission import SbomSubmission
from pg_atlas.procrastinate.tasks import process_sbom_submission
from pg_atlas.storage.artifacts import store_artifact

logger = logging.getLogger("pg_atlas.test_seed")


async def main() -> None:
    if not settings.DATABASE_URL:
        logger.error("PG_ATLAS_DATABASE_URL is required.")
        sys.exit(1)

    engine = create_async_engine(settings.DATABASE_URL, poolclass=NullPool)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    fixture_path = Path(__file__).parent / "data_fixtures" / "valid.spdx.json"
    if not fixture_path.exists():
        logger.error(f"Fixture not found: {fixture_path}")
        sys.exit(1)

    raw_body = fixture_path.read_bytes()
    digest = hashlib.sha256(raw_body).hexdigest()
    filename = f"sboms/test-seeder-{uuid.uuid4().hex[:8]}-{digest}.spdx.json"

    # Store artifact (NOTE: this respects ARTIFACT_S3_ENDPOINT)
    artifact_path, _ = await store_artifact(raw_body, filename)

    async with factory() as session:
        submission = SbomSubmission(
            repository_claim="test-org/test-repo",
            actor_claim="test-actor",
            sbom_content_hash=digest,
            artifact_path=artifact_path,
            status=SubmissionStatus.pending,
        )
        session.add(submission)
        await session.commit()
        await session.refresh(submission)

        logger.info(f"Created pending SbomSubmission(id={submission.id}). Deferring...")

        # Defer to queue
        await process_sbom_submission.defer_async(submission_id=submission.id)
        logger.info("Task deferred to Procrastinate 'sbom' queue.")

    await engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
