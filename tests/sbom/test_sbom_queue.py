"""
Tests for the A8 SBOM processing task.

Exercises the queued worker path independently from the HTTP request handler:
  - processing a pending submission successfully
  - marking submissions failed when the stored artifact is missing or invalid
  - no-op behavior for missing or already-terminal submissions
  - one end-to-end path that posts an SBOM, captures the deferred submission id,
    and then runs the worker task against the created row

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import uuid
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from pg_atlas.auth.oidc import verify_github_oidc_token
from pg_atlas.db_models.base import EdgeConfidence, SubmissionStatus
from pg_atlas.db_models.depends_on import DependsOn
from pg_atlas.db_models.repo_vertex import ExternalRepo, Repo
from pg_atlas.db_models.sbom_submission import SbomSubmission
from pg_atlas.db_models.session import maybe_db_session
from pg_atlas.main import app
from pg_atlas.storage.artifacts import store_artifact
from tests.conftest import MOCK_OIDC_CLAIMS, get_test_database_url
from tests.db_cleanup import DB_MODELS_TABLE_SPECS, capture_snapshot, cleanup_created_rows

try:
    from pg_atlas.procrastinate.tasks import process_sbom_submission
except ValueError:
    pytest.skip("PG_ATLAS_DATABASE_URL intentionally not set for queue tests", allow_module_level=True)


FIXTURES = Path(__file__).parent / "data_fixtures"


class _FakeGatewayResponse:
    """Minimal async HTTP response stub for Filebase gateway tests."""

    def __init__(self, *, status_code: int, content: bytes) -> None:
        self.status_code = status_code
        self.content = content

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"gateway status {self.status_code}")


@pytest.fixture
async def db_authenticated_client() -> AsyncGenerator[tuple[AsyncClient, AsyncSession], None]:
    """
    Authenticated HTTP client backed by a real DB session for end-to-end queue tests.
    """

    database_url = get_test_database_url()
    if not database_url:
        pytest.skip("PG_ATLAS_DATABASE_URL / PG_ATLAS_TEST_DATABASE_URL not set")

    engine = create_async_engine(database_url, poolclass=NullPool)
    factory: async_sessionmaker[AsyncSession] = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    async def _db_override() -> AsyncGenerator[AsyncSession, None]:
        async with factory() as session:
            yield session

    app.dependency_overrides[verify_github_oidc_token] = lambda: MOCK_OIDC_CLAIMS.copy()
    app.dependency_overrides[maybe_db_session] = _db_override

    async with factory() as seed_session:
        try:
            async with AsyncClient(
                transport=ASGITransport(app=app),
                base_url="http://test",
            ) as client:
                yield client, seed_session
        finally:
            app.dependency_overrides.clear()

    await engine.dispose()


@pytest.fixture
async def cleanup_db_rows_for_queue_tests(db_session: AsyncSession) -> AsyncGenerator[None, None]:
    """
    Remove only rows created by queued-processing tests.
    """

    snapshot = await capture_snapshot(db_session, DB_MODELS_TABLE_SPECS)
    yield
    await cleanup_created_rows(db_session, DB_MODELS_TABLE_SPECS, snapshot)


@pytest.fixture
async def patched_worker_session_factory(mocker: Any) -> AsyncGenerator[None, None]:
    """
    Patch the worker task to use a loop-local ``NullPool`` session factory.
    """

    database_url = get_test_database_url()
    if not database_url:
        pytest.skip("PG_ATLAS_DATABASE_URL / PG_ATLAS_TEST_DATABASE_URL not set")

    engine = create_async_engine(database_url, poolclass=NullPool)
    factory: async_sessionmaker[AsyncSession] = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    mocker.patch("pg_atlas.procrastinate.tasks.get_session_factory", return_value=factory)

    yield

    await engine.dispose()


def _artifact_filename(raw_body: bytes) -> str:
    """
    Build a unique artifact filename for one test payload.
    """

    digest = hashlib.sha256(raw_body).hexdigest()
    return f"sboms/test-{uuid.uuid4().hex}-{digest}.spdx.json"


async def _create_submission(
    session: AsyncSession,
    raw_body: bytes,
    *,
    status: SubmissionStatus = SubmissionStatus.pending,
    artifact_path: str | None = None,
    error_detail: str | None = None,
) -> SbomSubmission:
    """
    Insert an ``SbomSubmission`` row for worker-task testing.
    """

    if artifact_path is None:
        artifact_path, _ = await store_artifact(raw_body, _artifact_filename(raw_body))

    submission = SbomSubmission(
        repository_claim=MOCK_OIDC_CLAIMS["repository"],
        actor_claim=MOCK_OIDC_CLAIMS["actor"],
        sbom_content_hash=hashlib.sha256(raw_body).hexdigest(),
        artifact_path=artifact_path,
        status=status,
        error_detail=error_detail,
    )
    session.add(submission)
    await session.commit()
    await session.refresh(submission)

    return submission


async def test_process_sbom_submission_marks_pending_row_processed(
    db_session: AsyncSession,
    cleanup_db_rows_for_queue_tests: None,
    patched_worker_session_factory: None,
) -> None:
    """
    The worker task processes a pending valid submission into repos and verified edges.
    """

    raw_body = (FIXTURES / "valid.spdx.json").read_bytes()
    submission = await _create_submission(db_session, raw_body)

    await process_sbom_submission(submission_id=submission.id)

    await db_session.refresh(submission)
    updated = await db_session.get(SbomSubmission, submission.id)
    assert updated is not None
    assert updated.status == SubmissionStatus.processed
    assert updated.processed_at is not None

    expected_repo = "pkg:github/test-org/test-repo"

    # The DB may contain pre-existing rows from other tests. Ensure the
    # expected repo and external deps exist and that two verified edges were
    # created for the submitted repository.
    repo_row = (await db_session.execute(select(Repo).where(Repo.canonical_id == expected_repo))).scalars().first()
    assert repo_row is not None

    external_rows = (
        (
            await db_session.execute(
                select(ExternalRepo)
                .where(ExternalRepo.canonical_id.in_(["httpx", "requests"]))
                .order_by(ExternalRepo.canonical_id)
            )
        )
        .scalars()
        .all()
    )
    assert {r.canonical_id for r in external_rows} == {"httpx", "requests"}

    edge_rows = (await db_session.execute(select(DependsOn))).scalars().all()
    edges_for_repo = [
        e for e in edge_rows if e.in_node.canonical_id == expected_repo and e.confidence == EdgeConfidence.verified_sbom
    ]
    assert len(edges_for_repo) == 2
    assert {e.out_node.canonical_id for e in edges_for_repo} == {"httpx", "requests"}


async def test_process_sbom_submission_accepts_legacy_enveloped_artifact(
    db_session: AsyncSession,
    cleanup_db_rows_for_queue_tests: None,
    patched_worker_session_factory: None,
) -> None:
    """
    Worker processing remains compatible with legacy ``{"sbom": ...}`` artifacts.
    """

    inner = json.loads((FIXTURES / "valid.spdx.json").read_bytes())
    wrapped = json.dumps({"sbom": inner}).encode()
    submission = await _create_submission(db_session, wrapped)

    await process_sbom_submission(submission_id=submission.id)

    await db_session.refresh(submission)
    updated = await db_session.get(SbomSubmission, submission.id)
    assert updated is not None
    assert updated.status == SubmissionStatus.processed
    assert updated.processed_at is not None


async def test_process_sbom_submission_marks_missing_artifact_failed(
    db_session: AsyncSession,
    cleanup_db_rows_for_queue_tests: None,
    patched_worker_session_factory: None,
) -> None:
    """
    Missing artifacts are terminal worker failures recorded on the submission row.
    """

    raw_body = (FIXTURES / "valid.spdx.json").read_bytes()
    submission = await _create_submission(
        db_session,
        raw_body,
        artifact_path="sboms/does-not-exist.spdx.json",
    )

    await process_sbom_submission(submission_id=submission.id)

    await db_session.refresh(submission)
    updated = await db_session.get(SbomSubmission, submission.id)
    assert updated is not None
    assert updated.status == SubmissionStatus.failed
    assert updated.error_detail is not None


async def test_process_sbom_submission_marks_invalid_artifact_failed(
    db_session: AsyncSession,
    cleanup_db_rows_for_queue_tests: None,
    patched_worker_session_factory: None,
) -> None:
    """
    Invalid stored SPDX artifacts are marked failed when the worker re-validates them.
    """

    raw_body = (FIXTURES / "invalid.spdx.json").read_bytes()
    submission = await _create_submission(db_session, raw_body)

    await process_sbom_submission(submission_id=submission.id)

    await db_session.refresh(submission)
    updated = await db_session.get(SbomSubmission, submission.id)
    assert updated is not None
    assert updated.status == SubmissionStatus.failed
    assert updated.error_detail is not None
    assert "Invalid SPDX 2.3 document." in updated.error_detail


async def test_process_sbom_submission_reads_filebase_cid_artifact(
    db_session: AsyncSession,
    cleanup_db_rows_for_queue_tests: None,
    patched_worker_session_factory: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Pending submissions can be processed from a Filebase CID without a local artifact file.
    """

    from pg_atlas.config import settings

    raw_body = (FIXTURES / "valid.spdx.json").read_bytes()
    cid = "QmTestFilebaseCidForWorker"

    async def _fake_get(self: AsyncClient, url: str, *args: Any, **kwargs: Any) -> _FakeGatewayResponse:
        assert url.endswith(cid)
        return _FakeGatewayResponse(status_code=200, content=raw_body)

    monkeypatch.setattr(settings, "ARTIFACT_S3_ENDPOINT", "https://s3.filebase.com")
    monkeypatch.setattr(AsyncClient, "get", _fake_get)

    submission = await _create_submission(
        db_session,
        raw_body,
        artifact_path=cid,
    )

    await process_sbom_submission(submission_id=submission.id)

    await db_session.refresh(submission)
    updated = await db_session.get(SbomSubmission, submission.id)
    assert updated is not None
    assert updated.status == SubmissionStatus.processed
    assert updated.processed_at is not None


async def test_process_sbom_submission_is_noop_for_missing_submission(
    caplog: Any,
    patched_worker_session_factory: None,
) -> None:
    """
    Missing submission ids should be ignored without raising.
    """

    caplog.set_level("WARNING")
    await process_sbom_submission(submission_id=999999)

    assert "not found" in caplog.text


async def test_process_sbom_submission_is_noop_for_terminal_submission(
    db_session: AsyncSession,
    cleanup_db_rows_for_queue_tests: None,
    patched_worker_session_factory: None,
) -> None:
    """
    Already-terminal submissions are not reprocessed.
    """

    raw_body = (FIXTURES / "valid.spdx.json").read_bytes()
    submission = await _create_submission(db_session, raw_body, status=SubmissionStatus.processed)
    submission.processed_at = dt.datetime.now(dt.UTC)
    await db_session.commit()

    pre_repo_ids = (await db_session.execute(select(Repo.canonical_id))).scalars().all()
    await process_sbom_submission(submission_id=submission.id)

    await db_session.refresh(submission)
    updated = await db_session.get(SbomSubmission, submission.id)
    assert updated is not None
    assert updated.status == SubmissionStatus.processed

    # Ensure processing didn't add any new repos (compare canonical_id lists).
    post_repo_ids = (await db_session.execute(select(Repo.canonical_id))).scalars().all()
    assert post_repo_ids == pre_repo_ids


async def test_ingest_then_worker_persists_dependency_graph(
    db_authenticated_client: tuple[AsyncClient, AsyncSession],
    cleanup_db_rows_for_queue_tests: None,
    mocker: Any,
    db_session: AsyncSession,
    patched_worker_session_factory: None,
) -> None:
    """
    End-to-end A8 path: request creates the pending submission, worker persists the graph.
    """

    client, _ = db_authenticated_client
    captured_submission_ids: list[int] = []

    async def _capture_defer(submission_id: int) -> None:
        captured_submission_ids.append(submission_id)

    mocker.patch("pg_atlas.ingestion.persist.defer_sbom_processing", new=_capture_defer)

    raw_body = (FIXTURES / "valid.spdx.json").read_bytes()
    response = await client.post(
        "/ingest/sbom",
        content=raw_body,
        headers={"Content-Type": "application/json"},
    )

    assert response.status_code == 202
    assert captured_submission_ids

    await process_sbom_submission(submission_id=captured_submission_ids[0])

    submission = await db_session.get(SbomSubmission, captured_submission_ids[0])

    assert submission is not None
    assert submission.status == SubmissionStatus.processed

    expected_repo = "pkg:github/test-org/test-repo"
    repo_row = (await db_session.execute(select(Repo).where(Repo.canonical_id == expected_repo))).scalars().first()
    assert repo_row is not None

    edge_rows = (await db_session.execute(select(DependsOn))).scalars().all()
    edges_for_repo = [e for e in edge_rows if e.in_node.canonical_id == expected_repo]
    assert len(edges_for_repo) == 2
