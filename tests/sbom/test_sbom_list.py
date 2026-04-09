"""
Tests for GET /ingest/sbom and GET /ingest/sbom/{submission_id}.

Covers read-only SBOM submission listing and detail retrieval:
  - List with empty result set (no matching submissions)
  - List with seeded records
  - Filtering by repository query parameter
  - Pagination via limit/offset
  - Detail view for a single submission with raw artifact content
  - Detail view with missing artifact file
  - Detail view 404 for non-existent submission
  - HTTP 503 when no database is configured

DB integration tests require a live PostgreSQL instance configured via
``PG_ATLAS_DATABASE_URL`` and are automatically skipped when that variable is
absent.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient, Response
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from pg_atlas.config import settings
from pg_atlas.db_models.sbom_submission import SbomSubmission
from pg_atlas.db_models.session import maybe_db_session
from pg_atlas.main import app
from tests.conftest import get_test_database_url
from tests.db_cleanup import SBOM_DB_TABLE_SPECS, capture_snapshot, cleanup_created_rows

_DB_AVAILABLE = bool(get_test_database_url())


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _unique_repo(prefix: str = "test-org/list-test") -> str:
    """Return a unique repository name to avoid cross-test DB conflicts."""

    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _make_submission(
    repo: str,
    actor: str = "test-user",
    artifact_path: str | None = None,
) -> SbomSubmission:
    """
    Create an ``SbomSubmission`` instance with sensible defaults.

    Each invocation produces a unique ``sbom_content_hash`` to avoid
    deduplication collisions.
    """
    unique_bytes = f"{repo}-{uuid.uuid4().hex}".encode()
    h = hashlib.sha256(unique_bytes).hexdigest()

    return SbomSubmission(
        repository_claim=repo,
        actor_claim=actor,
        sbom_content_hash=h,
        artifact_path=artifact_path or f"sboms/{h}.spdx.json",
    )


class _FakeGatewayResponse:
    """Minimal async HTTP response stub for Filebase gateway tests."""

    def __init__(self, *, status_code: int, content: bytes) -> None:
        self.status_code = status_code
        self.content = content

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"gateway status {self.status_code}")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def no_db_client() -> AsyncGenerator[AsyncClient, None]:
    """
    HTTP client with ``maybe_db_session`` yielding ``None``.

    Used to test the HTTP 503 response when no database is configured.
    """

    async def _no_db() -> AsyncGenerator[None, None]:
        yield None

    app.dependency_overrides[maybe_db_session] = _no_db
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            yield client

    finally:
        app.dependency_overrides.pop(maybe_db_session, None)


@pytest.fixture
async def db_client() -> AsyncGenerator[tuple[AsyncClient, AsyncSession], None]:
    """
    HTTP client backed by a real database and a seed session for inserting test data.

    Each test gets a **fresh** engine with ``NullPool`` so that asyncpg connections
    are never shared across event loops.  The dependency override creates a new
    session per request; the ``seed_session`` returned alongside the client is used
    exclusively to insert test data (committed before queries).

    Skipped when ``PG_ATLAS_DATABASE_URL`` is not set.
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
        async with factory() as s:
            yield s

    app.dependency_overrides[maybe_db_session] = _db_override

    async with factory() as seed_session:
        try:
            async with AsyncClient(
                transport=ASGITransport(app=app),
                base_url="http://test",
            ) as client:
                yield client, seed_session

        finally:
            app.dependency_overrides.pop(maybe_db_session, None)

    await engine.dispose()


@pytest.fixture
async def cleanup_db_rows_for_db_client_tests(
    db_client: tuple[AsyncClient, AsyncSession],
) -> AsyncGenerator[None, None]:
    """
    Remove only rows created by tests that use ``db_client``.
    """
    _, seed_session = db_client
    snapshot = await capture_snapshot(seed_session, SBOM_DB_TABLE_SPECS)
    yield
    await cleanup_created_rows(seed_session, SBOM_DB_TABLE_SPECS, snapshot)


# ---------------------------------------------------------------------------
# 503 — No database configured
# ---------------------------------------------------------------------------


async def test_list_returns_503_without_db(no_db_client: AsyncClient) -> None:
    """GET /ingest/sbom returns 503 when no database session is available."""
    resp = await no_db_client.get("/ingest/sbom")

    assert resp.status_code == 503


async def test_detail_returns_503_without_db(no_db_client: AsyncClient) -> None:
    """GET /ingest/sbom/{id} returns 503 when no database session is available."""
    resp = await no_db_client.get("/ingest/sbom/1")

    assert resp.status_code == 503


# ---------------------------------------------------------------------------
# DB integration tests — List endpoint
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _DB_AVAILABLE, reason="PG_ATLAS_DATABASE_URL not set")
async def test_list_empty(
    db_client: tuple[AsyncClient, AsyncSession],
    cleanup_db_rows_for_db_client_tests: None,
) -> None:
    """GET /ingest/sbom with a non-matching repository filter returns an empty list."""
    client, _ = db_client

    resp = await client.get("/ingest/sbom", params={"repository": _unique_repo()})

    assert resp.status_code == 200
    body = resp.json()
    assert body["items"] == []
    assert body["total"] == 0
    assert body["limit"] == 50
    assert body["offset"] == 0


@pytest.mark.skipif(not _DB_AVAILABLE, reason="PG_ATLAS_DATABASE_URL not set")
async def test_list_with_records(
    db_client: tuple[AsyncClient, AsyncSession],
    cleanup_db_rows_for_db_client_tests: None,
) -> None:
    """GET /ingest/sbom returns seeded submissions when filtered by repository."""
    client, session = db_client
    repo = _unique_repo()

    for _ in range(3):
        session.add(_make_submission(repo))

    await session.commit()

    resp = await client.get("/ingest/sbom", params={"repository": repo})

    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 3
    assert len(body["items"]) == 3

    for item in body["items"]:
        assert item["repository_claim"] == repo


@pytest.mark.skipif(not _DB_AVAILABLE, reason="PG_ATLAS_DATABASE_URL not set")
async def test_list_filter_by_repository(
    db_client: tuple[AsyncClient, AsyncSession],
    cleanup_db_rows_for_db_client_tests: None,
) -> None:
    """GET /ingest/sbom?repository=... returns only matching submissions."""
    client, session = db_client
    repo_a = _unique_repo("org-a/repo-a")
    repo_b = _unique_repo("org-b/repo-b")

    session.add(_make_submission(repo_a))
    session.add(_make_submission(repo_b))
    session.add(_make_submission(repo_a))
    await session.commit()

    resp_a = await client.get("/ingest/sbom", params={"repository": repo_a})

    assert resp_a.status_code == 200
    body_a = resp_a.json()
    assert body_a["total"] == 2
    assert all(item["repository_claim"] == repo_a for item in body_a["items"])

    resp_b = await client.get("/ingest/sbom", params={"repository": repo_b})
    body_b = resp_b.json()
    assert body_b["total"] == 1


@pytest.mark.skipif(not _DB_AVAILABLE, reason="PG_ATLAS_DATABASE_URL not set")
async def test_list_pagination(
    db_client: tuple[AsyncClient, AsyncSession],
    cleanup_db_rows_for_db_client_tests: None,
) -> None:
    """GET /ingest/sbom respects limit and offset query parameters."""
    client, session = db_client
    repo = _unique_repo()

    for _ in range(5):
        session.add(_make_submission(repo))

    await session.commit()

    # First page: 2 items.
    resp = await client.get("/ingest/sbom", params={"repository": repo, "limit": 2, "offset": 0})
    body = resp.json()
    assert body["total"] == 5
    assert len(body["items"]) == 2
    assert body["limit"] == 2
    assert body["offset"] == 0

    # Second-to-last page: 2 items starting at offset 3.
    resp2 = await client.get("/ingest/sbom", params={"repository": repo, "limit": 2, "offset": 3})
    body2 = resp2.json()
    assert body2["total"] == 5
    assert len(body2["items"]) == 2

    # Past the end: empty.
    resp3 = await client.get("/ingest/sbom", params={"repository": repo, "limit": 2, "offset": 5})
    body3 = resp3.json()
    assert len(body3["items"]) == 0


# ---------------------------------------------------------------------------
# DB integration tests — Detail endpoint
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _DB_AVAILABLE, reason="PG_ATLAS_DATABASE_URL not set")
async def test_detail_with_artifact(
    db_client: tuple[AsyncClient, AsyncSession],
    cleanup_db_rows_for_db_client_tests: None,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GET /ingest/sbom/{id} returns the submission with raw artifact content."""
    client, session = db_client
    repo = _unique_repo()

    artifact_rel = "sboms/test-detail.spdx.json"
    artifact_content = '{"spdxVersion": "SPDX-2.3"}'

    # Write artifact to a temporary store.
    (tmp_path / "sboms").mkdir()
    (tmp_path / artifact_rel).write_text(artifact_content)
    monkeypatch.setattr(settings, "ARTIFACT_STORE_PATH", tmp_path)
    monkeypatch.setattr(settings, "ARTIFACT_S3_ENDPOINT", None)

    sub = _make_submission(repo, artifact_path=artifact_rel)
    session.add(sub)
    await session.commit()
    await session.refresh(sub)

    resp = await client.get(f"/ingest/sbom/{sub.id}")

    assert resp.status_code == 200
    body = resp.json()
    assert body["id"] == sub.id
    assert body["repository_claim"] == repo
    assert body["raw_artifact"] == artifact_content


@pytest.mark.skipif(not _DB_AVAILABLE, reason="PG_ATLAS_DATABASE_URL not set")
async def test_detail_missing_artifact(
    db_client: tuple[AsyncClient, AsyncSession],
    cleanup_db_rows_for_db_client_tests: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GET /ingest/sbom/{id} returns null raw_artifact when the file is missing."""
    client, session = db_client
    repo = _unique_repo()
    monkeypatch.setattr(settings, "ARTIFACT_S3_ENDPOINT", None)

    sub = _make_submission(repo, artifact_path="sboms/nonexistent.spdx.json")
    session.add(sub)
    await session.commit()
    await session.refresh(sub)

    resp = await client.get(f"/ingest/sbom/{sub.id}")

    assert resp.status_code == 200
    body = resp.json()
    assert body["id"] == sub.id
    assert body["raw_artifact"] is None


@pytest.mark.skipif(not _DB_AVAILABLE, reason="PG_ATLAS_DATABASE_URL not set")
async def test_detail_with_filebase_cid_artifact(
    db_client: tuple[AsyncClient, AsyncSession],
    cleanup_db_rows_for_db_client_tests: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GET /ingest/sbom/{id} reads raw_artifact from Filebase when artifact_path is a CID."""
    client, session = db_client
    repo = _unique_repo()
    cid = "QmTestFilebaseCidForDetailEndpoint"
    artifact_content = b'{"spdxVersion":"SPDX-2.3","name":"filebase"}'
    original_get = AsyncClient.get

    async def _fake_get(self: AsyncClient, url: str, *args: Any, **kwargs: Any) -> _FakeGatewayResponse | Response:
        if url.startswith("https://ipfs.filebase.io/ipfs/"):
            assert url.endswith(cid)
            return _FakeGatewayResponse(status_code=200, content=artifact_content)

        return await original_get(self, url, *args, **kwargs)

    monkeypatch.setattr(settings, "ARTIFACT_S3_ENDPOINT", "https://s3.filebase.com")
    monkeypatch.setattr(AsyncClient, "get", _fake_get)

    sub = _make_submission(repo, artifact_path=cid)
    session.add(sub)
    await session.commit()
    await session.refresh(sub)

    resp = await client.get(f"/ingest/sbom/{sub.id}")

    assert resp.status_code == 200
    body = resp.json()
    assert body["id"] == sub.id
    assert body["raw_artifact"] == artifact_content.decode("utf-8")


@pytest.mark.skipif(not _DB_AVAILABLE, reason="PG_ATLAS_DATABASE_URL not set")
async def test_detail_not_found(
    db_client: tuple[AsyncClient, AsyncSession],
    cleanup_db_rows_for_db_client_tests: None,
) -> None:
    """GET /ingest/sbom/{id} returns 404 for a non-existent submission."""
    client, _ = db_client

    resp = await client.get("/ingest/sbom/999999999")

    assert resp.status_code == 404
