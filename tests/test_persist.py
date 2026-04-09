"""
Tests for pg_atlas.ingestion.persist.

Unit tests of helper functions run without a database.

DB integration tests require a live PostgreSQL instance configured via
``PG_ATLAS_DATABASE_URL`` and are automatically skipped when that variable is
absent, so the default ``uv run pytest`` invocation (CI without a DB service)
remains green.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import json
import uuid
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from pg_atlas.db_models.base import SubmissionStatus
from pg_atlas.db_models.depends_on import DependsOn
from pg_atlas.db_models.repo_vertex import ExternalRepo, Repo
from pg_atlas.db_models.sbom_submission import SbomSubmission
from pg_atlas.ingestion.persist import (
    canonical_id_for_github_repo,
    canonical_id_for_spdx_package,
    handle_sbom_submission,
    process_pending_sbom_submission,
    strip_purl_version,
)
from pg_atlas.ingestion.spdx import compute_sbom_semantic_hash
from pg_atlas.storage.artifacts import read_artifact
from tests.conftest import get_test_database_url
from tests.db_cleanup import SBOM_DB_TABLE_SPECS, capture_snapshot, cleanup_created_rows

FIXTURES = Path(__file__).parent / "data_fixtures"

_DB_AVAILABLE = bool(get_test_database_url())


@pytest.fixture
async def cleanup_db_rows_for_db_tests(
    db_session: AsyncSession,
) -> AsyncGenerator[None, None]:
    """
    Remove only rows created by DB integration tests in this module.

    This fixture is applied only to DB integration tests in this module.
    """

    snapshot = await capture_snapshot(db_session, SBOM_DB_TABLE_SPECS)
    yield
    await cleanup_created_rows(db_session, SBOM_DB_TABLE_SPECS, snapshot)


def _unique_claims(owner: str = "test-org") -> dict[str, Any]:
    """Return claims with a unique per-invocation repository name to avoid DB conflicts."""
    suffix = uuid.uuid4().hex[:8]
    return {"repository": f"{owner}/test-repo-{suffix}", "actor": "test-user"}


async def _submission_for_payload(
    session: AsyncSession,
    raw_body: bytes,
    claims: dict[str, Any],
) -> SbomSubmission:
    """
    Load the submission row corresponding to one repository-specific payload.
    """

    content_hash_hex = compute_sbom_semantic_hash(raw_body)
    return (
        await session.execute(
            select(SbomSubmission)
            .where(SbomSubmission.sbom_content_hash == content_hash_hex)
            .where(SbomSubmission.repository_claim == claims["repository"])
        )
    ).scalar_one()


# ---------------------------------------------------------------------------
# Unit tests — pure helpers, no DB required
# ---------------------------------------------------------------------------


def test_canonical_id_for_github_repo() -> None:
    """canonical_id_for_github_repo produces a pkg:github PURL."""
    assert canonical_id_for_github_repo("owner/repo") == "pkg:github/owner/repo"
    assert canonical_id_for_github_repo("a/b") == "pkg:github/a/b"


def test_strip_purl_version_strips_at_suffix() -> None:
    """_strip_purl_version removes the @version component."""
    assert strip_purl_version("pkg:pypi/requests@2.32.0") == "pkg:pypi/requests"
    assert strip_purl_version("pkg:github/owner/repo@main") == "pkg:github/owner/repo"
    assert strip_purl_version("pkg:npm/react") == "pkg:npm/react"  # no @ — unchanged


def test_canonical_id_for_spdx_package_from_purl() -> None:
    """canonical_id_for_spdx_package extracts and strips the PURL from externalRefs."""

    class FakeRef:
        reference_type = "purl"
        locator = "pkg:pypi/requests@2.32.0"

    class FakePkg:
        name = "requests"
        external_references = [FakeRef()]

    assert canonical_id_for_spdx_package(FakePkg()) == "pkg:pypi/requests"


def test_canonical_id_for_spdx_package_fallback() -> None:
    """canonical_id_for_spdx_package falls back to lowercase name when no PURL."""

    class FakePkg:
        name = "MyPackage"
        external_references: list[Any] = []

    assert canonical_id_for_spdx_package(FakePkg()) == "mypackage"


# ---------------------------------------------------------------------------
# DB integration tests
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _DB_AVAILABLE, reason="PG_ATLAS_DATABASE_URL not set")
async def test_handle_sbom_submission_valid(
    db_session: AsyncSession,
    cleanup_db_rows_for_db_tests: None,
    mocker: Any,
) -> None:
    """
    A valid SPDX 2.3 submission records a pending audit row and defers one job.
    """
    claims = _unique_claims()
    valid_sbom = (FIXTURES / "valid.spdx.json").read_bytes()
    defer_mock = mocker.AsyncMock()
    mocker.patch("pg_atlas.ingestion.persist.defer_sbom_processing", new=defer_mock)
    result = await handle_sbom_submission(db_session, valid_sbom, claims)

    assert result.repository == claims["repository"]
    assert result.package_count == 2  # requests + httpx in valid.spdx.json

    sub = await _submission_for_payload(db_session, valid_sbom, claims)
    assert sub.status == SubmissionStatus.pending
    assert sub.actor_claim == claims["actor"]
    assert sub.processed_at is None
    defer_mock.assert_awaited_once_with(sub.id)

    repo_cid = canonical_id_for_github_repo(claims["repository"])
    repo = await db_session.scalar(select(Repo).where(Repo.canonical_id == repo_cid))
    assert repo is None


@pytest.mark.skipif(not _DB_AVAILABLE, reason="PG_ATLAS_DATABASE_URL not set")
async def test_handle_sbom_submission_is_idempotent(
    db_session: AsyncSession,
    cleanup_db_rows_for_db_tests: None,
    mocker: Any,
) -> None:
    """
    Re-submitting the same SBOM twice keeps one canonical repo and skips re-enqueueing.
    """
    claims = _unique_claims()
    valid_sbom = (FIXTURES / "valid.spdx.json").read_bytes()
    defer_mock = mocker.AsyncMock()
    mocker.patch("pg_atlas.ingestion.persist.defer_sbom_processing", new=defer_mock)

    await handle_sbom_submission(db_session, valid_sbom, claims)
    first_submission = await _submission_for_payload(db_session, valid_sbom, claims)
    await process_pending_sbom_submission(db_session, first_submission.id)

    second_result = await handle_sbom_submission(db_session, valid_sbom, claims)
    assert second_result.message == "duplicate skipped"

    repo_cid = canonical_id_for_github_repo(claims["repository"])
    repos = (await db_session.execute(select(Repo).where(Repo.canonical_id == repo_cid))).scalars().all()
    assert len(repos) == 1, "Re-ingestion must not create duplicate Repo vertices"
    assert defer_mock.await_count == 1


@pytest.mark.skipif(not _DB_AVAILABLE, reason="PG_ATLAS_DATABASE_URL not set")
async def test_handle_sbom_submission_github_dep_graph(
    db_session: AsyncSession,
    cleanup_db_rows_for_db_tests: None,
    mocker: Any,
) -> None:
    """
    A GitHub Dependency Graph SBOM (with PURL externalRefs and a DESCRIBES
    relationship for the subject package) is processed without duplicating the
    submitting repo as an ExternalRepo.

    The claims ``repository`` matches the subject package's PURL so the
    self-reference check fires correctly.  The test is idempotent — the
    SELECT-then-upsert pattern is safe to re-run.
    """
    # Use the exact owner/repo that appears in the SPDX fixture's PURL so that
    # the submitting_canonical_id check correctly identifies the subject package.
    claims: dict[str, Any] = {
        "repository": "SCF-Public-Goods-Maintenance/pg-atlas-sbom-action",
        "actor": "test-user",
    }
    defer_mock = mocker.AsyncMock()
    mocker.patch("pg_atlas.ingestion.persist.defer_sbom_processing", new=defer_mock)
    raw = (FIXTURES / "github_dep_graph.spdx.json").read_bytes()
    result = await handle_sbom_submission(db_session, raw, claims)
    assert result.repository == claims["repository"]
    submission = await _submission_for_payload(db_session, raw, claims)
    await process_pending_sbom_submission(db_session, submission.id)
    defer_mock.assert_awaited_once_with(submission.id)

    # Submitting repo → Repo vertex, NOT ExternalRepo
    repo_cid = canonical_id_for_github_repo(claims["repository"])
    repo = (await db_session.execute(select(Repo).where(Repo.canonical_id == repo_cid))).scalar_one()
    assert (
        await db_session.execute(select(ExternalRepo).where(ExternalRepo.canonical_id == repo_cid))
    ).scalar_one_or_none() is None, "Submitting repo must not be created as ExternalRepo"

    # Dependency (actions/checkout) → ExternalRepo
    dep_cid = "pkg:githubactions/actions/checkout"
    dep = (await db_session.execute(select(ExternalRepo).where(ExternalRepo.canonical_id == dep_cid))).scalar_one_or_none()
    assert dep is not None and dep.display_name == "actions/checkout"

    # DependsOn edge exists
    edges = (await db_session.execute(select(DependsOn).where(DependsOn.in_vertex_id == repo.id))).scalars().all()
    assert any(e.out_vertex_id == dep.id for e in edges)


@pytest.mark.skipif(not _DB_AVAILABLE, reason="PG_ATLAS_DATABASE_URL not set")
async def test_handle_sbom_submission_duplicate_edges(
    db_session: AsyncSession,
    cleanup_db_rows_for_db_tests: None,
    mocker: Any,
) -> None:
    """
    See how `handle_sbom_submission` handles duplicate PURLs within a single SBOM.
    It deduplicates after parsing and stores the last seen version identifier
    on the `depends_on` edge.
    """
    claims = _unique_claims()
    long_sbom = (FIXTURES / "py-stellar-sdk-a9b110.spdx.json").read_bytes()
    defer_mock = mocker.AsyncMock()
    mocker.patch("pg_atlas.ingestion.persist.defer_sbom_processing", new=defer_mock)
    result = await handle_sbom_submission(db_session, long_sbom, claims)

    assert result.repository == claims["repository"]
    assert result.package_count == 109
    submission = await _submission_for_payload(db_session, long_sbom, claims)
    await process_pending_sbom_submission(db_session, submission.id)
    defer_mock.assert_awaited_once_with(submission.id)

    repo_cid = canonical_id_for_github_repo(claims["repository"])
    repo = await db_session.scalar(select(Repo).where(Repo.canonical_id == repo_cid))
    assert repo, f"The Repo {repo_cid} was not created properly"

    edges = (await db_session.scalars(select(DependsOn).where(DependsOn.in_vertex_id == repo.id))).all()
    assert len(edges) == 106

    dependency_versions = {dep.out_node.canonical_id: dep.version_range for dep in edges}
    assert dependency_versions["pkg:pypi/sphinx"] == "8.2.3"
    assert dependency_versions["pkg:pypi/sphinx-autodoc-typehints"] == "3.4.0"


@pytest.mark.skipif(not _DB_AVAILABLE, reason="PG_ATLAS_DATABASE_URL not set")
async def test_handle_sbom_submission_invalid_records_failed_row(
    db_session: AsyncSession,
    cleanup_db_rows_for_db_tests: None,
) -> None:
    """
    An invalid SBOM must create a ``failed`` SbomSubmission row (so the raw
    bytes are retained for triage) and raise ``SpdxValidationError``.
    """
    from pg_atlas.ingestion.spdx import SpdxValidationError

    claims = _unique_claims()
    invalid_sbom = (FIXTURES / "invalid.spdx.json").read_bytes()

    with pytest.raises(SpdxValidationError):
        await handle_sbom_submission(db_session, invalid_sbom, claims)

    content_hash_hex = compute_sbom_semantic_hash(invalid_sbom)
    sub = (
        await db_session.execute(
            select(SbomSubmission)
            .where(SbomSubmission.sbom_content_hash == content_hash_hex)
            .where(SbomSubmission.repository_claim == claims["repository"])
        )
    ).scalar_one()
    assert sub.status == SubmissionStatus.failed
    assert sub.error_detail is not None


@pytest.mark.skipif(not _DB_AVAILABLE, reason="PG_ATLAS_DATABASE_URL not set")
async def test_handle_sbom_submission_stores_unwrapped_spdx_artifact(
    db_session: AsyncSession,
    cleanup_db_rows_for_db_tests: None,
    mocker: Any,
) -> None:
    """
    New SBOM submissions persist canonical unwrapped SPDX JSON artifacts.

    If the incoming payload is a GitHub API envelope (``{"sbom": {...}}``),
    the stored artifact must contain the inner SPDX document only.
    """

    claims = _unique_claims()
    inner = json.loads((FIXTURES / "github_dep_graph.spdx.json").read_bytes())
    wrapped = json.dumps({"sbom": inner}).encode()
    defer_mock = mocker.AsyncMock()
    mocker.patch("pg_atlas.ingestion.persist.defer_sbom_processing", new=defer_mock)

    result = await handle_sbom_submission(db_session, wrapped, claims)

    assert result.repository == claims["repository"]
    submission = await _submission_for_payload(db_session, wrapped, claims)
    defer_mock.assert_awaited_once_with(submission.id)

    stored = await read_artifact(submission.artifact_path)
    stored_json = json.loads(stored)

    assert isinstance(stored_json, dict)
    assert "sbom" not in stored_json
    assert stored_json["spdxVersion"] == "SPDX-2.3"
