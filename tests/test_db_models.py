"""
Database model smoke tests.

These tests require a live PostgreSQL instance with the schema already applied.

All tests in this module are automatically skipped when ``PG_ATLAS_DATABASE_URL`` is
not set in the environment.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from collections.abc import AsyncGenerator

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from pg_atlas.db_models import (
    DependsOn,
    ExternalRepo,
    Project,
    Repo,
    SbomSubmission,
)
from pg_atlas.db_models.base import (
    ActivityStatus,
    EdgeConfidence,
    ProjectType,
    RepoVertexType,
    SubmissionStatus,
    Visibility,
)
from tests.db_cleanup import DB_MODELS_TABLE_SPECS, capture_snapshot, cleanup_created_rows


@pytest.fixture(autouse=True)
async def _cleanup_db_rows_for_model_tests(
    request: pytest.FixtureRequest,
    db_session: AsyncSession,
) -> AsyncGenerator[None, None]:
    """
    Remove only rows created by model tests that use ``db_session``.
    """

    if "db_session" not in request.fixturenames:
        yield

        return

    snapshot = await capture_snapshot(db_session, DB_MODELS_TABLE_SPECS)
    yield
    await cleanup_created_rows(db_session, DB_MODELS_TABLE_SPECS, snapshot)


# ---------------------------------------------------------------------------
# Vertex round-trip tests
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("db_session")
async def test_project_roundtrip(db_session: AsyncSession) -> None:
    """Insert a Project and read it back by canonical_id."""
    proj = Project(
        canonical_id="daoip-5:stellar:project:test-project",
        display_name="Test Project",
        project_type=ProjectType.scf_project,
        activity_status=ActivityStatus.live,
    )
    db_session.add(proj)
    await db_session.flush()

    result = await db_session.scalar(select(Project).where(Project.canonical_id == "daoip-5:stellar:project:test-project"))
    assert result is not None
    assert result.display_name == "Test Project"
    assert result.project_type is ProjectType.scf_project
    assert result.updated_at is not None


@pytest.mark.usefixtures("db_session")
async def test_repo_vertex_roundtrip(db_session: AsyncSession) -> None:
    """Insert a Repo (JTI subtype) and confirm polymorphic identity is set correctly."""
    repo = Repo(
        canonical_id="github:test-org/test-repo",
        display_name="test-repo",
        visibility=Visibility.public,
        latest_version="1.0.0",
    )
    db_session.add(repo)
    await db_session.flush()

    # Query via the JTI base to verify polymorphic loading
    result = await db_session.scalar(select(Repo).where(Repo.canonical_id == "github:test-org/test-repo"))
    assert result is not None
    assert isinstance(result, Repo)
    assert result.vertex_type == RepoVertexType.repo


@pytest.mark.usefixtures("db_session")
async def test_external_repo_roundtrip(db_session: AsyncSession) -> None:
    """Insert an ExternalRepo and confirm the JTI discriminator is set correctly."""
    ext = ExternalRepo(
        canonical_id="npm:express",
        display_name="express",
        latest_version="4.18.2",
    )
    db_session.add(ext)
    await db_session.flush()

    result = await db_session.scalar(select(ExternalRepo).where(ExternalRepo.canonical_id == "npm:express"))
    assert result is not None
    assert result.vertex_type == RepoVertexType.external_repo


# ---------------------------------------------------------------------------
# Edge round-trip test
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("db_session")
async def test_depends_on_edge_roundtrip(db_session: AsyncSession) -> None:
    """Insert Repo → ExternalRepo depends_on edge and read it back."""
    repo = Repo(
        canonical_id="github:test-org/edge-test-repo",
        display_name="edge-test-repo",
        visibility=Visibility.public,
        latest_version="0.1.0",
    )
    ext = ExternalRepo(canonical_id="npm:lodash", display_name="lodash", latest_version="4.17.21")
    db_session.add_all([repo, ext])
    await db_session.flush()

    edge = DependsOn(
        in_vertex_id=repo.id,
        out_vertex_id=ext.id,
        version_range="^4.17.0",
        confidence=EdgeConfidence.verified_sbom,
    )
    db_session.add(edge)
    await db_session.flush()

    result = await db_session.scalar(
        select(DependsOn).where(
            DependsOn.in_vertex_id == repo.id,
            DependsOn.out_vertex_id == ext.id,
        )
    )
    assert result is not None
    assert result.version_range == "^4.17.0"
    assert result.confidence is EdgeConfidence.verified_sbom


# ---------------------------------------------------------------------------
# SbomSubmission content hash test
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("db_session")
async def test_sbom_submission_content_hash_roundtrip(db_session: AsyncSession) -> None:
    """Verify HexBinary stores and retrieves the SHA-256 digest correctly."""
    sha256_hex = "a" * 64  # 64-char hex string = 32 bytes
    sub = SbomSubmission(
        repository_claim="test-org/test-repo",
        actor_claim="test-user",
        sbom_content_hash=sha256_hex,
        artifact_path="",
    )
    db_session.add(sub)
    await db_session.flush()

    result = await db_session.scalar(select(SbomSubmission).where(SbomSubmission.id == sub.id))
    assert result is not None
    assert result.sbom_content_hash == sha256_hex
    assert result.status is SubmissionStatus.pending
    assert result.submitted_at is not None
