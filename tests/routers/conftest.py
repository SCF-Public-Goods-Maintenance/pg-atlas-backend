"""
Shared fixtures for API router tests.

Provides:
- ``no_db_client`` — HTTP client with no database (yields 503 on all endpoints).
- ``seeded_client`` — HTTP client backed by a real database with a full test graph
  pre-seeded: projects, repos, external repos, dependency edges, contributors,
  and contribution edges.

DB integration tests require ``PG_ATLAS_DATABASE_URL`` (or ``PG_ATLAS_TEST_DATABASE_URL``)
and are automatically skipped when neither is set.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import datetime as dt
import hashlib
import uuid
from collections.abc import AsyncGenerator
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from pg_atlas.db_models.base import (
    ActivityStatus,
    EdgeConfidence,
    ProjectType,
    SubmissionStatus,
    Visibility,
)
from pg_atlas.db_models.contributed_to import ContributedTo
from pg_atlas.db_models.contributor import Contributor
from pg_atlas.db_models.depends_on import DependsOn
from pg_atlas.db_models.gitlog_artifact import GitLogArtifact
from pg_atlas.db_models.project import Project
from pg_atlas.db_models.repo_vertex import ExternalRepo, Repo
from pg_atlas.db_models.session import maybe_db_session
from pg_atlas.main import app
from tests.conftest import get_test_database_url
from tests.db_cleanup import (
    TableSpec,
    capture_snapshot,
    cleanup_created_rows,
)

_DB_AVAILABLE = bool(get_test_database_url())

# Table specs for cleanup — edge tables first (FK order).
API_TABLE_SPECS: list[TableSpec] = [
    TableSpec("contributed_to", ("contributor_id", "repo_id")),
    TableSpec("depends_on", ("in_vertex_id", "out_vertex_id")),
    TableSpec("gitlog_artifacts", ("id",)),
    TableSpec("contributors", ("id",)),
    TableSpec("external_repos", ("id",)),
    TableSpec("repos", ("id",)),
    TableSpec("projects", ("id",)),
    TableSpec("repo_vertices", ("id",)),
]


# ---------------------------------------------------------------------------
# No-DB fixture (always available)
# ---------------------------------------------------------------------------


@pytest.fixture
async def no_db_client() -> AsyncGenerator[AsyncClient, None]:
    """
    HTTP client with ``maybe_db_session`` yielding ``None``.

    All DB-backed endpoints return HTTP 503.
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


# ---------------------------------------------------------------------------
# Seeded-DB fixture (skipped without database)
# ---------------------------------------------------------------------------


def _now() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


def _uid() -> str:
    """Short unique suffix to avoid collisions with existing data."""

    return uuid.uuid4().hex[:8]


@pytest.fixture
async def seeded_client() -> AsyncGenerator[tuple[AsyncClient, dict[str, Any]], None]:
    """
    HTTP client backed by a real database with a pre-seeded test graph.

    Yields a ``(client, seed_data)`` tuple.  ``seed_data`` is a dict with keys:
    ``project_a``, ``project_b``, ``repo_a1``, ``repo_a2``, ``repo_b1``,
    ``ext_repo``, ``contributor``.

    Cleanup removes only the rows created by this fixture.
    """
    database_url = get_test_database_url()
    if not database_url:
        pytest.skip("PG_ATLAS_DATABASE_URL / PG_ATLAS_TEST_DATABASE_URL not set")

    engine = create_async_engine(database_url, poolclass=NullPool)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    tag = _uid()

    async with session_factory() as seed_session:
        snapshot = await capture_snapshot(seed_session, API_TABLE_SPECS)

        # --- Seed data (unique per run) ---
        project_a = Project(
            canonical_id=f"test:alpha-{tag}",
            display_name=f"Alpha Project {tag}",
            project_type=ProjectType.public_good,
            activity_status=ActivityStatus.live,
            category="infrastructure",
            git_owner_url="https://github.com/alpha-org",
            project_metadata={
                "scf_submissions": [{"round": "SCF-1", "title": "Alpha funding"}],
                "description": "Test alpha project",
                "website": "https://alpha.example.com",
            },
        )
        project_b = Project(
            canonical_id=f"test:beta-{tag}",
            display_name=f"Beta Project {tag}",
            project_type=ProjectType.scf_project,
            activity_status=ActivityStatus.discontinued,
            category="defi",
        )

        seed_session.add_all([project_a, project_b])
        await seed_session.flush()

        repo_a1 = Repo(
            canonical_id=f"pkg:github/alpha-org/repo-a1-{tag}",
            display_name=f"repo-a1-{tag}",
            visibility=Visibility.public,
            latest_version="1.0.0",
            project_id=project_a.id,
            repo_url=f"https://github.com/alpha-org/repo-a1-{tag}",
            latest_commit_date=_now(),
            adoption_stars=42,
        )
        repo_a2 = Repo(
            canonical_id=f"pkg:github/alpha-org/repo-a2-{tag}",
            display_name=f"repo-a2-{tag}",
            visibility=Visibility.public,
            latest_version="2.0.0",
            project_id=project_a.id,
            repo_url=f"https://github.com/alpha-org/repo-a2-{tag}",
        )
        repo_b1 = Repo(
            canonical_id=f"pkg:github/beta-org/repo-b1-{tag}",
            display_name=f"repo-b1-{tag}",
            visibility=Visibility.public,
            latest_version="0.1.0",
            project_id=project_b.id,
            repo_url=f"https://github.com/beta-org/repo-b1-{tag}",
        )
        ext_repo = ExternalRepo(
            canonical_id=f"pkg:npm/test-ext-{tag}",
            display_name=f"test-ext-{tag}",
            latest_version="4.17.21",
            repo_url=f"https://github.com/test/ext-{tag}",
        )

        seed_session.add_all([repo_a1, repo_a2, repo_b1, ext_repo])
        await seed_session.flush()

        # Dependency edges:
        #   repo_a1 → repo_b1  (cross-project)
        #   repo_a1 → ext_repo (external)
        #   repo_b1 → repo_a2  (reverse cross-project)
        dep_a1_b1 = DependsOn(
            in_vertex_id=repo_a1.id,
            out_vertex_id=repo_b1.id,
            version_range=">=0.1.0",
            confidence=EdgeConfidence.inferred_shadow,
        )
        dep_a1_ext = DependsOn(
            in_vertex_id=repo_a1.id,
            out_vertex_id=ext_repo.id,
            version_range="^4.17.0",
        )
        dep_b1_a2 = DependsOn(
            in_vertex_id=repo_b1.id,
            out_vertex_id=repo_a2.id,
        )

        seed_session.add_all([dep_a1_b1, dep_a1_ext, dep_b1_a2])

        # Contributor with commit to repo_a1.
        email_hash = hashlib.sha256(f"test-contributor-{tag}".encode()).hexdigest()
        contributor = Contributor(
            email_hash=email_hash,
            name="Test Contributor",
        )
        seed_session.add(contributor)
        await seed_session.flush()

        contrib_edge = ContributedTo(
            contributor_id=contributor.id,
            repo_id=repo_a1.id,
            number_of_commits=15,
            first_commit_date=dt.datetime(2024, 1, 1, tzinfo=dt.UTC),
            last_commit_date=dt.datetime(2025, 6, 1, tzinfo=dt.UTC),
        )
        seed_session.add(contrib_edge)

        gitlog_artifact = GitLogArtifact(
            repo_id=repo_a1.id,
            since_months=24,
            artifact_path=None,
            gitlog_content_hash=None,
            status=SubmissionStatus.failed,
            error_detail="seeded test failure",
        )
        seed_session.add(gitlog_artifact)
        await seed_session.commit()

        seed_data: dict[str, Any] = {
            "project_a": project_a,
            "project_b": project_b,
            "repo_a1": repo_a1,
            "repo_a2": repo_a2,
            "repo_b1": repo_b1,
            "ext_repo": ext_repo,
            "contributor": contributor,
            "gitlog_artifact": gitlog_artifact,
        }

    # Override maybe_db_session to use our test engine.
    async def _test_session() -> AsyncGenerator[AsyncSession | None, None]:
        async with session_factory() as session:
            yield session

    app.dependency_overrides[maybe_db_session] = _test_session

    try:
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            yield client, seed_data

    finally:
        app.dependency_overrides.pop(maybe_db_session, None)

        async with session_factory() as cleanup_session:
            await cleanup_created_rows(cleanup_session, API_TABLE_SPECS, snapshot)

        await engine.dispose()
