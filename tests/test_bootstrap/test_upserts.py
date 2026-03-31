"""
DB integration tests for upserts.absorb_external_repo and find_repo_by_release_purl.

Require a live PostgreSQL instance configured via ``PG_ATLAS_DATABASE_URL``.
Automatically skipped when the variable is absent.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from unittest.mock import patch

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from pg_atlas.db_models.base import EdgeConfidence, Visibility
from pg_atlas.db_models.depends_on import DependsOn
from pg_atlas.db_models.repo_vertex import ExternalRepo, Repo, RepoVertex
from pg_atlas.procrastinate.upserts import absorb_external_repo, find_repo_by_release_purl
from tests.conftest import get_test_database_url
from tests.db_cleanup import SBOM_DB_TABLE_SPECS, capture_snapshot, cleanup_created_rows

_DB_AVAILABLE = bool(get_test_database_url())


@pytest.fixture
async def upsert_test_env() -> AsyncGenerator[tuple[async_sessionmaker[AsyncSession], AsyncSession]]:
    """
    Provide a session factory for upserts and a separate session for assertions.

    The factory is patched into ``upserts.get_session_factory`` so the upsert
    functions create their own sessions (with normal commit/close lifecycle).
    A separate assertion session is yielded for test setup and verification.
    """
    database_url = get_test_database_url()
    if not database_url:
        pytest.skip("No database configured")

    engine = create_async_engine(database_url, poolclass=NullPool)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    assert_session = factory()
    snapshot = await capture_snapshot(assert_session, SBOM_DB_TABLE_SPECS)

    try:
        yield factory, assert_session

    finally:
        await cleanup_created_rows(assert_session, SBOM_DB_TABLE_SPECS, snapshot)
        await assert_session.close()
        await engine.dispose()


# ---------------------------------------------------------------------------
# absorb_external_repo
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _DB_AVAILABLE, reason="No database configured")
async def test_absorb_external_repo_no_match(
    upsert_test_env: tuple[async_sessionmaker[AsyncSession], AsyncSession],
) -> None:
    """absorb_external_repo returns False when no ExternalRepo exists."""
    factory, session = upsert_test_env

    repo = Repo(
        canonical_id="pkg:github/test-org/test-repo-absorb-noop",
        display_name="test-repo",
        visibility=Visibility.public,
        latest_version="1.0.0",
    )
    session.add(repo)
    await session.commit()
    await session.refresh(repo)

    with patch("pg_atlas.procrastinate.upserts.get_session_factory", return_value=factory):
        result = await absorb_external_repo("pkg:cargo/nonexistent-pkg-xyzzy", repo.id)

    assert result is False


@pytest.mark.skipif(not _DB_AVAILABLE, reason="No database configured")
async def test_absorb_external_repo_repoints_edges(
    upsert_test_env: tuple[async_sessionmaker[AsyncSession], AsyncSession],
) -> None:
    """absorb_external_repo re-points edges and deletes the ExternalRepo."""
    factory, session = upsert_test_env

    repo = Repo(
        canonical_id="pkg:github/test-org/test-repo-absorb-ok",
        display_name="test-repo",
        visibility=Visibility.public,
        latest_version="1.0.0",
    )
    ext = ExternalRepo(
        canonical_id="pkg:cargo/test-pkg-absorb-ok",
        display_name="test-pkg",
        latest_version="2.0.0",
    )
    other = ExternalRepo(
        canonical_id="pkg:npm/other-dep-absorb-ok",
        display_name="other",
        latest_version="0.1.0",
    )
    session.add_all([repo, ext, other])
    await session.commit()
    await session.refresh(repo)
    await session.refresh(ext)
    await session.refresh(other)

    edge = DependsOn(
        in_vertex_id=other.id,
        out_vertex_id=ext.id,
        confidence=EdgeConfidence.inferred_shadow,
    )
    session.add(edge)
    await session.commit()

    with patch("pg_atlas.procrastinate.upserts.get_session_factory", return_value=factory):
        result = await absorb_external_repo("pkg:cargo/test-pkg-absorb-ok", repo.id)

    assert result is True

    # Clear the identity map to see committed changes from the other session.
    await session.reset()

    # ExternalRepo should be gone.
    gone = (
        await session.execute(select(RepoVertex).where(RepoVertex.canonical_id == "pkg:cargo/test-pkg-absorb-ok"))
    ).scalar_one_or_none()
    assert gone is None

    # Edge should now point to repo.
    edges = (await session.execute(select(DependsOn).where(DependsOn.in_vertex_id == other.id))).scalars().all()
    assert len(edges) == 1
    assert edges[0].out_vertex_id == repo.id


@pytest.mark.skipif(not _DB_AVAILABLE, reason="No database configured")
async def test_absorb_external_repo_deduplicates_conflicts(
    upsert_test_env: tuple[async_sessionmaker[AsyncSession], AsyncSession],
) -> None:
    """Conflicting edges are deduplicated during absorption."""
    factory, session = upsert_test_env

    repo = Repo(
        canonical_id="pkg:github/test-org/test-repo-dedup",
        display_name="test-repo",
        visibility=Visibility.public,
        latest_version="1.0.0",
    )
    ext = ExternalRepo(
        canonical_id="pkg:cargo/test-pkg-dedup",
        display_name="test-pkg",
        latest_version="2.0.0",
    )
    other = ExternalRepo(
        canonical_id="pkg:npm/other-dep-dedup",
        display_name="other",
        latest_version="0.1.0",
    )
    session.add_all([repo, ext, other])
    await session.commit()
    await session.refresh(repo)
    await session.refresh(ext)
    await session.refresh(other)

    # Both edges: other -> ext AND other -> repo.
    # After absorb, both would be other -> repo — conflict.
    session.add(
        DependsOn(
            in_vertex_id=other.id,
            out_vertex_id=ext.id,
            confidence=EdgeConfidence.inferred_shadow,
        )
    )
    session.add(
        DependsOn(
            in_vertex_id=other.id,
            out_vertex_id=repo.id,
            confidence=EdgeConfidence.inferred_shadow,
        )
    )
    await session.commit()

    with patch("pg_atlas.procrastinate.upserts.get_session_factory", return_value=factory):
        result = await absorb_external_repo("pkg:cargo/test-pkg-dedup", repo.id)

    assert result is True

    await session.reset()

    # Only one edge from other -> repo should remain.
    edges = (await session.execute(select(DependsOn).where(DependsOn.in_vertex_id == other.id))).scalars().all()
    assert len(edges) == 1
    assert edges[0].out_vertex_id == repo.id


# ---------------------------------------------------------------------------
# find_repo_by_release_purl
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _DB_AVAILABLE, reason="No database configured")
async def test_find_repo_by_release_purl_found(
    upsert_test_env: tuple[async_sessionmaker[AsyncSession], AsyncSession],
) -> None:
    """find_repo_by_release_purl matches a Repo by its releases[].purl."""
    factory, session = upsert_test_env

    repo = Repo(
        canonical_id="pkg:github/test-org/test-repo-purl-find",
        display_name="test-repo",
        visibility=Visibility.public,
        latest_version="1.0.0",
        releases=[
            {"version": "1.0.0", "purl": "pkg:cargo/test-find-pkg-unique"},
            {"version": "0.9.0", "purl": "pkg:cargo/test-find-pkg-unique"},
        ],
    )
    session.add(repo)
    await session.commit()
    await session.refresh(repo)

    with patch("pg_atlas.procrastinate.upserts.get_session_factory", return_value=factory):
        result = await find_repo_by_release_purl("pkg:cargo/test-find-pkg-unique")

    assert result is not None
    vertex_id, canonical_id, project_id = result
    assert vertex_id == repo.id
    assert canonical_id == "pkg:github/test-org/test-repo-purl-find"
    assert project_id is None


@pytest.mark.skipif(not _DB_AVAILABLE, reason="No database configured")
async def test_find_repo_by_release_purl_not_found(
    upsert_test_env: tuple[async_sessionmaker[AsyncSession], AsyncSession],
) -> None:
    """find_repo_by_release_purl returns None for unmatched PURL."""
    factory, _ = upsert_test_env

    with patch("pg_atlas.procrastinate.upserts.get_session_factory", return_value=factory):
        result = await find_repo_by_release_purl("pkg:npm/nonexistent-ever-zz")

    assert result is None
