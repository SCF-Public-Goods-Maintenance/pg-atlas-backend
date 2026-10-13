"""
DB integration tests for upserts.absorb_external_repo and find_repo_by_release_purl.

Require a live PostgreSQL instance configured via ``PG_ATLAS_DATABASE_URL``.
Automatically skipped when the variable is absent.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator
from unittest.mock import patch

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from pg_atlas.db_models.base import ActivityStatus, EdgeConfidence, ProjectType, Visibility
from pg_atlas.db_models.depends_on import DependsOn
from pg_atlas.db_models.project import Project
from pg_atlas.db_models.release import Release
from pg_atlas.db_models.repo_vertex import ExternalRepo, Repo, RepoVertex
from pg_atlas.procrastinate.upserts import (
    absorb_external_repo,
    clear_project_repo_associations,
    find_repo_by_release_purl,
    upsert_depends_on,
)
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


@pytest.mark.skipif(not _DB_AVAILABLE, reason="No database configured")
@pytest.mark.parametrize("out_direction", [True, False], ids=["out", "in"])
@pytest.mark.parametrize(
    ("target_confidence", "target_range", "external_confidence", "external_range", "expected_range"),
    [
        (EdgeConfidence.inferred_shadow, None, EdgeConfidence.verified_sbom, "1.2.3", "1.2.3"),
        (EdgeConfidence.verified_sbom, "2.0.0", EdgeConfidence.inferred_shadow, "1.2.3", "2.0.0"),
        (EdgeConfidence.verified_sbom, "2.0.0", EdgeConfidence.verified_sbom, "1.2.3", "2.0.0"),
        (EdgeConfidence.inferred_shadow, ">=1", EdgeConfidence.verified_sbom, None, None),
        (EdgeConfidence.verified_sbom, None, EdgeConfidence.verified_sbom, "1.2.3", None),
    ],
    ids=["upgrade", "preserve-verified", "both-verified", "copy-null-range", "preserve-verified-null-range"],
)
async def test_absorb_external_repo_preserves_verified_conflicts(
    upsert_test_env: tuple[async_sessionmaker[AsyncSession], AsyncSession],
    out_direction: bool,
    target_confidence: EdgeConfidence,
    target_range: str | None,
    external_confidence: EdgeConfidence,
    external_range: str | None,
    expected_range: str | None,
) -> None:
    """Verified evidence survives conflicts in either direction without changing unrelated edges."""
    factory, session = upsert_test_env
    repo = Repo(
        canonical_id="pkg:github/test-org/test-repo-verified-conflict",
        display_name="test-repo",
        visibility=Visibility.public,
        latest_version="1.0.0",
    )
    ext = ExternalRepo(
        canonical_id="pkg:cargo/test-pkg-verified-conflict",
        display_name="test-pkg",
        latest_version="1.0.0",
    )
    other = ExternalRepo(
        canonical_id="pkg:npm/other-verified-conflict",
        display_name="other",
        latest_version="1.0.0",
    )
    unrelated = ExternalRepo(
        canonical_id="pkg:npm/unrelated-verified-conflict",
        display_name="unrelated",
        latest_version="1.0.0",
    )
    session.add_all([repo, ext, other, unrelated])
    await session.commit()

    target_pair = (other.id, repo.id) if out_direction else (repo.id, other.id)
    external_pair = (other.id, ext.id) if out_direction else (ext.id, other.id)
    unrelated_pair = (unrelated.id, repo.id) if out_direction else (repo.id, unrelated.id)
    session.add_all(
        [
            DependsOn(
                in_vertex_id=target_pair[0],
                out_vertex_id=target_pair[1],
                confidence=target_confidence,
                version_range=target_range,
            ),
            DependsOn(
                in_vertex_id=external_pair[0],
                out_vertex_id=external_pair[1],
                confidence=external_confidence,
                version_range=external_range,
            ),
            DependsOn(
                in_vertex_id=unrelated_pair[0],
                out_vertex_id=unrelated_pair[1],
                confidence=EdgeConfidence.inferred_shadow,
                version_range=">=3",
            ),
        ]
    )
    await session.commit()

    with patch("pg_atlas.procrastinate.upserts.get_session_factory", return_value=factory):
        result = await absorb_external_repo(ext.canonical_id, repo.id)

    assert result is True
    await session.reset()

    gone = (await session.execute(select(RepoVertex).where(RepoVertex.id == ext.id))).scalar_one_or_none()
    assert gone is None
    target_edges = (
        (
            await session.execute(
                select(DependsOn).where(
                    DependsOn.in_vertex_id == target_pair[0],
                    DependsOn.out_vertex_id == target_pair[1],
                )
            )
        )
        .scalars()
        .all()
    )
    assert len(target_edges) == 1
    assert target_edges[0].confidence == EdgeConfidence.verified_sbom
    assert target_edges[0].version_range == expected_range
    unrelated_edge = (
        await session.execute(
            select(DependsOn).where(
                DependsOn.in_vertex_id == unrelated_pair[0],
                DependsOn.out_vertex_id == unrelated_pair[1],
            )
        )
    ).scalar_one()
    assert unrelated_edge.confidence == EdgeConfidence.inferred_shadow
    assert unrelated_edge.version_range == ">=3"


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
            Release(version="1.0.0", release_date="", purl="pkg:cargo/test-find-pkg-unique"),
            Release(version="0.9.0", release_date="", purl="pkg:cargo/test-find-pkg-unique"),
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


@pytest.mark.skipif(not _DB_AVAILABLE, reason="No database configured")
async def test_clear_project_repo_associations_is_scoped(
    upsert_test_env: tuple[async_sessionmaker[AsyncSession], AsyncSession],
) -> None:
    """Cleanup clears stale membership without affecting retained or other-project repos."""
    factory, session = upsert_test_env
    unique_suffix = uuid.uuid4().hex

    project = Project(
        canonical_id=f"test:reconcile-project-{unique_suffix}",
        display_name="Reconcile Project",
        project_type=ProjectType.scf_project,
        activity_status=ActivityStatus.live,
    )
    other_project = Project(
        canonical_id=f"test:reconcile-other-project-{unique_suffix}",
        display_name="Other Project",
        project_type=ProjectType.scf_project,
        activity_status=ActivityStatus.live,
    )
    session.add_all([project, other_project])
    await session.commit()
    await session.refresh(project)
    await session.refresh(other_project)

    retained_repo = Repo(
        canonical_id=f"pkg:github/test-org/retained-{unique_suffix}",
        display_name="retained",
        visibility=Visibility.public,
        latest_version="1.0.0",
        project_id=project.id,
    )
    stale_repo = Repo(
        canonical_id=f"pkg:github/test-org/stale-{unique_suffix}",
        display_name="stale",
        visibility=Visibility.public,
        latest_version="1.0.0",
        project_id=project.id,
    )
    unavailable_repo = Repo(
        canonical_id=f"pkg:github/test-org/unavailable-{unique_suffix}",
        display_name="unavailable",
        visibility=Visibility.public,
        latest_version="1.0.0",
    )
    other_project_repo = Repo(
        canonical_id=f"pkg:github/test-org/other-{unique_suffix}",
        display_name="other",
        visibility=Visibility.public,
        latest_version="1.0.0",
        project_id=other_project.id,
    )
    session.add_all([retained_repo, stale_repo, unavailable_repo, other_project_repo])
    await session.commit()
    await session.refresh(retained_repo)
    await session.refresh(stale_repo)
    await session.refresh(unavailable_repo)
    await session.refresh(other_project_repo)

    with patch("pg_atlas.procrastinate.upserts.get_session_factory", return_value=factory):
        await clear_project_repo_associations(project.id, [retained_repo.canonical_id, unavailable_repo.canonical_id])

    await session.reset()
    repos = (
        (
            await session.execute(
                select(Repo).where(Repo.id.in_([retained_repo.id, stale_repo.id, unavailable_repo.id, other_project_repo.id]))
            )
        )
        .scalars()
        .all()
    )
    repos_by_id = {repo.id: repo for repo in repos}

    assert repos_by_id[retained_repo.id].project_id == project.id
    assert repos_by_id[stale_repo.id].project_id is None
    assert repos_by_id[unavailable_repo.id].project_id is None
    assert repos_by_id[other_project_repo.id].project_id == other_project.id


@pytest.mark.skipif(not _DB_AVAILABLE, reason="No database configured")
async def test_upsert_repo_union_merges_releases(
    upsert_test_env: tuple[async_sessionmaker[AsyncSession], AsyncSession],
) -> None:
    """upsert_repo must union-merge releases instead of overwriting existing rows."""

    from pg_atlas.procrastinate.upserts import upsert_repo

    factory, session = upsert_test_env

    with patch("pg_atlas.procrastinate.upserts.get_session_factory", return_value=factory):
        repo_id = await upsert_repo(
            canonical_id="pkg:github/test-org/release-merge",
            display_name="release-merge",
            latest_version="1.0.0",
            releases=[Release(version="1.0.0", release_date="2025-01-01T00:00:00Z", purl="pkg:pub/release-merge")],
        )

        await upsert_repo(
            canonical_id="pkg:github/test-org/release-merge",
            display_name="release-merge",
            latest_version="1.1.0",
            releases=[Release(version="1.1.0", release_date="2025-02-01T00:00:00Z", purl="pkg:pub/release-merge")],
        )

    await session.reset()
    repo = (await session.execute(select(Repo).where(Repo.id == repo_id))).scalar_one()
    assert repo.releases is not None
    assert [release.version for release in repo.releases] == ["1.1.0", "1.0.0"]


@pytest.mark.skipif(not _DB_AVAILABLE, reason="No database configured")
async def test_upsert_depends_on_insert_update_and_noop(
    upsert_test_env: tuple[async_sessionmaker[AsyncSession], AsyncSession],
) -> None:
    factory, session = upsert_test_env
    del factory

    source = ExternalRepo(
        canonical_id="pkg:npm/upsert-source",
        display_name="source",
        latest_version="1.0.0",
    )
    target = ExternalRepo(
        canonical_id="pkg:npm/upsert-target",
        display_name="target",
        latest_version="1.0.0",
    )
    session.add_all([source, target])
    await session.flush()

    inserted = await upsert_depends_on(
        session=session,
        in_vertex_id=source.id,
        out_vertex_id=target.id,
        version_range="^1.0",
        confidence=EdgeConfidence.inferred_shadow,
    )
    await session.flush()

    updated = await upsert_depends_on(
        session=session,
        in_vertex_id=source.id,
        out_vertex_id=target.id,
        version_range="^2.0",
        confidence=EdgeConfidence.inferred_shadow,
    )
    await session.flush()

    noop = await upsert_depends_on(
        session=session,
        in_vertex_id=source.id,
        out_vertex_id=target.id,
        version_range="^2.0",
        confidence=EdgeConfidence.inferred_shadow,
    )

    await session.commit()
    await session.refresh(source)
    await session.refresh(target)

    edge = (
        await session.execute(
            select(DependsOn).where(
                DependsOn.in_vertex_id == source.id,
                DependsOn.out_vertex_id == target.id,
            )
        )
    ).scalar_one()

    assert inserted is True
    assert updated is True
    assert noop is False
    assert edge.version_range == "^2.0"


@pytest.mark.skipif(not _DB_AVAILABLE, reason="No database configured")
async def test_upsert_depends_on_preserves_verified_confidence(
    upsert_test_env: tuple[async_sessionmaker[AsyncSession], AsyncSession],
) -> None:
    factory, session = upsert_test_env
    del factory

    source = ExternalRepo(
        canonical_id="pkg:npm/upsert-source-verified",
        display_name="source",
        latest_version="1.0.0",
    )
    target = ExternalRepo(
        canonical_id="pkg:npm/upsert-target-verified",
        display_name="target",
        latest_version="1.0.0",
    )
    session.add_all([source, target])
    await session.flush()

    await upsert_depends_on(
        session=session,
        in_vertex_id=source.id,
        out_vertex_id=target.id,
        version_range="^1.0",
        confidence=EdgeConfidence.verified_sbom,
    )
    await session.flush()

    changed = await upsert_depends_on(
        session=session,
        in_vertex_id=source.id,
        out_vertex_id=target.id,
        version_range="^2.0",
        confidence=EdgeConfidence.inferred_shadow,
    )
    await session.commit()

    edge = (
        await session.execute(
            select(DependsOn).where(
                DependsOn.in_vertex_id == source.id,
                DependsOn.out_vertex_id == target.id,
            )
        )
    ).scalar_one()

    assert changed is True
    assert edge.version_range == "^2.0"
    assert edge.confidence == EdgeConfidence.verified_sbom
