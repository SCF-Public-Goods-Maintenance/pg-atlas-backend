"""
DB integration tests for upserts.absorb_external_repo, find_repo_by_release_purl,
and the monotonic ``pushed_at`` writer.

Require a live PostgreSQL instance configured via ``PG_ATLAS_DATABASE_URL``.
Automatically skipped when the variable is absent.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import datetime as dt
from collections.abc import AsyncGenerator
from unittest.mock import patch
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from pg_atlas.db_models.base import EdgeConfidence, Visibility
from pg_atlas.db_models.depends_on import DependsOn
from pg_atlas.db_models.release import Release
from pg_atlas.db_models.repo_vertex import ExternalRepo, Repo, RepoVertex
from pg_atlas.procrastinate.upserts import absorb_external_repo, find_repo_by_release_purl, upsert_depends_on
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


# ---------------------------------------------------------------------------
# record_pushed_at / persist_pushed_at
# ---------------------------------------------------------------------------


_T0 = dt.datetime(2026, 9, 1, 12, 0, tzinfo=dt.UTC)


async def _seed_pushed_repo(
    session: AsyncSession,
    marker: str,
    *,
    pushed_at: dt.datetime | None,
    observed_at: dt.datetime | None,
) -> int:
    suffix = uuid4().hex[:8]
    repo = Repo(
        canonical_id=f"pkg:github/test-org/pushed-at-{marker}-{suffix}",
        display_name=f"pushed-at-{marker}-{suffix}",
        visibility=Visibility.public,
        latest_version="1.0.0",
        pushed_at=pushed_at,
        pushed_at_observed_at=observed_at,
    )
    session.add(repo)
    await session.commit()

    return repo.id


async def _stored_push(session: AsyncSession, repo_id: int) -> tuple[dt.datetime | None, dt.datetime | None]:
    session.expire_all()
    row = (await session.execute(select(Repo.pushed_at, Repo.pushed_at_observed_at).where(Repo.id == repo_id))).one()

    return row[0], row[1]


@pytest.mark.skipif(not _DB_AVAILABLE, reason="No database configured")
async def test_record_pushed_at_later_push_moves_both_columns(
    upsert_test_env: tuple[async_sessionmaker[AsyncSession], AsyncSession],
) -> None:
    """A later push time replaces the stored one together with its observation time."""

    from pg_atlas.procrastinate.upserts import record_pushed_at

    _, session = upsert_test_env
    repo_id = await _seed_pushed_repo(session, "later", pushed_at=_T0, observed_at=_T0 + dt.timedelta(hours=5))

    changed = await record_pushed_at(session, repo_id, _T0 + dt.timedelta(days=1), _T0 + dt.timedelta(days=1, hours=1))
    await session.commit()

    assert changed is True
    assert await _stored_push(session, repo_id) == (_T0 + dt.timedelta(days=1), _T0 + dt.timedelta(days=1, hours=1))


@pytest.mark.skipif(not _DB_AVAILABLE, reason="No database configured")
async def test_record_pushed_at_fills_an_empty_row(
    upsert_test_env: tuple[async_sessionmaker[AsyncSession], AsyncSession],
) -> None:
    from pg_atlas.procrastinate.upserts import record_pushed_at

    _, session = upsert_test_env
    repo_id = await _seed_pushed_repo(session, "empty", pushed_at=None, observed_at=None)

    changed = await record_pushed_at(session, repo_id, _T0, _T0 + dt.timedelta(minutes=5))
    await session.commit()

    assert changed is True
    assert await _stored_push(session, repo_id) == (_T0, _T0 + dt.timedelta(minutes=5))


@pytest.mark.skipif(not _DB_AVAILABLE, reason="No database configured")
async def test_record_pushed_at_equal_push_keeps_the_latest_observation(
    upsert_test_env: tuple[async_sessionmaker[AsyncSession], AsyncSession],
) -> None:
    """Re-observing the same push only ever moves the observation time forward."""

    from pg_atlas.procrastinate.upserts import record_pushed_at

    _, session = upsert_test_env
    stored_observed = _T0 + dt.timedelta(hours=5)
    repo_id = await _seed_pushed_repo(session, "equal", pushed_at=_T0, observed_at=stored_observed)

    # An earlier observation of the same push (e.g. a cached listing) changes nothing.
    changed_earlier = await record_pushed_at(session, repo_id, _T0, _T0 + dt.timedelta(hours=1))
    await session.commit()

    assert changed_earlier is False
    assert await _stored_push(session, repo_id) == (_T0, stored_observed)

    # A later observation of the same push advances the observation time.
    changed_later = await record_pushed_at(session, repo_id, _T0, _T0 + dt.timedelta(days=2))
    await session.commit()

    assert changed_later is True
    assert await _stored_push(session, repo_id) == (_T0, _T0 + dt.timedelta(days=2))


@pytest.mark.skipif(not _DB_AVAILABLE, reason="No database configured")
async def test_record_pushed_at_equal_push_fills_a_missing_observation(
    upsert_test_env: tuple[async_sessionmaker[AsyncSession], AsyncSession],
) -> None:
    """An equal push fills a missing observation time."""

    from pg_atlas.procrastinate.upserts import record_pushed_at

    _, session = upsert_test_env
    repo_id = await _seed_pushed_repo(session, "equal-null", pushed_at=_T0, observed_at=None)

    changed = await record_pushed_at(session, repo_id, _T0, _T0 + dt.timedelta(hours=2))
    await session.commit()

    assert changed is True
    assert await _stored_push(session, repo_id) == (_T0, _T0 + dt.timedelta(hours=2))


@pytest.mark.skipif(not _DB_AVAILABLE, reason="No database configured")
async def test_record_pushed_at_earlier_push_changes_nothing(
    upsert_test_env: tuple[async_sessionmaker[AsyncSession], AsyncSession],
) -> None:
    """``pushed_at`` never moves backwards, whatever the incoming observation time."""

    from pg_atlas.procrastinate.upserts import record_pushed_at

    _, session = upsert_test_env
    stored_observed = _T0 + dt.timedelta(hours=5)
    repo_id = await _seed_pushed_repo(session, "earlier", pushed_at=_T0, observed_at=stored_observed)

    changed = await record_pushed_at(session, repo_id, _T0 - dt.timedelta(days=3), _T0 + dt.timedelta(days=10))
    await session.commit()

    assert changed is False
    assert await _stored_push(session, repo_id) == (_T0, stored_observed)


@pytest.mark.skipif(not _DB_AVAILABLE, reason="No database configured")
@pytest.mark.parametrize(
    ("pushed_at", "observed_at"),
    [
        (None, _T0),
        (_T0 + dt.timedelta(days=1), None),
        (dt.datetime(2026, 9, 2, 12, 0), _T0 + dt.timedelta(days=1)),
        (_T0 + dt.timedelta(days=1), dt.datetime(2026, 9, 2, 12, 0)),
    ],
    ids=["pushed-none", "observed-none", "pushed-naive", "observed-naive"],
)
async def test_record_pushed_at_rejects_missing_or_naive_values(
    upsert_test_env: tuple[async_sessionmaker[AsyncSession], AsyncSession],
    caplog: pytest.LogCaptureFixture,
    pushed_at: dt.datetime | None,
    observed_at: dt.datetime | None,
) -> None:
    from pg_atlas.procrastinate.upserts import record_pushed_at

    _, session = upsert_test_env
    stored_observed = _T0 + dt.timedelta(hours=5)
    repo_id = await _seed_pushed_repo(session, "reject", pushed_at=_T0, observed_at=stored_observed)

    with caplog.at_level("WARNING", logger="pg_atlas.procrastinate.upserts"):
        changed = await record_pushed_at(session, repo_id, pushed_at, observed_at)
    await session.commit()

    assert changed is False
    assert "record_pushed_at" in caplog.text
    assert await _stored_push(session, repo_id) == (_T0, stored_observed)


@pytest.mark.skipif(not _DB_AVAILABLE, reason="No database configured")
async def test_persist_pushed_at_commits_in_its_own_session(
    upsert_test_env: tuple[async_sessionmaker[AsyncSession], AsyncSession],
) -> None:
    from pg_atlas.procrastinate.upserts import persist_pushed_at

    factory, session = upsert_test_env
    repo_id = await _seed_pushed_repo(session, "wrapper", pushed_at=_T0, observed_at=_T0)

    with patch("pg_atlas.procrastinate.upserts.get_session_factory", return_value=factory):
        changed = await persist_pushed_at(repo_id, _T0 + dt.timedelta(days=1), _T0 + dt.timedelta(days=1, minutes=1))

    assert changed is True
    assert await _stored_push(session, repo_id) == (_T0 + dt.timedelta(days=1), _T0 + dt.timedelta(days=1, minutes=1))


@pytest.mark.skipif(not _DB_AVAILABLE, reason="No database configured")
async def test_record_pushed_at_identical_observation_leaves_the_row_untouched(
    upsert_test_env: tuple[async_sessionmaker[AsyncSession], AsyncSession],
) -> None:
    """Replaying the stored push and observation pair changes nothing, not even ``updated_at``."""

    from pg_atlas.procrastinate.upserts import record_pushed_at

    _, session = upsert_test_env
    stored_observed = _T0 + dt.timedelta(hours=5)
    repo_id = await _seed_pushed_repo(session, "identical", pushed_at=_T0, observed_at=stored_observed)
    updated_before = (await session.execute(select(Repo.updated_at).where(Repo.id == repo_id))).scalar_one()

    changed = await record_pushed_at(session, repo_id, _T0, stored_observed)
    await session.commit()

    assert changed is False
    assert await _stored_push(session, repo_id) == (_T0, stored_observed)
    updated_after = (await session.execute(select(Repo.updated_at).where(Repo.id == repo_id))).scalar_one()
    assert updated_after == updated_before


@pytest.mark.skipif(not _DB_AVAILABLE, reason="No database configured")
async def test_record_pushed_at_later_push_keeps_its_own_older_observation_time(
    upsert_test_env: tuple[async_sessionmaker[AsyncSession], AsyncSession],
) -> None:
    """A later push takes the observation time it was seen at, even one before the stored observation."""

    from pg_atlas.procrastinate.upserts import record_pushed_at

    _, session = upsert_test_env
    repo_id = await _seed_pushed_repo(session, "pairing", pushed_at=_T0, observed_at=_T0 + dt.timedelta(days=3))

    changed = await record_pushed_at(session, repo_id, _T0 + dt.timedelta(days=1), _T0 + dt.timedelta(days=2))
    await session.commit()

    assert changed is True
    assert await _stored_push(session, repo_id) == (_T0 + dt.timedelta(days=1), _T0 + dt.timedelta(days=2))
