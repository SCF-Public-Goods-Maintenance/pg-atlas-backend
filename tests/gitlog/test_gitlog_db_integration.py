"""
Database integration tests for the git log pipeline.

Requires a running PostgreSQL instance (docker compose up postgres).

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import datetime as dt
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pg_atlas.db_models.contributed_to import ContributedTo
from pg_atlas.db_models.contributor import Contributor
from pg_atlas.db_models.repo_vertex import Repo
from pg_atlas.gitlog.parser import ContributorStats, RepoParseResult, hash_email, parse_repo_with_raw_output
from pg_atlas.gitlog.persist import persist_repo_result
from tests.conftest import get_test_database_url
from tests.gitlog.conftest import create_test_repo

pytestmark = pytest.mark.skipif(
    not get_test_database_url(),
    reason="PG_ATLAS_DATABASE_URL / PG_ATLAS_TEST_DATABASE_URL not set; skipping database integration tests",
)


def _unique_repo_identity() -> tuple[str, str]:
    """Return unique canonical_id and repo_url values for DB-isolated tests."""

    suffix = uuid.uuid4().hex[:8]
    return (
        f"pkg:github/test-org/test-repo-{suffix}",
        f"https://github.com/test-org/test-repo-{suffix}",
    )


def _make_stats(email: str, name: str, commits: int) -> ContributorStats:
    return ContributorStats(
        email_hash=hash_email(email),
        display_name=name,
        number_of_commits=commits,
        first_commit_date=dt.datetime(2025, 1, 1, tzinfo=dt.UTC),
        last_commit_date=dt.datetime(2025, 6, 1, tzinfo=dt.UTC),
    )


def _make_result(
    repo_url: str,
    contributors: list[ContributorStats],
    total_commits: int | None = None,
    bot_commit_count: int = 0,
    bot_contributor_count: int = 0,
) -> RepoParseResult:
    latest = dt.datetime(2025, 6, 1, tzinfo=dt.UTC) if contributors else None
    return RepoParseResult(
        repo_url=repo_url,
        contributors=contributors,
        latest_commit_date=latest,
        total_commits=total_commits or sum(c.number_of_commits for c in contributors),
        bot_commit_count=bot_commit_count,
        bot_contributor_count=bot_contributor_count,
    )


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------


async def test_full_pipeline_with_real_db(
    db_session_factory: async_sessionmaker[AsyncSession], clean_gitlog_tables: None
) -> None:
    """Create a Repo, persist contributor data, verify all DB state."""
    canonical_id, repo_url = _unique_repo_identity()
    async with db_session_factory() as session:
        await create_test_repo(session, canonical_id=canonical_id, repo_url=repo_url)
        await session.commit()

    stats = [_make_stats("alice@ex.com", "Alice", 10), _make_stats("bob@ex.com", "Bob", 5)]
    result = _make_result(repo_url, stats)

    async with db_session_factory() as session:
        repo = (await session.execute(select(Repo).where(Repo.repo_url == repo_url))).scalar_one()
        persist = await persist_repo_result(session, repo, result)
        await session.commit()

    assert persist.contributors_created == 2
    assert persist.edges_created == 2

    async with db_session_factory() as session:
        expected_hashes = {hash_email("alice@ex.com"), hash_email("bob@ex.com")}
        contributors = (
            (await session.execute(select(Contributor).where(Contributor.email_hash.in_(expected_hashes)))).scalars().all()
        )
        repo = (await session.execute(select(Repo).where(Repo.repo_url == repo_url))).scalar_one()
        edges = (await session.execute(select(ContributedTo).where(ContributedTo.repo_id == repo.id))).scalars().all()

    assert len(contributors) == 2
    assert len(edges) == 2
    assert repo.latest_commit_date == dt.datetime(2025, 6, 1, tzinfo=dt.UTC)


async def test_idempotent_rerun(db_session_factory: async_sessionmaker[AsyncSession], clean_gitlog_tables: None) -> None:
    """Run twice with same data — no duplicates, counts updated."""
    canonical_id, repo_url = _unique_repo_identity()
    async with db_session_factory() as session:
        await create_test_repo(session, canonical_id=canonical_id, repo_url=repo_url)
        await session.commit()

    stats = [_make_stats("alice@ex.com", "Alice", 10)]
    result = _make_result(repo_url, stats)

    # First run
    async with db_session_factory() as session:
        repo = (await session.execute(select(Repo).where(Repo.repo_url == repo_url))).scalar_one()
        await persist_repo_result(session, repo, result)
        await session.commit()

    # Second run
    async with db_session_factory() as session:
        repo = (await session.execute(select(Repo).where(Repo.repo_url == repo_url))).scalar_one()
        persist2 = await persist_repo_result(session, repo, result)
        await session.commit()

    assert persist2.contributors_updated == 1
    assert persist2.edges_updated == 1

    # Verify no duplicates
    async with db_session_factory() as session:
        contributors = (
            (await session.execute(select(Contributor).where(Contributor.email_hash == hash_email("alice@ex.com"))))
            .scalars()
            .all()
        )
        repo = (await session.execute(select(Repo).where(Repo.repo_url == repo_url))).scalar_one()
        edges = (await session.execute(select(ContributedTo).where(ContributedTo.repo_id == repo.id))).scalars().all()
    assert len(contributors) == 1
    assert len(edges) == 1


async def test_multiple_repos_shared_contributor(
    db_session_factory: async_sessionmaker[AsyncSession], clean_gitlog_tables: None
) -> None:
    """Same email across repos — single Contributor, two ContributedTo edges."""
    async with db_session_factory() as session:
        await create_test_repo(session, "pkg:github/org/repo1", "https://github.com/org/repo1")
        await create_test_repo(session, "pkg:github/org/repo2", "https://github.com/org/repo2")
        await session.commit()

    stats = [_make_stats("shared@ex.com", "Shared Dev", 5)]

    for repo_url in ["https://github.com/org/repo1", "https://github.com/org/repo2"]:
        result = _make_result(repo_url, stats)
        async with db_session_factory() as session:
            repo = (await session.execute(select(Repo).where(Repo.repo_url == repo_url))).scalar_one()
            await persist_repo_result(session, repo, result)
            await session.commit()

    async with db_session_factory() as session:
        contributors = (
            (await session.execute(select(Contributor).where(Contributor.email_hash == hash_email("shared@ex.com"))))
            .scalars()
            .all()
        )
        repo_ids = (
            (
                await session.execute(
                    select(Repo.id).where(Repo.repo_url.in_(["https://github.com/org/repo1", "https://github.com/org/repo2"]))
                )
            )
            .scalars()
            .all()
        )
        edges = (await session.execute(select(ContributedTo).where(ContributedTo.repo_id.in_(repo_ids)))).scalars().all()

    assert len(contributors) == 1
    assert len(edges) == 2


async def test_bot_contributor_not_stored(
    db_session_factory: async_sessionmaker[AsyncSession], clean_gitlog_tables: None
) -> None:
    """
    Bots are filtered BEFORE persistence.

    The RepoParseResult.contributors list contains humans only — bots
    never reach persist_repo_result. Verify no bot data in DB.
    """
    canonical_id, repo_url = _unique_repo_identity()
    async with db_session_factory() as session:
        await create_test_repo(session, canonical_id=canonical_id, repo_url=repo_url)
        await session.commit()

    # Only human stats in the result (bots already filtered by parser)
    human_stats = [_make_stats("human@ex.com", "Human", 10)]
    result = _make_result(
        repo_url,
        human_stats,
        total_commits=15,  # 10 human + 5 bot (pre-filter)
        bot_commit_count=5,
        bot_contributor_count=1,
    )

    async with db_session_factory() as session:
        repo = (await session.execute(select(Repo).where(Repo.repo_url == repo_url))).scalar_one()
        await persist_repo_result(session, repo, result)
        await session.commit()

    async with db_session_factory() as session:
        contributors = (
            (await session.execute(select(Contributor).where(Contributor.email_hash == hash_email("human@ex.com"))))
            .scalars()
            .all()
        )
        edges = (await session.execute(select(ContributedTo).where(ContributedTo.repo_id == repo.id))).scalars().all()

    assert len(contributors) == 1
    assert contributors[0].name == "Human"
    assert len(edges) == 1


async def test_repo_latest_commit_date_is_at_least_max_contributed_to_last_commit_date(
    db_session_factory: async_sessionmaker[AsyncSession],
    clean_gitlog_tables: None,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Parser + persistence integration: Repo.latest_commit_date tracks parser max timestamp.

    The parsed max commit timestamp may come from a bot commit that never reaches
    ContributedTo. Repo.latest_commit_date must still be >= max edge last_commit_date.
    """

    canonical_id, repo_url = _unique_repo_identity()
    async with db_session_factory() as session:
        await create_test_repo(session, canonical_id=canonical_id, repo_url=repo_url)
        await session.commit()

    raw_git_log = (
        "Alice Dev\x00alice@example.com\x002025-06-01T09:00:00+00:00\x00h1\n"
        "Bob Dev\x00bob@example.com\x002025-06-15T09:00:00+00:00\x00h2\n"
        "dependabot[bot]\x0049699333+dependabot[bot]@users.noreply.github.com"
        "\x002025-07-01T12:30:00+00:00\x00b1\n"
    ).encode()

    async def _create_subprocess_exec(*_args: object, **_kwargs: object) -> MagicMock:
        if not hasattr(_create_subprocess_exec, "calls"):
            _create_subprocess_exec.calls = 0  # type: ignore[attr-defined]

        _create_subprocess_exec.calls += 1  # type: ignore[attr-defined]
        proc = MagicMock()
        proc.returncode = 0
        if _create_subprocess_exec.calls == 1:  # type: ignore[attr-defined]
            proc.communicate = AsyncMock(return_value=(b"", b""))
        else:
            proc.communicate = AsyncMock(return_value=(raw_git_log, b""))
        proc.kill = MagicMock()
        proc.wait = AsyncMock()

        return proc

    monkeypatch.setattr("asyncio.create_subprocess_exec", AsyncMock(side_effect=_create_subprocess_exec))
    result, _raw_output = await parse_repo_with_raw_output(repo_url, Path(tmp_path), since_months=24, timeout=30.0)

    async with db_session_factory() as session:
        repo = (await session.execute(select(Repo).where(Repo.repo_url == repo_url))).scalar_one()
        await persist_repo_result(session, repo, result)
        await session.commit()

    async with db_session_factory() as session:
        repo = (await session.execute(select(Repo).where(Repo.repo_url == repo_url))).scalar_one()
        max_edge_last_commit = await session.scalar(
            select(func.max(ContributedTo.last_commit_date)).where(ContributedTo.repo_id == repo.id)
        )

    assert repo.latest_commit_date == dt.datetime(2025, 7, 1, 12, 30, tzinfo=dt.UTC)
    assert repo.latest_commit_date is not None
    assert max_edge_last_commit is not None
    assert repo.latest_commit_date >= max_edge_last_commit
