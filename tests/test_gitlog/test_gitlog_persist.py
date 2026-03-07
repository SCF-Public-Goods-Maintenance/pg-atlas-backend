"""
Unit tests for database persistence in pg_atlas.gitlog.persist.

These tests require a running PostgreSQL instance (docker compose).

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import datetime
import os

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pg_atlas.db_models.contributed_to import ContributedTo
from pg_atlas.db_models.contributor import Contributor
from pg_atlas.db_models.repo_vertex import Repo
from pg_atlas.gitlog.parser import ContributorStats, RepoParseResult, hash_email
from pg_atlas.gitlog.persist import persist_repo_result, upsert_contributed_to, upsert_contributor
from tests.test_gitlog.conftest import create_test_repo

pytestmark = pytest.mark.skipif(
    not os.environ.get("PG_ATLAS_DATABASE_URL"),
    reason="PG_ATLAS_DATABASE_URL not set; skipping database tests",
)


# ---------------------------------------------------------------------------
# upsert_contributor
# ---------------------------------------------------------------------------


async def test_upsert_contributor_new(db_session_factory: async_sessionmaker[AsyncSession], clean_gitlog_tables: None) -> None:
    async with db_session_factory() as session:
        contributor, created = await upsert_contributor(session, hash_email("a@b.com"), "Alice")
        await session.commit()
    assert created is True
    assert contributor.name == "Alice"
    assert contributor.id is not None


async def test_upsert_contributor_existing(
    db_session_factory: async_sessionmaker[AsyncSession], clean_gitlog_tables: None
) -> None:
    email_hash = hash_email("a@b.com")
    async with db_session_factory() as session:
        await upsert_contributor(session, email_hash, "Old Name")
        await session.commit()

    async with db_session_factory() as session:
        contributor, created = await upsert_contributor(session, email_hash, "New Name")
        await session.commit()
    assert created is False
    assert contributor.name == "New Name"


async def test_upsert_contributor_duplicate_email_hash(
    db_session_factory: async_sessionmaker[AsyncSession], clean_gitlog_tables: None
) -> None:
    """No unique constraint — duplicate email_hash inserts two rows. Second lookup returns the first."""
    email_hash = hash_email("dup@ex.com")
    async with db_session_factory() as session:
        c1 = Contributor(email_hash=email_hash, name="First")
        c2 = Contributor(email_hash=email_hash, name="Second")
        session.add_all([c1, c2])
        await session.commit()

    async with db_session_factory() as session:
        contributor, created = await upsert_contributor(session, email_hash, "Updated")
        await session.commit()
    assert created is False


# ---------------------------------------------------------------------------
# upsert_contributed_to
# ---------------------------------------------------------------------------


async def test_upsert_contributed_to_new(
    db_session_factory: async_sessionmaker[AsyncSession], clean_gitlog_tables: None
) -> None:
    async with db_session_factory() as session:
        repo = await create_test_repo(session)
        contributor, _ = await upsert_contributor(session, hash_email("a@b.com"), "Alice")
        stats = ContributorStats(
            email_hash=hash_email("a@b.com"),
            display_name="Alice",
            number_of_commits=5,
            first_commit_date=datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC),
            last_commit_date=datetime.datetime(2025, 6, 1, tzinfo=datetime.UTC),
        )
        created = await upsert_contributed_to(session, contributor.id, repo.id, stats)
        await session.commit()
    assert created is True


async def test_upsert_contributed_to_existing_overwrites_count(
    db_session_factory: async_sessionmaker[AsyncSession], clean_gitlog_tables: None
) -> None:
    """On update, number_of_commits is OVERWRITTEN (not summed)."""
    async with db_session_factory() as session:
        repo = await create_test_repo(session)
        contributor, _ = await upsert_contributor(session, hash_email("a@b.com"), "Alice")
        stats1 = ContributorStats(
            email_hash="",
            display_name="Alice",
            number_of_commits=10,
            first_commit_date=datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC),
            last_commit_date=datetime.datetime(2025, 6, 1, tzinfo=datetime.UTC),
        )
        await upsert_contributed_to(session, contributor.id, repo.id, stats1)
        await session.commit()

    async with db_session_factory() as session:
        repo = (await session.execute(select(Repo))).scalar_one()
        stmt = select(Contributor).where(Contributor.email_hash == hash_email("a@b.com"))
        contributor = (await session.execute(stmt)).scalar_one()

        stats2 = ContributorStats(
            email_hash="",
            display_name="Alice",
            number_of_commits=7,
            first_commit_date=datetime.datetime(2025, 3, 1, tzinfo=datetime.UTC),
            last_commit_date=datetime.datetime(2025, 8, 1, tzinfo=datetime.UTC),
        )
        created = await upsert_contributed_to(session, contributor.id, repo.id, stats2)
        await session.commit()

        edge = (
            await session.execute(
                select(ContributedTo).where(
                    ContributedTo.contributor_id == contributor.id,
                    ContributedTo.repo_id == repo.id,
                )
            )
        ).scalar_one()
    assert created is False
    assert edge.number_of_commits == 7  # overwritten, not 10+7


async def test_upsert_contributed_to_merge_dates(
    db_session_factory: async_sessionmaker[AsyncSession], clean_gitlog_tables: None
) -> None:
    """Takes min of first_commit_date, max of last_commit_date."""
    async with db_session_factory() as session:
        repo = await create_test_repo(session)
        contributor, _ = await upsert_contributor(session, hash_email("a@b.com"), "Alice")
        stats1 = ContributorStats(
            email_hash="",
            display_name="Alice",
            number_of_commits=5,
            first_commit_date=datetime.datetime(2025, 3, 1, tzinfo=datetime.UTC),
            last_commit_date=datetime.datetime(2025, 6, 1, tzinfo=datetime.UTC),
        )
        await upsert_contributed_to(session, contributor.id, repo.id, stats1)
        await session.commit()

    async with db_session_factory() as session:
        stmt = select(Contributor).where(Contributor.email_hash == hash_email("a@b.com"))
        contributor = (await session.execute(stmt)).scalar_one()
        repo = (await session.execute(select(Repo))).scalar_one()

        stats2 = ContributorStats(
            email_hash="",
            display_name="Alice",
            number_of_commits=3,
            first_commit_date=datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC),  # earlier
            last_commit_date=datetime.datetime(2025, 5, 1, tzinfo=datetime.UTC),  # earlier (should NOT replace)
        )
        await upsert_contributed_to(session, contributor.id, repo.id, stats2)
        await session.commit()

        edge = (
            await session.execute(
                select(ContributedTo).where(
                    ContributedTo.contributor_id == contributor.id,
                    ContributedTo.repo_id == repo.id,
                )
            )
        ).scalar_one()

    assert edge.first_commit_date == datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)  # min
    assert edge.last_commit_date == datetime.datetime(2025, 6, 1, tzinfo=datetime.UTC)  # max (kept original)


# ---------------------------------------------------------------------------
# persist_repo_result
# ---------------------------------------------------------------------------


async def test_persist_repo_result_success(
    db_session_factory: async_sessionmaker[AsyncSession],
    sample_contributor_stats: list[ContributorStats],
    clean_gitlog_tables: None,
) -> None:
    async with db_session_factory() as session:
        await create_test_repo(session)
        await session.commit()

    result = RepoParseResult(
        repo_url="https://github.com/test-org/test-repo",
        contributors=sample_contributor_stats,
        latest_commit_date=datetime.datetime(2025, 8, 1, 14, 0, tzinfo=datetime.UTC),
        total_commits=6,
        bot_commit_count=2,
        bot_contributor_count=1,
    )

    async with db_session_factory() as session:
        repo = (await session.execute(select(Repo))).scalar_one()
        persist = await persist_repo_result(session, repo, result)
        await session.commit()

    assert persist.contributors_created == 2
    assert persist.edges_created == 2

    # Verify data in DB
    async with db_session_factory() as session:
        contributors = (await session.execute(select(Contributor))).scalars().all()
        edges = (await session.execute(select(ContributedTo))).scalars().all()
        repo = (await session.execute(select(Repo))).scalar_one()

    assert len(contributors) == 2
    assert len(edges) == 2
    assert repo.latest_commit_date == datetime.datetime(2025, 8, 1, 14, 0, tzinfo=datetime.UTC)


async def test_persist_repo_result_empty(
    db_session_factory: async_sessionmaker[AsyncSession], clean_gitlog_tables: None
) -> None:
    """No contributors — repo.latest_commit_date still updated."""
    async with db_session_factory() as session:
        await create_test_repo(session)
        await session.commit()

    result = RepoParseResult(
        repo_url="https://github.com/test-org/test-repo",
        contributors=[],
        latest_commit_date=None,
        total_commits=0,
        bot_commit_count=0,
        bot_contributor_count=0,
    )

    async with db_session_factory() as session:
        repo = (await session.execute(select(Repo))).scalar_one()
        persist = await persist_repo_result(session, repo, result)
        await session.commit()

    assert persist.contributors_created == 0
    assert persist.edges_created == 0


async def test_persist_repo_result_updates_latest_commit_date(
    db_session_factory: async_sessionmaker[AsyncSession], clean_gitlog_tables: None
) -> None:
    async with db_session_factory() as session:
        await create_test_repo(session)
        await session.commit()

    new_date = datetime.datetime(2025, 12, 25, tzinfo=datetime.UTC)
    result = RepoParseResult(
        repo_url="https://github.com/test-org/test-repo",
        contributors=[],
        latest_commit_date=new_date,
        total_commits=0,
        bot_commit_count=0,
        bot_contributor_count=0,
    )

    async with db_session_factory() as session:
        repo = (await session.execute(select(Repo))).scalar_one()
        await persist_repo_result(session, repo, result)
        await session.commit()

    async with db_session_factory() as session:
        repo = (await session.execute(select(Repo))).scalar_one()
    assert repo.latest_commit_date == new_date


async def test_persist_repo_result_multiple_contributors(
    db_session_factory: async_sessionmaker[AsyncSession], clean_gitlog_tables: None
) -> None:
    async with db_session_factory() as session:
        await create_test_repo(session)
        await session.commit()

    stats = [
        ContributorStats(
            hash_email(f"user{i}@ex.com"),
            f"User{i}",
            i + 1,
            datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC),
            datetime.datetime(2025, 6, 1, tzinfo=datetime.UTC),
        )
        for i in range(5)
    ]
    result = RepoParseResult(
        repo_url="https://github.com/test-org/test-repo",
        contributors=stats,
        latest_commit_date=datetime.datetime(2025, 6, 1, tzinfo=datetime.UTC),
        total_commits=15,
        bot_commit_count=0,
        bot_contributor_count=0,
    )

    async with db_session_factory() as session:
        repo = (await session.execute(select(Repo))).scalar_one()
        persist = await persist_repo_result(session, repo, result)
        await session.commit()

    assert persist.contributors_created == 5
    assert persist.edges_created == 5


async def test_persist_result_counts(db_session_factory: async_sessionmaker[AsyncSession], clean_gitlog_tables: None) -> None:
    """Run twice — first creates, second updates. Verify counts."""
    async with db_session_factory() as session:
        await create_test_repo(session)
        await session.commit()

    stats = [
        ContributorStats(
            hash_email("a@b.com"),
            "Alice",
            10,
            datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC),
            datetime.datetime(2025, 6, 1, tzinfo=datetime.UTC),
        ),
    ]
    result = RepoParseResult(
        repo_url="https://github.com/test-org/test-repo",
        contributors=stats,
        latest_commit_date=datetime.datetime(2025, 6, 1, tzinfo=datetime.UTC),
        total_commits=10,
        bot_commit_count=0,
        bot_contributor_count=0,
    )

    # First run
    async with db_session_factory() as session:
        repo = (await session.execute(select(Repo))).scalar_one()
        persist1 = await persist_repo_result(session, repo, result)
        await session.commit()

    assert persist1.contributors_created == 1
    assert persist1.edges_created == 1

    # Second run (same data)
    async with db_session_factory() as session:
        repo = (await session.execute(select(Repo))).scalar_one()
        persist2 = await persist_repo_result(session, repo, result)
        await session.commit()

    assert persist2.contributors_updated == 1
    assert persist2.edges_updated == 1
    assert persist2.contributors_created == 0
    assert persist2.edges_created == 0
