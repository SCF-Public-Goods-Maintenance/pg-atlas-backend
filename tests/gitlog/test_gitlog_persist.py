"""
Unit tests for database persistence in pg_atlas.gitlog.persist.

These tests require a running PostgreSQL instance (docker compose).

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import datetime as dt
import os

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pg_atlas.db_models.base import SubmissionStatus
from pg_atlas.db_models.contributed_to import ContributedTo
from pg_atlas.db_models.contributor import Contributor
from pg_atlas.db_models.gitlog_artifact import GitLogArtifact
from pg_atlas.db_models.repo_vertex import Repo
from pg_atlas.gitlog.parser import ContributorStats, RepoParseResult, hash_email
from pg_atlas.gitlog.persist import (
    GitLogAttemptAudit,
    persist_repo_result,
    record_gitlog_attempt,
    upsert_contributed_to,
    upsert_contributor,
)
from tests.gitlog.conftest import create_test_repo

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
        _contributor, created = await upsert_contributor(session, email_hash, "Updated")
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
            first_commit_date=dt.datetime(2025, 1, 1, tzinfo=dt.UTC),
            last_commit_date=dt.datetime(2025, 6, 1, tzinfo=dt.UTC),
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
        repo_id = repo.id
        contributor, _ = await upsert_contributor(session, hash_email("a@b.com"), "Alice")
        stats1 = ContributorStats(
            email_hash="",
            display_name="Alice",
            number_of_commits=10,
            first_commit_date=dt.datetime(2025, 1, 1, tzinfo=dt.UTC),
            last_commit_date=dt.datetime(2025, 6, 1, tzinfo=dt.UTC),
        )
        await upsert_contributed_to(session, contributor.id, repo.id, stats1)
        await session.commit()

    async with db_session_factory() as session:
        repo = (await session.execute(select(Repo).where(Repo.id == repo_id))).scalar_one()
        stmt = select(Contributor).where(Contributor.email_hash == hash_email("a@b.com"))
        contributor = (await session.execute(stmt)).scalar_one()

        stats2 = ContributorStats(
            email_hash="",
            display_name="Alice",
            number_of_commits=7,
            first_commit_date=dt.datetime(2025, 3, 1, tzinfo=dt.UTC),
            last_commit_date=dt.datetime(2025, 8, 1, tzinfo=dt.UTC),
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
        repo_id = repo.id
        contributor, _ = await upsert_contributor(session, hash_email("a@b.com"), "Alice")
        stats1 = ContributorStats(
            email_hash="",
            display_name="Alice",
            number_of_commits=5,
            first_commit_date=dt.datetime(2025, 3, 1, tzinfo=dt.UTC),
            last_commit_date=dt.datetime(2025, 6, 1, tzinfo=dt.UTC),
        )
        await upsert_contributed_to(session, contributor.id, repo.id, stats1)
        await session.commit()

    async with db_session_factory() as session:
        stmt = select(Contributor).where(Contributor.email_hash == hash_email("a@b.com"))
        contributor = (await session.execute(stmt)).scalar_one()
        repo = (await session.execute(select(Repo).where(Repo.id == repo_id))).scalar_one()

        stats2 = ContributorStats(
            email_hash="",
            display_name="Alice",
            number_of_commits=3,
            first_commit_date=dt.datetime(2025, 1, 1, tzinfo=dt.UTC),  # earlier
            last_commit_date=dt.datetime(2025, 5, 1, tzinfo=dt.UTC),  # earlier (should NOT replace)
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

    assert edge.first_commit_date == dt.datetime(2025, 1, 1, tzinfo=dt.UTC)  # min
    assert edge.last_commit_date == dt.datetime(2025, 6, 1, tzinfo=dt.UTC)  # max (kept original)


# ---------------------------------------------------------------------------
# persist_repo_result
# ---------------------------------------------------------------------------


async def test_persist_repo_result_success(
    db_session_factory: async_sessionmaker[AsyncSession],
    sample_contributor_stats: list[ContributorStats],
    clean_gitlog_tables: None,
) -> None:
    async with db_session_factory() as session:
        repo = await create_test_repo(session)
        repo_id = repo.id
        await session.commit()

    result = RepoParseResult(
        repo_url="https://github.com/test-org/test-repo",
        contributors=sample_contributor_stats,
        latest_commit_date=dt.datetime(2025, 8, 1, 14, 0, tzinfo=dt.UTC),
        total_commits=6,
        bot_commit_count=2,
        bot_contributor_count=1,
    )

    async with db_session_factory() as session:
        repo = (await session.execute(select(Repo).where(Repo.id == repo_id))).scalar_one()
        persist = await persist_repo_result(session, repo, result)
        await session.commit()

    assert persist.contributors_created == 2
    assert persist.edges_created == 2

    # Verify data in DB
    async with db_session_factory() as session:
        expected_hashes = [stats.email_hash for stats in sample_contributor_stats]
        contributors = (
            (await session.execute(select(Contributor).where(Contributor.email_hash.in_(expected_hashes)))).scalars().all()
        )
        edges = (await session.execute(select(ContributedTo).where(ContributedTo.repo_id == repo_id))).scalars().all()
        repo = (await session.execute(select(Repo).where(Repo.id == repo_id))).scalar_one()

    assert len(contributors) == 2
    assert len(edges) == 2
    assert repo.latest_commit_date == dt.datetime(2025, 8, 1, 14, 0, tzinfo=dt.UTC)


async def test_persist_repo_result_empty(
    db_session_factory: async_sessionmaker[AsyncSession], clean_gitlog_tables: None
) -> None:
    """No contributors — repo.latest_commit_date still updated."""
    async with db_session_factory() as session:
        repo = await create_test_repo(session)
        repo_id = repo.id
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
        repo = (await session.execute(select(Repo).where(Repo.id == repo_id))).scalar_one()
        persist = await persist_repo_result(session, repo, result)
        await session.commit()

    assert persist.contributors_created == 0
    assert persist.edges_created == 0


async def test_persist_repo_result_updates_latest_commit_date(
    db_session_factory: async_sessionmaker[AsyncSession], clean_gitlog_tables: None
) -> None:
    async with db_session_factory() as session:
        repo = await create_test_repo(session)
        repo_id = repo.id
        await session.commit()

    new_date = dt.datetime(2025, 12, 25, tzinfo=dt.UTC)
    result = RepoParseResult(
        repo_url="https://github.com/test-org/test-repo",
        contributors=[],
        latest_commit_date=new_date,
        total_commits=0,
        bot_commit_count=0,
        bot_contributor_count=0,
    )

    async with db_session_factory() as session:
        repo = (await session.execute(select(Repo).where(Repo.id == repo_id))).scalar_one()
        await persist_repo_result(session, repo, result)
        await session.commit()

    async with db_session_factory() as session:
        repo = (await session.execute(select(Repo).where(Repo.id == repo_id))).scalar_one()
    assert repo.latest_commit_date == new_date


async def test_persist_repo_result_does_not_lower_latest_commit_date(
    db_session_factory: async_sessionmaker[AsyncSession], clean_gitlog_tables: None
) -> None:
    """Persisting older parse results must not regress repo.latest_commit_date."""

    initial_date = dt.datetime(2025, 12, 25, tzinfo=dt.UTC)
    async with db_session_factory() as session:
        repo = await create_test_repo(session)
        repo.latest_commit_date = initial_date
        repo_id = repo.id
        await session.commit()

    older_date = dt.datetime(2025, 6, 1, tzinfo=dt.UTC)
    result = RepoParseResult(
        repo_url="https://github.com/test-org/test-repo",
        contributors=[],
        latest_commit_date=older_date,
        total_commits=0,
        bot_commit_count=0,
        bot_contributor_count=0,
    )

    async with db_session_factory() as session:
        repo = (await session.execute(select(Repo).where(Repo.id == repo_id))).scalar_one()
        await persist_repo_result(session, repo, result)
        await session.commit()

    async with db_session_factory() as session:
        repo = (await session.execute(select(Repo).where(Repo.id == repo_id))).scalar_one()

    assert repo.latest_commit_date == initial_date


async def test_persist_repo_result_multiple_contributors(
    db_session_factory: async_sessionmaker[AsyncSession], clean_gitlog_tables: None
) -> None:
    async with db_session_factory() as session:
        repo = await create_test_repo(session)
        repo_id = repo.id
        await session.commit()

    stats = [
        ContributorStats(
            hash_email(f"user{i}@ex.com"),
            f"User{i}",
            i + 1,
            dt.datetime(2025, 1, 1, tzinfo=dt.UTC),
            dt.datetime(2025, 6, 1, tzinfo=dt.UTC),
        )
        for i in range(5)
    ]
    result = RepoParseResult(
        repo_url="https://github.com/test-org/test-repo",
        contributors=stats,
        latest_commit_date=dt.datetime(2025, 6, 1, tzinfo=dt.UTC),
        total_commits=15,
        bot_commit_count=0,
        bot_contributor_count=0,
    )

    async with db_session_factory() as session:
        repo = (await session.execute(select(Repo).where(Repo.id == repo_id))).scalar_one()
        persist = await persist_repo_result(session, repo, result)
        await session.commit()

    assert persist.contributors_created == 5
    assert persist.edges_created == 5


async def test_persist_result_counts(db_session_factory: async_sessionmaker[AsyncSession], clean_gitlog_tables: None) -> None:
    """Run twice — first creates, second updates. Verify counts."""
    async with db_session_factory() as session:
        repo = await create_test_repo(session)
        repo_id = repo.id
        await session.commit()

    stats = [
        ContributorStats(
            hash_email("a@b.com"),
            "Alice",
            10,
            dt.datetime(2025, 1, 1, tzinfo=dt.UTC),
            dt.datetime(2025, 6, 1, tzinfo=dt.UTC),
        ),
    ]
    result = RepoParseResult(
        repo_url="https://github.com/test-org/test-repo",
        contributors=stats,
        latest_commit_date=dt.datetime(2025, 6, 1, tzinfo=dt.UTC),
        total_commits=10,
        bot_commit_count=0,
        bot_contributor_count=0,
    )

    # First run
    async with db_session_factory() as session:
        repo = (await session.execute(select(Repo).where(Repo.id == repo_id))).scalar_one()
        persist1 = await persist_repo_result(session, repo, result)
        await session.commit()

    assert persist1.contributors_created == 1
    assert persist1.edges_created == 1

    # Second run (same data)
    async with db_session_factory() as session:
        repo = (await session.execute(select(Repo).where(Repo.id == repo_id))).scalar_one()
        persist2 = await persist_repo_result(session, repo, result)
        await session.commit()

    assert persist2.contributors_updated == 1
    assert persist2.edges_updated == 1
    assert persist2.contributors_created == 0
    assert persist2.edges_created == 0


async def test_record_gitlog_attempt_success(
    db_session_factory: async_sessionmaker[AsyncSession], clean_gitlog_tables: None
) -> None:
    """record_gitlog_attempt writes a processed audit row for one repo."""

    async with db_session_factory() as session:
        repo = await create_test_repo(session)
        repo_id = repo.id
        await session.commit()

    async with db_session_factory() as session:
        row = await record_gitlog_attempt(
            session,
            repo_id,
            GitLogAttemptAudit(
                since_months=24,
                status=SubmissionStatus.processed,
                artifact_path="git-logs/test-org/test-repo.gitlog",
                artifact_content_hash=hash_email("artifact@example.com"),
                null_previous_artifact_paths=False,
            ),
        )
        await session.commit()

    assert row.id is not None

    async with db_session_factory() as session:
        rows = (await session.execute(select(GitLogArtifact).where(GitLogArtifact.repo_id == repo_id))).scalars().all()

    assert len(rows) == 1
    assert rows[0].status == SubmissionStatus.processed
    assert rows[0].artifact_path == "git-logs/test-org/test-repo.gitlog"


async def test_record_gitlog_attempt_nulls_previous_artifact_paths(
    db_session_factory: async_sessionmaker[AsyncSession], clean_gitlog_tables: None
) -> None:
    """New successful artifact row nulls historical artifact_path pointers for the same repo."""

    async with db_session_factory() as session:
        repo = await create_test_repo(session)
        repo_id = repo.id
        first_hash = hash_email("first-artifact@example.com")
        second_hash = hash_email("second-artifact@example.com")

        await record_gitlog_attempt(
            session,
            repo_id,
            GitLogAttemptAudit(
                since_months=24,
                status=SubmissionStatus.processed,
                artifact_path="git-logs/test-org/test-repo.gitlog",
                artifact_content_hash=first_hash,
                null_previous_artifact_paths=False,
            ),
        )

        await record_gitlog_attempt(
            session,
            repo_id,
            GitLogAttemptAudit(
                since_months=24,
                status=SubmissionStatus.processed,
                artifact_path="git-logs/test-org/test-repo.gitlog",
                artifact_content_hash=second_hash,
                null_previous_artifact_paths=True,
            ),
        )
        await session.commit()

    async with db_session_factory() as session:
        rows = (
            (
                await session.execute(
                    select(GitLogArtifact).where(GitLogArtifact.repo_id == repo_id).order_by(GitLogArtifact.id.asc())
                )
            )
            .scalars()
            .all()
        )

    assert len(rows) == 2
    assert rows[0].artifact_path is None
    assert rows[1].artifact_path == "git-logs/test-org/test-repo.gitlog"
