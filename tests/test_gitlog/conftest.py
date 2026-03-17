"""
Shared fixtures for git log parser tests.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import datetime
from collections.abc import AsyncGenerator, Callable
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from pg_atlas.config import settings
from pg_atlas.db_models.base import Visibility
from pg_atlas.db_models.repo_vertex import Repo
from pg_atlas.gitlog.parser import CommitRecord, ContributorStats, hash_email

# ---------------------------------------------------------------------------
# Raw git log output fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_git_log_output() -> str:
    """
    Multi-line string simulating raw ``git log`` output.

    Null-delimited format: ``%aN\\x00%aE\\x00%aI\\x00%H``
    Contains:
    - 3 commits from alice (human)
    - 2 commits from dependabot[bot] (bot)
    - 1 commit from bob (human, different email)
    - 1 malformed line (wrong number of fields)
    - 1 commit with empty email
    """
    return (
        "Alice Dev\x00alice@example.com\x002025-06-15T10:00:00+00:00\x00aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111\n"
        "Alice Dev\x00alice@example.com\x002025-07-20T14:30:00+02:00\x00aaaa2222aaaa2222aaaa2222aaaa2222aaaa2222\n"
        "Alice Dev\x00alice@example.com\x002025-08-01T09:00:00-05:00\x00aaaa3333aaaa3333aaaa3333aaaa3333aaaa3333\n"
        "dependabot[bot]\x0049699333+dependabot[bot]@users.noreply.github.com"
        "\x002025-07-01T00:00:00+00:00\x00bbbb1111bbbb1111bbbb1111bbbb1111bbbb1111\n"
        "dependabot[bot]\x0049699333+dependabot[bot]@users.noreply.github.com"
        "\x002025-07-15T00:00:00+00:00\x00bbbb2222bbbb2222bbbb2222bbbb2222bbbb2222\n"
        "Bob Coder\x00bob@company.org\x002025-06-20T16:45:00+00:00\x00cccc1111cccc1111cccc1111cccc1111cccc1111\n"
        "malformed-line-missing-fields\x00only-two-fields\n"
        "Ghost User\x00\x002025-05-01T00:00:00+00:00\x00dddd1111dddd1111dddd1111dddd1111dddd1111\n"
    )


# ---------------------------------------------------------------------------
# Pre-parsed commit records
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_commit_records() -> list[CommitRecord]:
    """Pre-parsed CommitRecord objects matching the valid human + bot lines."""
    return [
        CommitRecord(
            author_name="Alice Dev",
            author_email="alice@example.com",
            timestamp=datetime.datetime(2025, 6, 15, 10, 0, tzinfo=datetime.UTC),
            commit_hash="aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111",
        ),
        CommitRecord(
            author_name="Alice Dev",
            author_email="alice@example.com",
            timestamp=datetime.datetime(2025, 7, 20, 12, 30, tzinfo=datetime.UTC),  # +02:00 -> UTC
            commit_hash="aaaa2222aaaa2222aaaa2222aaaa2222aaaa2222",
        ),
        CommitRecord(
            author_name="Alice Dev",
            author_email="alice@example.com",
            timestamp=datetime.datetime(2025, 8, 1, 14, 0, tzinfo=datetime.UTC),  # -05:00 -> UTC
            commit_hash="aaaa3333aaaa3333aaaa3333aaaa3333aaaa3333",
        ),
        CommitRecord(
            author_name="dependabot[bot]",
            author_email="49699333+dependabot[bot]@users.noreply.github.com",
            timestamp=datetime.datetime(2025, 7, 1, 0, 0, tzinfo=datetime.UTC),
            commit_hash="bbbb1111bbbb1111bbbb1111bbbb1111bbbb1111",
        ),
        CommitRecord(
            author_name="dependabot[bot]",
            author_email="49699333+dependabot[bot]@users.noreply.github.com",
            timestamp=datetime.datetime(2025, 7, 15, 0, 0, tzinfo=datetime.UTC),
            commit_hash="bbbb2222bbbb2222bbbb2222bbbb2222bbbb2222",
        ),
        CommitRecord(
            author_name="Bob Coder",
            author_email="bob@company.org",
            timestamp=datetime.datetime(2025, 6, 20, 16, 45, tzinfo=datetime.UTC),
            commit_hash="cccc1111cccc1111cccc1111cccc1111cccc1111",
        ),
    ]


# ---------------------------------------------------------------------------
# Pre-aggregated contributor stats (humans only)
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_contributor_stats() -> list[ContributorStats]:
    """Pre-aggregated stats for 2 human contributors (bots excluded)."""
    return [
        ContributorStats(
            email_hash=hash_email("alice@example.com"),
            display_name="Alice Dev",
            number_of_commits=3,
            first_commit_date=datetime.datetime(2025, 6, 15, 10, 0, tzinfo=datetime.UTC),
            last_commit_date=datetime.datetime(2025, 8, 1, 14, 0, tzinfo=datetime.UTC),
        ),
        ContributorStats(
            email_hash=hash_email("bob@company.org"),
            display_name="Bob Coder",
            number_of_commits=1,
            first_commit_date=datetime.datetime(2025, 6, 20, 16, 45, tzinfo=datetime.UTC),
            last_commit_date=datetime.datetime(2025, 6, 20, 16, 45, tzinfo=datetime.UTC),
        ),
    ]


# ---------------------------------------------------------------------------
# Temp clone directory
# ---------------------------------------------------------------------------


@pytest.fixture
def tmp_clone_dir(tmp_path: Path) -> Path:
    """Temporary directory for clone operations."""
    clone_dir = tmp_path / "clones"
    clone_dir.mkdir()
    return clone_dir


# ---------------------------------------------------------------------------
# Mock git subprocess
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_git_subprocess(monkeypatch: pytest.MonkeyPatch) -> Callable[..., AsyncMock]:
    """
    Factory fixture that patches ``asyncio.create_subprocess_exec``.

    Supports single calls and multi-call sequences via side_effect.

    Usage::

        # Single call
        mock_git_subprocess(stdout=b"output", returncode=0)

        # Multiple sequential calls
        mock_git_subprocess(side_effect=[(b"out1", 0), (b"out2", 0)])
    """

    def _factory(
        *,
        stdout: bytes = b"",
        returncode: int = 0,
        side_effect: list[tuple[bytes, int]] | None = None,
    ) -> AsyncMock:
        if side_effect is not None:
            call_index = 0

            async def _create(*args, **kwargs):
                nonlocal call_index
                idx = min(call_index, len(side_effect) - 1)
                out, rc = side_effect[idx]
                call_index += 1
                proc = MagicMock()
                proc.returncode = rc
                proc.communicate = AsyncMock(return_value=(out, b"" if rc == 0 else b"error"))
                proc.kill = MagicMock()
                proc.wait = AsyncMock()
                return proc

            mock = AsyncMock(side_effect=_create)
        else:
            proc = MagicMock()
            proc.returncode = returncode
            proc.communicate = AsyncMock(return_value=(stdout, b"" if returncode == 0 else b"error"))
            proc.kill = MagicMock()
            proc.wait = AsyncMock()
            mock = AsyncMock(return_value=proc)

        monkeypatch.setattr("asyncio.create_subprocess_exec", mock)
        return mock

    return _factory


# ---------------------------------------------------------------------------
# Database fixtures (shared by test_persist.py and test_db_integration.py)
# ---------------------------------------------------------------------------


@pytest.fixture
async def db_engine() -> AsyncGenerator[Any, None]:
    if not settings.DATABASE_URL:
        pytest.skip("PG_ATLAS_DATABASE_URL not set")
    engine = create_async_engine(settings.DATABASE_URL, poolclass=NullPool)
    yield engine
    await engine.dispose()


@pytest.fixture
async def db_session_factory(db_engine: Any) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(db_engine, class_=AsyncSession, expire_on_commit=False)


@pytest.fixture(autouse=False)
async def clean_gitlog_tables(db_session_factory: async_sessionmaker[AsyncSession]) -> AsyncGenerator[None, None]:
    """Truncate gitlog-affected tables before and after each test."""
    async with db_session_factory() as session:
        await session.execute(text("TRUNCATE TABLE contributed_to, contributors, repos, repo_vertices CASCADE"))
        await session.commit()
    yield
    async with db_session_factory() as session:
        await session.execute(text("TRUNCATE TABLE contributed_to, contributors, repos, repo_vertices CASCADE"))
        await session.commit()


async def create_test_repo(
    session: AsyncSession,
    canonical_id: str = "pkg:github/test-org/test-repo",
    repo_url: str = "https://github.com/test-org/test-repo",
) -> Repo:
    """Create a valid Repo with all required JTI fields."""
    repo = Repo(
        canonical_id=canonical_id,
        display_name=canonical_id.split("/")[-1],
        visibility=Visibility.public,
        latest_version="0.0.0",
        repo_url=repo_url,
    )
    session.add(repo)
    await session.flush()
    return repo
