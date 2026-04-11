"""
Unit tests for git log parsing in pg_atlas.gitlog.parser.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import asyncio
import datetime as dt
from collections.abc import Callable
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from pg_atlas.gitlog.parser import (
    CommitRecord,
    _repo_url_to_path,
    aggregate_contributors,
    clone_or_fetch_repo,
    hash_email,
    normalize_email,
    parse_git_log,
    parse_repo,
)

# ---------------------------------------------------------------------------
# Email helpers
# ---------------------------------------------------------------------------


def test_normalize_email() -> None:
    assert normalize_email("  Alice@Example.COM  ") == "alice@example.com"


def test_normalize_email_already_clean() -> None:
    assert normalize_email("alice@example.com") == "alice@example.com"


def test_hash_email() -> None:
    """Verify SHA-256 hex digest of normalized email."""
    import hashlib

    expected = hashlib.sha256("alice@example.com".encode()).hexdigest()
    assert hash_email("  Alice@Example.COM  ") == expected
    assert len(hash_email("test@test.com")) == 64


def test_hash_email_consistency() -> None:
    """Same email with different whitespace/case produces same hash."""
    assert hash_email("A@b.com") == hash_email("  a@B.COM  ")


# ---------------------------------------------------------------------------
# URL-to-path helper
# ---------------------------------------------------------------------------


def test_repo_url_to_path_basic() -> None:
    assert _repo_url_to_path("https://github.com/Org/repo.git") == "github.com/Org/repo"


def test_repo_url_to_path_trailing_slash() -> None:
    assert _repo_url_to_path("https://github.com/Org/repo/") == "github.com/Org/repo"


def test_repo_url_to_path_query_params() -> None:
    assert _repo_url_to_path("https://github.com/Org/repo?ref=main") == "github.com/Org/repo"


def test_repo_url_to_path_port_stripped() -> None:
    result = _repo_url_to_path("https://github.example.com:8443/Org/repo")
    assert result == "github.example.com/Org/repo"


def test_repo_url_to_path_no_collision() -> None:
    """Different orgs with same repo name produce different paths."""
    path_a = _repo_url_to_path("https://github.com/OrgA/sdk")
    path_b = _repo_url_to_path("https://github.com/OrgB/sdk")
    assert path_a != path_b
    assert "OrgA" in path_a
    assert "OrgB" in path_b


# ---------------------------------------------------------------------------
# Git subprocess — clone_or_fetch_repo
# ---------------------------------------------------------------------------


async def test_clone_repo_success(mock_git_subprocess: Callable[..., AsyncMock], tmp_clone_dir: Path) -> None:
    mock_git_subprocess(stdout=b"", returncode=0)
    result = await clone_or_fetch_repo("https://github.com/org/repo.git", tmp_clone_dir, timeout=30.0)
    assert result == tmp_clone_dir / "github.com/org/repo"


async def test_fetch_existing_repo(mock_git_subprocess: Callable[..., AsyncMock], tmp_clone_dir: Path) -> None:
    """Existing .git dir triggers fetch + set-head, not clone."""
    target = tmp_clone_dir / "github.com/org/repo"
    target.mkdir(parents=True)
    (target / ".git").mkdir()

    # fetch + set-head = 2 subprocess calls
    mock = mock_git_subprocess(side_effect=[(b"", 0), (b"", 0)])
    result = await clone_or_fetch_repo("https://github.com/org/repo.git", tmp_clone_dir, timeout=30.0)
    assert result == target
    assert mock.call_count == 2


async def test_clone_repo_timeout(tmp_clone_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Timeout during clone raises asyncio.TimeoutError."""

    async def _timeout_create(*args, **kwargs):
        proc = MagicMock()

        async def _communicate():
            raise asyncio.TimeoutError

        proc.communicate = _communicate
        proc.kill = MagicMock()
        proc.wait = AsyncMock()
        proc.returncode = -9
        return proc

    monkeypatch.setattr("asyncio.create_subprocess_exec", AsyncMock(side_effect=_timeout_create))

    with pytest.raises(asyncio.TimeoutError):
        await clone_or_fetch_repo("https://github.com/org/repo.git", tmp_clone_dir, timeout=0.001)


async def test_clone_repo_failure(mock_git_subprocess: Callable[..., AsyncMock], tmp_clone_dir: Path) -> None:
    """Non-zero exit code raises RuntimeError."""
    mock_git_subprocess(stdout=b"", returncode=128)
    with pytest.raises(RuntimeError, match="git command failed"):
        await clone_or_fetch_repo("https://github.com/org/repo.git", tmp_clone_dir, timeout=30.0)


async def test_clone_repo_path_traversal(mock_git_subprocess: Callable[..., AsyncMock], tmp_clone_dir: Path) -> None:
    """URL with '..' that escapes clone_dir is rejected."""
    mock_git_subprocess(stdout=b"", returncode=0)
    with pytest.raises(ValueError, match="outside clone_dir"):
        await clone_or_fetch_repo("https://evil.com/../../etc/passwd", tmp_clone_dir, timeout=30.0)


# ---------------------------------------------------------------------------
# Git log parsing
# ---------------------------------------------------------------------------


async def test_parse_git_log_output(
    mock_git_subprocess: Callable[..., AsyncMock], tmp_path: Path, sample_git_log_output: str
) -> None:
    mock_git_subprocess(stdout=sample_git_log_output.encode(), returncode=0)
    records = await parse_git_log(tmp_path, since_months=24)
    # 3 alice + 2 dependabot + 1 bob = 6 valid records (malformed + empty email skipped)
    assert len(records) == 6


async def test_parse_git_log_empty_repo(mock_git_subprocess: Callable[..., AsyncMock], tmp_path: Path) -> None:
    mock_git_subprocess(stdout=b"", returncode=0)
    records = await parse_git_log(tmp_path, since_months=24)
    assert records == []


async def test_parse_git_log_malformed_line(mock_git_subprocess: Callable[..., AsyncMock], tmp_path: Path) -> None:
    """Malformed lines are skipped, valid lines are parsed."""
    raw = "Alice\x00alice@ex.com\x002025-01-01T00:00:00+00:00\x00abcd1234\nmalformed\n"
    mock_git_subprocess(stdout=raw.encode(), returncode=0)
    records = await parse_git_log(tmp_path, since_months=24)
    assert len(records) == 1
    assert records[0].author_name == "Alice"


async def test_empty_email_skipped(mock_git_subprocess: Callable[..., AsyncMock], tmp_path: Path) -> None:
    """Commits with empty email are filtered out."""
    raw = "Ghost\x00\x002025-01-01T00:00:00+00:00\x00abcd1234\n"
    mock_git_subprocess(stdout=raw.encode(), returncode=0)
    records = await parse_git_log(tmp_path, since_months=24)
    assert len(records) == 0


async def test_unparseable_timestamp_skipped(mock_git_subprocess: Callable[..., AsyncMock], tmp_path: Path) -> None:
    """Commits with invalid timestamps are skipped gracefully."""
    raw = "Alice\x00alice@ex.com\x00not-a-date\x00abcd1234\n"
    mock_git_subprocess(stdout=raw.encode(), returncode=0)
    records = await parse_git_log(tmp_path, since_months=24)
    assert len(records) == 0


async def test_timezone_conversion_to_utc(mock_git_subprocess: Callable[..., AsyncMock], tmp_path: Path) -> None:
    """Various timezone offsets are normalized to UTC."""
    raw = "A\x00a@b.com\x002025-01-01T12:00:00+05:30\x00abcd1234\n"
    mock_git_subprocess(stdout=raw.encode(), returncode=0)
    records = await parse_git_log(tmp_path, since_months=24)
    assert records[0].timestamp == dt.datetime(2025, 1, 1, 6, 30, tzinfo=dt.UTC)


async def test_null_delimited_parsing(mock_git_subprocess: Callable[..., AsyncMock], tmp_path: Path) -> None:
    """Author names with pipes/tabs are handled by null-delimited format."""
    raw = "Alice | Bob\x00alice@ex.com\x002025-01-01T00:00:00+00:00\x00abcd1234\n"
    mock_git_subprocess(stdout=raw.encode(), returncode=0)
    records = await parse_git_log(tmp_path, since_months=24)
    assert records[0].author_name == "Alice | Bob"


async def test_since_months_parameter(mock_git_subprocess: Callable[..., AsyncMock], tmp_path: Path) -> None:
    """Verify --since flag includes the months parameter."""
    mock = mock_git_subprocess(stdout=b"", returncode=0)
    await parse_git_log(tmp_path, since_months=12)
    # Check the git command includes --since=12 months ago
    call_args = mock.call_args_list[0]
    cmd = call_args[0] if call_args[0] else call_args[1].get("args", [])
    cmd_str = " ".join(str(a) for a in cmd)
    assert "--since=12 months ago" in cmd_str


async def test_no_merges_flag(mock_git_subprocess: Callable[..., AsyncMock], tmp_path: Path) -> None:
    """Verify --no-merges is passed to git log."""
    mock = mock_git_subprocess(stdout=b"", returncode=0)
    await parse_git_log(tmp_path, since_months=24)
    call_args = mock.call_args_list[0]
    cmd = call_args[0] if call_args[0] else call_args[1].get("args", [])
    assert "--no-merges" in cmd


async def test_origin_head_ref(mock_git_subprocess: Callable[..., AsyncMock], tmp_path: Path) -> None:
    """Verify origin/HEAD is passed as the ref argument."""
    mock = mock_git_subprocess(stdout=b"", returncode=0)
    await parse_git_log(tmp_path, since_months=24)
    call_args = mock.call_args_list[0]
    cmd = call_args[0] if call_args[0] else call_args[1].get("args", [])
    assert "origin/HEAD" in cmd


async def test_origin_head_fallback_to_main(mock_git_subprocess: Callable[..., AsyncMock], tmp_path: Path) -> None:
    """origin/HEAD fails, origin/main succeeds."""
    mock_git_subprocess(
        side_effect=[
            (b"", 128),  # origin/HEAD fails
            (b"A\x00a@b.com\x002025-01-01T00:00:00+00:00\x00abcd1234\n", 0),  # origin/main succeeds
        ]
    )
    records = await parse_git_log(tmp_path, since_months=24)
    assert len(records) == 1


async def test_origin_head_fallback_to_master(mock_git_subprocess: Callable[..., AsyncMock], tmp_path: Path) -> None:
    """origin/HEAD and origin/main fail, origin/master succeeds."""
    mock_git_subprocess(
        side_effect=[
            (b"", 128),  # origin/HEAD fails
            (b"", 128),  # origin/main fails
            (b"A\x00a@b.com\x002025-01-01T00:00:00+00:00\x00abcd1234\n", 0),  # origin/master succeeds
        ]
    )
    records = await parse_git_log(tmp_path, since_months=24)
    assert len(records) == 1


async def test_origin_head_all_fallbacks_fail(mock_git_subprocess: Callable[..., AsyncMock], tmp_path: Path) -> None:
    """All three refs fail — raises RuntimeError."""
    mock_git_subprocess(
        side_effect=[
            (b"", 128),
            (b"", 128),
            (b"", 128),
        ]
    )
    with pytest.raises(RuntimeError, match="All ref fallbacks failed"):
        await parse_git_log(tmp_path, since_months=24)


async def test_commit_hash_captured(mock_git_subprocess: Callable[..., AsyncMock], tmp_path: Path) -> None:
    raw = "Alice\x00alice@ex.com\x002025-01-01T00:00:00+00:00\x00deadbeef12345678\n"
    mock_git_subprocess(stdout=raw.encode(), returncode=0)
    records = await parse_git_log(tmp_path, since_months=24)
    assert records[0].commit_hash == "deadbeef12345678"


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------


def test_aggregate_contributors(sample_commit_records: list[CommitRecord]) -> None:
    stats, bot_commits, bot_contribs = aggregate_contributors(sample_commit_records)
    # 2 humans (alice=3, bob=1), 1 bot (dependabot=2)
    assert len(stats) == 2
    assert bot_commits == 2
    assert bot_contribs == 1


def test_aggregate_same_person_different_names() -> None:
    """Same email with different names — latest name wins."""
    commits = [
        CommitRecord("Old Name", "dev@ex.com", dt.datetime(2025, 1, 1, tzinfo=dt.UTC), "a1"),
        CommitRecord("New Name", "dev@ex.com", dt.datetime(2025, 6, 1, tzinfo=dt.UTC), "a2"),
    ]
    stats, _, _ = aggregate_contributors(commits)
    assert len(stats) == 1
    assert stats[0].display_name == "New Name"


def test_aggregate_sort_order() -> None:
    """Results sorted by commit count descending."""
    commits = [
        CommitRecord("Few", "few@ex.com", dt.datetime(2025, 1, 1, tzinfo=dt.UTC), "a1"),
        CommitRecord("Many", "many@ex.com", dt.datetime(2025, 1, 1, tzinfo=dt.UTC), "b1"),
        CommitRecord("Many", "many@ex.com", dt.datetime(2025, 2, 1, tzinfo=dt.UTC), "b2"),
        CommitRecord("Many", "many@ex.com", dt.datetime(2025, 3, 1, tzinfo=dt.UTC), "b3"),
    ]
    stats, _, _ = aggregate_contributors(commits)
    assert stats[0].display_name == "Many"
    assert stats[0].number_of_commits == 3
    assert stats[1].number_of_commits == 1


def test_aggregate_bot_excluded() -> None:
    """Bot contributors are not in the returned list."""
    commits = [
        CommitRecord("dependabot[bot]", "bot@noreply.github.com", dt.datetime(2025, 1, 1, tzinfo=dt.UTC), "b1"),
        CommitRecord("Human", "human@ex.com", dt.datetime(2025, 1, 1, tzinfo=dt.UTC), "h1"),
    ]
    stats, _, _ = aggregate_contributors(commits)
    assert len(stats) == 1
    assert stats[0].display_name == "Human"


def test_aggregate_bot_commit_count() -> None:
    """Bot commits counted in second return value."""
    commits = [
        CommitRecord("dependabot[bot]", "bot@noreply.github.com", dt.datetime(2025, 1, 1, tzinfo=dt.UTC), "b1"),
        CommitRecord("dependabot[bot]", "bot@noreply.github.com", dt.datetime(2025, 2, 1, tzinfo=dt.UTC), "b2"),
    ]
    _, bot_commits, _ = aggregate_contributors(commits)
    assert bot_commits == 2


def test_aggregate_bot_contributor_count() -> None:
    """Bot authors counted in third return value."""
    commits = [
        CommitRecord("dependabot[bot]", "d@noreply.github.com", dt.datetime(2025, 1, 1, tzinfo=dt.UTC), "b1"),
        CommitRecord("renovate[bot]", "r@noreply.github.com", dt.datetime(2025, 1, 1, tzinfo=dt.UTC), "b2"),
    ]
    _, _, bot_contribs = aggregate_contributors(commits)
    assert bot_contribs == 2


def test_aggregate_human_only() -> None:
    """All-human commits return full list with zero bot counts."""
    commits = [
        CommitRecord("Alice", "a@ex.com", dt.datetime(2025, 1, 1, tzinfo=dt.UTC), "a1"),
        CommitRecord("Bob", "b@ex.com", dt.datetime(2025, 1, 1, tzinfo=dt.UTC), "b1"),
    ]
    stats, bot_commits, bot_contribs = aggregate_contributors(commits)
    assert len(stats) == 2
    assert bot_commits == 0
    assert bot_contribs == 0


def test_aggregate_mixed_bot_and_human() -> None:
    """3 humans + 1 bot -> list has 3 entries."""
    commits = [
        CommitRecord("A", "a@ex.com", dt.datetime(2025, 1, 1, tzinfo=dt.UTC), "a1"),
        CommitRecord("B", "b@ex.com", dt.datetime(2025, 1, 1, tzinfo=dt.UTC), "b1"),
        CommitRecord("C", "c@ex.com", dt.datetime(2025, 1, 1, tzinfo=dt.UTC), "c1"),
        CommitRecord("dependabot[bot]", "bot@x.com", dt.datetime(2025, 1, 1, tzinfo=dt.UTC), "d1"),
        CommitRecord("dependabot[bot]", "bot@x.com", dt.datetime(2025, 2, 1, tzinfo=dt.UTC), "d2"),
    ]
    stats, bot_commits, bot_contribs = aggregate_contributors(commits)
    assert len(stats) == 3
    assert bot_commits == 2
    assert bot_contribs == 1


def test_aggregate_all_bots() -> None:
    """All contributors are bots -> empty list."""
    commits = [
        CommitRecord("dependabot[bot]", "d@x.com", dt.datetime(2025, 1, 1, tzinfo=dt.UTC), "d1"),
        CommitRecord("renovate[bot]", "r@x.com", dt.datetime(2025, 1, 1, tzinfo=dt.UTC), "r1"),
    ]
    stats, bot_commits, bot_contribs = aggregate_contributors(commits)
    assert stats == []
    assert bot_commits == 2
    assert bot_contribs == 2


# ---------------------------------------------------------------------------
# parse_repo (orchestrator)
# ---------------------------------------------------------------------------


async def test_parse_repo_success(
    mock_git_subprocess: Callable[..., AsyncMock], tmp_clone_dir: Path, sample_git_log_output: str
) -> None:
    """End-to-end with mocked git: clone (1 call) + log (1 call)."""
    mock_git_subprocess(
        side_effect=[
            (b"", 0),  # clone
            (sample_git_log_output.encode(), 0),  # git log
        ]
    )
    result = await parse_repo("https://github.com/org/repo.git", tmp_clone_dir, since_months=24, timeout=30.0)
    assert result.repo_url == "https://github.com/org/repo.git"
    assert len(result.contributors) == 2  # alice + bob (bots excluded)
    assert result.bot_commit_count == 2
    assert result.bot_contributor_count == 1
    assert result.total_commits == 6
    assert result.latest_commit_date is not None
    assert result.errors == []


async def test_parse_repo_clone_failure(mock_git_subprocess: Callable[..., AsyncMock], tmp_clone_dir: Path) -> None:
    """Clone failure produces a partial result with error."""
    mock_git_subprocess(stdout=b"", returncode=128)
    result = await parse_repo("https://github.com/org/repo.git", tmp_clone_dir, since_months=24, timeout=30.0)
    assert result.contributors == []
    assert result.latest_commit_date is None
    assert len(result.errors) == 1
    assert "RuntimeError" in result.errors[0]


async def test_parse_repo_bot_commit_count(mock_git_subprocess: Callable[..., AsyncMock], tmp_clone_dir: Path) -> None:
    """Bot counts are computed correctly in end-to-end flow."""
    raw = (
        "Human\x00human@ex.com\x002025-01-01T00:00:00+00:00\x00h1\n"
        "dependabot[bot]\x00bot@noreply.github.com\x002025-01-02T00:00:00+00:00\x00b1\n"
        "dependabot[bot]\x00bot@noreply.github.com\x002025-01-03T00:00:00+00:00\x00b2\n"
    )
    mock_git_subprocess(
        side_effect=[
            (b"", 0),  # clone
            (raw.encode(), 0),  # git log
        ]
    )
    result = await parse_repo("https://github.com/org/repo.git", tmp_clone_dir, since_months=24, timeout=30.0)
    assert len(result.contributors) == 1
    assert result.bot_commit_count == 2
    assert result.bot_contributor_count == 1
    assert result.total_commits == 3
