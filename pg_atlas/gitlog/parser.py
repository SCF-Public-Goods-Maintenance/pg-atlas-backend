"""
Git clone and log parsing for contributor statistics.

Pure data extraction — no database dependency. Imports bot detection
from filters.py to separate human contributors from bots before
results reach the persistence layer.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import logging
import re
import urllib.parse
from dataclasses import dataclass, field
from operator import attrgetter
from pathlib import Path

from pg_atlas.gitlog.filters import is_bot

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class CommitRecord:
    """A single parsed commit from git log output."""

    author_name: str
    author_email: str  # raw, before normalization
    timestamp: dt.datetime
    commit_hash: str


@dataclass
class ContributorStats:
    """Aggregated stats for one human contributor to one repo."""

    email_hash: str  # SHA-256 hex of normalized email
    display_name: str  # most recent author name seen
    number_of_commits: int
    first_commit_date: dt.datetime
    last_commit_date: dt.datetime


@dataclass
class RepoParseResult:
    """Complete parse result for one repository."""

    repo_url: str
    contributors: list[ContributorStats]  # human contributors ONLY (bots excluded)
    latest_commit_date: dt.datetime | None  # None if no commits
    total_commits: int  # parsed commits in window (before bot filtering)
    bot_commit_count: int  # commits excluded because author is a bot
    bot_contributor_count: int  # unique bot authors excluded
    errors: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Email helpers
# ---------------------------------------------------------------------------


def normalize_email(email: str) -> str:
    """Lowercase and strip whitespace from an email address."""
    return email.strip().lower()


def hash_email(email: str) -> str:
    """
    Return the SHA-256 hex digest of the normalized email.

    Must produce a 64-char lowercase hex string matching the
    ``HexBinary(32)`` column type in the database.
    """
    return hashlib.sha256(normalize_email(email).encode()).hexdigest()


# ---------------------------------------------------------------------------
# URL-to-path helper
# ---------------------------------------------------------------------------


def _repo_url_to_path(repo_url: str) -> str:
    """
    Convert a repo URL to a safe, unique filesystem path.

    Example: ``https://github.com/org/repo.git`` → ``github.com/org/repo``
    """
    parsed = urllib.parse.urlparse(repo_url)
    hostname = parsed.hostname or ""
    path = parsed.path
    # Strip .git suffix and trailing slashes
    path = path.removesuffix(".git")
    path = path.rstrip("/")
    combined = f"{hostname}{path}"
    # Replace unsafe characters
    return re.sub(r"[^a-zA-Z0-9/\-_.]", "_", combined)


# ---------------------------------------------------------------------------
# Git subprocess helpers
# ---------------------------------------------------------------------------


async def clone_or_fetch_repo(repo_url: str, clone_dir: Path, timeout: float) -> Path:
    """
    Clone a repo (blobless) or fetch updates if it already exists.

    Returns the path to the local clone directory.
    """
    target = clone_dir / _repo_url_to_path(repo_url)

    # Guard against path traversal (e.g. repo_url containing "..")
    try:
        target.resolve().relative_to(clone_dir.resolve())
    except ValueError:
        raise ValueError(f"repo_url produces a path outside clone_dir: {repo_url}") from None

    if (target / ".git").is_dir():
        # Existing clone — fetch updates
        await _run_git(["git", "fetch", "--all"], cwd=target, timeout=timeout)
        # Update origin/HEAD to track the remote's default branch
        await _run_git(["git", "remote", "set-head", "origin", "--auto"], cwd=target, timeout=timeout)
    else:
        # Fresh blobless clone (commit graph only, no file contents)
        # Let git create the target directory — pre-creating it causes
        # "destination path already exists" errors if a previous clone failed.
        target.parent.mkdir(parents=True, exist_ok=True)
        await _run_git(
            ["git", "clone", "--filter=blob:none", "--no-checkout", repo_url, str(target)],
            cwd=clone_dir,
            timeout=timeout,
        )

    return target


async def _run_git(cmd: list[str], *, cwd: Path, timeout: float) -> bytes:
    """
    Run a git command via ``asyncio.create_subprocess_exec``.

    Returns stdout on success. Raises ``RuntimeError`` on non-zero exit
    or ``asyncio.TimeoutError`` on timeout.
    """
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=cwd,
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        raise

    if proc.returncode != 0:
        stderr_text = stderr.decode(errors="replace").strip()
        raise RuntimeError(f"git command failed ({cmd[0:2]!r}, rc={proc.returncode}): {stderr_text}")

    return stdout


# ---------------------------------------------------------------------------
# Git log parsing
# ---------------------------------------------------------------------------

_FALLBACK_REFS = ("origin/HEAD", "origin/main", "origin/master")


async def parse_git_log(repo_path: Path, since_months: int) -> list[CommitRecord]:
    """
    Parse ``git log`` output for the default branch over the given window.

    Tries ``origin/HEAD`` first, then falls back to ``origin/main`` and
    ``origin/master``.
    """
    since_arg = f"--since={since_months} months ago"
    fmt = "--format=%aN%x00%aE%x00%aI%x00%H"

    stdout: bytes | None = None
    last_err: RuntimeError | None = None

    for ref in _FALLBACK_REFS:
        try:
            stdout = await _run_git(
                ["git", "log", "--no-merges", fmt, since_arg, ref],
                cwd=repo_path,
                timeout=60.0,
            )
            break
        except RuntimeError as exc:
            last_err = exc
            logger.debug("ref %s failed for %s: %s", ref, repo_path, exc)
            continue

    if stdout is None:
        msg = f"All ref fallbacks failed for {repo_path}"
        if last_err:
            msg = f"{msg}: {last_err}"
        raise RuntimeError(msg)

    # TODO: store raw output as artifact
    return _parse_log_output(stdout.decode(errors="replace"))


def _parse_log_output(raw: str) -> list[CommitRecord]:
    """Parse null-delimited git log output into CommitRecord objects."""
    records: list[CommitRecord] = []
    for line in raw.strip().splitlines():
        parts = line.split("\x00")
        if len(parts) != 4:
            logger.warning("Skipping malformed git log line: %r", line[:120])
            continue

        name, email, iso_ts, commit_hash = parts

        # Skip commits with empty email
        if not email or not email.strip():
            logger.warning("Skipping commit %s with empty email", commit_hash)
            continue

        try:
            ts = dt.datetime.fromisoformat(iso_ts).astimezone(dt.UTC)
        except ValueError:
            logger.warning("Skipping commit %s with unparseable timestamp: %r", commit_hash, iso_ts)
            continue

        records.append(CommitRecord(author_name=name, author_email=email, timestamp=ts, commit_hash=commit_hash))

    return records


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------


def aggregate_contributors(commits: list[CommitRecord]) -> tuple[list[ContributorStats], int, int]:
    """
    Group commits by normalized email and separate humans from bots.

    Returns a tuple of:
    - ``list[ContributorStats]`` — human contributors sorted by commit count descending
    - ``int`` — total bot commits excluded
    - ``int`` — unique bot authors excluded
    """
    # Group by normalized email
    groups: dict[str, list[CommitRecord]] = {}
    for commit in commits:
        key = normalize_email(commit.author_email)
        groups.setdefault(key, []).append(commit)

    human_stats: list[ContributorStats] = []
    bot_commit_count = 0
    bot_contributor_count = 0

    for _email_key, group_commits in groups.items():
        # Find the most recent commit for display_name and bot check
        latest = max(group_commits, key=attrgetter("timestamp"))
        display_name = latest.author_name
        raw_email = latest.author_email

        if is_bot(display_name, raw_email):
            bot_commit_count += len(group_commits)
            bot_contributor_count += 1
            continue

        human_stats.append(
            ContributorStats(
                email_hash=hash_email(group_commits[0].author_email),
                display_name=display_name,
                number_of_commits=len(group_commits),
                first_commit_date=min(c.timestamp for c in group_commits),
                last_commit_date=max(c.timestamp for c in group_commits),
            )
        )

    # Sort by commit count descending
    human_stats.sort(key=attrgetter("number_of_commits"), reverse=True)

    return human_stats, bot_commit_count, bot_contributor_count


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def parse_repo(
    repo_url: str,
    clone_dir: Path,
    since_months: int,
    timeout: float,
) -> RepoParseResult:
    """
    Clone/fetch a repo, parse its git log, and aggregate contributor stats.

    Catches errors from clone/fetch/parse steps and returns a partial
    result with error messages rather than raising.
    """
    try:
        repo_path = await clone_or_fetch_repo(repo_url, clone_dir, timeout)
        commits = await parse_git_log(repo_path, since_months)
    except (RuntimeError, asyncio.TimeoutError, OSError) as exc:
        logger.exception("Failed to clone/parse %s", repo_url)
        return RepoParseResult(
            repo_url=repo_url,
            contributors=[],
            latest_commit_date=None,
            total_commits=0,
            bot_commit_count=0,
            bot_contributor_count=0,
            errors=[f"{type(exc).__name__}: {exc}"],
        )

    contributors, bot_commit_count, bot_contributor_count = aggregate_contributors(commits)

    latest_commit_date: dt.datetime | None = None
    if commits:
        latest_commit_date = max(c.timestamp for c in commits)

    return RepoParseResult(
        repo_url=repo_url,
        contributors=contributors,
        latest_commit_date=latest_commit_date,
        total_commits=len(commits),
        bot_commit_count=bot_commit_count,
        bot_contributor_count=bot_contributor_count,
    )
