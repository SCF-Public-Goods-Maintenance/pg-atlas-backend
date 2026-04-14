"""
Database persistence for git log contributor data.

Upserts Contributor vertices and ContributedTo edges, and updates
Repo.latest_commit_date. By the time data reaches this module, bots
have already been filtered out by the parser.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from pg_atlas.db_models.base import SubmissionStatus
from pg_atlas.db_models.contributed_to import ContributedTo
from pg_atlas.db_models.contributor import Contributor
from pg_atlas.db_models.gitlog_artifact import GitLogArtifact
from pg_atlas.db_models.repo_vertex import Repo
from pg_atlas.gitlog.parser import ContributorStats, RepoParseResult


@dataclass
class PersistResult:
    """Summary of database writes for one repo."""

    contributors_created: int = field(default=0)
    contributors_updated: int = field(default=0)
    edges_created: int = field(default=0)
    edges_updated: int = field(default=0)


@dataclass
class GitLogAttemptAudit:
    """Inputs required to store one GitLogArtifact audit row."""

    since_months: int
    status: SubmissionStatus
    seed_run_ordinal: int = 0
    error_detail: str | None = None
    artifact_path: str | None = None
    artifact_content_hash: str | None = None
    null_previous_artifact_paths: bool = False


async def upsert_contributor(session: AsyncSession, email_hash: str, name: str) -> tuple[Contributor, bool]:
    """
    Find or create a Contributor by email_hash.

    If the contributor exists, update the display name (most recent wins).
    Uses a simple SELECT-then-INSERT pattern — no unique constraint on
    email_hash, so IntegrityError race handling does not apply.

    Returns a tuple of (Contributor, created) where created is True if
    a new row was inserted.
    """
    stmt = select(Contributor).where(Contributor.email_hash == email_hash)
    result = await session.execute(stmt)
    contributor = result.scalars().first()

    if contributor is not None:
        contributor.name = name
        return contributor, False

    contributor = Contributor(email_hash=email_hash, name=name)
    session.add(contributor)
    await session.flush()
    return contributor, True


async def upsert_contributed_to(
    session: AsyncSession,
    contributor_id: int,
    repo_id: int,
    stats: ContributorStats,
) -> bool:
    """
    Find or create a ContributedTo edge.

    On update: overwrites commit count (reflects current window), takes
    min of first_commit_date and max of last_commit_date.

    Returns True if created, False if updated.
    """
    stmt = select(ContributedTo).where(
        ContributedTo.contributor_id == contributor_id,
        ContributedTo.repo_id == repo_id,
    )
    result = await session.execute(stmt)
    edge = result.scalar_one_or_none()

    if edge is not None:
        edge.number_of_commits = stats.number_of_commits
        edge.first_commit_date = min(edge.first_commit_date, stats.first_commit_date)
        edge.last_commit_date = max(edge.last_commit_date, stats.last_commit_date)
        return False

    edge = ContributedTo(
        contributor_id=contributor_id,
        repo_id=repo_id,
        number_of_commits=stats.number_of_commits,
        first_commit_date=stats.first_commit_date,
        last_commit_date=stats.last_commit_date,
    )
    session.add(edge)
    await session.flush()
    return True


async def persist_repo_result(
    session: AsyncSession,
    repo: Repo,
    result: RepoParseResult,
) -> PersistResult:
    """
    Persist all contributor data for one repo.

    Re-attaches the (possibly detached) Repo object via ``session.merge``,
    then upserts each contributor and edge. Updates
    ``repo.latest_commit_date``. Does NOT commit — the caller manages the
    transaction boundary.
    """
    repo = await session.merge(repo)
    persist = PersistResult()

    for stats in result.contributors:
        contributor, contributor_created = await upsert_contributor(session, stats.email_hash, stats.display_name)
        if contributor_created:
            persist.contributors_created += 1
        else:
            persist.contributors_updated += 1

        edge_created = await upsert_contributed_to(session, contributor.id, repo.id, stats)
        if edge_created:
            persist.edges_created += 1
        else:
            persist.edges_updated += 1

    # invariant: `repo.latest_commit_date` is monotonically increasing
    if result.latest_commit_date and result.latest_commit_date > (
        repo.latest_commit_date or dt.datetime(1, 1, 1, tzinfo=dt.UTC)
    ):
        repo.latest_commit_date = result.latest_commit_date

    return persist


async def record_gitlog_attempt(
    session: AsyncSession,
    repo_id: int,
    attempt: GitLogAttemptAudit,
) -> GitLogArtifact:
    """
    Insert one GitLogArtifact row for a repo processing attempt.

    When ``attempt.null_previous_artifact_paths`` is true and the current row has
    a non-null ``artifact_path``, older rows for the same repo are updated to
    ``artifact_path=NULL`` in the same transaction.
    """

    processed_at = dt.datetime.now(dt.UTC)
    row = GitLogArtifact(
        repo_id=repo_id,
        seed_run_ordinal=attempt.seed_run_ordinal,
        since_months=attempt.since_months,
        artifact_path=attempt.artifact_path,
        gitlog_content_hash=attempt.artifact_content_hash,
        status=attempt.status,
        error_detail=attempt.error_detail[:4096] if attempt.error_detail is not None else None,
    )
    row.processed_at = processed_at
    session.add(row)
    await session.flush()

    if attempt.null_previous_artifact_paths and attempt.artifact_path:
        await session.execute(
            update(GitLogArtifact)
            .where(GitLogArtifact.repo_id == repo_id)
            .where(GitLogArtifact.id != row.id)
            .where(GitLogArtifact.artifact_path.isnot(None))
            .values(artifact_path=None)
        )

    return row
