"""
Database persistence for git log contributor data.

Upserts Contributor vertices and ContributedTo edges, and updates
Repo.latest_commit_date. By the time data reaches this module, bots
have already been filtered out by the parser.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from dataclasses import dataclass, field

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from pg_atlas.db_models.contributed_to import ContributedTo
from pg_atlas.db_models.contributor import Contributor
from pg_atlas.db_models.repo_vertex import Repo
from pg_atlas.gitlog.parser import ContributorStats, RepoParseResult


@dataclass
class PersistResult:
    """Summary of database writes for one repo."""

    contributors_created: int = field(default=0)
    contributors_updated: int = field(default=0)
    edges_created: int = field(default=0)
    edges_updated: int = field(default=0)


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

    repo.latest_commit_date = result.latest_commit_date

    return persist
