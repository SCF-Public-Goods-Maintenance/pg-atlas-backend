"""
CLI entry point for the git log parser.

Usage::

    uv run python -m pg_atlas.gitlog https://github.com/org/repo1 https://github.com/org/repo2
    uv run python -m pg_atlas.gitlog --from-db
    uv run python -m pg_atlas.gitlog --from-db --since-months 12

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from pg_atlas.config import settings
from pg_atlas.db_models.repo_vertex import Repo
from pg_atlas.gitlog.parser import RepoParseResult, parse_repo
from pg_atlas.gitlog.persist import PersistResult, persist_repo_result

logger = logging.getLogger(__name__)


def _log_repo_summary(result: RepoParseResult, persist: PersistResult | None, index: int, total: int) -> None:
    """Log a structured per-repo summary at INFO level."""
    since = settings.GITLOG_SINCE_MONTHS
    bot_pct = (result.bot_commit_count / result.total_commits * 100) if result.total_commits else 0.0
    human_count = len(result.contributors)
    flyby = sum(1 for c in result.contributors if c.number_of_commits == 1)
    flyby_str = f"{flyby} of {human_count} ({flyby / human_count * 100:.1f}%)" if human_count else "N/A"
    top = ", ".join(f"{c.display_name} ({c.number_of_commits})" for c in result.contributors[:5])

    logger.info(
        "[%d/%d] Repo: %s\n"
        "  Commits in window: %d (%d months)\n"
        "  Bot commits excluded: %d from %d bots (%.1f%% of total)\n"
        "  Human contributors: %d\n"
        "  Fly-by contributors: %s\n"
        "  Top contributors: %s",
        index,
        total,
        result.repo_url,
        result.total_commits,
        since,
        result.bot_commit_count,
        result.bot_contributor_count,
        bot_pct,
        human_count,
        flyby_str,
        top or "(none)",
    )

    if result.errors:
        for err in result.errors:
            logger.warning("  Error: %s", err)

    if persist:
        logger.info(
            "  DB: %d contributors created, %d updated; %d edges created, %d updated",
            persist.contributors_created,
            persist.contributors_updated,
            persist.edges_created,
            persist.edges_updated,
        )


async def _resolve_repos(
    session_factory: async_sessionmaker[AsyncSession],
    from_db: bool,
    repo_urls: list[str],
) -> list[Repo]:
    """
    Resolve the list of Repo objects to process.

    In ``--from-db`` mode, queries all Repos with a non-null repo_url.
    In explicit URL mode, looks up each URL and skips unmatched ones.
    """
    async with session_factory() as session:
        if from_db:
            stmt = select(Repo).where(Repo.repo_url.isnot(None), Repo.repo_url != "")
            db_result = await session.execute(stmt)
            found = list(db_result.scalars().all())
            logger.info("Found %d repos with repo_url in database", len(found))
            return found

        matched: list[Repo] = []
        for url in repo_urls:
            stmt = select(Repo).where(Repo.repo_url == url)
            db_result = await session.execute(stmt)
            repo = db_result.scalar_one_or_none()
            if repo is None:
                logger.warning("No Repo found for URL %s — skipping", url)
                continue
            matched.append(repo)
        return matched


async def main() -> None:
    """Parse arguments, resolve repos, and run the git log pipeline."""
    parser = argparse.ArgumentParser(description="PG Atlas git log parser")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("repo_urls", nargs="*", default=[], help="Repo URLs to process")
    group.add_argument("--from-db", action="store_true", help="Process all repos from the database")
    parser.add_argument("--since-months", type=int, default=settings.GITLOG_SINCE_MONTHS)
    parser.add_argument("--clone-dir", type=str, default=settings.GITLOG_CLONE_DIR)
    args = parser.parse_args()

    logging.basicConfig(level=settings.LOG_LEVEL)

    if not args.repo_urls and not args.from_db:
        parser.error("provide repo URLs or --from-db")

    if not settings.DATABASE_URL:
        logger.error("PG_ATLAS_DATABASE_URL is required for git log parsing")
        raise SystemExit(1)

    clone_dir = Path(args.clone_dir)
    clone_dir.mkdir(parents=True, exist_ok=True)

    engine = create_async_engine(settings.DATABASE_URL, pool_pre_ping=True)
    try:
        session_factory: async_sessionmaker[AsyncSession] = async_sessionmaker(
            engine,
            expire_on_commit=False,
        )

        repos = await _resolve_repos(session_factory, args.from_db, args.repo_urls)
        if not repos:
            logger.warning("No repos to process")
            return

        # Tracking for final summary
        total_contributors_stored = 0
        total_bot_commits = 0
        total_bot_contributors = 0
        total_edges_created = 0
        total_edges_updated = 0
        error_urls: list[str] = []

        for i, repo in enumerate(repos, 1):
            result = await parse_repo(
                repo.repo_url,  # type: ignore[arg-type]  # repo_url is non-null (filtered above)
                clone_dir,
                args.since_months,
                settings.GITLOG_CLONE_TIMEOUT,
            )

            persist_result: PersistResult | None = None
            async with session_factory() as session:
                try:
                    persist_result = await persist_repo_result(session, repo, result)
                    await session.commit()
                except SQLAlchemyError, ValueError:
                    await session.rollback()
                    logger.exception("Failed to persist %s", repo.repo_url)
                    error_urls.append(repo.repo_url or "(unknown)")
                    _log_repo_summary(result, None, i, len(repos))
                    continue

            if result.errors:
                error_urls.append(repo.repo_url or "(unknown)")

            _log_repo_summary(result, persist_result, i, len(repos))

            # Accumulate totals
            total_bot_commits += result.bot_commit_count
            total_bot_contributors += result.bot_contributor_count
            if persist_result:
                total_contributors_stored += persist_result.contributors_created + persist_result.contributors_updated
                total_edges_created += persist_result.edges_created
                total_edges_updated += persist_result.edges_updated

            # Rate limit between repos (skip after last)
            if i < len(repos):
                await asyncio.sleep(settings.GITLOG_CLONE_DELAY)

        # Final summary
        logger.info(
            "Git log parsing complete:\n"
            "  Repos processed: %d (%d errors)\n"
            "  Total contributors stored: %d\n"
            "  Total bot commits excluded: %d from %d unique bots\n"
            "  Total edges created/updated: %d created, %d updated",
            len(repos),
            len(error_urls),
            total_contributors_stored,
            total_bot_commits,
            total_bot_contributors,
            total_edges_created,
            total_edges_updated,
        )
        if error_urls:
            logger.info("  Errors: %s", error_urls)

    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
