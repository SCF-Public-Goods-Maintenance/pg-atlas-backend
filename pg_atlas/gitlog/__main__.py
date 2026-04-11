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

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from pg_atlas.config import settings
from pg_atlas.gitlog.runtime import resolve_repos, run_gitlog_pipeline

logger = logging.getLogger(__name__)


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

    engine = create_async_engine(settings.DATABASE_URL, pool_pre_ping=True)
    try:
        session_factory: async_sessionmaker[AsyncSession] = async_sessionmaker(
            engine,
            expire_on_commit=False,
        )

        repos = await resolve_repos(
            session_factory,
            from_db=args.from_db,
            repo_urls=args.repo_urls,
            exclude_private_for_db=True,
        )
        if not repos:
            logger.warning("No repos to process")
            return

        await run_gitlog_pipeline(
            session_factory,
            repos,
            since_months=args.since_months,
            clone_dir=clone_dir,
            clone_timeout=settings.GITLOG_CLONE_TIMEOUT,
            clone_delay=settings.GITLOG_CLONE_DELAY,
            max_rate_limit_retries=settings.GITLOG_RATE_LIMIT_MAX_RETRIES,
            initial_backoff_seconds=settings.GITLOG_RATE_LIMIT_INITIAL_BACKOFF_SECONDS,
            max_backoff_seconds=settings.GITLOG_RATE_LIMIT_MAX_BACKOFF_SECONDS,
            mark_terminal_failures_private=args.from_db,
        )

    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
