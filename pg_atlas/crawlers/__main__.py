"""
CLI entry point for running PG Atlas registry crawlers.

Usage::

    uv run python -m pg_atlas.crawlers pubdev stellar_flutter_sdk
    uv run python -m pg_atlas.crawlers packagist soneso/stellar-php-sdk

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import argparse
import asyncio
import logging

import httpx
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from pg_atlas.config import settings

logger = logging.getLogger(__name__)


async def main() -> None:
    """Parse arguments, configure the crawler, and run the crawl."""
    parser = argparse.ArgumentParser(description="PG Atlas registry crawler")
    parser.add_argument("registry", choices=["pubdev", "packagist"])
    parser.add_argument("packages", nargs="+", help="Package names to crawl")
    args = parser.parse_args()

    logging.basicConfig(level=settings.LOG_LEVEL)

    if not settings.DATABASE_URL:
        logger.error("PG_ATLAS_DATABASE_URL is required for crawling")
        raise SystemExit(1)

    # Import crawlers here to avoid circular imports at module level
    from pg_atlas.crawlers.packagist import PackagistCrawler
    from pg_atlas.crawlers.pubdev import PubDevCrawler

    engine = create_async_engine(settings.DATABASE_URL, pool_pre_ping=True)
    try:
        session_factory: async_sessionmaker[AsyncSession] = async_sessionmaker(
            engine,
            expire_on_commit=False,
        )

        async with httpx.AsyncClient(
            timeout=httpx.Timeout(settings.CRAWLER_TIMEOUT, connect=10.0),
            follow_redirects=True,
            headers={"User-Agent": "pg-atlas-crawler/0.1"},
        ) as client:
            crawler_cls = PubDevCrawler if args.registry == "pubdev" else PackagistCrawler
            crawler = crawler_cls(
                client=client,
                session_factory=session_factory,
                rate_limit=settings.CRAWLER_RATE_LIMIT,
                max_retries=settings.CRAWLER_MAX_RETRIES,
            )
            result = await crawler.crawl_and_persist(args.packages)
    finally:
        await engine.dispose()

    logger.info(
        "Crawl complete: %d packages, %d vertices, %d edges, %d skipped, %d errors",
        result.packages_processed,
        result.vertices_upserted,
        result.edges_created,
        result.edges_skipped,
        len(result.errors),
    )
    for error in result.errors:
        logger.warning("  Error: %s", error)


if __name__ == "__main__":
    asyncio.run(main())
