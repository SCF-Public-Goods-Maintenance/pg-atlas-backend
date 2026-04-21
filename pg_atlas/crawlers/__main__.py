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

from pg_atlas.config import settings
from pg_atlas.crawlers.factory import build_registry_crawler, normalize_registry_system
from pg_atlas.db_models.session import get_session_factory

logger = logging.getLogger(__name__)


async def main() -> None:
    """Parse arguments, configure the crawler, and run the crawl."""
    parser = argparse.ArgumentParser(description="PG Atlas registry crawler")
    parser.add_argument(
        "registry",
        choices=[
            "pubdev",
            "packagist",
            "dart",
            "composer",
            "flutter",
            "php",
            "npm",
            "node",
            "nodejs",
            "cargo",
            "crates",
            "cratesio",
            "pypi",
            "pip",
        ],
    )
    parser.add_argument("packages", nargs="+", help="Package names to crawl")
    args = parser.parse_args()

    logging.basicConfig(level=settings.LOG_LEVEL)

    if not settings.DATABASE_URL:
        logger.error("PG_ATLAS_DATABASE_URL is required for crawling")
        raise SystemExit(1)

    session_factory = get_session_factory()
    normalized_system = normalize_registry_system(args.registry)
    if normalized_system is None:
        logger.error(f"Unsupported registry argument: {args.registry}")
        raise SystemExit(2)

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(settings.CRAWLER_TIMEOUT, connect=10.0),
        follow_redirects=True,
        headers={"User-Agent": "pg-atlas-crawler/0.1"},
    ) as client:
        crawler = build_registry_crawler(
            normalized_system,
            client=client,
            session_factory=session_factory,
            rate_limit=settings.CRAWLER_RATE_LIMIT,
            max_retries=settings.CRAWLER_MAX_RETRIES,
        )

        if crawler is None:
            logger.error(f"No crawler available for system: {normalized_system}")
            raise SystemExit(2)

        result = await crawler.crawl_and_persist(args.packages)

    logger.info(
        f"Crawl complete: {result.packages_processed} packages, {result.vertices_upserted} vertices, "
        f"{result.edges_created} edges, {result.edges_skipped} skipped, {len(result.errors)} errors"
    )
    for error in result.errors:
        logger.warning(f"  Error: {error}")


if __name__ == "__main__":
    asyncio.run(main())
