"""
Shared factory helpers for registry crawler construction.

These helpers keep queue-driven task execution and CLI invocation aligned on
the same system normalization and crawler mapping behavior.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import logging

import httpx
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pg_atlas.crawlers.base import RegistryCrawler
from pg_atlas.crawlers.cargo import CargoCrawler
from pg_atlas.crawlers.npm import NpmCrawler
from pg_atlas.crawlers.packagist import PackagistCrawler
from pg_atlas.crawlers.pubdev import PubDevCrawler
from pg_atlas.crawlers.pypi import PyPICrawler

logger = logging.getLogger(__name__)


def normalize_registry_system(system: str) -> str | None:
    """
    Normalize one registry ecosystem alias to its canonical system token.
    """

    normalized = system.strip().upper()

    match normalized:
        case "DART" | "FLUTTER" | "PUB" | "PUBDEV":
            return "DART"
        case "COMPOSER" | "PHP" | "PACKAGIST":
            return "COMPOSER"
        case "NPM" | "NODE" | "NODEJS":
            return "NPM"
        case "CARGO" | "CRATES" | "CRATESIO":
            return "CARGO"
        case "PYPI" | "PIP":
            return "PYPI"
        case _:
            return None


def build_registry_crawler(
    system: str,
    client: httpx.AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
    rate_limit: float,
    max_retries: int,
) -> RegistryCrawler | None:
    """
    Build a concrete crawler instance for the requested registry system.
    """

    normalized = normalize_registry_system(system)
    if normalized is None:
        logger.warning(f"Unsupported registry system: {system}")

        return None

    match normalized:
        case "DART":
            return PubDevCrawler(
                client=client,
                session_factory=session_factory,
                rate_limit=rate_limit,
                max_retries=max_retries,
            )
        case "COMPOSER":
            return PackagistCrawler(
                client=client,
                session_factory=session_factory,
                rate_limit=rate_limit,
                max_retries=max_retries,
            )
        case "NPM":
            return NpmCrawler(
                client=client,
                session_factory=session_factory,
                rate_limit=rate_limit,
                max_retries=max_retries,
            )
        case "CARGO":
            return CargoCrawler(
                client=client,
                session_factory=session_factory,
                rate_limit=rate_limit,
                max_retries=max_retries,
            )
        case "PYPI":
            return PyPICrawler(
                client=client,
                session_factory=session_factory,
                rate_limit=rate_limit,
                max_retries=max_retries,
            )
        case _:
            logger.warning(f"Unsupported registry system after normalization: {normalized}")

            return None
