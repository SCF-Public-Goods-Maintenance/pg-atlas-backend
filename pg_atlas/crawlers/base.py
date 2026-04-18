"""
Abstract base class and shared data types for PG Atlas registry crawlers.

Provides ``RegistryCrawler`` — the base class that all concrete crawlers
(pub.dev, Packagist, etc.) must extend.  Shared logic includes HTTP retry
handling, rate limiting, vertex upsert, edge creation with confidence
preservation, and per-package transaction boundaries.

The name "crawler" does not really apply yet, since we don't traverse any
of the found dependency edges. We might extend this behavior in the future.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pg_atlas.db_models.repo_vertex import Repo
from pg_atlas.db_models.vertex_ops import upsert_external_repo
from pg_atlas.metrics.adoption import merge_download_into_repo_metadata
from pg_atlas.procrastinate.upserts import find_repo_by_release_purl, upsert_depends_on

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class CrawledDependency:
    """A single dependency extracted from a registry package."""

    canonical_id: str
    display_name: str
    version_range: str | None


@dataclass
class CrawledDependent:
    """A single reverse dependent discovered from a registry search."""

    canonical_id: str
    display_name: str


@dataclass
class CrawledPackage:
    """Parsed metadata for one registry package."""

    canonical_id: str
    display_name: str
    latest_version: str
    repo_url: str | None
    downloads_30d: int | None
    stars: int | None
    metadata: dict[str, Any]
    dependencies: list[CrawledDependency]


@dataclass
class CrawlResult:
    """Accumulator for crawl run statistics."""

    packages_processed: int = 0
    vertices_upserted: int = 0
    edges_created: int = 0
    edges_skipped: int = 0
    errors: list[str] = field(default_factory=list[str])


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class SourceRepoNotFound(Exception): ...


# ---------------------------------------------------------------------------
# Abstract base crawler
# ---------------------------------------------------------------------------


class RegistryCrawler(ABC):
    """
    Base class for registry crawlers.

    Concrete subclasses implement ``fetch_package`` and ``fetch_dependents`` for
    their specific registry API.  The shared ``crawl_and_persist`` method handles
    DB writes, transaction boundaries, and rate limiting.
    """

    def __init__(
        self,
        client: httpx.AsyncClient,
        session_factory: async_sessionmaker[AsyncSession],
        rate_limit: float = 1.0,
        max_retries: int = 3,
    ) -> None:
        self.client = client
        self.session_factory = session_factory
        self.rate_limit = rate_limit
        self.max_retries = max_retries

    @abstractmethod
    async def fetch_package(self, package_name: str) -> CrawledPackage:
        """Fetch package metadata from the registry API."""
        ...

    @abstractmethod
    async def fetch_dependents(self, package_name: str) -> list[CrawledDependent]:
        """Fetch reverse dependencies from the registry API."""
        ...

    async def _request_with_retry(self, url: str) -> httpx.Response:
        """
        Make an HTTP GET request with retry logic for 429 and 5xx responses.

        Raises ``httpx.HTTPStatusError`` for 404 (no retry).
        Returns the response for 200.
        """
        last_exc: Exception | None = None
        for attempt in range(self.max_retries):
            try:
                resp = await self.client.get(url)
            except httpx.TimeoutException as exc:
                last_exc = exc
                if attempt < self.max_retries - 1:
                    wait = 2 ** (attempt + 1)
                    logger.warning(f"Timeout fetching {url}, retrying in {wait}s")
                    await asyncio.sleep(wait)
                    continue
                raise

            if resp.status_code == 200:
                return resp

            if resp.status_code == 404:
                resp.raise_for_status()

            if resp.status_code == 429:
                try:
                    retry_after = int(resp.headers.get("Retry-After", 2 ** (attempt + 1)))
                except ValueError:
                    retry_after = 2 ** (attempt + 1)
                logger.warning(f"Rate limited on {url}, waiting {retry_after}s")
                await asyncio.sleep(retry_after)
                continue

            if resp.status_code >= 500:
                last_exc = httpx.HTTPStatusError(
                    f"Server error {resp.status_code}",
                    request=resp.request,
                    response=resp,
                )
                if attempt < self.max_retries - 1:
                    wait = 2 ** (attempt + 1)
                    logger.warning(f"Server error {resp.status_code} on {url}, retrying in {wait}s")
                    await asyncio.sleep(wait)
                    continue
                raise last_exc

            resp.raise_for_status()

        if last_exc is not None:
            raise last_exc
        raise RuntimeError(f"Exhausted retries for {url}")

    async def crawl_and_persist(
        self,
        package_names: list[str],
    ) -> CrawlResult:
        """
        Crawl a list of packages and persist vertices/edges to the database.

        Each package is processed in its own transaction (commit per-package).
        Failures are logged and collected in ``CrawlResult.errors`` — the crawl
        continues with the remaining packages.
        """
        result = CrawlResult()

        for i, package_name in enumerate(package_names):
            async with self.session_factory() as session:
                try:
                    await self._process_package(
                        session,
                        package_name,
                        result,
                    )
                    await session.commit()
                    result.packages_processed += 1
                except Exception as exc:
                    await session.rollback()
                    result.errors.append(f"{package_name}: {exc}")
                    logger.warning(f"Crawl failed for {package_name}: {exc}")

            # Rate limit between packages (skip after last)
            if i < len(package_names) - 1:
                await asyncio.sleep(self.rate_limit)

        return result

    async def _process_package(
        self,
        session: AsyncSession,
        package_name: str,
        result: CrawlResult,
    ) -> None:
        """
        Merge package metadata into the source Repo, and create dependency edges.

        This runs inside a session context managed by ``crawl_and_persist``.
        """
        crawled = await self.fetch_package(package_name)
        source_repo: Repo | None = None
        if crawled.repo_url:
            package_repo_url = crawled.repo_url.removesuffix(".git")
            source_repo = await session.scalar(select(Repo).where(Repo.repo_url == package_repo_url))

        if not source_repo:
            repo_match = await find_repo_by_release_purl(crawled.canonical_id)
            if repo_match:
                vertex_id, _, _ = repo_match
                source_repo = await session.get(Repo, vertex_id)

        if not source_repo:
            raise SourceRepoNotFound(f"No source repo found for package {package_name}")

        if crawled.downloads_30d is not None:
            source_repo.repo_metadata = merge_download_into_repo_metadata(
                source_repo.repo_metadata,
                package_purl=crawled.canonical_id,
                downloads=crawled.downloads_30d,
                repo_canonical_id=source_repo.canonical_id,
            )
            await session.flush()

        # Forward dependencies: this package depends on each dep
        for dep in crawled.dependencies:
            dep_vertex = await upsert_external_repo(
                session,
                canonical_id=dep.canonical_id,
                display_name=dep.display_name,
                latest_version="",
                repo_url=None,
            )
            result.vertices_upserted += 1

            edge_result = await upsert_depends_on(
                session,
                in_vertex_id=source_repo.id,
                out_vertex_id=dep_vertex.id,
                version_range=dep.version_range,
            )
            if edge_result is True:
                result.edges_created += 1
            else:
                result.edges_skipped += 1

        # Reverse dependents: each dependent depends on this package
        dependents = await self.fetch_dependents(package_name)
        for dependent in dependents:
            dep_vertex = await upsert_external_repo(
                session,
                canonical_id=dependent.canonical_id,
                display_name=dependent.display_name,
                latest_version="",
                repo_url=None,
            )
            result.vertices_upserted += 1

            edge_result = await upsert_depends_on(
                session,
                in_vertex_id=dep_vertex.id,
                out_vertex_id=source_repo.id,
                version_range=None,
            )
            if edge_result is True:
                result.edges_created += 1
            else:
                result.edges_skipped += 1
