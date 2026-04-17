"""
Abstract base class and shared data types for PG Atlas registry crawlers.

Provides ``RegistryCrawler`` — the base class that all concrete crawlers
(pub.dev, Packagist, etc.) must extend.  Shared logic includes HTTP retry
handling, rate limiting, vertex upsert, edge creation with confidence
preservation, and per-package transaction boundaries.

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
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pg_atlas.db_models.base import EdgeConfidence
from pg_atlas.db_models.depends_on import DependsOn
from pg_atlas.db_models.repo_vertex import ExternalRepo, Repo, RepoVertex
from pg_atlas.db_models.vertex_ops import get_vertex

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
    downloads: int | None
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
# Vertex upsert (self-contained — does NOT import from persist.py)
# ---------------------------------------------------------------------------


async def _upsert_vertex(
    session: AsyncSession,
    canonical_id: str,
    display_name: str,
    latest_version: str,
    repo_url: str | None,
) -> RepoVertex:
    """
    Upsert a RepoVertex, preserving the existing subtype.

    - If a Repo exists with this canonical_id: update mutable fields, return it.
    - If an ExternalRepo exists: update mutable fields, return it.
    - If nothing exists: create as ExternalRepo and return it.

    NEVER create a Repo — only SBOM ingestion creates Repos (via OIDC claims).
    NEVER downgrade a Repo to ExternalRepo.
    """
    vertex = await get_vertex(session, canonical_id)

    if vertex is not None:
        # Both Repo and ExternalRepo have these attributes but RepoVertex (the
        # JTI base) does not declare them, so we narrow the type for mypy.
        if isinstance(vertex, (Repo, ExternalRepo)):
            vertex.display_name = display_name
            if latest_version:
                vertex.latest_version = latest_version
            if repo_url:
                vertex.repo_url = repo_url
        await session.flush()
        return vertex

    ext = ExternalRepo(
        canonical_id=canonical_id,
        display_name=display_name,
        latest_version=latest_version,
        repo_url=repo_url,
    )
    session.add(ext)
    try:
        async with session.begin_nested():
            await session.flush()
    except IntegrityError:
        session.expunge(ext)
        ext = await get_vertex(session, canonical_id)
    return ext


# ---------------------------------------------------------------------------
# Edge upsert (never overwrites verified_sbom with inferred_shadow)
# ---------------------------------------------------------------------------


async def _upsert_edge(
    session: AsyncSession,
    in_vertex_id: int,
    out_vertex_id: int,
    version_range: str | None,
) -> bool | None:
    """
    Create or update a DependsOn edge with inferred_shadow confidence.

    Returns True if an edge was created, False if an inferred edge was updated,
    or None if a verified_sbom edge was preserved (skipped).
    """
    existing = await session.execute(
        select(DependsOn).where(
            DependsOn.in_vertex_id == in_vertex_id,
            DependsOn.out_vertex_id == out_vertex_id,
        )
    )
    edge = existing.scalar_one_or_none()

    if edge is None:
        new_edge = DependsOn(
            in_vertex_id=in_vertex_id,
            out_vertex_id=out_vertex_id,
            version_range=version_range or None,
            confidence=EdgeConfidence.inferred_shadow,
        )
        session.add(new_edge)
        try:
            async with session.begin_nested():
                await session.flush()
        except IntegrityError as exc:
            session.expunge(new_edge)
            logger.debug(f"Edge {in_vertex_id}->{out_vertex_id} already exists: {exc}")
            return None
        return True

    if edge.confidence == EdgeConfidence.inferred_shadow:
        edge.version_range = version_range or None
        await session.flush()
        return False

    # verified_sbom — leave untouched
    return None


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

    async def crawl_and_persist(self, package_names: list[str]) -> CrawlResult:
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
                    await self._process_package(session, package_name, result)
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
        Fetch, upsert, and create edges for a single package.

        This runs inside a session context managed by ``crawl_and_persist``.
        """
        crawled = await self.fetch_package(package_name)

        # Upsert the main package vertex
        vertex = await _upsert_vertex(
            session,
            canonical_id=crawled.canonical_id,
            display_name=crawled.display_name,
            latest_version=crawled.latest_version,
            repo_url=crawled.repo_url,
        )
        result.vertices_upserted += 1

        # Write adoption data only if this is a Repo (Rule 1)
        if isinstance(vertex, Repo):
            if crawled.downloads is not None:
                vertex.adoption_downloads = crawled.downloads
            if crawled.stars is not None:
                vertex.adoption_stars = crawled.stars
            if crawled.metadata:
                vertex.repo_metadata = crawled.metadata
            await session.flush()

        # Forward dependencies: this package depends on each dep
        for dep in crawled.dependencies:
            dep_vertex = await _upsert_vertex(
                session,
                canonical_id=dep.canonical_id,
                display_name=dep.display_name,
                latest_version="",
                repo_url=None,
            )
            result.vertices_upserted += 1

            edge_result = await _upsert_edge(
                session,
                in_vertex_id=vertex.id,
                out_vertex_id=dep_vertex.id,
                version_range=dep.version_range,
            )
            if edge_result is True:
                result.edges_created += 1
            elif edge_result is None:
                result.edges_skipped += 1

        # Reverse dependents: each dependent depends on this package
        dependents = await self.fetch_dependents(package_name)
        for dependent in dependents:
            dep_vertex = await _upsert_vertex(
                session,
                canonical_id=dependent.canonical_id,
                display_name=dependent.display_name,
                latest_version="",
                repo_url=None,
            )
            result.vertices_upserted += 1

            edge_result = await _upsert_edge(
                session,
                in_vertex_id=dep_vertex.id,
                out_vertex_id=vertex.id,
                version_range=None,
            )
            if edge_result is True:
                result.edges_created += 1
            elif edge_result is None:
                result.edges_skipped += 1
