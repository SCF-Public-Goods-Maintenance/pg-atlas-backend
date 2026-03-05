"""
Database integration tests for the registry crawler write path.

Tests the critical DB logic: vertex upsert, edge confidence preservation,
adoption column gating on Repo vs ExternalRepo, idempotency, and edge direction.

These tests require a running PostgreSQL instance configured via
``PG_ATLAS_DATABASE_URL``. They are skipped automatically when the variable
is not set (e.g. in CI without a database service).

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import os
from collections.abc import AsyncGenerator
from typing import Any
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from pg_atlas.crawlers.base import (
    CrawledDependency,
    CrawledDependent,
    CrawledPackage,
    RegistryCrawler,
)
from pg_atlas.db_models.base import EdgeConfidence, Visibility
from pg_atlas.db_models.depends_on import DependsOn
from pg_atlas.db_models.repo_vertex import ExternalRepo, Repo, RepoVertex

# ---------------------------------------------------------------------------
# Skip when no DB configured
# ---------------------------------------------------------------------------

pytestmark = pytest.mark.skipif(
    not os.environ.get("PG_ATLAS_DATABASE_URL"),
    reason="PG_ATLAS_DATABASE_URL not set; skipping database integration tests",
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def db_engine() -> AsyncGenerator[Any, None]:
    """Create a fresh async engine with NullPool for test isolation."""
    engine = create_async_engine(os.environ["PG_ATLAS_DATABASE_URL"], poolclass=NullPool)
    yield engine
    await engine.dispose()


@pytest.fixture
async def db_session_factory(db_engine: Any) -> async_sessionmaker[AsyncSession]:
    """Session factory for crawler tests."""
    return async_sessionmaker(db_engine, class_=AsyncSession, expire_on_commit=False)


@pytest.fixture(autouse=True)
async def clean_tables(db_session_factory: async_sessionmaker[AsyncSession]) -> AsyncGenerator[None, None]:
    """
    Truncate crawler-affected tables before and after each test.

    Truncation runs with CASCADE to handle FK constraints between tables.
    """
    async with db_session_factory() as session:
        await session.execute(text("TRUNCATE TABLE depends_on, repos, external_repos, repo_vertices CASCADE"))
        await session.commit()

    yield

    async with db_session_factory() as session:
        await session.execute(text("TRUNCATE TABLE depends_on, repos, external_repos, repo_vertices CASCADE"))
        await session.commit()


# ---------------------------------------------------------------------------
# Stub crawler for integration tests
# ---------------------------------------------------------------------------


class IntegrationStubCrawler(RegistryCrawler):
    """Concrete crawler that returns pre-configured data for integration tests."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        packages: dict[str, CrawledPackage] | None = None,
        dependents: dict[str, list[CrawledDependent]] | None = None,
    ) -> None:
        client = AsyncMock()
        super().__init__(client=client, session_factory=session_factory, rate_limit=0.0)
        self._packages = packages or {}
        self._dependents = dependents or {}

    async def fetch_package(self, package_name: str) -> CrawledPackage:
        return self._packages[package_name]

    async def fetch_dependents(self, package_name: str) -> list[CrawledDependent]:
        return self._dependents.get(package_name, [])


def _make_package(
    canonical_id: str = "pkg:pub/test_pkg",
    display_name: str = "test_pkg",
    latest_version: str = "1.0.0",
    repo_url: str | None = None,
    downloads: int | None = 100,
    stars: int | None = 5,
    dependencies: list[CrawledDependency] | None = None,
) -> CrawledPackage:
    return CrawledPackage(
        canonical_id=canonical_id,
        display_name=display_name,
        latest_version=latest_version,
        repo_url=repo_url,
        downloads=downloads,
        stars=stars,
        metadata={},
        dependencies=dependencies or [],
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_crawl_creates_external_repo_vertex(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Crawling a new package creates an ExternalRepo vertex."""
    pkg = _make_package(canonical_id="pkg:pub/new_pkg", display_name="new_pkg")
    crawler = IntegrationStubCrawler(
        session_factory=db_session_factory,
        packages={"new_pkg": pkg},
    )
    result = await crawler.crawl_and_persist(["new_pkg"])

    assert result.packages_processed == 1
    assert result.vertices_upserted >= 1

    async with db_session_factory() as session:
        vertex = (
            await session.execute(select(ExternalRepo).where(ExternalRepo.canonical_id == "pkg:pub/new_pkg"))
        ).scalar_one()
        assert vertex.display_name == "new_pkg"


async def test_crawl_updates_existing_repo_adoption(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Adoption columns are updated when vertex is a Repo (not ExternalRepo)."""
    # Pre-create a Repo (as SBOM ingestion would)
    async with db_session_factory() as session:
        repo = Repo(
            canonical_id="pkg:pub/my_sdk",
            display_name="my_sdk",
            visibility=Visibility.public,
            latest_version="1.0.0",
        )
        session.add(repo)
        await session.commit()

    pkg = _make_package(
        canonical_id="pkg:pub/my_sdk",
        display_name="my_sdk",
        downloads=500,
        stars=20,
    )
    crawler = IntegrationStubCrawler(
        session_factory=db_session_factory,
        packages={"my_sdk": pkg},
    )
    await crawler.crawl_and_persist(["my_sdk"])

    async with db_session_factory() as session:
        repo = (await session.execute(select(Repo).where(Repo.canonical_id == "pkg:pub/my_sdk"))).scalar_one()
        assert repo.adoption_downloads == 500
        assert repo.adoption_stars == 20


async def test_crawl_skips_adoption_on_external_repo(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """No AttributeError when crawling produces an ExternalRepo (no adoption columns)."""
    pkg = _make_package(canonical_id="pkg:pub/ext_pkg", display_name="ext_pkg", downloads=100, stars=5)
    crawler = IntegrationStubCrawler(
        session_factory=db_session_factory,
        packages={"ext_pkg": pkg},
    )
    # This should NOT raise AttributeError
    result = await crawler.crawl_and_persist(["ext_pkg"])
    assert result.packages_processed == 1


async def test_crawl_creates_depends_on_edges(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Forward dependency edges are created with correct direction."""
    dep = CrawledDependency(canonical_id="pkg:pub/dep_a", display_name="dep_a", version_range="^1.0")
    pkg = _make_package(
        canonical_id="pkg:pub/main_pkg",
        display_name="main_pkg",
        dependencies=[dep],
    )
    crawler = IntegrationStubCrawler(
        session_factory=db_session_factory,
        packages={"main_pkg": pkg},
    )
    result = await crawler.crawl_and_persist(["main_pkg"])

    assert result.edges_created >= 1

    async with db_session_factory() as session:
        main_v = (await session.execute(select(RepoVertex).where(RepoVertex.canonical_id == "pkg:pub/main_pkg"))).scalar_one()
        dep_v = (await session.execute(select(RepoVertex).where(RepoVertex.canonical_id == "pkg:pub/dep_a"))).scalar_one()

        edge = (
            await session.execute(
                select(DependsOn).where(DependsOn.in_vertex_id == main_v.id, DependsOn.out_vertex_id == dep_v.id)
            )
        ).scalar_one()
        assert edge.confidence == EdgeConfidence.inferred_shadow
        assert edge.version_range == "^1.0"


async def test_crawl_preserves_verified_sbom_edges(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Existing verified_sbom edge is NOT overwritten by inferred_shadow."""
    # Pre-create vertices and a verified_sbom edge
    async with db_session_factory() as session:
        repo = Repo(
            canonical_id="pkg:pub/sbom_pkg",
            display_name="sbom_pkg",
            visibility=Visibility.public,
            latest_version="1.0.0",
        )
        dep_ext = ExternalRepo(
            canonical_id="pkg:pub/sbom_dep",
            display_name="sbom_dep",
            latest_version="2.0.0",
        )
        session.add_all([repo, dep_ext])
        await session.flush()
        edge = DependsOn(
            in_vertex_id=repo.id,
            out_vertex_id=dep_ext.id,
            version_range="=2.0.0",
            confidence=EdgeConfidence.verified_sbom,
        )
        session.add(edge)
        await session.commit()

    # Crawl with the same edge but inferred_shadow
    dep = CrawledDependency(canonical_id="pkg:pub/sbom_dep", display_name="sbom_dep", version_range="^2.0")
    pkg = _make_package(
        canonical_id="pkg:pub/sbom_pkg",
        display_name="sbom_pkg",
        dependencies=[dep],
    )
    crawler = IntegrationStubCrawler(
        session_factory=db_session_factory,
        packages={"sbom_pkg": pkg},
    )
    result = await crawler.crawl_and_persist(["sbom_pkg"])

    assert result.edges_skipped >= 1

    # Verify the edge still has verified_sbom confidence and original version_range
    async with db_session_factory() as session:
        repo = (await session.execute(select(Repo).where(Repo.canonical_id == "pkg:pub/sbom_pkg"))).scalar_one()
        dep_v = (await session.execute(select(RepoVertex).where(RepoVertex.canonical_id == "pkg:pub/sbom_dep"))).scalar_one()
        edge = (
            await session.execute(
                select(DependsOn).where(DependsOn.in_vertex_id == repo.id, DependsOn.out_vertex_id == dep_v.id)
            )
        ).scalar_one()
        assert edge.confidence == EdgeConfidence.verified_sbom
        assert edge.version_range == "=2.0.0"


async def test_crawl_updates_inferred_shadow_edges(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Existing inferred_shadow edge has its version_range updated."""
    # Pre-create vertices and an inferred_shadow edge
    async with db_session_factory() as session:
        ext1 = ExternalRepo(canonical_id="pkg:pub/inf_pkg", display_name="inf_pkg", latest_version="1.0.0")
        ext2 = ExternalRepo(canonical_id="pkg:pub/inf_dep", display_name="inf_dep", latest_version="1.0.0")
        session.add_all([ext1, ext2])
        await session.flush()
        edge = DependsOn(
            in_vertex_id=ext1.id,
            out_vertex_id=ext2.id,
            version_range="^1.0",
            confidence=EdgeConfidence.inferred_shadow,
        )
        session.add(edge)
        await session.commit()

    # Crawl with updated version range
    dep = CrawledDependency(canonical_id="pkg:pub/inf_dep", display_name="inf_dep", version_range="^2.0")
    pkg = _make_package(
        canonical_id="pkg:pub/inf_pkg",
        display_name="inf_pkg",
        dependencies=[dep],
    )
    crawler = IntegrationStubCrawler(
        session_factory=db_session_factory,
        packages={"inf_pkg": pkg},
    )
    await crawler.crawl_and_persist(["inf_pkg"])

    async with db_session_factory() as session:
        ext1 = (await session.execute(select(RepoVertex).where(RepoVertex.canonical_id == "pkg:pub/inf_pkg"))).scalar_one()
        ext2 = (await session.execute(select(RepoVertex).where(RepoVertex.canonical_id == "pkg:pub/inf_dep"))).scalar_one()
        edge = (
            await session.execute(
                select(DependsOn).where(DependsOn.in_vertex_id == ext1.id, DependsOn.out_vertex_id == ext2.id)
            )
        ).scalar_one()
        assert edge.version_range == "^2.0"
        assert edge.confidence == EdgeConfidence.inferred_shadow


async def test_crawl_idempotent(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Running the same crawl twice produces the same DB state."""
    dep = CrawledDependency(canonical_id="pkg:pub/idemp_dep", display_name="idemp_dep", version_range="^1.0")
    pkg = _make_package(
        canonical_id="pkg:pub/idemp_pkg",
        display_name="idemp_pkg",
        dependencies=[dep],
    )
    crawler = IntegrationStubCrawler(
        session_factory=db_session_factory,
        packages={"idemp_pkg": pkg},
    )

    result1 = await crawler.crawl_and_persist(["idemp_pkg"])
    result2 = await crawler.crawl_and_persist(["idemp_pkg"])

    assert result1.packages_processed == 1
    assert result2.packages_processed == 1

    # Should have same number of vertices and edges
    async with db_session_factory() as session:
        vertices = (await session.execute(select(RepoVertex))).scalars().all()
        edges = (await session.execute(select(DependsOn))).scalars().all()
        assert len(vertices) == 2  # main pkg + dep
        assert len(edges) == 1


async def test_crawl_does_not_downgrade_repo_to_external(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Existing Repo is NOT replaced by ExternalRepo when crawled."""
    # Pre-create a Repo
    async with db_session_factory() as session:
        repo = Repo(
            canonical_id="pkg:pub/keep_repo",
            display_name="keep_repo",
            visibility=Visibility.public,
            latest_version="1.0.0",
        )
        session.add(repo)
        await session.commit()

    pkg = _make_package(canonical_id="pkg:pub/keep_repo", display_name="keep_repo")
    crawler = IntegrationStubCrawler(
        session_factory=db_session_factory,
        packages={"keep_repo": pkg},
    )
    await crawler.crawl_and_persist(["keep_repo"])

    async with db_session_factory() as session:
        vertex = (await session.execute(select(RepoVertex).where(RepoVertex.canonical_id == "pkg:pub/keep_repo"))).scalar_one()
        assert isinstance(vertex, Repo)


async def test_crawl_empty_dependency_list(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Package with zero deps creates only the package vertex, no edges."""
    pkg = _make_package(canonical_id="pkg:pub/no_deps", display_name="no_deps", dependencies=[])
    crawler = IntegrationStubCrawler(
        session_factory=db_session_factory,
        packages={"no_deps": pkg},
    )
    result = await crawler.crawl_and_persist(["no_deps"])

    assert result.edges_created == 0
    async with db_session_factory() as session:
        edges = (await session.execute(select(DependsOn))).scalars().all()
        assert len(edges) == 0


async def test_crawl_empty_dependents_list(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Package with zero dependents creates no reverse edges."""
    pkg = _make_package(canonical_id="pkg:pub/no_rev", display_name="no_rev")
    crawler = IntegrationStubCrawler(
        session_factory=db_session_factory,
        packages={"no_rev": pkg},
        dependents={"no_rev": []},
    )
    result = await crawler.crawl_and_persist(["no_rev"])

    assert result.packages_processed == 1
    async with db_session_factory() as session:
        edges = (await session.execute(select(DependsOn))).scalars().all()
        assert len(edges) == 0


async def test_crawl_result_counts(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """CrawlResult numbers are accurate."""
    deps = [
        CrawledDependency(canonical_id="pkg:pub/dep_x", display_name="dep_x", version_range="^1.0"),
        CrawledDependency(canonical_id="pkg:pub/dep_y", display_name="dep_y", version_range=None),
    ]
    dependents = [
        CrawledDependent(canonical_id="pkg:pub/rev_z", display_name="rev_z"),
    ]
    pkg = _make_package(canonical_id="pkg:pub/counted", display_name="counted", dependencies=deps)
    crawler = IntegrationStubCrawler(
        session_factory=db_session_factory,
        packages={"counted": pkg},
        dependents={"counted": dependents},
    )
    result = await crawler.crawl_and_persist(["counted"])

    assert result.packages_processed == 1
    # 1 (main pkg) + 2 (deps) + 1 (reverse dep) = 4 vertices
    assert result.vertices_upserted == 4
    # 2 forward edges + 1 reverse edge = 3
    assert result.edges_created == 3
    assert result.edges_skipped == 0
    assert result.errors == []
