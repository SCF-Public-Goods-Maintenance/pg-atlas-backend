"""
Database integration tests for the registry crawler write path.

These tests validate the refactored crawler contract:
- package crawls resolve to an existing source Repo
- dependency edges are anchored on that source Repo
- download counts are written only to repo metadata
  (adoption_downloads_by_purl), not scalar adoption columns

These tests require a running PostgreSQL instance configured via
``PG_ATLAS_DATABASE_URL``. They are skipped automatically when the variable
is not set.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator
from typing import Any
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select
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
from tests.conftest import get_test_database_url
from tests.db_cleanup import SBOM_DB_TABLE_SPECS, capture_snapshot, cleanup_created_rows

pytestmark = pytest.mark.skipif(
    not get_test_database_url(),
    reason="PG_ATLAS_DATABASE_URL / PG_ATLAS_TEST_DATABASE_URL not set; skipping database integration tests",
)


@pytest.fixture
async def db_engine() -> AsyncGenerator[Any, None]:
    """Create a fresh async engine with NullPool for test isolation."""

    database_url = get_test_database_url()
    assert database_url is not None
    engine = create_async_engine(database_url, poolclass=NullPool)
    yield engine
    await engine.dispose()


@pytest.fixture
async def db_session_factory(db_engine: Any) -> async_sessionmaker[AsyncSession]:
    """Session factory for crawler tests."""

    return async_sessionmaker(db_engine, class_=AsyncSession, expire_on_commit=False)


@pytest.fixture(autouse=True)
async def clean_tables(db_session_factory: async_sessionmaker[AsyncSession]) -> AsyncGenerator[None, None]:
    """Remove only rows created by each crawler DB integration test."""

    async with db_session_factory() as session:
        snapshot = await capture_snapshot(session, SBOM_DB_TABLE_SPECS)

    yield

    async with db_session_factory() as session:
        await cleanup_created_rows(session, SBOM_DB_TABLE_SPECS, snapshot)


class IntegrationStubCrawler(RegistryCrawler):
    """Concrete crawler returning pre-configured data for integration tests."""

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


async def _seed_source_repo(
    session: AsyncSession,
    *,
    canonical_id: str,
    repo_url: str,
    display_name: str,
) -> Repo:
    """Create one source Repo row used as crawl anchor."""

    repo = Repo(
        canonical_id=canonical_id,
        display_name=display_name,
        visibility=Visibility.public,
        latest_version="1.0.0",
        repo_url=repo_url,
    )
    session.add(repo)
    await session.flush()

    return repo


def _make_package(
    canonical_id: str,
    display_name: str,
    repo_url: str,
    downloads_30d: int | None = 100,
    dependencies: list[CrawledDependency] | None = None,
) -> CrawledPackage:
    """Build a CrawledPackage for DB integration tests."""

    return CrawledPackage(
        canonical_id=canonical_id,
        display_name=display_name,
        latest_version="1.0.0",
        repo_url=repo_url,
        downloads_30d=downloads_30d,
        metadata={},
        dependencies=dependencies or [],
        releases=[],
    )


def _unique_suffix() -> str:
    """Return a short unique suffix to avoid collisions with pre-existing rows."""

    return uuid.uuid4().hex[:8]


async def test_crawl_writes_downloads_to_source_repo_metadata(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Crawler should write download counts into source repo metadata map only."""

    suffix = _unique_suffix()
    source_repo_url = f"https://github.com/test-org/source-repo-{suffix}"
    source_repo_canonical_id = f"pkg:github/test-org/source-repo-{suffix}"
    package_canonical_id = f"pkg:pub/my-sdk-{suffix}"

    async with db_session_factory() as session:
        await _seed_source_repo(
            session,
            canonical_id=source_repo_canonical_id,
            repo_url=source_repo_url,
            display_name=f"source-repo-{suffix}",
        )
        await session.commit()

    crawler = IntegrationStubCrawler(
        session_factory=db_session_factory,
        packages={
            "my_sdk": _make_package(
                canonical_id=package_canonical_id,
                display_name=f"my_sdk_{suffix}",
                repo_url=f"{source_repo_url}.git",
                downloads_30d=500,
            )
        },
    )

    result = await crawler.crawl_and_persist(["my_sdk"])

    assert result.packages_processed == 1
    assert result.errors == []

    async with db_session_factory() as session:
        repo = (await session.execute(select(Repo).where(Repo.canonical_id == source_repo_canonical_id))).scalar_one()
        assert repo.adoption_downloads is None
        assert isinstance(repo.repo_metadata, dict)
        metadata = repo.repo_metadata or {}
        downloads_by_purl = metadata.get("adoption_downloads_by_purl")
        assert downloads_by_purl == {package_canonical_id: 500}
        assert repo.releases is not None
        assert {(release.purl, release.version) for release in repo.releases} == {(package_canonical_id, "1.0.0")}


async def test_crawl_creates_forward_dependency_edges_from_source_repo(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Forward dependencies should create edges from source Repo to dependency vertices."""

    suffix = _unique_suffix()
    source_repo_url = f"https://github.com/test-org/forward-repo-{suffix}"
    source_repo_canonical_id = f"pkg:github/test-org/forward-repo-{suffix}"
    dep_canonical_id = f"pkg:pub/dep-a-{suffix}"

    async with db_session_factory() as session:
        source_repo = await _seed_source_repo(
            session,
            canonical_id=source_repo_canonical_id,
            repo_url=source_repo_url,
            display_name=f"forward-repo-{suffix}",
        )
        await session.commit()

    dep = CrawledDependency(canonical_id=dep_canonical_id, display_name=f"dep_a_{suffix}", version_range="^1.0")
    crawler = IntegrationStubCrawler(
        session_factory=db_session_factory,
        packages={
            "main_pkg": _make_package(
                canonical_id=f"pkg:pub/main-pkg-{suffix}",
                display_name=f"main_pkg_{suffix}",
                repo_url=source_repo_url,
                dependencies=[dep],
            )
        },
    )

    result = await crawler.crawl_and_persist(["main_pkg"])

    assert result.packages_processed == 1
    assert result.edges_created == 1

    async with db_session_factory() as session:
        dep_vertex = (
            await session.execute(select(RepoVertex).where(RepoVertex.canonical_id == dep_canonical_id))
        ).scalar_one()
        edge = (
            await session.execute(
                select(DependsOn).where(
                    DependsOn.in_vertex_id == source_repo.id,
                    DependsOn.out_vertex_id == dep_vertex.id,
                )
            )
        ).scalar_one()
        assert edge.confidence == EdgeConfidence.inferred_shadow
        assert edge.version_range == "^1.0"


async def test_crawl_creates_reverse_dependent_edges_to_source_repo(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Reverse dependents should create edges from dependent vertices to source Repo."""

    suffix = _unique_suffix()
    source_repo_url = f"https://github.com/test-org/reverse-repo-{suffix}"
    source_repo_canonical_id = f"pkg:github/test-org/reverse-repo-{suffix}"
    dependent_canonical_id = f"pkg:pub/rev-a-{suffix}"

    async with db_session_factory() as session:
        source_repo = await _seed_source_repo(
            session,
            canonical_id=source_repo_canonical_id,
            repo_url=source_repo_url,
            display_name=f"reverse-repo-{suffix}",
        )
        await session.commit()

    crawler = IntegrationStubCrawler(
        session_factory=db_session_factory,
        packages={
            "main_pkg": _make_package(
                canonical_id=f"pkg:pub/main-pkg-{suffix}",
                display_name=f"main_pkg_{suffix}",
                repo_url=source_repo_url,
                dependencies=[],
            )
        },
        dependents={
            "main_pkg": [
                CrawledDependent(canonical_id=dependent_canonical_id, display_name=f"rev_a_{suffix}"),
            ]
        },
    )

    result = await crawler.crawl_and_persist(["main_pkg"])

    assert result.packages_processed == 1
    assert result.edges_created == 1

    async with db_session_factory() as session:
        dependent_vertex = (
            await session.execute(select(RepoVertex).where(RepoVertex.canonical_id == dependent_canonical_id))
        ).scalar_one()
        edge = (
            await session.execute(
                select(DependsOn).where(
                    DependsOn.in_vertex_id == dependent_vertex.id,
                    DependsOn.out_vertex_id == source_repo.id,
                )
            )
        ).scalar_one()
        assert edge.confidence == EdgeConfidence.inferred_shadow
        assert edge.version_range is None


async def test_crawl_updates_existing_edge_version_without_changing_confidence(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Existing edge confidence is preserved while version_range is refreshed."""

    suffix = _unique_suffix()
    source_repo_url = f"https://github.com/test-org/version-repo-{suffix}"
    source_repo_canonical_id = f"pkg:github/test-org/version-repo-{suffix}"
    dep_canonical_id = f"pkg:pub/versioned-dep-{suffix}"

    async with db_session_factory() as session:
        source_repo = await _seed_source_repo(
            session,
            canonical_id=source_repo_canonical_id,
            repo_url=source_repo_url,
            display_name=f"version-repo-{suffix}",
        )
        dep_ext = ExternalRepo(
            canonical_id=dep_canonical_id,
            display_name=f"versioned_dep_{suffix}",
            latest_version="1.0.0",
        )
        session.add(dep_ext)
        await session.flush()
        edge = DependsOn(
            in_vertex_id=source_repo.id,
            out_vertex_id=dep_ext.id,
            version_range="^1.0",
            confidence=EdgeConfidence.verified_sbom,
        )
        session.add(edge)
        await session.commit()

    dep = CrawledDependency(canonical_id=dep_canonical_id, display_name=f"versioned_dep_{suffix}", version_range="^2.0")
    crawler = IntegrationStubCrawler(
        session_factory=db_session_factory,
        packages={
            "main_pkg": _make_package(
                canonical_id=f"pkg:pub/main-pkg-{suffix}",
                display_name=f"main_pkg_{suffix}",
                repo_url=source_repo_url,
                dependencies=[dep],
            )
        },
    )

    result = await crawler.crawl_and_persist(["main_pkg"])

    assert result.packages_processed == 1

    async with db_session_factory() as session:
        edge = (
            await session.execute(
                select(DependsOn).where(
                    DependsOn.in_vertex_id == source_repo.id,
                    DependsOn.out_vertex_id == dep_ext.id,
                )
            )
        ).scalar_one()
        assert edge.confidence == EdgeConfidence.verified_sbom
        assert edge.version_range == "^2.0"


async def test_crawl_is_idempotent_for_vertices_and_edges(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Running the same crawl twice should not create duplicate vertices or edges."""

    suffix = _unique_suffix()
    source_repo_url = f"https://github.com/test-org/idempotent-{suffix}"
    source_repo_canonical_id = f"pkg:github/test-org/idempotent-{suffix}"
    dep_canonical_id = f"pkg:pub/idempotent-dep-{suffix}"

    async with db_session_factory() as session:
        await _seed_source_repo(
            session,
            canonical_id=source_repo_canonical_id,
            repo_url=source_repo_url,
            display_name=f"idempotent-{suffix}",
        )
        await session.commit()

    dep = CrawledDependency(canonical_id=dep_canonical_id, display_name="idempotent_dep", version_range="^1.0")
    crawler = IntegrationStubCrawler(
        session_factory=db_session_factory,
        packages={
            "main_pkg": _make_package(
                canonical_id=f"pkg:pub/idempotent-main-{suffix}",
                display_name="idempotent_main",
                repo_url=source_repo_url,
                dependencies=[dep],
                downloads_30d=42,
            )
        },
    )

    result1 = await crawler.crawl_and_persist(["main_pkg"])
    result2 = await crawler.crawl_and_persist(["main_pkg"])

    assert result1.packages_processed == 1
    assert result2.packages_processed == 1

    async with db_session_factory() as session:
        source_repo = (await session.execute(select(Repo).where(Repo.canonical_id == source_repo_canonical_id))).scalar_one()
        dep_vertex = (
            await session.execute(select(RepoVertex).where(RepoVertex.canonical_id == dep_canonical_id))
        ).scalar_one()

        edges = (
            (
                await session.execute(
                    select(DependsOn).where(
                        DependsOn.in_vertex_id == source_repo.id,
                        DependsOn.out_vertex_id == dep_vertex.id,
                    )
                )
            )
            .scalars()
            .all()
        )
        assert len(edges) == 1

        assert isinstance(source_repo.repo_metadata, dict)
        metadata = source_repo.repo_metadata or {}
        downloads_by_purl = metadata.get("adoption_downloads_by_purl")
        assert downloads_by_purl == {f"pkg:pub/idempotent-main-{suffix}": 42}


async def test_crawl_result_counts_include_dependency_and_dependent_vertices(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """CrawlResult counts should reflect dependency/dependent vertex and edge writes."""

    suffix = _unique_suffix()
    source_repo_url = f"https://github.com/test-org/count-repo-{suffix}"
    source_repo_canonical_id = f"pkg:github/test-org/count-repo-{suffix}"
    main_package_canonical_id = f"pkg:pub/counted-{suffix}"
    dep_x_canonical_id = f"pkg:pub/dep-x-{suffix}"
    dep_y_canonical_id = f"pkg:pub/dep-y-{suffix}"
    rev_z_canonical_id = f"pkg:pub/rev-z-{suffix}"

    async with db_session_factory() as session:
        await _seed_source_repo(
            session,
            canonical_id=source_repo_canonical_id,
            repo_url=source_repo_url,
            display_name=f"count-repo-{suffix}",
        )
        await session.commit()

    deps = [
        CrawledDependency(canonical_id=dep_x_canonical_id, display_name=f"dep_x_{suffix}", version_range="^1.0"),
        CrawledDependency(canonical_id=dep_y_canonical_id, display_name=f"dep_y_{suffix}", version_range=None),
    ]
    dependents = [
        CrawledDependent(canonical_id=rev_z_canonical_id, display_name=f"rev_z_{suffix}"),
    ]
    crawler = IntegrationStubCrawler(
        session_factory=db_session_factory,
        packages={
            "counted": _make_package(
                canonical_id=main_package_canonical_id,
                display_name=f"counted_{suffix}",
                repo_url=source_repo_url,
                dependencies=deps,
            )
        },
        dependents={"counted": dependents},
    )

    result = await crawler.crawl_and_persist(["counted"])

    assert result.packages_processed == 1
    assert result.vertices_upserted == 3
    assert result.edges_created == 3
    assert result.errors == []
