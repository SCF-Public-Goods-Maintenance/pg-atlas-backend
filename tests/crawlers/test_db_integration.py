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

import asyncio
import datetime as dt
import logging
import uuid
from collections.abc import AsyncGenerator
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from pg_atlas.crawlers.base import (
    CrawledDependency,
    CrawledDependent,
    CrawledPackage,
    RegistryCrawler,
)
from pg_atlas.crawlers.github_dependents import GitHubDependentsCrawler
from pg_atlas.db_models.base import EdgeConfidence, GithubDependentsRunStatus, Visibility
from pg_atlas.db_models.depends_on import DependsOn
from pg_atlas.db_models.github_dependents_observation import (
    GithubDependentObservation,
    GithubDependentsCrawlRun,
)
from pg_atlas.db_models.repo_vertex import ExternalRepo, Repo, RepoVertex
from pg_atlas.metrics.active_subgraph import project_active_subgraph
from pg_atlas.metrics.criticality import compute_criticality
from pg_atlas.metrics.graph_builder import build_dependency_graph
from pg_atlas.routers.common import PaginationParams
from pg_atlas.routers.repos import get_repo_github_dependents
from tests.conftest import get_test_database_url
from tests.crawlers.conftest import gh_entry_row, gh_headers, gh_page, gh_response, gh_selector_page
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


@pytest.mark.parametrize(
    "package_canonical_id",
    [
        "pkg:pub/my-sdk",
        "pkg:npm/my-sdk",
        "pkg:cargo/my-sdk",
        "pkg:pypi/my-sdk",
    ],
)
async def test_crawl_writes_downloads_to_source_repo_metadata(
    db_session_factory: async_sessionmaker[AsyncSession],
    package_canonical_id: str,
) -> None:
    """Crawler should write download counts into source repo metadata map only."""

    suffix = _unique_suffix()
    source_repo_url = f"https://github.com/test-org/source-repo-{suffix}"
    source_repo_canonical_id = f"pkg:github/test-org/source-repo-{suffix}"
    package_canonical_id = f"{package_canonical_id}-{suffix}"

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


# ---------------------------------------------------------------------------
# GitHub dependents crawler: runs + observations (no graph writes)
# ---------------------------------------------------------------------------


def _gh_crawler(
    session_factory: async_sessionmaker[AsyncSession],
    pages: list[str] | str,
    **caps: int,
) -> GitHubDependentsCrawler:
    """Real crawler with a mocked HTTP client returning the given pages."""

    client = AsyncMock()
    if isinstance(pages, str):
        client.get = AsyncMock(return_value=gh_response(pages))
    else:
        client.get = AsyncMock(side_effect=[gh_response(p) for p in pages])
    return GitHubDependentsCrawler(client=client, session_factory=session_factory, rate_limit=0.0, **caps)


def _commit_intercept_factory(db_engine: Any, *, fail_on: int, commit_first: bool) -> async_sessionmaker[AsyncSession]:
    """
    Session factory whose FIRST session fails its ``fail_on``-th commit.

    ``commit_first=False`` raises instead of committing (the server rolled
    back); ``commit_first=True`` performs the real commit and then raises (the
    acknowledgement was lost after a durable server-side commit). Sessions
    created later — the crawler's failure reconciliation — behave normally.
    """

    state = {"sessions": 0}

    class InterceptSession(AsyncSession):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            state["sessions"] += 1
            self._intercepted = state["sessions"] == 1
            self._commit_calls = 0

        async def commit(self) -> None:
            if self._intercepted:
                self._commit_calls += 1
                if self._commit_calls == fail_on:
                    if commit_first:
                        await super().commit()
                    raise OperationalError(
                        "simulated commit failure",
                        None,
                        RuntimeError("ack lost" if commit_first else "commit rejected"),
                    )
            await super().commit()

    return async_sessionmaker(db_engine, class_=InterceptSession, expire_on_commit=False)


async def _gh_seed_source(db_session_factory: async_sessionmaker[AsyncSession], suffix: str) -> tuple[str, int]:
    """Seed a tracked source repo; returns (package_name, repo_id)."""

    async with db_session_factory() as session:
        repo = await _seed_source_repo(
            session,
            canonical_id=f"pkg:github/test-org/gh-src-{suffix}",
            repo_url=f"https://github.com/test-org/gh-src-{suffix}",
            display_name=f"gh-src-{suffix}",
        )
        await session.commit()
        repo_id = repo.id

    return f"test-org/gh-src-{suffix}", repo_id


async def _gh_state(
    db_session_factory: async_sessionmaker[AsyncSession], source_repo_id: int
) -> tuple[list[GithubDependentsCrawlRun], dict[str, GithubDependentObservation]]:
    """Load all runs (by id) and observations (by dependent_key) for a source."""

    async with db_session_factory() as session:
        runs = (
            (
                await session.execute(
                    select(GithubDependentsCrawlRun)
                    .where(GithubDependentsCrawlRun.source_repo_id == source_repo_id)
                    .order_by(GithubDependentsCrawlRun.id)
                )
            )
            .scalars()
            .all()
        )
        observations = (
            (
                await session.execute(
                    select(GithubDependentObservation).where(GithubDependentObservation.source_repo_id == source_repo_id)
                )
            )
            .scalars()
            .all()
        )

    return list(runs), {o.dependent_key: o for o in observations}


async def test_github_first_complete_run_inserts_observations(
    db_session_factory: async_sessionmaker[AsyncSession],
    github_dependents_page1_html: str,
    github_dependents_page2_html: str,
) -> None:
    """A first complete crawl persists a complete run row and one observation per dependent."""

    suffix = _unique_suffix()
    package_name, repo_id = await _gh_seed_source(db_session_factory, suffix)

    crawler = _gh_crawler(db_session_factory, [github_dependents_page1_html, github_dependents_page2_html])
    result = await crawler.crawl_and_persist([package_name])

    assert result.packages_processed == 1
    assert result.errors == []

    runs, observations = await _gh_state(db_session_factory, repo_id)
    assert len(runs) == 1
    run = runs[0]
    assert run.status == GithubDependentsRunStatus.complete
    assert run.listing_complete is True and run.counts_complete is True
    assert run.repos_total_reported == 87 and run.packages_total_reported == 6
    assert run.public_repos_observed == 7
    assert run.pages_fetched == 2 and run.request_count == 2
    assert run.parser_version == "1"
    assert run.snapshot_fingerprint is not None
    assert run.finished_at is not None

    assert len(observations) == 7
    monorepo = observations["habitanexus/monorepo"]
    assert monorepo.dependent_canonical_id == "pkg:github/habitanexus/monorepo"
    assert (monorepo.observed_owner, monorepo.observed_repo_name) == ("HabitaNexus", "monorepo")
    assert monorepo.dependent_repo_url == "https://github.com/HabitaNexus/monorepo"
    assert monorepo.last_seen_run_id == run.id
    assert monorepo.retired_at is None
    assert monorepo.resolved_repo_id is None


async def test_github_crawl_writes_no_graph_objects(
    db_session_factory: async_sessionmaker[AsyncSession],
    github_dependents_page1_html: str,
    github_dependents_page2_html: str,
) -> None:
    """The crawl leaves the metric graph identical: metadata, attributes, and criticality."""

    # Fixed reference date so a run crossing midnight cannot change
    # days_since_commit between the before and after builds.
    reference_date = dt.date(2026, 8, 1)
    suffix = _unique_suffix()
    package_name, _ = await _gh_seed_source(db_session_factory, suffix)

    async with db_session_factory() as session:
        graph_before = await build_dependency_graph(session, reference_date)
        edges_before = (await session.execute(select(func.count()).select_from(DependsOn))).scalar_one()
        vertices_before = (await session.execute(select(func.count()).select_from(RepoVertex))).scalar_one()

    crawler = _gh_crawler(db_session_factory, [github_dependents_page1_html, github_dependents_page2_html])
    result = await crawler.crawl_and_persist([package_name])
    assert result.packages_processed == 1

    async with db_session_factory() as session:
        graph_after = await build_dependency_graph(session, reference_date)
        edges_after = (await session.execute(select(func.count()).select_from(DependsOn))).scalar_one()
        vertices_after = (await session.execute(select(func.count()).select_from(RepoVertex))).scalar_one()

    assert edges_after == edges_before
    assert vertices_after == vertices_before

    assert graph_after.graph == graph_before.graph
    assert {n: d for n, d in graph_after.nodes(data=True)} == {n: d for n, d in graph_before.nodes(data=True)}
    assert {(u, v): d for u, v, d in graph_after.edges(data=True)} == {(u, v): d for u, v, d in graph_before.edges(data=True)}

    active_before = project_active_subgraph(graph_before)
    active_after = project_active_subgraph(graph_after)
    assert active_after.graph == active_before.graph
    assert {n: d for n, d in active_after.nodes(data=True)} == {n: d for n, d in active_before.nodes(data=True)}
    assert {(u, v): d for u, v, d in active_after.edges(data=True)} == {
        (u, v): d for u, v, d in active_before.edges(data=True)
    }
    assert compute_criticality(active_after) == compute_criticality(active_before)


async def test_github_repeat_complete_run_is_idempotent(
    db_session_factory: async_sessionmaker[AsyncSession],
    github_dependents_page1_html: str,
    github_dependents_page2_html: str,
) -> None:
    """An identical second complete crawl refreshes rows in place: no duplicates, no retirement."""

    suffix = _unique_suffix()
    package_name, repo_id = await _gh_seed_source(db_session_factory, suffix)
    pages = [github_dependents_page1_html, github_dependents_page2_html]

    await _gh_crawler(db_session_factory, list(pages)).crawl_and_persist([package_name])
    _, obs1 = await _gh_state(db_session_factory, repo_id)
    await _gh_crawler(db_session_factory, list(pages)).crawl_and_persist([package_name])
    runs2, obs2 = await _gh_state(db_session_factory, repo_id)

    assert len(runs2) == 2 and all(r.status == GithubDependentsRunStatus.complete for r in runs2)
    # Identical page sequences produce the identical stored sha256 fingerprint.
    fingerprints = [r.snapshot_fingerprint for r in runs2]
    assert all(f is not None and len(f) == 64 for f in fingerprints)
    assert fingerprints[0] == fingerprints[1]
    assert set(obs2.keys()) == set(obs1.keys())
    for key, row in obs2.items():
        assert row.id == obs1[key].id
        assert row.first_seen_at == obs1[key].first_seen_at
        assert row.last_seen_run_id == runs2[1].id
        assert row.retired_at is None


async def test_github_later_complete_run_retires_and_reactivates(
    db_session_factory: async_sessionmaker[AsyncSession],
    github_dependents_page1_html: str,
    github_dependents_page2_html: str,
) -> None:
    """A complete run retires unseen observations; a later complete run reactivates them in place."""

    suffix = _unique_suffix()
    package_name, repo_id = await _gh_seed_source(db_session_factory, suffix)
    full_pages = [github_dependents_page1_html, github_dependents_page2_html]

    await _gh_crawler(db_session_factory, list(full_pages)).crawl_and_persist([package_name])
    _, obs_full = await _gh_state(db_session_factory, repo_id)
    assert len(obs_full) == 7

    # Shrunken complete listing: only the terminal page's three dependents.
    await _gh_crawler(db_session_factory, github_dependents_page2_html).crawl_and_persist([package_name])
    runs, obs = await _gh_state(db_session_factory, repo_id)
    active = {k for k, o in obs.items() if o.retired_at is None}
    retired = {k for k, o in obs.items() if o.retired_at is not None}
    assert len(active) == 3 and len(retired) == 4
    assert all(obs[k].retired_by_run_id == runs[1].id for k in retired)

    # The full listing returns: retired rows reactivate in place.
    await _gh_crawler(db_session_factory, list(full_pages)).crawl_and_persist([package_name])
    _, obs3 = await _gh_state(db_session_factory, repo_id)
    assert all(o.retired_at is None for o in obs3.values())
    assert {o.id for o in obs3.values()} == {o.id for o in obs.values()}
    assert all(obs3[k].first_seen_at == obs_full[k].first_seen_at for k in obs_full)


async def test_github_partial_run_adds_but_never_retires(
    db_session_factory: async_sessionmaker[AsyncSession],
    github_dependents_page1_html: str,
    github_dependents_page2_html: str,
) -> None:
    """A partial (entry-capped) run refreshes what it saw and retires nothing."""

    suffix = _unique_suffix()
    package_name, repo_id = await _gh_seed_source(db_session_factory, suffix)

    await _gh_crawler(db_session_factory, [github_dependents_page1_html, github_dependents_page2_html]).crawl_and_persist(
        [package_name]
    )

    await _gh_crawler(db_session_factory, github_dependents_page1_html, entry_cap=2).crawl_and_persist([package_name])

    runs, obs = await _gh_state(db_session_factory, repo_id)
    partial = runs[1]
    assert partial.status == GithubDependentsRunStatus.partial
    assert partial.listing_complete is False
    assert partial.listing_incomplete_reason == "entry-cap"
    assert all(o.retired_at is None for o in obs.values())
    refreshed = [k for k, o in obs.items() if o.last_seen_run_id == partial.id]
    assert len(refreshed) == 2
    # Freshness of the observation set still points at the complete run.
    assert runs[0].listing_complete is True


async def test_github_second_retiring_run_preserves_original_retirement(
    db_session_factory: async_sessionmaker[AsyncSession],
    github_dependents_page1_html: str,
    github_dependents_page2_html: str,
) -> None:
    """Already-retired rows keep the run that first retired them across later retiring runs."""

    suffix = _unique_suffix()
    package_name, repo_id = await _gh_seed_source(db_session_factory, suffix)

    await _gh_crawler(db_session_factory, [github_dependents_page1_html, github_dependents_page2_html]).crawl_and_persist(
        [package_name]
    )
    await _gh_crawler(db_session_factory, github_dependents_page2_html).crawl_and_persist([package_name])
    runs2, obs2 = await _gh_state(db_session_factory, repo_id)
    retired_keys = {k for k, o in obs2.items() if o.retired_at is not None}
    assert len(retired_keys) == 4

    await _gh_crawler(db_session_factory, github_dependents_page2_html).crawl_and_persist([package_name])

    runs3, obs3 = await _gh_state(db_session_factory, repo_id)
    assert runs3[2].status == GithubDependentsRunStatus.complete
    for key in retired_keys:
        assert obs3[key].retired_by_run_id == runs2[1].id
        assert obs3[key].retired_at == obs2[key].retired_at


async def test_github_null_last_seen_run_row_is_retired(
    db_session_factory: async_sessionmaker[AsyncSession],
    github_dependents_page2_html: str,
) -> None:
    """Retirement matches rows whose last_seen_run_id is NULL (IS DISTINCT FROM semantics)."""

    suffix = _unique_suffix()
    package_name, repo_id = await _gh_seed_source(db_session_factory, suffix)

    async with db_session_factory() as session:
        session.add(
            GithubDependentObservation(
                source_repo_id=repo_id,
                dependent_key=f"legacy/never-crawled-{suffix}",
                dependent_canonical_id=f"pkg:github/legacy/never-crawled-{suffix}",
                observed_owner="legacy",
                observed_repo_name=f"never-crawled-{suffix}",
                dependent_repo_url=f"https://github.com/legacy/never-crawled-{suffix}",
            )
        )
        await session.commit()

    await _gh_crawler(db_session_factory, github_dependents_page2_html).crawl_and_persist([package_name])

    runs, obs = await _gh_state(db_session_factory, repo_id)
    legacy = obs[f"legacy/never-crawled-{suffix}"]
    assert legacy.retired_at is not None
    assert legacy.retired_by_run_id == runs[0].id


async def test_github_package_ids_union_and_replace_through_db(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Partial listings union package memberships in SQL; complete listings replace them."""

    suffix = _unique_suffix()
    package_name, repo_id = await _gh_seed_source(db_session_factory, suffix)
    selector = gh_selector_page(["PKGA", "PKGB"])

    # Run 1 (complete): app-x under PKGA only, app-y under PKGB.
    await _gh_crawler(
        db_session_factory,
        [
            selector,
            gh_page(gh_entry_row("acme", "app-x"), headers=gh_headers(1, 1)),
            gh_page(gh_entry_row("acme", "app-y"), headers=gh_headers(1, 1)),
        ],
    ).crawl_and_persist([package_name])
    _, obs = await _gh_state(db_session_factory, repo_id)
    assert obs["acme/app-x"].package_ids == ["PKGA"]

    # Run 2 (partial via page cap): app-x observed under PKGB only — the SQL
    # union must keep the unseen PKGA membership.
    await _gh_crawler(
        db_session_factory,
        [
            selector,
            gh_page(gh_entry_row("acme", "app-z"), headers=gh_headers(1, 1)),
            gh_page(gh_entry_row("acme", "app-x"), cursor="CUR1", headers=gh_headers(2, 1)),
        ],
        pages_cap=1,
    ).crawl_and_persist([package_name])
    runs, obs = await _gh_state(db_session_factory, repo_id)
    partial = runs[1]
    assert partial.status == GithubDependentsRunStatus.partial
    assert partial.listing_incomplete_reason == "page-cap"
    assert sorted(obs["acme/app-x"].package_ids or []) == ["PKGA", "PKGB"]
    assert obs["acme/app-z"].package_ids == ["PKGA"]
    assert obs["acme/app-y"].retired_at is None

    # Run 3 (complete): app-x under PKGB only — replace drops the stale PKGA
    # membership; the unseen dependents retire.
    await _gh_crawler(
        db_session_factory,
        [
            selector,
            gh_page("", headers=gh_headers(0, 1)),
            gh_page(gh_entry_row("acme", "app-x"), headers=gh_headers(1, 1)),
        ],
    ).crawl_and_persist([package_name])
    runs3, obs3 = await _gh_state(db_session_factory, repo_id)
    assert runs3[2].status == GithubDependentsRunStatus.complete
    assert obs3["acme/app-x"].package_ids == ["PKGB"]
    assert obs3["acme/app-x"].retired_at is None
    assert obs3["acme/app-y"].retired_at is not None
    assert obs3["acme/app-z"].retired_at is not None


async def test_github_failed_run_changes_nothing(
    db_session_factory: async_sessionmaker[AsyncSession],
    github_dependents_page1_html: str,
    github_dependents_page2_html: str,
    github_dependents_layout_changed_html: str,
) -> None:
    """A layout-drift failure records a failed run and leaves observations untouched."""

    suffix = _unique_suffix()
    package_name, repo_id = await _gh_seed_source(db_session_factory, suffix)

    await _gh_crawler(db_session_factory, [github_dependents_page1_html, github_dependents_page2_html]).crawl_and_persist(
        [package_name]
    )
    _, obs1 = await _gh_state(db_session_factory, repo_id)

    result = await _gh_crawler(db_session_factory, github_dependents_layout_changed_html).crawl_and_persist([package_name])
    assert result.packages_processed == 0
    assert len(result.errors) == 1

    runs2, obs2 = await _gh_state(db_session_factory, repo_id)
    failed = runs2[1]
    assert failed.status == GithubDependentsRunStatus.failed
    assert failed.finished_at is not None
    assert failed.error_detail is not None and "No dependent counts" in failed.error_detail
    assert {k: o.last_seen_run_id for k, o in obs2.items()} == {k: o.last_seen_run_id for k, o in obs1.items()}
    assert all(o.retired_at is None for o in obs2.values())


async def test_github_valid_zero_complete_run_retires_all(
    db_session_factory: async_sessionmaker[AsyncSession],
    github_dependents_page1_html: str,
    github_dependents_page2_html: str,
    github_dependents_empty_html: str,
) -> None:
    """A valid zero-count complete run retires every previously active observation."""

    suffix = _unique_suffix()
    package_name, repo_id = await _gh_seed_source(db_session_factory, suffix)

    await _gh_crawler(db_session_factory, [github_dependents_page1_html, github_dependents_page2_html]).crawl_and_persist(
        [package_name]
    )

    await _gh_crawler(db_session_factory, github_dependents_empty_html).crawl_and_persist([package_name])

    runs, obs = await _gh_state(db_session_factory, repo_id)
    assert runs[1].status == GithubDependentsRunStatus.complete
    assert runs[1].public_repos_observed == 0
    assert all(o.retired_at is not None for o in obs.values())


async def test_github_ambiguous_zero_run_retires_none(
    db_session_factory: async_sessionmaker[AsyncSession],
    github_dependents_page1_html: str,
    github_dependents_page2_html: str,
) -> None:
    """A positive-count/zero-row page is a partial run and must not retire observations."""

    suffix = _unique_suffix()
    package_name, repo_id = await _gh_seed_source(db_session_factory, suffix)

    await _gh_crawler(db_session_factory, [github_dependents_page1_html, github_dependents_page2_html]).crawl_and_persist(
        [package_name]
    )

    await _gh_crawler(db_session_factory, gh_page("")).crawl_and_persist([package_name])

    runs, obs = await _gh_state(db_session_factory, repo_id)
    ambiguous = runs[1]
    assert ambiguous.status == GithubDependentsRunStatus.partial
    assert ambiguous.listing_incomplete_reason == "positive-count-no-visible-rows"
    assert all(o.retired_at is None for o in obs.values())
    assert len(obs) == 7


async def test_github_out_of_order_snapshots_are_superseded(
    db_session_factory: async_sessionmaker[AsyncSession],
    github_dependents_page1_html: str,
    github_dependents_page2_html: str,
) -> None:
    """An older snapshot applying after a newer applied run is superseded and mutates nothing."""

    suffix = _unique_suffix()
    package_name, repo_id = await _gh_seed_source(db_session_factory, suffix)

    # Build two snapshots up front (older run A: full listing; newer run B: shrunken listing).
    crawler_a = _gh_crawler(db_session_factory, [github_dependents_page1_html, github_dependents_page2_html])
    crawler_b = _gh_crawler(db_session_factory, github_dependents_page2_html)
    snapshot_a = await crawler_a.collect_snapshot(package_name)
    snapshot_b = await crawler_b.collect_snapshot(package_name)

    async with db_session_factory() as session:
        run_a = GithubDependentsCrawlRun(source_repo_id=repo_id)
        session.add(run_a)
        await session.commit()
        run_a_id = run_a.id
    async with db_session_factory() as session:
        run_b = GithubDependentsCrawlRun(source_repo_id=repo_id)
        session.add(run_b)
        await session.commit()
        run_b_id = run_b.id
    assert run_a_id < run_b_id

    # Newer run B applies first; older complete run A finishes later.
    async with db_session_factory() as session:
        assert await crawler_b._apply_snapshot(session, repo_id, run_b_id, snapshot_b) is True
        await session.commit()
    async with db_session_factory() as session:
        assert await crawler_a._apply_snapshot(session, repo_id, run_a_id, snapshot_a) is False

    runs, obs = await _gh_state(db_session_factory, repo_id)
    by_id = {r.id: r for r in runs}
    assert by_id[run_a_id].status == GithubDependentsRunStatus.superseded
    assert by_id[run_b_id].status == GithubDependentsRunStatus.complete
    # Only run B's three dependents exist; run A's snapshot never landed.
    assert len(obs) == 3
    assert all(o.last_seen_run_id == run_b_id for o in obs.values())

    # Older PARTIAL snapshot after a newer applied run is equally superseded.
    crawler_c = _gh_crawler(db_session_factory, github_dependents_page1_html, entry_cap=2)
    snapshot_c = await crawler_c.collect_snapshot(package_name)
    async with db_session_factory() as session:
        run_c = GithubDependentsCrawlRun(source_repo_id=repo_id)
        session.add(run_c)
        await session.commit()
        run_c_id = run_c.id
    async with db_session_factory() as session:
        run_d = GithubDependentsCrawlRun(source_repo_id=repo_id)
        session.add(run_d)
        await session.commit()
        run_d_id = run_d.id
    crawler_d = _gh_crawler(db_session_factory, github_dependents_page2_html)
    snapshot_d = await crawler_d.collect_snapshot(package_name)

    async with db_session_factory() as session:
        assert await crawler_d._apply_snapshot(session, repo_id, run_d_id, snapshot_d) is True
        await session.commit()
    async with db_session_factory() as session:
        assert await crawler_c._apply_snapshot(session, repo_id, run_c_id, snapshot_c) is False

    runs2, obs2 = await _gh_state(db_session_factory, repo_id)
    assert {r.id: r.status for r in runs2}[run_c_id] == GithubDependentsRunStatus.superseded
    assert len([o for o in obs2.values() if o.retired_at is None]) == 3


async def test_github_tracked_dependent_resolves_case_insensitively(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """A dependent that is a tracked repo links via resolved_repo_id despite case differences."""

    suffix = _unique_suffix()
    package_name, repo_id = await _gh_seed_source(db_session_factory, suffix)

    async with db_session_factory() as session:
        tracked = await _seed_source_repo(
            session,
            canonical_id=f"pkg:github/Dep-Org/Tracked-{suffix}",
            repo_url=f"https://github.com/Dep-Org/Tracked-{suffix}",
            display_name=f"tracked-{suffix}",
        )
        await session.commit()
        tracked_id = tracked.id

    page = gh_page(gh_entry_row("dep-org", f"tracked-{suffix}") + gh_entry_row("someone", f"untracked-{suffix}"))
    await _gh_crawler(db_session_factory, page).crawl_and_persist([package_name])

    _, obs = await _gh_state(db_session_factory, repo_id)
    assert obs[f"dep-org/tracked-{suffix}"].resolved_repo_id == tracked_id
    assert obs[f"someone/untracked-{suffix}"].resolved_repo_id is None


async def test_github_ambiguous_source_fails_loudly(
    db_session_factory: async_sessionmaker[AsyncSession],
    github_dependents_empty_html: str,
) -> None:
    """Two tracked repos matching the normalized source identity fail the crawl with no run row."""

    suffix = _unique_suffix()
    async with db_session_factory() as session:
        first = await _seed_source_repo(
            session,
            canonical_id=f"pkg:github/Amb-Org/Repo-{suffix}",
            repo_url=f"https://github.com/Amb-Org/Repo-{suffix}",
            display_name=f"amb-a-{suffix}",
        )
        await _seed_source_repo(
            session,
            canonical_id=f"pkg:github/amb-org/repo-{suffix}",
            repo_url=f"https://github.com/amb-org/repo-{suffix}",
            display_name=f"amb-b-{suffix}",
        )
        await session.commit()
        first_id = first.id

    result = await _gh_crawler(db_session_factory, github_dependents_empty_html).crawl_and_persist([f"amb-org/repo-{suffix}"])
    assert result.packages_processed == 0
    assert len(result.errors) == 1

    runs, _ = await _gh_state(db_session_factory, first_id)
    assert runs == []


async def test_github_concurrent_same_source_crawls_serialize(
    db_session_factory: async_sessionmaker[AsyncSession],
    github_dependents_page1_html: str,
    github_dependents_page2_html: str,
) -> None:
    """Two overlapping crawls of one source serialize; the stale one is superseded, state stays consistent."""

    suffix = _unique_suffix()
    package_name, repo_id = await _gh_seed_source(db_session_factory, suffix)

    # Deterministic interleaving without timing assumptions: the slow (older)
    # crawl serves page 1 immediately but holds its terminal page until the
    # fast (newer) crawl has fully applied, and the fast crawl starts only
    # after the slow crawl's run row is durably committed — so the run-id
    # order and the apply order are both fixed.
    fast_applied = asyncio.Event()
    slow_pages = iter([github_dependents_page1_html, github_dependents_page2_html])

    async def slow_get(url: str) -> httpx.Response:
        page = next(slow_pages)
        if page is github_dependents_page2_html:
            await fast_applied.wait()
        return gh_response(page)

    slow_client = AsyncMock()
    slow_client.get = AsyncMock(side_effect=slow_get)
    slow_crawler = GitHubDependentsCrawler(client=slow_client, session_factory=db_session_factory, rate_limit=0.0)
    fast_crawler = _gh_crawler(db_session_factory, github_dependents_page2_html)

    async def fast_after_slow_run_row() -> None:
        while True:
            async with db_session_factory() as session:
                run_rows = await session.scalar(
                    select(func.count())
                    .select_from(GithubDependentsCrawlRun)
                    .where(GithubDependentsCrawlRun.source_repo_id == repo_id)
                )
            if run_rows:
                break

            await asyncio.sleep(0.01)

        await fast_crawler.crawl_and_persist([package_name])
        fast_applied.set()

    await asyncio.gather(slow_crawler.crawl_and_persist([package_name]), fast_after_slow_run_row())

    runs, obs = await _gh_state(db_session_factory, repo_id)
    assert len(runs) == 2
    statuses = sorted(r.status.value for r in runs)
    # The slower, older run must be superseded; the fast newer run applied.
    assert statuses == ["complete", "superseded"]
    applied = next(r for r in runs if r.status == GithubDependentsRunStatus.complete)
    active = {k for k, o in obs.items() if o.retired_at is None}
    assert len(active) == applied.public_repos_observed == 3


async def test_github_schema_essentials(
    db_engine: Any,
) -> None:
    """Migration created the tables with the uniqueness and enum the code relies on."""

    async with db_engine.connect() as conn:

        def _inspect(sync_conn: Any) -> tuple[list[str], list[dict[str, Any]]]:
            from sqlalchemy import inspect as sa_inspect

            inspector = sa_inspect(sync_conn)
            tables = inspector.get_table_names()
            uniques = inspector.get_unique_constraints("github_dependent_observations")
            return tables, uniques

        tables, uniques = await conn.run_sync(_inspect)

    assert "github_dependents_crawl_runs" in tables
    assert "github_dependent_observations" in tables
    assert any(sorted(u["column_names"]) == ["dependent_key", "source_repo_id"] for u in uniques)


async def test_github_dependents_endpoint(
    db_session_factory: async_sessionmaker[AsyncSession],
    github_dependents_page1_html: str,
    github_dependents_page2_html: str,
) -> None:
    """The read endpoint reports freshness split by axis and paginated active observations."""

    suffix = _unique_suffix()
    package_name, repo_id = await _gh_seed_source(db_session_factory, suffix)
    canonical_id = f"pkg:github/test-org/gh-src-{suffix}"

    await _gh_crawler(db_session_factory, [github_dependents_page1_html, github_dependents_page2_html]).crawl_and_persist(
        [package_name]
    )
    # A later partial attempt must not shift the observation freshness.
    await _gh_crawler(db_session_factory, github_dependents_page1_html, entry_cap=2).crawl_and_persist([package_name])

    async with db_session_factory() as session:
        response = await get_repo_github_dependents(canonical_id, session, PaginationParams(limit=3, offset=0))

    runs, _ = await _gh_state(db_session_factory, repo_id)
    complete_run, partial_run = runs[0], runs[1]

    assert response.summary.active_observations == 7
    assert response.summary.latest_attempt_status == "partial"
    assert response.summary.latest_attempt_listing_incomplete_reason == "entry-cap"
    assert response.summary.observations_as_of == complete_run.finished_at
    assert response.summary.reported_counts_as_of == partial_run.finished_at
    assert response.summary.repos_total_reported == 87

    assert response.observations.total == 7
    assert len(response.observations.items) == 3
    keys = [item.dependent_canonical_id for item in response.observations.items]
    assert keys == sorted(keys)


async def test_github_older_complete_after_newer_partial_is_superseded(
    db_session_factory: async_sessionmaker[AsyncSession],
    github_dependents_page1_html: str,
    github_dependents_page2_html: str,
) -> None:
    """An older complete snapshot cannot retire or overwrite rows a newer partial run added."""

    suffix = _unique_suffix()
    package_name, repo_id = await _gh_seed_source(db_session_factory, suffix)

    crawler_full = _gh_crawler(db_session_factory, [github_dependents_page1_html, github_dependents_page2_html])
    snapshot_full = await crawler_full.collect_snapshot(package_name)
    crawler_partial = _gh_crawler(db_session_factory, github_dependents_page1_html, entry_cap=2)
    snapshot_partial = await crawler_partial.collect_snapshot(package_name)
    assert snapshot_partial.listing_complete is False

    async with db_session_factory() as session:
        run_older = GithubDependentsCrawlRun(source_repo_id=repo_id)
        session.add(run_older)
        await session.commit()
        run_older_id = run_older.id
    async with db_session_factory() as session:
        run_newer = GithubDependentsCrawlRun(source_repo_id=repo_id)
        session.add(run_newer)
        await session.commit()
        run_newer_id = run_newer.id

    # The newer PARTIAL run applies first; the older complete snapshot lands late.
    async with db_session_factory() as session:
        assert await crawler_partial._apply_snapshot(session, repo_id, run_newer_id, snapshot_partial) is True
        await session.commit()
    async with db_session_factory() as session:
        assert await crawler_full._apply_snapshot(session, repo_id, run_older_id, snapshot_full) is False

    runs, obs = await _gh_state(db_session_factory, repo_id)
    by_id = {r.id: r for r in runs}
    assert by_id[run_older_id].status == GithubDependentsRunStatus.superseded
    assert by_id[run_newer_id].status == GithubDependentsRunStatus.partial
    # Only the partial run's rows exist, untouched: not retired, not refreshed.
    assert len(obs) == 2
    assert all(o.last_seen_run_id == run_newer_id and o.retired_at is None for o in obs.values())


async def test_github_two_sources_share_a_dependent_key(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Two tracked sources can observe the same normalized dependent without collision."""

    suffix = _unique_suffix()
    package_a, repo_a_id = await _gh_seed_source(db_session_factory, suffix + "a")
    package_b, repo_b_id = await _gh_seed_source(db_session_factory, suffix + "b")
    shared_key = f"shared/dep-{suffix}"
    page = gh_page(gh_entry_row("shared", f"dep-{suffix}"))

    await _gh_crawler(db_session_factory, page).crawl_and_persist([package_a])
    await _gh_crawler(db_session_factory, page).crawl_and_persist([package_b])

    _, obs_a = await _gh_state(db_session_factory, repo_a_id)
    _, obs_b = await _gh_state(db_session_factory, repo_b_id)
    assert obs_a[shared_key].source_repo_id == repo_a_id
    assert obs_b[shared_key].source_repo_id == repo_b_id
    assert obs_a[shared_key].id != obs_b[shared_key].id
    assert obs_a[shared_key].retired_at is None and obs_b[shared_key].retired_at is None


async def test_github_fk_deletion_semantics(
    db_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Deleting a resolved dependent Repo nulls the link; deleting a source with audit rows is restricted."""

    suffix = _unique_suffix()
    package_name, repo_id = await _gh_seed_source(db_session_factory, suffix)

    async with db_session_factory() as session:
        tracked = await _seed_source_repo(
            session,
            canonical_id=f"pkg:github/dep-org/deletable-{suffix}",
            repo_url=f"https://github.com/dep-org/deletable-{suffix}",
            display_name=f"deletable-{suffix}",
        )
        await session.commit()
        tracked_id = tracked.id

    page = gh_page(gh_entry_row("dep-org", f"deletable-{suffix}"))
    await _gh_crawler(db_session_factory, page).crawl_and_persist([package_name])
    _, obs = await _gh_state(db_session_factory, repo_id)
    assert obs[f"dep-org/deletable-{suffix}"].resolved_repo_id == tracked_id

    # ON DELETE SET NULL: the observation survives the tracked repo's deletion.
    async with db_session_factory() as session:
        tracked_repo = await session.get(Repo, tracked_id)
        assert tracked_repo is not None
        await session.delete(tracked_repo)
        await session.commit()

    _, obs = await _gh_state(db_session_factory, repo_id)
    row = obs[f"dep-org/deletable-{suffix}"]
    assert row.resolved_repo_id is None
    assert row.retired_at is None

    # Restrictive FK: a source with runs and observations cannot be deleted.
    async with db_session_factory() as session:
        source_repo = await session.get(Repo, repo_id)
        assert source_repo is not None
        await session.delete(source_repo)
        with pytest.raises(IntegrityError):
            await session.commit()


async def test_github_apply_phase_failure_reconciles_to_failed(
    db_session_factory: async_sessionmaker[AsyncSession],
    github_dependents_page1_html: str,
    github_dependents_page2_html: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An exception inside snapshot application produces a failed run and mutates no observation."""

    suffix = _unique_suffix()
    package_name, repo_id = await _gh_seed_source(db_session_factory, suffix)
    pages = [github_dependents_page1_html, github_dependents_page2_html]

    await _gh_crawler(db_session_factory, list(pages)).crawl_and_persist([package_name])
    _, obs_before = await _gh_state(db_session_factory, repo_id)

    crawler = _gh_crawler(db_session_factory, list(pages))

    async def _raise_in_apply(session: AsyncSession, snapshot: Any) -> dict[str, int]:
        raise RuntimeError("apply-phase boom")

    monkeypatch.setattr(crawler, "_resolve_dependent_repo_ids", _raise_in_apply)
    result = await crawler.crawl_and_persist([package_name])
    assert result.packages_processed == 0
    assert len(result.errors) == 1

    runs, obs_after = await _gh_state(db_session_factory, repo_id)
    failed = runs[1]
    assert failed.status == GithubDependentsRunStatus.failed
    assert failed.error_detail is not None and "apply-phase boom" in failed.error_detail
    assert failed.finished_at is not None
    assert {k: (o.last_seen_run_id, o.retired_at) for k, o in obs_after.items()} == {
        k: (o.last_seen_run_id, o.retired_at) for k, o in obs_before.items()
    }


async def test_github_ambiguous_resolution_preserves_existing_link(
    db_session_factory: async_sessionmaker[AsyncSession],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A later ambiguous dependent lookup keeps the previously resolved tracked-repo link."""

    suffix = _unique_suffix()
    package_name, repo_id = await _gh_seed_source(db_session_factory, suffix)

    async with db_session_factory() as session:
        tracked = await _seed_source_repo(
            session,
            canonical_id=f"pkg:github/Dep-Org/Linked-{suffix}",
            repo_url=f"https://github.com/Dep-Org/Linked-{suffix}",
            display_name=f"linked-a-{suffix}",
        )
        await session.commit()
        tracked_id = tracked.id

    page = gh_page(gh_entry_row("dep-org", f"linked-{suffix}"))
    await _gh_crawler(db_session_factory, page).crawl_and_persist([package_name])
    _, obs = await _gh_state(db_session_factory, repo_id)
    assert obs[f"dep-org/linked-{suffix}"].resolved_repo_id == tracked_id

    # A case-variant second tracked repo makes the identity ambiguous.
    async with db_session_factory() as session:
        await _seed_source_repo(
            session,
            canonical_id=f"pkg:github/DEP-ORG/LINKED-{suffix}",
            repo_url=f"https://github.com/DEP-ORG/LINKED-{suffix}",
            display_name=f"linked-b-{suffix}",
        )
        await session.commit()

    caplog.set_level(logging.WARNING, logger="pg_atlas.crawlers.github_dependents")
    await _gh_crawler(db_session_factory, page).crawl_and_persist([package_name])

    runs, obs = await _gh_state(db_session_factory, repo_id)
    assert runs[1].status == GithubDependentsRunStatus.complete
    assert obs[f"dep-org/linked-{suffix}"].resolved_repo_id == tracked_id
    assert obs[f"dep-org/linked-{suffix}"].last_seen_run_id == runs[1].id
    assert any("ambiguous dependent identity" in record.message for record in caplog.records)


async def test_github_precommit_failure_rolls_back_and_reconciles(
    db_engine: Any,
    db_session_factory: async_sessionmaker[AsyncSession],
    github_dependents_page1_html: str,
    github_dependents_page2_html: str,
) -> None:
    """A rejected application commit rolls back observations; reconciliation marks the run failed."""

    suffix = _unique_suffix()
    package_name, repo_id = await _gh_seed_source(db_session_factory, suffix)

    # Commit #1 = run row, commit #2 = snapshot application.
    factory = _commit_intercept_factory(db_engine, fail_on=2, commit_first=False)
    crawler = _gh_crawler(factory, [github_dependents_page1_html, github_dependents_page2_html])

    result = await crawler.crawl_and_persist([package_name])
    assert result.packages_processed == 0
    assert len(result.errors) == 1 and "simulated commit failure" in result.errors[0]

    runs, obs = await _gh_state(db_session_factory, repo_id)
    assert len(runs) == 1
    failed = runs[0]
    assert failed.status == GithubDependentsRunStatus.failed
    assert failed.error_detail is not None and "commit rejected" in failed.error_detail
    assert len(failed.error_detail) <= 4096
    assert failed.finished_at is not None
    assert obs == {}


async def test_github_lost_commit_ack_preserves_applied_run(
    db_engine: Any,
    db_session_factory: async_sessionmaker[AsyncSession],
    github_dependents_page1_html: str,
    github_dependents_page2_html: str,
) -> None:
    """A durable commit whose acknowledgement is lost stays applied and is not marked failed."""

    suffix = _unique_suffix()
    package_name, repo_id = await _gh_seed_source(db_session_factory, suffix)

    # The application commit reaches the server; the acknowledgement error
    # surfaces afterwards.
    factory = _commit_intercept_factory(db_engine, fail_on=2, commit_first=True)
    crawler = _gh_crawler(factory, [github_dependents_page1_html, github_dependents_page2_html])

    result = await crawler.crawl_and_persist([package_name])
    # The crawl is treated as processed: no false failure, no duplicate retry.
    assert result.packages_processed == 1
    assert result.errors == []

    runs, obs = await _gh_state(db_session_factory, repo_id)
    assert len(runs) == 1
    applied = runs[0]
    assert applied.status == GithubDependentsRunStatus.complete
    assert applied.error_detail is None
    assert len(obs) == 7
    assert all(o.last_seen_run_id == applied.id and o.retired_at is None for o in obs.values())
