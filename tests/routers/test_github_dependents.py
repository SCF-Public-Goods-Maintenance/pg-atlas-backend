"""
Tests for the /repos/{canonical_id}/github-dependents endpoint over ASGI.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from pg_atlas.crawlers.base import CrawlResult
from pg_atlas.crawlers.github_dependents import GitHubDependentsCrawler
from pg_atlas.db_models.github_dependents_observation import GithubDependentsCrawlRun
from tests.conftest import get_test_database_url
from tests.crawlers.conftest import gh_entry_row, gh_page, gh_response

_DRIFTED_PAGE = "<html><body><p>layout drifted: no count markers</p></body></html>"

# ---------------------------------------------------------------------------
# No-DB tests
# ---------------------------------------------------------------------------


async def test_github_dependents_db_unavailable_returns_503(no_db_client: AsyncClient) -> None:
    """The endpoint returns 503 when no database is configured."""

    resp = await no_db_client.get("/repos/pkg:github/test/repo/github-dependents")
    assert resp.status_code == 503


# ---------------------------------------------------------------------------
# DB integration tests
# ---------------------------------------------------------------------------


async def _run_crawler(package_name: str, pages: list[str] | str) -> CrawlResult:
    """Run the real crawler against the test database with mocked pages."""

    database_url = get_test_database_url()
    assert database_url is not None
    engine = create_async_engine(database_url, poolclass=NullPool)
    try:
        session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        http_client = AsyncMock()
        if isinstance(pages, str):
            http_client.get = AsyncMock(return_value=gh_response(pages))
        else:
            http_client.get = AsyncMock(side_effect=[gh_response(p) for p in pages])
        crawler = GitHubDependentsCrawler(client=http_client, session_factory=session_factory, rate_limit=0.0)

        return await crawler.crawl_and_persist([package_name])
    finally:
        await engine.dispose()


async def _insert_running_run(source_repo_id: int) -> None:
    """Insert a bare running run row, as an abandoned attempt would leave behind."""

    database_url = get_test_database_url()
    assert database_url is not None
    engine = create_async_engine(database_url, poolclass=NullPool)
    try:
        session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with session_factory() as session:
            session.add(GithubDependentsCrawlRun(source_repo_id=source_repo_id))
            await session.commit()
    finally:
        await engine.dispose()


def _source(seed: dict[str, Any]) -> tuple[str, str]:
    """Return (canonical_id, crawler package_name) for the seeded source repo."""

    canonical_id: str = seed["repo_a1"].canonical_id

    return canonical_id, canonical_id.removeprefix("pkg:github/")


async def test_github_dependents_unknown_repo_returns_404(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """An unknown canonical id returns 404."""

    client, _ = seeded_client
    resp = await client.get("/repos/pkg:github/nobody/nothing/github-dependents")
    assert resp.status_code == 404


async def test_github_dependents_without_runs_returns_empty_summary(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """A tracked repo that was never crawled yields an empty summary and no items."""

    client, seed = seeded_client
    resp = await client.get(f"/repos/{seed['repo_a1'].canonical_id}/github-dependents")
    assert resp.status_code == 200

    data = resp.json()
    assert data["summary"]["latest_attempt_status"] is None
    assert data["summary"]["observations_as_of"] is None
    assert data["summary"]["reported_counts_as_of"] is None
    assert data["summary"]["active_observations"] == 0
    assert data["observations"]["items"] == []
    assert data["observations"]["total"] == 0


async def test_github_dependents_pagination_and_resolution(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """The endpoint serves the exact key-ordered slice and resolves tracked dependents."""

    client, seed = seeded_client
    source_canonical_id, package_name = _source(seed)
    tracked_owner, tracked_name = seed["repo_a2"].canonical_id.removeprefix("pkg:github/").split("/")

    rows = (
        gh_entry_row(tracked_owner, tracked_name)
        + gh_entry_row("zeta", "app1")
        + gh_entry_row("beta-app", "wallet")
        + gh_entry_row("gamma", "svc")
        + gh_entry_row("delta", "tool")
    )
    result = await _run_crawler(package_name, gh_page(rows))
    assert result.packages_processed == 1

    resp = await client.get(f"/repos/{source_canonical_id}/github-dependents")
    assert resp.status_code == 200
    data = resp.json()
    assert data["summary"]["latest_attempt_status"] == "complete"
    assert data["summary"]["repos_total_reported"] == 87
    assert data["summary"]["active_observations"] == 5
    assert data["summary"]["observations_as_of"] is not None
    assert data["summary"]["observations_as_of"] == data["summary"]["reported_counts_as_of"]

    by_id = {item["dependent_canonical_id"]: item for item in data["observations"]["items"]}
    tracked_item = by_id[f"pkg:github/{tracked_owner}/{tracked_name}".lower()]
    assert tracked_item["resolved_repo_canonical_id"] == seed["repo_a2"].canonical_id
    assert all(
        item["resolved_repo_canonical_id"] is None for item in data["observations"]["items"] if item is not tracked_item
    )

    # Exact slice: keys are served in dependent_key order.
    all_keys = sorted(
        f"{owner}/{name}".lower()
        for owner, name in [
            (tracked_owner, tracked_name),
            ("zeta", "app1"),
            ("beta-app", "wallet"),
            ("gamma", "svc"),
            ("delta", "tool"),
        ]
    )
    resp = await client.get(
        f"/repos/{source_canonical_id}/github-dependents",
        params={"limit": 2, "offset": 2},
    )
    assert resp.status_code == 200
    page = resp.json()
    assert page["observations"]["total"] == 5
    assert page["observations"]["limit"] == 2 and page["observations"]["offset"] == 2
    sliced = [item["dependent_canonical_id"] for item in page["observations"]["items"]]
    assert sliced == [f"pkg:github/{key}" for key in all_keys[2:4]]


async def test_github_dependents_failed_attempt_does_not_freshen(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """A later failed attempt surfaces as latest status but shifts neither freshness axis."""

    client, seed = seeded_client
    source_canonical_id, package_name = _source(seed)

    result = await _run_crawler(package_name, gh_page(gh_entry_row("zeta", "app1")))
    assert result.packages_processed == 1
    baseline = (await client.get(f"/repos/{source_canonical_id}/github-dependents")).json()["summary"]
    assert baseline["latest_attempt_status"] == "complete"
    assert baseline["observations_as_of"] is not None

    failed_result = await _run_crawler(package_name, _DRIFTED_PAGE)
    assert failed_result.packages_processed == 0

    summary = (await client.get(f"/repos/{source_canonical_id}/github-dependents")).json()["summary"]
    assert summary["latest_attempt_status"] == "failed"
    assert summary["observations_as_of"] == baseline["observations_as_of"]
    assert summary["reported_counts_as_of"] == baseline["reported_counts_as_of"]
    assert summary["active_observations"] == 1


async def test_github_dependents_stale_running_attempt_does_not_freshen(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """A stale running attempt is visible as the latest attempt but freshens neither axis."""

    client, seed = seeded_client
    source_canonical_id, package_name = _source(seed)

    result = await _run_crawler(package_name, gh_page(gh_entry_row("zeta", "app1")))
    assert result.packages_processed == 1
    baseline = (await client.get(f"/repos/{source_canonical_id}/github-dependents")).json()["summary"]

    await _insert_running_run(seed["repo_a1"].id)

    summary = (await client.get(f"/repos/{source_canonical_id}/github-dependents")).json()["summary"]
    assert summary["latest_attempt_status"] == "running"
    assert summary["latest_attempt_at"] is not None
    assert summary["observations_as_of"] == baseline["observations_as_of"]
    assert summary["reported_counts_as_of"] == baseline["reported_counts_as_of"]


async def test_github_dependents_retired_rows_are_unreachable(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """Retired observations are not served, and no include_retired parameter exists in the API."""

    client, seed = seeded_client
    source_canonical_id, package_name = _source(seed)

    full = gh_page(gh_entry_row("zeta", "app1") + gh_entry_row("gamma", "svc") + gh_entry_row("delta", "tool"))
    assert (await _run_crawler(package_name, full)).packages_processed == 1
    shrunken = gh_page(gh_entry_row("zeta", "app1"))
    assert (await _run_crawler(package_name, shrunken)).packages_processed == 1

    resp = await client.get(
        f"/repos/{source_canonical_id}/github-dependents",
        params={"include_retired": "true", "limit": 50},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["observations"]["total"] == 1
    served = {item["dependent_canonical_id"] for item in data["observations"]["items"]}
    assert served == {"pkg:github/zeta/app1"}

    # The public API contract has no retired-access parameter at all.
    openapi = (await client.get("/openapi.json")).json()
    parameters = openapi["paths"]["/repos/{canonical_id}/github-dependents"]["get"].get("parameters", [])
    parameter_names = {parameter["name"] for parameter in parameters}
    assert "include_retired" not in parameter_names
    assert {"limit", "offset"} <= parameter_names
