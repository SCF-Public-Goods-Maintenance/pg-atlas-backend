"""
Live API integration tests for registry crawlers.

Validates that real API response structures match our parsing expectations.
Skipped by default — enable with ``PG_ATLAS_TEST_LIVE_APIS=1``.

These tests are the early warning system for API changes in package registry APIs.
They make real HTTP requests and do NOT write to any database.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pg_atlas.crawlers.base import USER_AGENT
from pg_atlas.crawlers.cargo import CargoCrawler
from pg_atlas.crawlers.github_dependents import (
    GitHubDependentsCrawler,
    _dependents_url,
    find_next_cursor,
    parse_dependent_counts,
    parse_dependent_entries,
    parse_package_selector,
)
from pg_atlas.crawlers.npm import NpmCrawler
from pg_atlas.crawlers.packagist import PackagistCrawler
from pg_atlas.crawlers.pubdev import PubDevCrawler
from pg_atlas.crawlers.pypi import PyPICrawler

pytestmark = pytest.mark.skipif(
    not os.environ.get("PG_ATLAS_TEST_LIVE_APIS"),
    reason="Set PG_ATLAS_TEST_LIVE_APIS=1 to run live API tests",
)


@pytest.fixture
async def live_client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Real httpx client for live API calls."""
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(30.0, connect=10.0),
        follow_redirects=True,
        headers={"User-Agent": USER_AGENT},
    ) as client:
        yield client


def _dummy_session_factory() -> async_sessionmaker[AsyncSession]:
    return AsyncMock(spec=async_sessionmaker)


# ---------------------------------------------------------------------------
# pub.dev live tests
# ---------------------------------------------------------------------------


async def test_pubdev_live_fetch(live_client: httpx.AsyncClient) -> None:
    """Fetch stellar_flutter_sdk from real pub.dev and validate structure."""
    crawler = PubDevCrawler(client=live_client, session_factory=_dummy_session_factory(), rate_limit=0.0)
    pkg = await crawler.fetch_package("stellar_flutter_sdk")

    assert pkg.canonical_id == "pkg:pub/stellar_flutter_sdk"
    assert pkg.display_name == "stellar_flutter_sdk"
    assert pkg.latest_version  # non-empty
    assert isinstance(pkg.dependencies, list)


async def test_pubdev_live_metrics(live_client: httpx.AsyncClient) -> None:
    """Fetch metrics for stellar_flutter_sdk — weekly sums should be ints."""
    crawler = PubDevCrawler(client=live_client, session_factory=_dummy_session_factory(), rate_limit=0.0)
    pkg = await crawler.fetch_package("stellar_flutter_sdk")

    if pkg.downloads_30d is not None:
        assert isinstance(pkg.downloads_30d, int)
        assert pkg.downloads_30d >= 0

    # Weekly aggregations should be present from scorecard
    assert isinstance(pkg.metadata.get("download_count_4w"), int)
    assert isinstance(pkg.metadata.get("download_count_12w"), int)
    assert isinstance(pkg.metadata.get("download_count_52w"), int)
    assert isinstance(pkg.metadata.get("download_count_30d"), int)


async def test_pubdev_live_dependents(live_client: httpx.AsyncClient) -> None:
    """Fetch dependents for stellar_flutter_sdk — should return a list."""
    crawler = PubDevCrawler(client=live_client, session_factory=_dummy_session_factory(), rate_limit=0.0)
    dependents = await crawler.fetch_dependents("stellar_flutter_sdk")

    assert isinstance(dependents, list)
    for dep in dependents:
        assert dep.canonical_id.startswith("pkg:pub/")


# ---------------------------------------------------------------------------
# Packagist live tests
# ---------------------------------------------------------------------------


async def test_packagist_live_fetch(live_client: httpx.AsyncClient) -> None:
    """Fetch soneso/stellar-php-sdk from real Packagist and validate structure."""
    crawler = PackagistCrawler(client=live_client, session_factory=_dummy_session_factory(), rate_limit=0.0)
    pkg = await crawler.fetch_package("soneso/stellar-php-sdk")

    assert pkg.canonical_id == "pkg:composer/soneso/stellar-php-sdk"
    assert pkg.display_name == "soneso/stellar-php-sdk"
    assert pkg.latest_version  # non-empty
    assert isinstance(pkg.dependencies, list)


async def test_packagist_live_downloads(live_client: httpx.AsyncClient) -> None:
    """Fetch downloads for soneso/stellar-php-sdk — downloads should be ints."""
    crawler = PackagistCrawler(client=live_client, session_factory=_dummy_session_factory(), rate_limit=0.0)
    pkg = await crawler.fetch_package("soneso/stellar-php-sdk")

    # downloads may be None if downloads endpoint fails, but should be int if present
    if pkg.downloads_30d is not None:
        assert isinstance(pkg.downloads_30d, int)
        assert pkg.downloads_30d >= 0


async def test_packagist_live_dependents(live_client: httpx.AsyncClient) -> None:
    """Fetch dependents for soneso/stellar-php-sdk — should return a list."""
    crawler = PackagistCrawler(client=live_client, session_factory=_dummy_session_factory(), rate_limit=0.0)
    dependents = await crawler.fetch_dependents("soneso/stellar-php-sdk")

    assert isinstance(dependents, list)
    for dep in dependents:
        assert dep.canonical_id.startswith("pkg:composer/")


# ---------------------------------------------------------------------------
# npm / crates.io / PyPI live tests
# ---------------------------------------------------------------------------


async def test_npm_live_fetch(live_client: httpx.AsyncClient) -> None:
    """Fetch lodash from the live npm APIs and validate the parsed structure."""

    crawler = NpmCrawler(client=live_client, session_factory=_dummy_session_factory(), rate_limit=0.0)
    pkg = await crawler.fetch_package("lodash")

    assert pkg.canonical_id == "pkg:npm/lodash"
    assert pkg.display_name == "lodash"
    assert pkg.latest_version
    assert isinstance(pkg.dependencies, list)


async def test_cargo_live_fetch(live_client: httpx.AsyncClient) -> None:
    """Fetch serde from the live crates.io APIs and validate the parsed structure."""

    crawler = CargoCrawler(client=live_client, session_factory=_dummy_session_factory(), rate_limit=0.0)
    pkg = await crawler.fetch_package("serde")

    assert pkg.canonical_id == "pkg:cargo/serde"
    assert pkg.display_name == "serde"
    assert pkg.latest_version
    assert isinstance(pkg.dependencies, list)


async def test_pypi_live_fetch(live_client: httpx.AsyncClient) -> None:
    """Fetch requests from live PyPI and PyPIStats and validate the parsed structure."""

    crawler = PyPICrawler(client=live_client, session_factory=_dummy_session_factory(), rate_limit=0.0)
    pkg = await crawler.fetch_package("requests")

    assert pkg.canonical_id == "pkg:pypi/requests"
    assert pkg.display_name == "requests"
    assert pkg.latest_version
    assert isinstance(pkg.dependencies, list)


# ---------------------------------------------------------------------------
# GitHub dependents live tests
# ---------------------------------------------------------------------------


async def test_github_dependents_live_markers(live_client: httpx.AsyncClient) -> None:
    """
    Fetch one real dependents page and assert every marker class still matches.

    This is the early-warning system for GitHub dependents layout drift: counts
    header, dependent entry link, and the pagination cursor.
    """

    url = "https://github.com/Soneso/stellar_flutter_sdk/network/dependents?dependent_type=REPOSITORY"
    resp = await live_client.get(url)
    assert resp.status_code == 200
    html = resp.text

    repos_total, packages_total = parse_dependent_counts(html)
    assert repos_total is not None
    assert packages_total is not None

    entries = parse_dependent_entries(html)
    assert entries
    first = entries[0]
    assert first.owner
    assert first.repo

    # Assumes the listing spans more than one page (dozens of dependents);
    # a single-page listing would fail here and needs a bigger probe repo.
    assert find_next_cursor(html)


async def test_github_dependents_live_fetch_package(live_client: httpx.AsyncClient) -> None:
    """fetch_package returns a repo-shaped package with integer header totals."""

    crawler = GitHubDependentsCrawler(client=live_client, session_factory=_dummy_session_factory(), rate_limit=0.0)
    pkg = await crawler.fetch_package("Soneso/stellar_flutter_sdk")

    assert pkg.canonical_id == "pkg:github/Soneso/stellar_flutter_sdk"
    assert pkg.repo_url == "https://github.com/Soneso/stellar_flutter_sdk"
    assert pkg.dependencies == []
    assert isinstance(pkg.metadata["repos_total_reported"], int)
    assert isinstance(pkg.metadata["public_repos_observed"], int)


async def test_github_dependents_live_multi_package_selector(live_client: httpx.AsyncClient) -> None:
    """
    A multi-package repository renders a package selector, and a populated
    package-scoped page parses fully: counts with package_id-carrying header
    links, entry rows, and the pagination cursor.
    """

    resp = await live_client.get(_dependents_url("stellar", "js-stellar-sdk", "REPOSITORY"))
    assert resp.status_code == 200
    package_ids = parse_package_selector(resp.text)
    assert len(package_ids) > 1

    # Probe until a package with listed dependents is found — the default menu
    # entry is frequently a fork's ghost package with zero rows.
    populated_page: str | None = None
    populated_total = 0
    for package_id in package_ids[:6]:
        await asyncio.sleep(1)
        scoped = await live_client.get(_dependents_url("stellar", "js-stellar-sdk", "REPOSITORY", package_id=package_id))
        assert scoped.status_code == 200
        repos_total, packages_total = parse_dependent_counts(scoped.text)
        assert repos_total is not None
        assert packages_total is not None
        if parse_dependent_entries(scoped.text):
            populated_page = scoped.text
            populated_total = repos_total
            break

    assert populated_page is not None, "no populated package among the first probed ids"
    # A listing far larger than one page must expose a Next cursor.
    if populated_total > 30:
        assert find_next_cursor(populated_page)
