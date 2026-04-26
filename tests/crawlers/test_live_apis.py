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

import os
from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pg_atlas.crawlers.base import USER_AGENT
from pg_atlas.crawlers.cargo import CargoCrawler
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
