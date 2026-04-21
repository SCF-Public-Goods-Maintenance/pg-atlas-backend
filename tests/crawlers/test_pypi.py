"""
Tests for the PyPI registry crawler.

Unit tests use mocked HTTP responses and monkeypatched PyPIStats calls.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import httpx

from pg_atlas.crawlers.pypi import PypiCrawler


def _response(
    data: dict[str, Any],
    status_code: int = 200,
    url: str = "https://pypi.org/pypi",
) -> httpx.Response:
    """Create a mock PyPI JSON API response."""

    return httpx.Response(
        status_code=status_code,
        json=data,
        request=httpx.Request("GET", url),
    )


def _make_crawler(client: AsyncMock) -> PypiCrawler:
    """Create a PyPI crawler backed by mocked HTTP and DB clients."""

    session_factory = AsyncMock()
    return PypiCrawler(
        client=client,
        session_factory=session_factory,
        rate_limit=0.0,
        max_retries=3,
    )


async def test_fetch_package_parses_metadata_dependencies_and_downloads(
    mock_http_client: AsyncMock,
    pypi_package_data: dict[str, Any],
    pypi_stats_recent_data: dict[str, Any],
) -> None:
    """PyPI JSON plus PyPIStats monthly totals become repo metadata, releases, and runtime deps."""

    mock_http_client.get = AsyncMock(
        side_effect=[
            _response(pypi_package_data, url="https://pypi.org/pypi/requests/json"),
            _response(
                pypi_stats_recent_data,
                url="https://pypistats.org/api/packages/requests/recent?period=month&mirrors=true",
            ),
        ]
    )
    crawler = _make_crawler(mock_http_client)
    pkg = await crawler.fetch_package("requests")

    assert pkg.canonical_id == "pkg:pypi/requests"
    assert pkg.display_name == "requests"
    assert pkg.latest_version == "2.33.1"
    assert pkg.repo_url == "https://github.com/psf/requests"
    assert pkg.downloads_30d == 350
    assert pkg.metadata["download_count_30d"] == 350
    assert pkg.metadata["download_source"] == "pypistats"
    assert pkg.metadata["download_period"] == "month"
    assert [(release.version, release.release_date) for release in pkg.releases] == [
        ("2.33.1", "2026-03-30T16:09:13.830020Z"),
        ("2.33.0", "2026-03-01T10:00:00.000000Z"),
    ]

    dep_ids = {dep.canonical_id for dep in pkg.dependencies}
    assert dep_ids == {
        "pkg:pypi/charset-normalizer",
        "pkg:pypi/idna",
        "pkg:pypi/colorama",
    }
    assert all(dep.canonical_id != "pkg:pypi/pysocks" for dep in pkg.dependencies)


async def test_fetch_package_handles_stats_failure(
    mock_http_client: AsyncMock,
    pypi_package_data: dict[str, Any],
) -> None:
    """A PyPIStats failure leaves the package crawl usable with metadata-only fields."""

    mock_http_client.get = AsyncMock(
        side_effect=[
            _response(pypi_package_data, url="https://pypi.org/pypi/requests/json"),
            httpx.TimeoutException("boom"),
            httpx.TimeoutException("boom"),
            httpx.TimeoutException("boom"),
        ]
    )
    crawler = _make_crawler(mock_http_client)
    pkg = await crawler.fetch_package("requests")

    assert pkg.canonical_id == "pkg:pypi/requests"
    assert pkg.downloads_30d is None
    assert "download_count_30d" not in pkg.metadata


async def test_fetch_dependents_is_explicit_stub(mock_http_client: AsyncMock) -> None:
    """PyPI dependents are intentionally stubbed until there is a practical first-party API."""

    crawler = _make_crawler(mock_http_client)
    dependents = await crawler.fetch_dependents("requests")

    assert dependents == []
    mock_http_client.get.assert_not_called()
