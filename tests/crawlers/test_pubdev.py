"""
Tests for the pub.dev registry crawler.

Unit tests use mocked HTTP responses — no network or database required.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import httpx

from pg_atlas.crawlers.pubdev import PubDevCrawler


def _response(data: dict[str, Any], status_code: int = 200) -> httpx.Response:
    """Create a mock httpx.Response with JSON content."""
    return httpx.Response(
        status_code=status_code,
        json=data,
        request=httpx.Request("GET", "https://pub.dev/api"),
    )


def _make_crawler(client: AsyncMock) -> PubDevCrawler:
    """Create a PubDevCrawler with a mocked HTTP client and dummy session factory."""
    session_factory = AsyncMock()
    return PubDevCrawler(
        client=client,
        session_factory=session_factory,
        rate_limit=0.0,
        max_retries=3,
    )


# ---------------------------------------------------------------------------
# fetch_package tests
# ---------------------------------------------------------------------------


async def test_fetch_package_parses_metadata(
    mock_http_client: AsyncMock,
    pubdev_package_data: dict[str, Any],
    pubdev_metrics_data: dict[str, Any],
) -> None:
    """Correct name, version, and URL are extracted from the API response."""
    mock_http_client.get = AsyncMock(side_effect=[_response(pubdev_package_data), _response(pubdev_metrics_data)])
    crawler = _make_crawler(mock_http_client)
    pkg = await crawler.fetch_package("stellar_flutter_sdk")

    assert pkg.canonical_id == "pkg:pub/stellar_flutter_sdk"
    assert pkg.display_name == "stellar_flutter_sdk"
    assert pkg.latest_version == "1.8.6"
    assert pkg.repo_url == "https://github.com/Soneso/stellar_flutter_sdk"
    assert pkg.releases
    assert {release.version for release in pkg.releases} >= {"1.8.6", "1.7.0"}
    assert all(release.purl == "pkg:pub/stellar_flutter_sdk" for release in pkg.releases)


async def test_fetch_package_parses_dependencies(
    mock_http_client: AsyncMock,
    pubdev_package_data: dict[str, Any],
    pubdev_metrics_data: dict[str, Any],
) -> None:
    """Runtime deps extracted, framework/dev deps excluded, canonical IDs lowercase."""
    mock_http_client.get = AsyncMock(side_effect=[_response(pubdev_package_data), _response(pubdev_metrics_data)])
    crawler = _make_crawler(mock_http_client)
    pkg = await crawler.fetch_package("stellar_flutter_sdk")

    dep_ids = {d.canonical_id for d in pkg.dependencies}
    dep_names = {d.display_name for d in pkg.dependencies}

    assert dep_ids == {
        "pkg:pub/http",
        "pkg:pub/typed_data",
        "pkg:pub/pointycastle",
        "pkg:pub/pinenacl",
    }
    # Framework packages filtered
    assert "flutter" not in dep_names
    assert "flutter_test" not in dep_names
    # Dev dependencies filtered
    assert "flutter_lints" not in dep_names
    # Canonical IDs are lowercase
    for dep in pkg.dependencies:
        assert dep.canonical_id.startswith("pkg:pub/")
        assert dep.canonical_id == dep.canonical_id.lower()


async def test_fetch_package_parses_scores_and_downloads(
    mock_http_client: AsyncMock,
    pubdev_package_data: dict[str, Any],
    pubdev_metrics_data: dict[str, Any],
) -> None:
    """Downloads, stars, pub points, and weekly sums are correctly extracted."""
    mock_http_client.get = AsyncMock(side_effect=[_response(pubdev_package_data), _response(pubdev_metrics_data)])
    crawler = _make_crawler(mock_http_client)
    pkg = await crawler.fetch_package("stellar_flutter_sdk")

    # adoption_downloads = last 30 days
    assert pkg.downloads_30d == 1469

    assert pkg.metadata["pub_points"] == 140
    assert pkg.metadata["pub_points_max"] == 160
    assert pkg.metadata["download_count_30d"] == 1469

    # Weekly sums computed from scorecard
    weekly = pubdev_metrics_data["scorecard"]["weeklyVersionDownloads"]["totalWeeklyDownloads"]
    assert pkg.metadata["download_count_4w"] == sum(weekly[:4])
    assert pkg.metadata["download_count_12w"] == sum(weekly[:12])
    assert pkg.metadata["download_count_52w"] == sum(weekly[:52])


async def test_fetch_package_no_scorecard(
    mock_http_client: AsyncMock,
    pubdev_package_data: dict[str, Any],
) -> None:
    """Missing scorecard in metrics does not crash; weekly sums are absent."""
    metrics_no_scorecard: dict[str, Any] = {
        "score": {"downloadCount30Days": 100, "grantedPoints": 120, "maxPoints": 160},
    }
    mock_http_client.get = AsyncMock(side_effect=[_response(pubdev_package_data), _response(metrics_no_scorecard)])
    crawler = _make_crawler(mock_http_client)
    pkg = await crawler.fetch_package("stellar_flutter_sdk")

    assert pkg.downloads_30d == 100  # downloadCount30Days from score
    assert "download_count_4w" not in pkg.metadata
    assert "download_count_12w" not in pkg.metadata
    assert "download_count_52w" not in pkg.metadata


async def test_fetch_package_short_weekly_list(
    mock_http_client: AsyncMock,
    pubdev_package_data: dict[str, Any],
) -> None:
    """Package with only 30 weeks of history uses all available data."""
    weekly_30 = [50 + i for i in range(30)]
    metrics_short: dict[str, Any] = {
        "score": {"downloadCount30Days": 200},
        "scorecard": {
            "weeklyVersionDownloads": {
                "totalWeeklyDownloads": weekly_30,
                "newestDate": "2026-03-03T00:00:00.000Z",
            }
        },
    }
    mock_http_client.get = AsyncMock(side_effect=[_response(pubdev_package_data), _response(metrics_short)])
    crawler = _make_crawler(mock_http_client)
    pkg = await crawler.fetch_package("stellar_flutter_sdk")

    assert pkg.downloads_30d == 200  # downloadCount30Days from score
    assert pkg.metadata["download_count_52w"] == sum(weekly_30)
    assert pkg.metadata["download_count_4w"] == sum(weekly_30[:4])
    assert pkg.metadata["download_count_12w"] == sum(weekly_30[:12])


async def test_fetch_package_metrics_failure(
    mock_http_client: AsyncMock,
    pubdev_package_data: dict[str, Any],
) -> None:
    """Metrics endpoint failure still returns package with deps, just no scores."""
    mock_http_client.get = AsyncMock(
        side_effect=[
            _response(pubdev_package_data),
            # _request_with_retry retries on timeout (max_retries=3)
            httpx.TimeoutException("timeout"),
            httpx.TimeoutException("timeout"),
            httpx.TimeoutException("timeout"),
        ]
    )
    crawler = _make_crawler(mock_http_client)
    pkg = await crawler.fetch_package("stellar_flutter_sdk")

    assert pkg.canonical_id == "pkg:pub/stellar_flutter_sdk"
    assert pkg.latest_version == "1.8.6"
    assert len(pkg.dependencies) > 0
    assert pkg.downloads_30d is None  # no metrics data available


async def test_fetch_package_handles_missing_fields(
    mock_http_client: AsyncMock,
    pubdev_package_minimal_data: dict[str, Any],
    pubdev_metrics_data: dict[str, Any],
) -> None:
    """Gracefully handles absent homepage, absent dependencies, absent score data."""
    mock_http_client.get = AsyncMock(side_effect=[_response(pubdev_package_minimal_data), _response(pubdev_metrics_data)])
    crawler = _make_crawler(mock_http_client)
    pkg = await crawler.fetch_package("tiny_package")

    assert pkg.canonical_id == "pkg:pub/tiny_package"
    assert pkg.repo_url is None
    assert pkg.dependencies == []


async def test_fetch_package_handles_null_dependencies(
    mock_http_client: AsyncMock,
) -> None:
    """Dependencies key explicitly set to null does not crash."""
    package_data: dict[str, Any] = {
        "name": "null_deps_pkg",
        "latest": {
            "version": "0.1.0",
            "pubspec": {"name": "null_deps_pkg", "dependencies": None},
        },
        "versions": [],
    }
    minimal_metrics: dict[str, Any] = {"score": {}, "scorecard": {}}
    mock_http_client.get = AsyncMock(side_effect=[_response(package_data), _response(minimal_metrics)])
    crawler = _make_crawler(mock_http_client)
    pkg = await crawler.fetch_package("null_deps_pkg")

    assert pkg.dependencies == []


async def test_fetch_package_filters_sdk_dependencies(
    mock_http_client: AsyncMock,
) -> None:
    """SDK dependencies (dict constraints like {"sdk": "flutter"}) are excluded."""
    package_data: dict[str, Any] = {
        "name": "sdk_deps_pkg",
        "latest": {
            "version": "1.0.0",
            "pubspec": {
                "name": "sdk_deps_pkg",
                "dependencies": {
                    "http": "^1.0.0",
                    "flutter_lints": {"sdk": "flutter"},
                    "real_dep": "^2.0.0",
                },
            },
        },
        "versions": [],
    }
    minimal_metrics: dict[str, Any] = {"score": {}, "scorecard": {}}
    mock_http_client.get = AsyncMock(side_effect=[_response(package_data), _response(minimal_metrics)])
    crawler = _make_crawler(mock_http_client)
    pkg = await crawler.fetch_package("sdk_deps_pkg")

    dep_names = {d.display_name for d in pkg.dependencies}
    assert "http" in dep_names
    assert "real_dep" in dep_names
    assert "flutter_lints" not in dep_names
    assert len(pkg.dependencies) == 2


# ---------------------------------------------------------------------------
# fetch_dependents tests
# ---------------------------------------------------------------------------


async def test_fetch_dependents_returns_list(
    mock_http_client: AsyncMock,
    pubdev_search_data: dict[str, Any],
) -> None:
    """Dependent packages are correctly extracted from search results."""
    mock_http_client.get = AsyncMock(return_value=_response(pubdev_search_data))
    crawler = _make_crawler(mock_http_client)
    dependents = await crawler.fetch_dependents("stellar_flutter_sdk")

    assert len(dependents) == 3
    ids = {d.canonical_id for d in dependents}
    assert "pkg:pub/stellar_wallet_flutter_sdk" in ids
    assert "pkg:pub/soroban_helper" in ids


async def test_fetch_dependents_empty(
    mock_http_client: AsyncMock,
    pubdev_search_empty_data: dict[str, Any],
) -> None:
    """Empty packages list returns empty list."""
    mock_http_client.get = AsyncMock(return_value=_response(pubdev_search_empty_data))
    crawler = _make_crawler(mock_http_client)
    dependents = await crawler.fetch_dependents("stellar_flutter_sdk")

    assert dependents == []


async def test_fetch_dependents_paginates(
    mock_http_client: AsyncMock,
) -> None:
    """Handles 'next' URL for pagination."""
    page1 = {
        "packages": [{"package": "dep1"}],
        "next": "https://pub.dev/api/search?q=dependency:pkg&page=2",
    }
    page2 = {
        "packages": [{"package": "dep2"}],
    }
    mock_http_client.get = AsyncMock(side_effect=[_response(page1), _response(page2)])
    crawler = _make_crawler(mock_http_client)
    dependents = await crawler.fetch_dependents("pkg")

    assert len(dependents) == 2
    assert dependents[0].canonical_id == "pkg:pub/dep1"
    assert dependents[1].canonical_id == "pkg:pub/dep2"


async def test_fetch_dependents_stops_paginating_at_limit(
    mock_http_client: AsyncMock,
) -> None:
    """Once max_dependents is reached, no further pages are fetched."""
    big_page = {
        "packages": [{"package": f"dep{i}"} for i in range(600)],
        "next": "https://pub.dev/api/search?q=dependency:popular_pkg&page=2",
    }
    mock_http_client.get = AsyncMock(return_value=_response(big_page))
    crawler = _make_crawler(mock_http_client)
    dependents = await crawler.fetch_dependents("popular_pkg")

    # All 600 from the single page are returned, but the "next" page is not fetched
    assert len(dependents) == 600
    assert mock_http_client.get.call_count == 1


async def test_fetch_package_handles_malformed_package_payload(
    mock_http_client: AsyncMock,
    pubdev_metrics_data: dict[str, Any],
    caplog: Any,
) -> None:
    """Malformed package payload logs warning and still returns a safe package result."""

    malformed_package_data: dict[str, Any] = {
        "name": ["stellar_flutter_sdk"],
        "latest": "invalid",
        "versions": "invalid",
    }
    mock_http_client.get = AsyncMock(side_effect=[_response(malformed_package_data), _response(pubdev_metrics_data)])
    crawler = _make_crawler(mock_http_client)

    with caplog.at_level("WARNING"):
        pkg = await crawler.fetch_package("stellar_flutter_sdk")

    assert "Failed to decode pub.dev package payload" in caplog.text
    assert pkg.canonical_id == "pkg:pub/"
    assert pkg.dependencies == []
    assert pkg.releases == []


async def test_fetch_package_converts_float_download_metrics(
    mock_http_client: AsyncMock,
    pubdev_package_data: dict[str, Any],
) -> None:
    """Float metrics are normalized to int at decode boundary."""

    metrics_with_floats: dict[str, Any] = {
        "score": {
            "downloadCount30Days": 1469.9,
            "grantedPoints": 140.0,
            "maxPoints": 160.0,
        },
        "scorecard": {
            "weeklyVersionDownloads": {
                "totalWeeklyDownloads": [10.2, 20.8, 30.0, 40.4],
            }
        },
    }
    mock_http_client.get = AsyncMock(side_effect=[_response(pubdev_package_data), _response(metrics_with_floats)])
    crawler = _make_crawler(mock_http_client)
    pkg = await crawler.fetch_package("stellar_flutter_sdk")

    assert pkg.downloads_30d == 1469
    assert pkg.metadata["download_count_4w"] == 100
    assert pkg.metadata["pub_points"] == 140
    assert pkg.metadata["pub_points_max"] == 160
