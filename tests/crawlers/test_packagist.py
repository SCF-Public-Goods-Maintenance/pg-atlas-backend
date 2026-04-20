"""
Tests for the Packagist registry crawler.

Unit tests use mocked HTTP responses — no network or database required.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import httpx

from pg_atlas.crawlers.packagist import PackagistCrawler


def _response(
    data: dict[str, Any] | None = None,
    status_code: int = 200,
    content_type: str = "application/json",
    text: str = "",
) -> httpx.Response:
    """Create a mock httpx.Response."""
    if data is not None:
        return httpx.Response(
            status_code=status_code,
            json=data,
            request=httpx.Request("GET", "https://packagist.org"),
        )
    return httpx.Response(
        status_code=status_code,
        text=text,
        headers={"Content-Type": content_type},
        request=httpx.Request("GET", "https://packagist.org"),
    )


def _make_crawler(client: AsyncMock) -> PackagistCrawler:
    """Create a PackagistCrawler with a mocked HTTP client and dummy session factory."""
    session_factory = AsyncMock()
    return PackagistCrawler(
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
    packagist_package_data: dict[str, Any],
    packagist_downloads_data: dict[str, Any],
) -> None:
    """Correct name, version, and repo URL are extracted."""
    mock_http_client.get = AsyncMock(side_effect=[_response(packagist_package_data), _response(packagist_downloads_data)])
    crawler = _make_crawler(mock_http_client)
    pkg = await crawler.fetch_package("soneso/stellar-php-sdk")

    assert pkg.canonical_id == "pkg:composer/soneso/stellar-php-sdk"
    assert pkg.display_name == "soneso/stellar-php-sdk"
    assert pkg.latest_version == "1.9.4"
    assert pkg.repo_url == "https://github.com/Soneso/stellar-php-sdk.git"
    assert pkg.releases
    assert {release.version for release in pkg.releases} >= {"1.9.4", "1.8.0", "dev-main"}
    assert all(release.purl == "pkg:composer/soneso/stellar-php-sdk" for release in pkg.releases)


async def test_fetch_package_parses_dependencies(
    mock_http_client: AsyncMock,
    packagist_package_data: dict[str, Any],
    packagist_downloads_data: dict[str, Any],
) -> None:
    """require dict parsed; php, ext-* filtered; canonical IDs correct."""
    mock_http_client.get = AsyncMock(side_effect=[_response(packagist_package_data), _response(packagist_downloads_data)])
    crawler = _make_crawler(mock_http_client)
    pkg = await crawler.fetch_package("soneso/stellar-php-sdk")

    dep_ids = {d.canonical_id for d in pkg.dependencies}
    dep_names = {d.display_name for d in pkg.dependencies}

    assert dep_ids == {
        "pkg:composer/guzzlehttp/guzzle",
        "pkg:composer/phpseclib/phpseclib",
        "pkg:composer/soneso/stellar-xdr",
    }
    assert "php" not in dep_names
    assert not any(n.startswith("ext-") for n in dep_names)
    for dep in pkg.dependencies:
        assert dep.canonical_id.startswith("pkg:composer/")


async def test_fetch_package_parses_stats(
    mock_http_client: AsyncMock,
    packagist_package_data: dict[str, Any],
    packagist_downloads_data: dict[str, Any],
) -> None:
    """Downloads (last 30 days), daily, and total are extracted; stars reserved for GitHub."""
    mock_http_client.get = AsyncMock(side_effect=[_response(packagist_package_data), _response(packagist_downloads_data)])
    crawler = _make_crawler(mock_http_client)
    pkg = await crawler.fetch_package("soneso/stellar-php-sdk")

    assert pkg.downloads_30d == 1970  # monthly (last 30 days per spec)
    assert pkg.stars is None  # reserved for GitHub
    assert pkg.metadata["downloads_total"] == 46925
    assert pkg.metadata["download_count_30d"] == 1970
    assert pkg.metadata["downloads_daily"] == 68


async def test_fetch_package_handles_missing_downloads(
    mock_http_client: AsyncMock,
    packagist_package_data: dict[str, Any],
) -> None:
    """Graceful handling when downloads API returns an error."""
    mock_http_client.get = AsyncMock(
        side_effect=[
            _response(packagist_package_data),
            _response(status_code=404, text="Not Found", content_type="text/html"),
        ]
    )
    crawler = _make_crawler(mock_http_client)
    pkg = await crawler.fetch_package("soneso/stellar-php-sdk")

    assert pkg.canonical_id == "pkg:composer/soneso/stellar-php-sdk"
    assert pkg.downloads_30d is None


async def test_fetch_package_handles_malformed_downloads(
    mock_http_client: AsyncMock,
    packagist_package_data: dict[str, Any],
) -> None:
    """Malformed downloads response (missing nested keys) produces None downloads."""
    mock_http_client.get = AsyncMock(side_effect=[_response(packagist_package_data), _response({"package": {}})])
    crawler = _make_crawler(mock_http_client)
    pkg = await crawler.fetch_package("soneso/stellar-php-sdk")

    assert pkg.canonical_id == "pkg:composer/soneso/stellar-php-sdk"
    assert pkg.downloads_30d is None
    assert "download_count_30d" not in pkg.metadata
    assert "downloads_daily" not in pkg.metadata


# ---------------------------------------------------------------------------
# fetch_dependents tests
# ---------------------------------------------------------------------------


async def test_fetch_dependents_returns_list(
    mock_http_client: AsyncMock,
    packagist_dependents_data: dict[str, Any],
) -> None:
    """Dependent packages are correctly extracted."""
    mock_http_client.get = AsyncMock(return_value=_response(packagist_dependents_data))
    crawler = _make_crawler(mock_http_client)
    dependents = await crawler.fetch_dependents("soneso/stellar-php-sdk")

    assert len(dependents) == 2
    ids = {d.canonical_id for d in dependents}
    assert "pkg:composer/example/anchor-client" in ids
    assert "pkg:composer/example/wallet-sdk" in ids


async def test_fetch_dependents_empty(
    mock_http_client: AsyncMock,
    packagist_dependents_empty_data: dict[str, Any],
) -> None:
    """Empty list when no dependents."""
    mock_http_client.get = AsyncMock(return_value=_response(packagist_dependents_empty_data))
    crawler = _make_crawler(mock_http_client)
    dependents = await crawler.fetch_dependents("soneso/stellar-php-sdk")

    assert dependents == []


async def test_fetch_dependents_html_fallback(
    mock_http_client: AsyncMock,
) -> None:
    """Returns empty list when response has Content-Type text/html instead of JSON."""
    mock_http_client.get = AsyncMock(return_value=_response(text="<html>...</html>", content_type="text/html"))
    crawler = _make_crawler(mock_http_client)
    dependents = await crawler.fetch_dependents("soneso/stellar-php-sdk")

    assert dependents == []


async def test_fetch_dependents_timeout(
    mock_http_client: AsyncMock,
) -> None:
    """Timeout on dependents endpoint returns empty list."""
    mock_http_client.get = AsyncMock(
        side_effect=[
            httpx.TimeoutException("timeout"),
            httpx.TimeoutException("timeout"),
            httpx.TimeoutException("timeout"),
        ]
    )
    crawler = _make_crawler(mock_http_client)
    dependents = await crawler.fetch_dependents("soneso/stellar-php-sdk")

    assert dependents == []


# ---------------------------------------------------------------------------
# Version selection
# ---------------------------------------------------------------------------


async def test_latest_stable_version_selected(
    mock_http_client: AsyncMock,
    packagist_package_data: dict[str, Any],
    packagist_downloads_data: dict[str, Any],
) -> None:
    """dev-* branches are skipped; highest stable version is picked."""
    mock_http_client.get = AsyncMock(side_effect=[_response(packagist_package_data), _response(packagist_downloads_data)])
    crawler = _make_crawler(mock_http_client)
    pkg = await crawler.fetch_package("soneso/stellar-php-sdk")

    assert pkg.latest_version == "1.9.4"


async def test_dev_only_package(
    mock_http_client: AsyncMock,
    packagist_package_dev_only_data: dict[str, Any],
    packagist_downloads_data: dict[str, Any],
) -> None:
    """When no stable versions exist, use dev-main if available."""
    mock_http_client.get = AsyncMock(
        side_effect=[_response(packagist_package_dev_only_data), _response(packagist_downloads_data)]
    )
    crawler = _make_crawler(mock_http_client)
    pkg = await crawler.fetch_package("example/dev-only-pkg")

    assert pkg.latest_version == "dev-main"


async def test_dev_suffix_version_skipped(
    mock_http_client: AsyncMock,
    packagist_downloads_data: dict[str, Any],
) -> None:
    """Versions ending with -dev are treated as dev branches."""
    package_data: dict[str, Any] = {
        "package": {
            "name": "test/dev-suffix",
            "versions": {
                "1.0.0-dev": {"name": "test/dev-suffix", "version": "1.0.0-dev", "require": {}},
                "0.9.0": {"name": "test/dev-suffix", "version": "0.9.0", "require": {}},
            },
        }
    }
    mock_http_client.get = AsyncMock(side_effect=[_response(package_data), _response(packagist_downloads_data)])
    crawler = _make_crawler(mock_http_client)
    pkg = await crawler.fetch_package("test/dev-suffix")

    assert pkg.latest_version == "0.9.0"


async def test_dev_only_fallback_first_branch(
    mock_http_client: AsyncMock,
    packagist_downloads_data: dict[str, Any],
) -> None:
    """When only non-main dev branches exist, first one is used."""
    package_data: dict[str, Any] = {
        "package": {
            "name": "test/dev-only",
            "versions": {
                "dev-feature": {"name": "test/dev-only", "version": "dev-feature", "require": {}},
            },
        }
    }
    mock_http_client.get = AsyncMock(side_effect=[_response(package_data), _response(packagist_downloads_data)])
    crawler = _make_crawler(mock_http_client)
    pkg = await crawler.fetch_package("test/dev-only")

    assert pkg.latest_version == "dev-feature"


async def test_empty_versions_dict(
    mock_http_client: AsyncMock,
    packagist_downloads_data: dict[str, Any],
) -> None:
    """Package with no versions at all returns empty latest_version."""
    package_data: dict[str, Any] = {
        "package": {
            "name": "test/empty",
            "versions": {},
        }
    }
    mock_http_client.get = AsyncMock(side_effect=[_response(package_data), _response(packagist_downloads_data)])
    crawler = _make_crawler(mock_http_client)
    pkg = await crawler.fetch_package("test/empty")

    assert pkg.latest_version == ""


async def test_version_key_non_numeric_segment(
    mock_http_client: AsyncMock,
    packagist_downloads_data: dict[str, Any],
) -> None:
    """Version strings with non-numeric segments are handled gracefully."""
    package_data: dict[str, Any] = {
        "package": {
            "name": "test/alpha-ver",
            "versions": {
                "1.0.0-beta1": {"name": "test/alpha-ver", "version": "1.0.0-beta1", "require": {}},
                "0.9.0": {"name": "test/alpha-ver", "version": "0.9.0", "require": {}},
            },
        }
    }
    mock_http_client.get = AsyncMock(side_effect=[_response(package_data), _response(packagist_downloads_data)])
    crawler = _make_crawler(mock_http_client)
    pkg = await crawler.fetch_package("test/alpha-ver")

    # "1.0.0-beta1" → (1, 0, 0) after splitting on "." and int("0-beta1") → 0
    # Actually splits to ["1", "0", "0-beta1"] where "0-beta1" → ValueError → 0
    # So (1, 0, 0) vs (0, 9, 0) → picks "1.0.0-beta1"
    assert pkg.latest_version == "1.0.0-beta1"


async def test_version_sorting_semver(
    mock_http_client: AsyncMock,
    packagist_downloads_data: dict[str, Any],
) -> None:
    """Verify 1.10.0 > 1.9.4 (not string sort)."""
    package_data: dict[str, Any] = {
        "package": {
            "name": "test/semver-pkg",
            "favers": 0,
            "versions": {
                "1.9.4": {
                    "name": "test/semver-pkg",
                    "version": "1.9.4",
                    "require": {},
                },
                "1.10.0": {
                    "name": "test/semver-pkg",
                    "version": "1.10.0",
                    "require": {},
                },
                "1.2.0": {
                    "name": "test/semver-pkg",
                    "version": "1.2.0",
                    "require": {},
                },
            },
        }
    }
    mock_http_client.get = AsyncMock(side_effect=[_response(package_data), _response(packagist_downloads_data)])
    crawler = _make_crawler(mock_http_client)
    pkg = await crawler.fetch_package("test/semver-pkg")

    assert pkg.latest_version == "1.10.0"
