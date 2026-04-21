"""
Tests for the npm registry crawler.

Unit tests use mocked HTTP responses and do not write to the database.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import httpx

from pg_atlas.crawlers.npm import NpmCrawler


def _response(data: dict[str, Any], status_code: int = 200) -> httpx.Response:
    """Create a mock npm API response."""

    return httpx.Response(
        status_code=status_code,
        json=data,
        request=httpx.Request("GET", "https://registry.npmjs.org"),
    )


def _make_crawler(client: AsyncMock) -> NpmCrawler:
    """Create an npm crawler backed by mocked HTTP and DB clients."""

    session_factory = AsyncMock()
    return NpmCrawler(
        client=client,
        session_factory=session_factory,
        rate_limit=0.0,
        max_retries=3,
    )


async def test_fetch_package_parses_metadata_and_releases(
    mock_http_client: AsyncMock,
    npm_package_data: dict[str, Any],
    npm_downloads_data: dict[str, Any],
) -> None:
    """Scoped package metadata is normalized into a package PURL and release list."""

    mock_http_client.get = AsyncMock(side_effect=[_response(npm_package_data), _response(npm_downloads_data)])
    crawler = _make_crawler(mock_http_client)
    pkg = await crawler.fetch_package("@npmcli/arborist")

    assert pkg.canonical_id == "pkg:npm/%40npmcli/arborist"
    assert pkg.display_name == "@npmcli/arborist"
    assert pkg.latest_version == "9.4.2"
    assert pkg.repo_url == "https://github.com/npm/cli.git"
    assert pkg.releases
    assert [(release.version, release.release_date) for release in pkg.releases] == [
        ("9.4.2", "2026-03-18T21:20:35.937Z"),
        ("8.0.5", "2025-12-11T14:58:53.612566Z"),
    ]


async def test_fetch_package_parses_dependencies_and_downloads(
    mock_http_client: AsyncMock,
    npm_package_data: dict[str, Any],
    npm_downloads_data: dict[str, Any],
) -> None:
    """Runtime dependencies and last-month downloads are captured from npm APIs."""

    mock_http_client.get = AsyncMock(side_effect=[_response(npm_package_data), _response(npm_downloads_data)])
    crawler = _make_crawler(mock_http_client)
    pkg = await crawler.fetch_package("@npmcli/arborist")

    dep_ids = {dep.canonical_id for dep in pkg.dependencies}
    assert dep_ids == {
        "pkg:npm/nopt",
        "pkg:npm/semver",
        "pkg:npm/%40npmcli/fs",
    }
    assert pkg.downloads_30d == 17095128
    assert pkg.metadata["download_count_30d"] == 17095128
    assert pkg.metadata["downloads_start"] == "2026-03-21"
    assert pkg.metadata["downloads_end"] == "2026-04-19"


async def test_fetch_package_handles_missing_downloads(
    mock_http_client: AsyncMock,
    npm_package_data: dict[str, Any],
) -> None:
    """Download endpoint failures keep the package crawl usable without adoption data."""

    mock_http_client.get = AsyncMock(
        side_effect=[
            _response(npm_package_data),
            _response({"error": "not found"}, status_code=404),
        ]
    )
    crawler = _make_crawler(mock_http_client)
    pkg = await crawler.fetch_package("@npmcli/arborist")

    assert pkg.canonical_id == "pkg:npm/%40npmcli/arborist"
    assert pkg.downloads_30d is None
    assert "download_count_30d" not in pkg.metadata


async def test_fetch_dependents_is_explicit_stub(mock_http_client: AsyncMock) -> None:
    """npm dependents are intentionally stubbed until a practical first-party API exists."""

    crawler = _make_crawler(mock_http_client)
    dependents = await crawler.fetch_dependents("@npmcli/arborist")

    assert dependents == []
    mock_http_client.get.assert_not_called()
