"""
Tests for the crates.io registry crawler.

Unit tests use mocked HTTP responses and do not write to the database.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import httpx

from pg_atlas.crawlers.cargo import CargoCrawler


def _response(data: dict[str, Any], status_code: int = 200) -> httpx.Response:
    """Create a mock crates.io API response."""

    return httpx.Response(
        status_code=status_code,
        json=data,
        request=httpx.Request("GET", "https://crates.io/api/v1"),
    )


def _make_crawler(client: AsyncMock) -> CargoCrawler:
    """Create a Cargo crawler backed by mocked HTTP and DB clients."""

    session_factory = AsyncMock()
    return CargoCrawler(
        client=client,
        session_factory=session_factory,
        rate_limit=0.0,
        max_retries=3,
    )


async def test_fetch_package_parses_metadata_downloads_and_dependencies(
    mock_http_client: AsyncMock,
    crates_package_data: dict[str, Any],
    crates_dependencies_data: dict[str, Any],
) -> None:
    """crates.io package fetch captures repo URL, releases, monthly downloads, and normal deps."""

    mock_http_client.get = AsyncMock(side_effect=[_response(crates_package_data), _response(crates_dependencies_data)])
    crawler = _make_crawler(mock_http_client)
    pkg = await crawler.fetch_package("serde")

    assert pkg.canonical_id == "pkg:cargo/serde"
    assert pkg.display_name == "serde"
    assert pkg.latest_version == "1.0.228"
    assert pkg.repo_url == "https://github.com/serde-rs/serde"
    assert pkg.downloads_30d == 52240048
    assert pkg.metadata["recent_downloads_90d"] == 156720146
    assert pkg.metadata["download_count_30d"] == 52240048
    assert [(release.version, release.release_date) for release in pkg.releases] == [
        ("1.0.228", "2026-04-10T12:00:00.000000Z"),
        ("1.0.227", "2026-03-01T08:30:00.000000Z"),
        ("1.0.0-beta.1", "2025-12-01T00:00:00.000000Z"),
    ]

    dep_ids = {dep.canonical_id for dep in pkg.dependencies}
    assert dep_ids == {
        "pkg:cargo/serde_core",
        "pkg:cargo/serde_derive",
    }


async def test_fetch_dependents_paginates_and_filters_non_runtime_edges(
    mock_http_client: AsyncMock,
    crates_reverse_dependencies_data: dict[str, Any],
) -> None:
    """Reverse dependencies page until exhausted and keep only normal reverse dependents."""

    page_two: dict[str, Any] = {
        "dependencies": [
            {
                "id": 14077247,
                "version_id": 1457812,
                "crate_id": "serde",
                "req": "^1",
                "optional": False,
                "default_features": True,
                "features": [],
                "target": None,
                "kind": "normal",
                "downloads": 1000,
            }
        ],
        "versions": [
            {
                "id": 1457812,
                "crate": "versions-extra",
                "num": "1.0.0",
                "created_at": "2025-02-25T06:38:36.053622Z",
                "repository": "https://github.com/example/versions-extra",
            }
        ],
        "meta": {"total": 12},
    }
    mock_http_client.get = AsyncMock(side_effect=[_response(crates_reverse_dependencies_data), _response(page_two)])
    crawler = _make_crawler(mock_http_client)
    dependents = await crawler.fetch_dependents("serde")

    assert [dependent.canonical_id for dependent in dependents] == [
        "pkg:cargo/podcast",
        "pkg:cargo/versions-extra",
    ]


async def test_fetch_package_handles_malformed_metadata_payload(
    mock_http_client: AsyncMock,
    caplog: Any,
) -> None:
    """Malformed metadata payload logs a warning and still returns a crawled package."""

    malformed_metadata: dict[str, Any] = {
        "crate": {
            "name": ["serde"],
            "recent_downloads": "bad-value",
        },
        "versions": "not-a-list",
    }
    mock_http_client.get = AsyncMock(side_effect=[_response(malformed_metadata)])
    crawler = _make_crawler(mock_http_client)

    with caplog.at_level("WARNING"):
        pkg = await crawler.fetch_package("serde")

    assert "Failed to decode crates.io metadata payload" in caplog.text
    assert pkg.canonical_id == "pkg:cargo/"
    assert pkg.dependencies == []
    assert pkg.releases == []
    assert pkg.downloads_30d is None


async def test_fetch_dependents_handles_malformed_page_payload(
    mock_http_client: AsyncMock,
    caplog: Any,
) -> None:
    """Malformed reverse-dependency pages log warnings and return partial dependents."""

    page_one: dict[str, Any] = {
        "dependencies": [{"version_id": 1, "kind": "normal"}],
        "versions": [{"id": 1, "crate": "dep-ok"}],
        "meta": {"total": 20},
    }
    malformed_page_two: dict[str, Any] = {
        "dependencies": "not-a-list",
        "versions": [],
        "meta": {"total": "unknown"},
    }
    mock_http_client.get = AsyncMock(side_effect=[_response(page_one), _response(malformed_page_two)])
    crawler = _make_crawler(mock_http_client)

    with caplog.at_level("WARNING"):
        dependents = await crawler.fetch_dependents("serde")

    assert "Failed to decode crates.io reverse dependencies payload" in caplog.text
    assert [dependent.canonical_id for dependent in dependents] == ["pkg:cargo/dep-ok"]
