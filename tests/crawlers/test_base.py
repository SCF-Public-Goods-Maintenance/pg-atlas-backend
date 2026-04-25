"""
Tests for the base registry crawler infrastructure.

Tests HTTP retry logic and CrawlResult error accumulation using a concrete
mock implementation of RegistryCrawler.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest

from pg_atlas.crawlers.base import (
    CrawledDependent,
    CrawledPackage,
    CrawlResult,
    ExhaustedRetries,
    RegistryCrawler,
)


class StubCrawler(RegistryCrawler):
    """Concrete RegistryCrawler for testing the base class methods."""

    def __init__(self, client: AsyncMock, **kwargs: Any) -> None:
        session_factory = AsyncMock()
        super().__init__(client=client, session_factory=session_factory, **kwargs)

    async def fetch_package(self, package_name: str) -> CrawledPackage:
        raise NotImplementedError

    async def fetch_dependents(self, package_name: str) -> list[CrawledDependent]:
        raise NotImplementedError


def _mock_response(status_code: int = 200, headers: dict[str, str] | None = None) -> httpx.Response:
    return httpx.Response(
        status_code=status_code,
        json={},
        headers=headers or {},
        request=httpx.Request("GET", "https://example.com/test"),
    )


# ---------------------------------------------------------------------------
# HTTP retry tests
# ---------------------------------------------------------------------------


async def test_request_with_retry_success(mock_http_client: AsyncMock) -> None:
    """Normal 200 response returns immediately."""
    mock_http_client.get = AsyncMock(return_value=_mock_response(200))
    crawler = StubCrawler(mock_http_client, rate_limit=0.0, max_retries=3)

    resp = await crawler._request_with_retry("https://example.com/test")
    assert resp.status_code == 200
    assert mock_http_client.get.call_count == 1


async def test_request_with_retry_429(mock_http_client: AsyncMock) -> None:
    """Retries on 429, respects Retry-After header."""
    mock_http_client.get = AsyncMock(
        side_effect=[
            _mock_response(429, headers={"Retry-After": "0"}),
            _mock_response(200),
        ]
    )
    crawler = StubCrawler(mock_http_client, rate_limit=0.0, max_retries=3)

    resp = await crawler._request_with_retry("https://example.com/test")
    assert resp.status_code == 200
    assert mock_http_client.get.call_count == 2


async def test_request_with_retry_5xx(mock_http_client: AsyncMock) -> None:
    """Retries on server error, then gives up."""
    mock_http_client.get = AsyncMock(
        side_effect=[
            _mock_response(500),
            _mock_response(500),
            _mock_response(500),
        ]
    )
    crawler = StubCrawler(mock_http_client, rate_limit=0.0, max_retries=3)

    with pytest.raises(httpx.HTTPStatusError):
        await crawler._request_with_retry("https://example.com/test")
    assert mock_http_client.get.call_count == 3


async def test_request_with_retry_404(mock_http_client: AsyncMock) -> None:
    """404 raises immediately without retry."""
    mock_http_client.get = AsyncMock(return_value=_mock_response(404))
    crawler = StubCrawler(mock_http_client, rate_limit=0.0, max_retries=3)

    with pytest.raises(httpx.HTTPStatusError):
        await crawler._request_with_retry("https://example.com/test")
    assert mock_http_client.get.call_count == 1


async def test_request_with_retry_timeout(mock_http_client: AsyncMock) -> None:
    """Retries once on timeout, then raises."""
    mock_http_client.get = AsyncMock(
        side_effect=[
            httpx.TimeoutException("timeout"),
            httpx.TimeoutException("timeout"),
        ]
    )
    crawler = StubCrawler(mock_http_client, rate_limit=0.0, max_retries=2)

    with pytest.raises(httpx.TimeoutException):
        await crawler._request_with_retry("https://example.com/test")
    assert mock_http_client.get.call_count == 2


async def test_request_with_retry_429_non_integer_retry_after(mock_http_client: AsyncMock) -> None:
    """Non-integer Retry-After header falls back to exponential backoff."""
    mock_http_client.get = AsyncMock(
        side_effect=[
            _mock_response(429, headers={"Retry-After": "not-a-number"}),
            _mock_response(200),
        ]
    )
    crawler = StubCrawler(mock_http_client, rate_limit=0.0, max_retries=3)

    resp = await crawler._request_with_retry("https://example.com/test")
    assert resp.status_code == 200


async def test_request_with_retry_unexpected_status(mock_http_client: AsyncMock) -> None:
    """Unexpected status code (e.g. 403) raises immediately."""
    mock_http_client.get = AsyncMock(return_value=_mock_response(403))
    crawler = StubCrawler(mock_http_client, rate_limit=0.0, max_retries=3)

    with pytest.raises(httpx.HTTPStatusError):
        await crawler._request_with_retry("https://example.com/test")
    assert mock_http_client.get.call_count == 1


async def test_request_with_retry_exhausted_429(mock_http_client: AsyncMock) -> None:
    """All retries consumed by 429 raises RuntimeError."""
    mock_http_client.get = AsyncMock(
        side_effect=[
            _mock_response(429, headers={"Retry-After": "0"}),
            _mock_response(429, headers={"Retry-After": "0"}),
            _mock_response(429, headers={"Retry-After": "0"}),
        ]
    )
    crawler = StubCrawler(mock_http_client, rate_limit=0.0, max_retries=3)

    with pytest.raises(ExhaustedRetries, match="Exhausted retries"):
        await crawler._request_with_retry("https://example.com/test")
    assert mock_http_client.get.call_count == 3


async def test_request_with_retry_5xx_then_429_exhausted(mock_http_client: AsyncMock) -> None:
    """5xx sets last_exc, then 429 exhausts retries — raises last_exc."""
    mock_http_client.get = AsyncMock(
        side_effect=[
            _mock_response(500),
            _mock_response(429, headers={"Retry-After": "0"}),
            _mock_response(429, headers={"Retry-After": "0"}),
        ]
    )
    crawler = StubCrawler(mock_http_client, rate_limit=0.0, max_retries=3)

    with pytest.raises(httpx.HTTPStatusError):
        await crawler._request_with_retry("https://example.com/test")
    assert mock_http_client.get.call_count == 3


async def test_crawl_result_accumulates_errors() -> None:
    """CrawlResult collects errors while crawl continues."""
    result = CrawlResult()
    result.errors.append("pkg1: connection timeout")
    result.errors.append("pkg2: 404 not found")
    result.packages_processed = 3

    assert len(result.errors) == 2
    assert result.packages_processed == 3
