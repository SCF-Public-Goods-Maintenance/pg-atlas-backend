"""
Tests for API rate limiting middleware.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from pg_atlas.rate_limit import ApiRateLimitMiddleware


@dataclass
class RecordingLimiter:
    """Minimal async limiter fake that tracks calls per client identity."""

    limit_per_key: int
    calls: list[tuple[str, bool]] = field(default_factory=list[tuple[str, bool]])
    counts: dict[str, int] = field(default_factory=dict[str, int])

    async def try_acquire_async(
        self,
        name: str = "pyrate",
        weight: int = 1,
        blocking: bool = True,
        timeout: int | float = -1,
    ) -> bool:
        del weight, timeout
        self.calls.append((name, blocking))
        current = self.counts.get(name, 0)
        if current >= self.limit_per_key:
            return False

        self.counts[name] = current + 1

        return True


def make_rate_limited_app(
    default_limit_per_minute: int,
    *,
    route_limits_per_minute: dict[tuple[str, str], int] | None = None,
    limiter_factory: Callable[[int, tuple[str, str]], RecordingLimiter] | None = None,
) -> FastAPI:
    """Build a small test app with configurable limiter construction."""

    app = FastAPI()
    app.add_middleware(
        ApiRateLimitMiddleware,
        default_limit_per_minute=default_limit_per_minute,
        route_limits_per_minute=route_limits_per_minute
        or {
            ("GET", "/health"): 4,
            ("POST", "/ingest/sbom"): 4,
        },
        limiter_factory=limiter_factory,
    )

    async def limited() -> dict[str, str]:
        return {"route": "limited"}

    app.get("/limited")(limited)

    async def other() -> dict[str, str]:
        return {"route": "other"}

    app.get("/other")(other)

    async def method_get() -> dict[str, str]:
        return {"route": "method-get"}

    app.get("/method")(method_get)

    async def method_post() -> dict[str, str]:
        return {"route": "method-post"}

    app.post("/method")(method_post)

    async def item(item_id: str) -> dict[str, str]:
        return {"item_id": item_id}

    app.get("/items/{item_id}")(item)

    async def health() -> dict[str, str]:
        return {"status": "ok"}

    app.get("/health")(health)

    async def ingest_sbom() -> dict[str, str]:
        return {"status": "accepted"}

    app.post("/ingest/sbom")(ingest_sbom)

    return app


async def test_rate_limit_exhaustion_and_retry_after() -> None:
    """Exhausted endpoint policies should return 429 with Retry-After."""

    app = make_rate_limited_app(default_limit_per_minute=2)

    async with AsyncClient(
        transport=ASGITransport(app=app, client=("127.0.0.1", 12345)),
        base_url="http://test",
    ) as client:
        response_one = await client.get("/limited")
        response_two = await client.get("/limited")
        response_three = await client.get("/limited")

    assert response_one.status_code == 200
    assert response_two.status_code == 200
    assert response_three.status_code == 429
    assert response_three.json() == {"detail": "Rate limit exceeded"}
    assert response_three.headers["Retry-After"] == "30"


async def test_rate_limit_uses_separate_buckets_per_client_ip() -> None:
    """Different client IPs should not share one endpoint policy bucket."""

    fake_limiters: dict[tuple[str, str], RecordingLimiter] = {}

    def limiter_factory(limit: int, key: tuple[str, str]) -> RecordingLimiter:
        limiter = RecordingLimiter(limit_per_key=limit)
        fake_limiters[key] = limiter
        return limiter

    app = make_rate_limited_app(default_limit_per_minute=1, limiter_factory=limiter_factory)

    async with AsyncClient(
        transport=ASGITransport(app=app, client=("127.0.0.1", 12345)),
        base_url="http://test",
    ) as client:
        allowed = await client.get("/limited", headers={"X-Forwarded-For": "203.0.113.10"})
        denied = await client.get("/limited", headers={"X-Forwarded-For": "203.0.113.10"})
        separate_ip = await client.get("/limited", headers={"X-Forwarded-For": "198.51.100.20"})

    assert allowed.status_code == 200
    assert denied.status_code == 429
    assert separate_ip.status_code == 200
    assert fake_limiters[("GET", "/limited")].calls == [
        ("203.0.113.10", False),
        ("203.0.113.10", False),
        ("198.51.100.20", False),
    ]


async def test_rate_limit_keeps_method_policies_separate_on_the_same_path() -> None:
    """GET and POST on one path should use different limiter instances."""

    fake_limiters: dict[tuple[str, str], RecordingLimiter] = {}

    def limiter_factory(limit: int, key: tuple[str, str]) -> RecordingLimiter:
        limiter = RecordingLimiter(limit_per_key=limit)
        fake_limiters[key] = limiter
        return limiter

    app = make_rate_limited_app(default_limit_per_minute=1, limiter_factory=limiter_factory)

    async with AsyncClient(
        transport=ASGITransport(app=app, client=("127.0.0.1", 12345)),
        base_url="http://test",
    ) as client:
        get_allowed = await client.get("/method")
        get_denied = await client.get("/method")
        post_allowed = await client.post("/method")

    assert get_allowed.status_code == 200
    assert get_denied.status_code == 429
    assert post_allowed.status_code == 200
    assert set(fake_limiters) >= {
        ("GET", "/method"),
        ("POST", "/method"),
    }


async def test_rate_limit_keeps_default_endpoint_policies_separate() -> None:
    """Two default-rate endpoints should not share one limiter instance."""

    app = make_rate_limited_app(default_limit_per_minute=1)

    async with AsyncClient(
        transport=ASGITransport(app=app, client=("127.0.0.1", 12345)),
        base_url="http://test",
    ) as client:
        limited_first = await client.get("/limited")
        other_first = await client.get("/other")
        limited_second = await client.get("/limited")

    assert limited_first.status_code == 200
    assert other_first.status_code == 200
    assert limited_second.status_code == 429


async def test_rate_limit_groups_route_templates_with_path_params() -> None:
    """Concrete URLs should collapse to one route-template policy."""

    app = make_rate_limited_app(default_limit_per_minute=2)

    async with AsyncClient(
        transport=ASGITransport(app=app, client=("127.0.0.1", 12345)),
        base_url="http://test",
    ) as client:
        first = await client.get("/items/alpha")
        second = await client.get("/items/beta")
        third = await client.get("/items/gamma")

    assert first.status_code == 200
    assert second.status_code == 200
    assert third.status_code == 429


async def test_rate_limit_groups_unmatched_paths_by_method() -> None:
    """Unmatched paths should still share one fallback policy per method."""

    app = make_rate_limited_app(default_limit_per_minute=2)

    async with AsyncClient(
        transport=ASGITransport(app=app, client=("127.0.0.1", 12345)),
        base_url="http://test",
    ) as client:
        first = await client.get("/missing/alpha")
        second = await client.get("/missing/beta")
        third = await client.get("/missing/gamma")

    assert first.status_code == 404
    assert second.status_code == 404
    assert third.status_code == 429


async def test_rate_limit_uses_first_forwarded_ip_when_valid() -> None:
    """The first valid X-Forwarded-For IP should be the acquisition key."""

    fake_limiters: dict[tuple[str, str], RecordingLimiter] = {}

    def limiter_factory(limit: int, key: tuple[str, str]) -> RecordingLimiter:
        limiter = RecordingLimiter(limit_per_key=limit)
        fake_limiters[key] = limiter
        return limiter

    app = make_rate_limited_app(default_limit_per_minute=1, limiter_factory=limiter_factory)

    async with AsyncClient(
        transport=ASGITransport(app=app, client=("127.0.0.1", 12345)),
        base_url="http://test",
    ) as client:
        first = await client.get(
            "/limited",
            headers={"X-Forwarded-For": "203.0.113.1, 198.51.100.10"},
        )
        denied = await client.get(
            "/limited",
            headers={"X-Forwarded-For": "203.0.113.1, 192.0.2.99"},
        )
        separate_first_ip = await client.get(
            "/limited",
            headers={"X-Forwarded-For": "198.51.100.10, 203.0.113.1"},
        )

    assert first.status_code == 200
    assert denied.status_code == 429
    assert separate_first_ip.status_code == 200
    assert fake_limiters[("GET", "/limited")].calls == [
        ("203.0.113.1", False),
        ("203.0.113.1", False),
        ("198.51.100.10", False),
    ]


async def test_rate_limit_falls_back_to_client_host_when_forwarded_for_is_invalid() -> None:
    """Invalid forwarded headers should fall back to the socket peer IP."""

    fake_limiters: dict[tuple[str, str], RecordingLimiter] = {}

    def limiter_factory(limit: int, key: tuple[str, str]) -> RecordingLimiter:
        limiter = RecordingLimiter(limit_per_key=limit)
        fake_limiters[key] = limiter
        return limiter

    app = make_rate_limited_app(default_limit_per_minute=1, limiter_factory=limiter_factory)

    async with AsyncClient(
        transport=ASGITransport(app=app, client=("198.51.100.55", 12345)),
        base_url="http://test",
    ) as client:
        first = await client.get("/limited", headers={"X-Forwarded-For": "not-an-ip"})
        denied = await client.get("/limited", headers={"X-Forwarded-For": "not-an-ip"})

    assert first.status_code == 200
    assert denied.status_code == 429
    assert fake_limiters[("GET", "/limited")].calls == [
        ("198.51.100.55", False),
        ("198.51.100.55", False),
    ]


async def test_rate_limit_applies_configured_endpoint_overrides() -> None:
    """Endpoint-specific overrides should get their own higher-rate policies."""

    app = make_rate_limited_app(default_limit_per_minute=2)

    async with AsyncClient(
        transport=ASGITransport(app=app, client=("127.0.0.1", 12345)),
        base_url="http://test",
    ) as client:
        health_responses = [await client.get("/health") for _ in range(5)]
        sbom_responses = [await client.post("/ingest/sbom") for _ in range(5)]

    assert [response.status_code for response in health_responses[:4]] == [200, 200, 200, 200]
    assert health_responses[4].status_code == 429
    assert [response.status_code for response in sbom_responses[:4]] == [200, 200, 200, 200]
    assert sbom_responses[4].status_code == 429
