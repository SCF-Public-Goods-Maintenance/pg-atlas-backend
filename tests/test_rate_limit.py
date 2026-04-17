"""
Tests for API rate limiting middleware.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from pg_atlas.rate_limit import ApiRateLimitMiddleware


class FakeClock:
    """Mutable monotonic clock used for deterministic token bucket tests."""

    def __init__(self) -> None:
        self.current = 0.0

    def __call__(self) -> float:
        return self.current

    def advance(self, seconds: float) -> None:
        self.current += seconds


def make_rate_limited_app(default_limit_per_minute: int, clock: FakeClock) -> FastAPI:
    """Build a small test app with the middleware under deterministic control."""

    app = FastAPI()
    app.add_middleware(
        ApiRateLimitMiddleware,
        default_limit_per_minute=default_limit_per_minute,
        route_limits_per_minute={
            ("GET", "/health"): 4,
            ("POST", "/ingest/sbom"): 4,
        },
        clock=clock,
    )

    async def limited() -> dict[str, str]:
        return {"route": "limited"}

    app.get("/limited")(limited)

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
    """Exhausted buckets should return 429 with a Retry-After header."""

    clock = FakeClock()
    app = make_rate_limited_app(default_limit_per_minute=2, clock=clock)

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


async def test_rate_limit_refills_after_time_passes() -> None:
    """Buckets should refill over time according to the configured rate."""

    clock = FakeClock()
    app = make_rate_limited_app(default_limit_per_minute=2, clock=clock)

    async with AsyncClient(
        transport=ASGITransport(app=app, client=("127.0.0.1", 12345)),
        base_url="http://test",
    ) as client:
        await client.get("/limited")
        await client.get("/limited")
        denied = await client.get("/limited")
        clock.advance(30)
        allowed = await client.get("/limited")

    assert denied.status_code == 429
    assert allowed.status_code == 200


async def test_rate_limit_uses_separate_buckets_per_client_ip() -> None:
    """Different client IPs should not share rate-limit state."""

    clock = FakeClock()
    app = make_rate_limited_app(default_limit_per_minute=1, clock=clock)

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


async def test_rate_limit_uses_separate_buckets_per_method() -> None:
    """GET and POST on the same path should be rate-limited independently."""

    clock = FakeClock()
    app = make_rate_limited_app(default_limit_per_minute=1, clock=clock)

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


async def test_rate_limit_groups_route_templates_with_path_params() -> None:
    """Different path parameters on one route template should share a bucket."""

    clock = FakeClock()
    app = make_rate_limited_app(default_limit_per_minute=2, clock=clock)

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
    """Unmatched routes should still be grouped deterministically by method."""

    clock = FakeClock()
    app = make_rate_limited_app(default_limit_per_minute=2, clock=clock)

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
    """The first valid X-Forwarded-For IP should define the bucket key."""

    clock = FakeClock()
    app = make_rate_limited_app(default_limit_per_minute=1, clock=clock)

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


async def test_rate_limit_falls_back_to_client_host_when_forwarded_for_is_invalid() -> None:
    """Invalid X-Forwarded-For values should fall back to the socket client IP."""

    clock = FakeClock()
    app = make_rate_limited_app(default_limit_per_minute=1, clock=clock)

    async with AsyncClient(
        transport=ASGITransport(app=app, client=("198.51.100.55", 12345)),
        base_url="http://test",
    ) as client:
        first = await client.get("/limited", headers={"X-Forwarded-For": "not-an-ip"})
        denied = await client.get("/limited", headers={"X-Forwarded-For": "not-an-ip"})

    assert first.status_code == 200
    assert denied.status_code == 429


async def test_rate_limit_applies_configured_endpoint_overrides() -> None:
    """Configured overrides should allow higher per-minute throughput."""

    clock = FakeClock()
    app = make_rate_limited_app(default_limit_per_minute=2, clock=clock)

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
