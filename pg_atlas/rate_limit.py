"""
In-process API rate limiting.

Implements a small token-bucket middleware keyed by client IP, HTTP method, and
matched route template. This provides deterministic protection for the API
without adding external infrastructure or persistence.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import asyncio
import ipaddress
import math
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import cast

from starlette.responses import JSONResponse
from starlette.routing import Match
from starlette.types import ASGIApp, Receive, Scope, Send

DEFAULT_LIMIT_PER_MINUTE = 100
ROUTE_LIMITS_PER_MINUTE: dict[tuple[str, str], int] = {
    ("GET", "/health"): 600,
    ("POST", "/ingest/sbom"): 600,
}
CLEANUP_INTERVAL_SECONDS = 60.0
IDLE_BUCKET_TTL_SECONDS = 60.0


@dataclass
class TokenBucket:
    """Mutable token-bucket state for one client/method/route key."""

    capacity: float
    tokens: float
    updated_at: float
    last_seen_at: float


@dataclass(frozen=True)
class RateLimitDecision:
    """Result of applying one request against a token bucket."""

    allowed: bool
    retry_after_seconds: int | None = None


class TokenBucketStore:
    """In-memory token buckets with lazy cleanup of idle, fully refilled state."""

    def __init__(
        self,
        *,
        clock: Callable[[], float] | None = None,
        cleanup_interval_seconds: float = CLEANUP_INTERVAL_SECONDS,
        idle_bucket_ttl_seconds: float = IDLE_BUCKET_TTL_SECONDS,
    ) -> None:
        self._clock = clock or time.monotonic
        self._cleanup_interval_seconds = cleanup_interval_seconds
        self._idle_bucket_ttl_seconds = idle_bucket_ttl_seconds
        self._buckets: dict[tuple[str, str, str], TokenBucket] = {}
        self._last_cleanup_at = 0.0
        self._lock = asyncio.Lock()

    async def consume(
        self,
        key: tuple[str, str, str],
        *,
        limit_per_minute: int,
    ) -> RateLimitDecision:
        """Consume one token for the given key or return a retry interval."""

        now = self._clock()
        async with self._lock:
            self._cleanup_if_due(now)

            capacity = float(limit_per_minute)
            refill_rate = capacity / 60.0
            bucket = self._buckets.get(key)

            if bucket is None:
                bucket = TokenBucket(
                    capacity=capacity,
                    tokens=capacity,
                    updated_at=now,
                    last_seen_at=now,
                )
                self._buckets[key] = bucket
            else:
                self._refill_bucket(bucket, now=now, refill_rate=refill_rate)

            bucket.last_seen_at = now

            if bucket.tokens >= 1.0:
                bucket.tokens -= 1.0
                bucket.updated_at = now
                return RateLimitDecision(allowed=True)

            seconds_until_token = max(1, math.ceil((1.0 - bucket.tokens) / refill_rate))
            bucket.updated_at = now
            return RateLimitDecision(
                allowed=False,
                retry_after_seconds=seconds_until_token,
            )

    def _refill_bucket(self, bucket: TokenBucket, *, now: float, refill_rate: float) -> None:
        """Refill a bucket based on elapsed monotonic time."""

        elapsed = max(0.0, now - bucket.updated_at)
        if elapsed == 0.0:
            return
        bucket.tokens = min(bucket.capacity, bucket.tokens + (elapsed * refill_rate))
        bucket.updated_at = now

    def _cleanup_if_due(self, now: float) -> None:
        """Drop idle buckets only once per cleanup interval."""

        if now - self._last_cleanup_at < self._cleanup_interval_seconds:
            return

        self._last_cleanup_at = now
        keys_to_delete: list[tuple[str, str, str]] = []
        for key, bucket in self._buckets.items():
            idle_for = now - bucket.last_seen_at
            fully_refilled = bucket.tokens >= (bucket.capacity - 1e-9)
            if idle_for >= self._idle_bucket_ttl_seconds and fully_refilled:
                keys_to_delete.append(key)

        for key in keys_to_delete:
            del self._buckets[key]


class ApiRateLimitMiddleware:
    """ASGI middleware that enforces token-bucket rate limits per route."""

    def __init__(
        self,
        app: ASGIApp,
        *,
        default_limit_per_minute: int = DEFAULT_LIMIT_PER_MINUTE,
        route_limits_per_minute: Mapping[tuple[str, str], int] | None = None,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self.app = app
        self.default_limit_per_minute = default_limit_per_minute
        self.route_limits_per_minute = dict(route_limits_per_minute or ROUTE_LIMITS_PER_MINUTE)
        self.bucket_store = TokenBucketStore(clock=clock)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Rate-limit HTTP requests before they reach the application stack."""

        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        method = scope["method"].upper()
        endpoint_key = resolve_endpoint_key(scope)
        client_ip = resolve_client_ip(scope)
        limit_per_minute = self.route_limits_per_minute.get(
            (method, endpoint_key),
            self.default_limit_per_minute,
        )
        decision = await self.bucket_store.consume(
            (client_ip, method, endpoint_key),
            limit_per_minute=limit_per_minute,
        )

        if not decision.allowed:
            response = JSONResponse(
                {"detail": "Rate limit exceeded"},
                status_code=429,
                headers={"Retry-After": str(decision.retry_after_seconds)},
            )
            await response(scope, receive, send)
            return

        await self.app(scope, receive, send)


def resolve_client_ip(scope: Scope) -> str:
    """Resolve the client IP from X-Forwarded-For or the socket peer."""

    forwarded_for = get_header(scope, "x-forwarded-for")
    if forwarded_for is not None:
        first_hop = forwarded_for.split(",", maxsplit=1)[0].strip()
        if is_valid_ip_address(first_hop):
            return first_hop

    client = cast(tuple[str, int] | None, scope.get("client"))
    if client is not None and client[0]:
        return client[0]
    return "unknown"


def resolve_endpoint_key(scope: Scope) -> str:
    """Resolve the matched route template or a grouped unmatched fallback key."""

    method = scope["method"].upper()
    app = scope.get("app")
    router = getattr(app, "router", None)
    if router is not None:
        for route in router.routes:
            match, _ = route.matches(scope)
            if match is Match.FULL:
                route_path = getattr(route, "path", None)
                if isinstance(route_path, str):
                    return route_path

    return f"__unmatched__:{method}"


def get_header(scope: Scope, name: str) -> str | None:
    """Return one HTTP header from the ASGI scope, decoded as latin-1."""

    target = name.lower().encode("latin-1")
    headers = cast(list[tuple[bytes, bytes]], scope.get("headers", []))
    for header in headers:
        header_name, header_value = header
        if header_name == target:
            return header_value.decode("latin-1")
    return None


def is_valid_ip_address(value: str) -> bool:
    """Return whether the supplied string is a syntactically valid IP address."""

    try:
        ipaddress.ip_address(value)
    except ValueError:
        return False
    return True
