"""
In-process API rate limiting.

Wraps ``PyrateLimiter`` in a small ASGI middleware keyed by client IP,
HTTP method, and matched route template. Each endpoint policy gets its own
in-memory limiter instance, while the acquisition key inside that policy is the
resolved client IP.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import asyncio
import ipaddress
import math
from collections.abc import Callable, Mapping
from typing import Protocol, cast

from pyrate_limiter import Duration, Limiter, Rate
from starlette.responses import JSONResponse
from starlette.routing import Match
from starlette.types import ASGIApp, Receive, Scope, Send

DEFAULT_LIMIT_PER_MINUTE = 100
ROUTE_LIMITS_PER_MINUTE: dict[tuple[str, str], int] = {
    ("GET", "/health"): 600,
    ("POST", "/ingest/sbom"): 600,
}


class AsyncLimiterLike(Protocol):
    """Protocol for the async limiter surface used by the middleware."""

    async def try_acquire_async(
        self,
        name: str = "pyrate",
        weight: int = 1,
        blocking: bool = True,
        timeout: int | float = -1,
    ) -> bool:
        """Attempt to acquire one permit asynchronously."""

        ...


type LimiterFactory = Callable[[int, tuple[str, str]], AsyncLimiterLike]


def _default_limiter_factory(limit_per_minute: int, policy_key: tuple[str, str]) -> Limiter:
    """
    Build the default in-memory limiter for one endpoint policy.

    The policy key is accepted for symmetry with test doubles and future
    extensibility, but the default implementation only needs the rate itself.
    """
    del policy_key

    return Limiter(Rate(limit_per_minute, Duration.MINUTE))


class ApiRateLimitMiddleware:
    """ASGI middleware that enforces per-endpoint, per-client request quotas."""

    def __init__(
        self,
        app: ASGIApp,
        *,
        default_limit_per_minute: int = DEFAULT_LIMIT_PER_MINUTE,
        route_limits_per_minute: Mapping[tuple[str, str], int] | None = None,
        limiter_factory: LimiterFactory | None = None,
    ) -> None:
        self.app = app
        self.default_limit_per_minute = default_limit_per_minute
        self.route_limits_per_minute = dict(route_limits_per_minute or ROUTE_LIMITS_PER_MINUTE)
        self.limiter_factory = limiter_factory or _default_limiter_factory
        self._limiters: dict[tuple[str, str], AsyncLimiterLike] = {}
        self._limiter_lock = asyncio.Lock()

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Rate-limit HTTP requests before they reach the application stack."""

        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        policy_key = resolve_policy_key(scope)
        limit_per_minute = self.route_limits_per_minute.get(policy_key, self.default_limit_per_minute)
        limiter = await self._get_limiter(policy_key, limit_per_minute)
        client_ip = resolve_client_ip(scope)
        allowed = await limiter.try_acquire_async(client_ip, blocking=False)

        if not allowed:
            retry_after_seconds = seconds_until_next_token(limit_per_minute)
            response = JSONResponse(
                {"detail": "Rate limit exceeded"},
                status_code=429,
                headers={"Retry-After": str(retry_after_seconds)},
            )
            await response(scope, receive, send)
            return

        await self.app(scope, receive, send)

    async def _get_limiter(
        self,
        policy_key: tuple[str, str],
        limit_per_minute: int,
    ) -> AsyncLimiterLike:
        """Return the limiter instance for one method-and-endpoint policy."""

        limiter = self._limiters.get(policy_key)
        if limiter is not None:
            return limiter

        async with self._limiter_lock:
            limiter = self._limiters.get(policy_key)
            if limiter is not None:
                return limiter

            limiter = self.limiter_factory(limit_per_minute, policy_key)
            self._limiters[policy_key] = limiter

            return limiter


def seconds_until_next_token(limit_per_minute: int) -> int:
    """Return the minimum whole-second wait for one permit at the given rate."""

    return max(1, math.ceil(60 / limit_per_minute))


def resolve_policy_key(scope: Scope) -> tuple[str, str]:
    """Resolve the limiter policy key as ``(method, route template)``."""

    method = scope["method"].upper()
    return (method, resolve_endpoint_key(scope))


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
    """Resolve the matched route template or an unmatched-route fallback key."""

    app = scope.get("app")
    router = getattr(app, "router", None)
    if router is not None:
        for route in router.routes:
            match, _ = route.matches(scope)
            if match is Match.FULL:
                route_path = getattr(route, "path", None)
                if isinstance(route_path, str):
                    return route_path

    return "__unmatched__"


def get_header(scope: Scope, name: str) -> str | None:
    """Return one HTTP header from the ASGI scope, decoded as latin-1."""

    target = name.lower().encode("latin-1")
    headers = cast(list[tuple[bytes, bytes]], scope.get("headers", []))
    for header_name, header_value in headers:
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
