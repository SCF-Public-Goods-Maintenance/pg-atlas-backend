"""
Tests for the maintenance collector's transport and budget bounds: page,
request, and time caps, rate-limit handling, transport failure containment,
event-loop liveness, and deadline enforcement on in-flight requests.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import threading
import time
from types import SimpleNamespace
from typing import Any

import pytest
from github import GithubException

import pg_atlas.procrastinate.github_maintenance as gm
import pg_atlas.procrastinate.github_maintenance_schema as gms
from pg_atlas.config import settings
from pg_atlas.metrics.maintenance import (
    STATE_INCOMPLETE,
    STATE_OK,
)
from pg_atlas.procrastinate.github_maintenance import (
    collect_maintenance_for_repo,
)
from tests.metrics.maintenance_support import (
    NOW,
    FakeRequester,
    actor,
    happy_path_handler,
    iso,
    patch_client,
    rate_limited_exception,
    releases_repo,
)


class TestCapContainment:
    async def test_cohort_page_cap_leaves_other_signals_collectable(
        self, monkeypatch: pytest.MonkeyPatch, default_parameters: None
    ) -> None:
        """A capped issue cohort is incomplete by itself; PR collection continues."""

        monkeypatch.setattr(settings, "MAINTENANCE_ITEM_PAGE_CAP", 1)

        def handler(query: str, variables: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
            if query == gms.ISSUE_COHORT_QUERY:
                return {}, {
                    "data": {
                        "repository": {
                            "issues": {
                                "nodes": [
                                    {
                                        "number": 1,
                                        "createdAt": iso(30),
                                        "author": actor("alice"),
                                        "comments": {"nodes": [], "pageInfo": {"hasNextPage": False, "endCursor": None}},
                                    }
                                ],
                                "pageInfo": {"hasNextPage": True, "endCursor": "more"},
                            }
                        }
                    }
                }

            return happy_path_handler(query, variables)

        patch_client(monkeypatch, FakeRequester(handler))

        collected = await collect_maintenance_for_repo("owner", "repo", need_release_fallback=False, now=NOW)
        signals = collected.signals

        assert signals.issue_responsiveness is not None
        assert signals.issue_responsiveness.state == STATE_INCOMPLETE
        assert signals.issue_responsiveness.incomplete_reason == gm.REASON_PAGE_CAP
        assert signals.issue_responsiveness.response_fraction is None
        assert signals.pr_responsiveness is not None
        assert signals.pr_responsiveness.state == STATE_OK
        assert signals.pr_responsiveness.response_fraction == 0.5

    async def test_release_fallback_page_cap_marks_incomplete(
        self, monkeypatch: pytest.MonkeyPatch, default_parameters: None
    ) -> None:
        """A listing longer than the page cap is incomplete; no page beyond the cap is fetched."""

        monkeypatch.setattr(gm, "_RELEASE_PAGE_CAP", 1)

        fake_repo, paginated = releases_repo(
            [SimpleNamespace(draft=True, prerelease=False, published_at=NOW - dt.timedelta(days=i)) for i in range(60)]
        )

        def _get_repo(path: str) -> SimpleNamespace:
            return fake_repo

        patch_client(monkeypatch, FakeRequester(happy_path_handler), get_repo=_get_repo)

        collected = await collect_maintenance_for_repo("owner", "repo", need_release_fallback=True, now=NOW)

        fallback = collected.signals.release_fallback
        assert fallback is not None
        assert fallback.state == STATE_INCOMPLETE
        assert fallback.complete is False
        assert fallback.incomplete_reason == gm.REASON_PAGE_CAP
        assert fallback.publication_dates == []
        assert paginated.page_calls == 1

    async def test_release_fallback_finds_latest_publication_regardless_of_listing_order(
        self, monkeypatch: pytest.MonkeyPatch, default_parameters: None
    ) -> None:
        """The listing carries no publication-order contract; the walk must not stop at the first N distinct dates."""

        monkeypatch.setattr(settings, "MAINTENANCE_CADENCE_LAST_N_EVENTS", 3)

        # Creation order differs from publication order here: page one holds
        # three distinct older publication dates, and the newest publication
        # sits on page two.
        distinct_old = [NOW - dt.timedelta(days=age) for age in (40, 30, 20)]
        items = [SimpleNamespace(draft=False, prerelease=False, published_at=date) for date in distinct_old]
        items += [SimpleNamespace(draft=False, prerelease=False, published_at=distinct_old[0]) for _ in range(27)]
        items.append(SimpleNamespace(draft=False, prerelease=False, published_at=NOW - dt.timedelta(days=2)))
        fake_repo, paginated = releases_repo(items)

        def _get_repo(path: str) -> SimpleNamespace:
            return fake_repo

        patch_client(monkeypatch, FakeRequester(happy_path_handler), get_repo=_get_repo)

        collected = await collect_maintenance_for_repo("owner", "repo", need_release_fallback=True, now=NOW)

        fallback = collected.signals.release_fallback
        assert fallback is not None
        assert fallback.state == STATE_OK
        assert paginated.page_calls == 2
        assert fallback.publication_dates == sorted(
            (NOW - dt.timedelta(days=age)).date().isoformat() for age in (40, 30, 20, 2)
        )


class TestRateLimitWaitSeconds:
    def test_graphql_envelope_with_reset_header(self) -> None:
        exc = rate_limited_exception({"X-RateLimit-Reset": str(time.time() + 30)})
        wait = gm._rate_limit_wait_seconds(exc)
        assert wait is not None
        assert 25 < wait <= 31

    def test_retry_after_takes_precedence(self) -> None:
        exc = rate_limited_exception({"Retry-After": "17", "X-RateLimit-Reset": str(time.time() + 3000)})
        assert gm._rate_limit_wait_seconds(exc) == 17.0

    def test_malformed_headers_fall_back_to_default_wait(self) -> None:
        exc = rate_limited_exception({"Retry-After": "soon", "X-RateLimit-Reset": "later"})
        assert gm._rate_limit_wait_seconds(exc) == 60.0

    def test_secondary_limit_429_with_retry_after(self) -> None:
        exc = GithubException(429, {"message": "slow down"}, {"Retry-After": "9"})
        assert gm._rate_limit_wait_seconds(exc) == 9.0

    def test_403_without_rate_headers_is_not_a_rate_limit(self) -> None:
        exc = GithubException(403, {"message": "forbidden"}, {})
        assert gm._rate_limit_wait_seconds(exc) is None

    def test_ordinary_error_is_not_a_rate_limit(self) -> None:
        exc = GithubException(500, {"message": "boom"}, {"Retry-After": "9"})
        assert gm._rate_limit_wait_seconds(exc) is None


class TestRateLimitRetry:
    async def test_bounded_wait_then_retry_succeeds(self, monkeypatch: pytest.MonkeyPatch, default_parameters: None) -> None:
        """A short advertised reset is waited out once and collection completes."""

        open_issue_attempts = {"count": 0}

        def handler(query: str, variables: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
            if query == gms.OPEN_ISSUES_QUERY:
                open_issue_attempts["count"] += 1
                if open_issue_attempts["count"] == 1:
                    raise rate_limited_exception({"X-RateLimit-Reset": str(time.time() - 5)})

            return happy_path_handler(query, variables)

        patch_client(monkeypatch, FakeRequester(handler))

        collected = await collect_maintenance_for_repo("owner", "repo", need_release_fallback=False, now=NOW)

        assert open_issue_attempts["count"] == 2
        assert collected.signals.issue_backlog is not None
        assert collected.signals.issue_backlog.state == STATE_OK
        assert collected.signals.pr_responsiveness is not None
        assert collected.signals.pr_responsiveness.state == STATE_OK


class TestReleaseFallbackBudget:
    async def test_budget_charged_at_every_request_boundary(
        self, monkeypatch: pytest.MonkeyPatch, default_parameters: None
    ) -> None:
        """The repository lookup and each fetched page cost one charge each."""

        fake_repo, paginated = releases_repo(
            [SimpleNamespace(draft=True, prerelease=False, published_at=NOW - dt.timedelta(days=i)) for i in range(65)]
        )

        def _get_repo(path: str) -> SimpleNamespace:
            return fake_repo

        patch_client(monkeypatch, FakeRequester(happy_path_handler), get_repo=_get_repo)
        collector = gm._MaintenanceCollector("owner", "repo", now=NOW)

        fallback = await collector.collect_release_fallback()

        assert paginated.page_calls == 3
        assert collector.requests_used == 1 + paginated.page_calls
        assert fallback.state == STATE_OK
        assert fallback.complete is True
        assert fallback.publication_dates == []

    async def test_request_cap_stops_before_the_next_page_fetch(
        self, monkeypatch: pytest.MonkeyPatch, default_parameters: None
    ) -> None:
        """Once the cap is exhausted, no further page request starts."""

        fake_repo, paginated = releases_repo(
            [SimpleNamespace(draft=True, prerelease=False, published_at=NOW - dt.timedelta(days=i)) for i in range(65)]
        )

        def _get_repo(path: str) -> SimpleNamespace:
            return fake_repo

        patch_client(monkeypatch, FakeRequester(happy_path_handler), get_repo=_get_repo)
        collector = gm._MaintenanceCollector("owner", "repo", now=NOW)
        collector._budget.request_cap = 2

        with pytest.raises(gm._CollectionHalt) as halt:
            await collector.collect_release_fallback()

        assert halt.value.reason == gm.REASON_REQUEST_CAP
        assert paginated.page_calls == 1
        assert collector.requests_used == 2


class TestTransportBoundaries:
    async def test_transport_failure_becomes_typed_halt(
        self, monkeypatch: pytest.MonkeyPatch, default_parameters: None
    ) -> None:
        """A connection failure surfaces as a query-error halt, never raw."""

        import requests

        def handler(query: str, variables: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
            raise requests.exceptions.ConnectionError("connection reset")

        patch_client(monkeypatch, FakeRequester(handler))
        collector = gm._MaintenanceCollector("owner", "repo", now=NOW)

        with pytest.raises(gm._CollectionHalt) as halt:
            await collector.fetch_overview()

        assert halt.value.reason == gm.REASON_QUERY_ERROR
        assert "ConnectionError" in halt.value.detail

    async def test_rate_limit_wait_exceeding_remaining_deadline_halts(
        self, monkeypatch: pytest.MonkeyPatch, default_parameters: None
    ) -> None:
        """A reset wait longer than the remaining time budget halts immediately."""

        def handler(query: str, variables: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
            raise rate_limited_exception({"X-RateLimit-Reset": str(time.time() + 30)})

        patch_client(monkeypatch, FakeRequester(handler))
        collector = gm._MaintenanceCollector("owner", "repo", now=NOW)
        collector._budget.deadline_monotonic = time.monotonic() + 5.0

        started = time.monotonic()
        with pytest.raises(gm._CollectionHalt) as halt:
            await collector.fetch_overview()

        assert halt.value.reason == gm.REASON_RATE_LIMITED
        assert time.monotonic() - started < 2.0


async def _wait_for_event(event: threading.Event, timeout: float = 5.0) -> None:
    """Poll a worker-thread event from the loop; the timeout is a hang guard."""

    deadline = time.monotonic() + timeout
    while not event.is_set() and time.monotonic() < deadline:
        await asyncio.sleep(0.005)

    assert event.is_set(), "worker never entered the target callable"


class TestEventLoopAndWaitBounds:
    async def test_event_loop_stays_live_during_a_slow_request(
        self, monkeypatch: pytest.MonkeyPatch, default_parameters: None
    ) -> None:
        """The synchronous transport call runs in a worker thread."""

        request_started = threading.Event()
        release_request = threading.Event()

        def handler(query: str, variables: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
            request_started.set()
            assert release_request.wait(timeout=5.0), "test never released the worker"
            return {}, {"data": {"repository": {"pushedAt": iso(2), "hasIssuesEnabled": True, "isArchived": False}}}

        patch_client(monkeypatch, FakeRequester(handler))
        collector = gm._MaintenanceCollector("owner", "repo", now=NOW)

        ticks = 0

        async def ticker() -> None:
            nonlocal ticks
            while True:
                ticks += 1
                await asyncio.sleep(0.001)

        ticker_task = asyncio.create_task(ticker())
        fetch_task = asyncio.create_task(collector.fetch_overview())
        try:
            await _wait_for_event(request_started)
            hang_guard = time.monotonic() + 5.0
            while ticks < 2 and time.monotonic() < hang_guard:
                await asyncio.sleep(0.001)

            assert ticks >= 2
        finally:
            release_request.set()
            ticker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await ticker_task

        overview = await fetch_task
        assert overview.hasIssuesEnabled is True

    async def test_wait_margin_counts_against_the_cap(self, monkeypatch: pytest.MonkeyPatch, default_parameters: None) -> None:
        """An advertised reset just under the cap still halts once the retry margin is added."""

        def handler(query: str, variables: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
            raise rate_limited_exception({"X-RateLimit-Reset": str(time.time() + 119.5)})

        patch_client(monkeypatch, FakeRequester(handler))
        collector = gm._MaintenanceCollector("owner", "repo", now=NOW)

        started = time.monotonic()
        with pytest.raises(gm._CollectionHalt) as halt:
            await collector.fetch_overview()

        assert halt.value.reason == gm.REASON_RATE_LIMITED
        assert time.monotonic() - started < 2.0


class TestDeadlineGovernsInFlightRequests:
    async def test_graphql_request_is_cancelled_at_the_deadline(
        self, monkeypatch: pytest.MonkeyPatch, default_parameters: None
    ) -> None:
        """A slow GraphQL request becomes a time-cap halt, not a late success."""

        request_started = threading.Event()
        release_request = threading.Event()

        def handler(query: str, variables: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
            request_started.set()
            assert release_request.wait(timeout=5.0), "test never released the worker"
            return {}, {"data": {"repository": {"pushedAt": iso(2), "hasIssuesEnabled": True, "isArchived": False}}}

        patch_client(monkeypatch, FakeRequester(handler))
        collector = gm._MaintenanceCollector("owner", "repo", now=NOW)
        collector._budget.deadline_monotonic = time.monotonic() + 0.5

        fetch_task = asyncio.create_task(collector.fetch_overview())
        try:
            await _wait_for_event(request_started)

            with pytest.raises(gm._CollectionHalt) as halt:
                await fetch_task

            assert halt.value.reason == gm.REASON_TIME_CAP
            # The worker entered the transport call before the deadline and is
            # still held when the halt is observed: the deadline governed the
            # await of an in-flight request, and the late result is discarded.
            assert request_started.is_set()
            assert not release_request.is_set()
        finally:
            release_request.set()
            with contextlib.suppress(gm._CollectionHalt):
                await fetch_task

    async def test_rest_lookup_is_cancelled_at_the_deadline(
        self, monkeypatch: pytest.MonkeyPatch, default_parameters: None
    ) -> None:
        lookup_started = threading.Event()
        release_request = threading.Event()
        fake_repo, _ = releases_repo([])

        def _held_get_repo(path: str) -> SimpleNamespace:
            # Cancelling the await does not stop this worker thread: it is
            # released during cleanup and its late result is discarded.
            lookup_started.set()
            assert release_request.wait(timeout=5.0), "test never released the worker"
            return fake_repo

        patch_client(monkeypatch, FakeRequester(happy_path_handler), get_repo=_held_get_repo)
        collector = gm._MaintenanceCollector("owner", "repo", now=NOW)
        collector._budget.deadline_monotonic = time.monotonic() + 0.5

        fallback_task = asyncio.create_task(collector.collect_release_fallback())
        try:
            await _wait_for_event(lookup_started)

            with pytest.raises(gm._CollectionHalt) as halt:
                await fallback_task

            assert halt.value.reason == gm.REASON_TIME_CAP
            assert lookup_started.is_set()
            assert not release_request.is_set()
        finally:
            release_request.set()
            with contextlib.suppress(gm._CollectionHalt):
                await fallback_task

    async def test_final_release_page_cannot_complete_past_the_deadline(
        self, monkeypatch: pytest.MonkeyPatch, default_parameters: None
    ) -> None:
        """The last page finishing after the cap halts instead of reporting complete."""

        fake_repo, paginated = releases_repo(
            [SimpleNamespace(draft=False, prerelease=False, published_at=NOW - dt.timedelta(days=10))]
        )
        original_get_page = paginated.get_page
        page_started = threading.Event()
        release_page = threading.Event()

        def _held_get_page(index: int) -> list[SimpleNamespace]:
            page_started.set()
            assert release_page.wait(timeout=5.0), "test never released the worker"
            return original_get_page(index)

        paginated.get_page = _held_get_page  # type: ignore[method-assign]

        def _get_repo(path: str) -> SimpleNamespace:
            return fake_repo

        patch_client(monkeypatch, FakeRequester(happy_path_handler), get_repo=_get_repo)
        collector = gm._MaintenanceCollector("owner", "repo", now=NOW)
        collector._budget.deadline_monotonic = time.monotonic() + 0.5

        fallback_task = asyncio.create_task(collector.collect_release_fallback())
        try:
            await _wait_for_event(page_started)

            with pytest.raises(gm._CollectionHalt) as halt:
                await fallback_task

            assert halt.value.reason == gm.REASON_TIME_CAP
            # Entry into the page fetch itself is established, so the halt
            # interrupted the final page rather than the repository lookup.
            assert page_started.is_set()
            assert not release_page.is_set()
        finally:
            release_page.set()
            with contextlib.suppress(gm._CollectionHalt):
                await fallback_task
