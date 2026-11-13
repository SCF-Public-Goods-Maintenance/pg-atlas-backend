"""
Shared fakes and canned GraphQL responses for the maintenance test modules.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any

import pytest
from github import GithubException

import pg_atlas.procrastinate.github_maintenance as gm
import pg_atlas.procrastinate.github_maintenance_schema as gms
from pg_atlas.metrics.maintenance import (
    STATE_OK,
    BacklogSignal,
    MaintenanceSignals,
    ResponsivenessSignal,
)

NOW = dt.datetime(2026, 9, 9, 12, 0, tzinfo=dt.UTC)


def iso(days_ago: float) -> str:
    return (NOW - dt.timedelta(days=days_ago)).isoformat()


def actor(login: str, typename: str = "User") -> dict[str, str]:
    return {"__typename": typename, "login": login}


QueryHandler = Callable[[str, dict[str, Any]], tuple[dict[str, Any], dict[str, Any]]]


class FakeRequester:
    """Dispatch GraphQL queries to a canned handler, recording every call."""

    def __init__(self, handler: QueryHandler) -> None:
        self._handler = handler
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def graphql_query(self, query: str, variables: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        self.calls.append((query, variables))

        return self._handler(query, variables)


def patch_client(monkeypatch: pytest.MonkeyPatch, requester: FakeRequester, **extra: Any) -> None:
    client = SimpleNamespace(**{"_Github__requester": requester}, **extra)
    monkeypatch.setattr(gm, "_get_maintenance_client", lambda: client)


class FakePaginatedReleases:
    """Serve canned releases in 30-item pages, counting page fetches."""

    def __init__(self, items: list[SimpleNamespace]) -> None:
        self._items = items
        self.page_calls = 0

    def get_page(self, index: int) -> list[SimpleNamespace]:
        self.page_calls += 1

        return self._items[index * 30 : (index + 1) * 30]


def releases_repo(items: list[SimpleNamespace]) -> tuple[SimpleNamespace, FakePaginatedReleases]:
    paginated = FakePaginatedReleases(items)

    return SimpleNamespace(get_releases=lambda: paginated), paginated


def happy_path_handler(query: str, variables: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    if query == gms.OVERVIEW_QUERY:
        return {}, {"data": {"repository": {"pushedAt": iso(2), "hasIssuesEnabled": True, "isArchived": False}}}

    if query == gms.OPEN_ISSUES_QUERY:
        return {}, {
            "data": {
                "repository": {
                    "issues": {
                        "totalCount": 2,
                        "nodes": [{"createdAt": iso(100)}, {"createdAt": iso(10)}],
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                    }
                }
            }
        }

    if query == gms.OPEN_PULLS_QUERY:
        return {}, {
            "data": {
                "repository": {
                    "pullRequests": {
                        "totalCount": 1,
                        "nodes": [{"createdAt": iso(20)}],
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                    }
                }
            }
        }

    if query == gms.ISSUE_COHORT_QUERY:
        assert variables["after"] is None, "listing must stop at the window boundary"
        return {}, {
            "data": {
                "repository": {
                    "issues": {
                        "nodes": [
                            {
                                "number": 1,
                                "createdAt": iso(30),
                                "author": actor("alice"),
                                "comments": {
                                    "nodes": [{"createdAt": iso(29), "authorAssociation": "MEMBER", "author": actor("bob")}],
                                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                                },
                            },
                            {
                                "number": 6,
                                "createdAt": iso(33),
                                "author": actor("erin"),
                                "comments": {"nodes": [], "pageInfo": {"hasNextPage": False, "endCursor": None}},
                                "timelineItems": {
                                    "nodes": [
                                        {
                                            "__typename": "ClosedEvent",
                                            "createdAt": iso(31),
                                            "closer": {"__typename": "PullRequest"},
                                        }
                                    ],
                                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                                },
                            },
                            {
                                "number": 4,
                                "createdAt": iso(35),
                                "authorAssociation": "MEMBER",
                                "author": actor("bob"),
                                "comments": {"nodes": [], "pageInfo": {"hasNextPage": False, "endCursor": None}},
                            },
                            {
                                "number": 2,
                                "createdAt": iso(40),
                                "author": actor("carol"),
                                "comments": {
                                    "nodes": [
                                        {"createdAt": iso(39), "authorAssociation": "OWNER", "author": actor("carol")},
                                        {
                                            "createdAt": iso(38),
                                            "authorAssociation": "MEMBER",
                                            "author": actor("helper[bot]", "Bot"),
                                        },
                                        {"createdAt": iso(37), "authorAssociation": "NONE", "author": actor("rando")},
                                    ],
                                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                                },
                            },
                            {
                                "number": 7,
                                "createdAt": iso(45),
                                "author": actor("frank"),
                                "comments": {"nodes": [], "pageInfo": {"hasNextPage": False, "endCursor": None}},
                                "timelineItems": {
                                    "nodes": [{"__typename": "ClosedEvent", "createdAt": iso(44), "closer": None}],
                                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                                },
                            },
                            {
                                "number": 3,
                                "createdAt": iso(200),
                                "author": actor("old"),
                                "comments": {"nodes": [], "pageInfo": {"hasNextPage": False, "endCursor": None}},
                            },
                        ],
                        "pageInfo": {"hasNextPage": True, "endCursor": "cursor-must-not-be-followed"},
                    }
                }
            }
        }

    if query == gms.PULL_COHORT_QUERY:
        return {}, {
            "data": {
                "repository": {
                    "pullRequests": {
                        "nodes": [
                            {
                                "number": 13,
                                "createdAt": iso(25),
                                "isDraft": False,
                                "authorAssociation": "OWNER",
                                "author": actor("grace"),
                                "reviews": {"nodes": [], "pageInfo": {"hasNextPage": False, "endCursor": None}},
                                "timelineItems": {"nodes": [], "pageInfo": {"hasNextPage": False}},
                            },
                            {
                                "number": 10,
                                "createdAt": iso(30),
                                "isDraft": False,
                                "author": actor("dave"),
                                "reviews": {"nodes": [], "pageInfo": {"hasNextPage": False, "endCursor": None}},
                                "timelineItems": {
                                    "nodes": [{"__typename": "MergedEvent", "createdAt": iso(29), "actor": actor("bob")}],
                                    "pageInfo": {"hasNextPage": False},
                                },
                            },
                            {
                                "number": 11,
                                "createdAt": iso(50),
                                "isDraft": False,
                                "author": actor("erin"),
                                "reviews": {"nodes": [], "pageInfo": {"hasNextPage": False, "endCursor": None}},
                                "timelineItems": {
                                    "nodes": [{"__typename": "ClosedEvent", "createdAt": iso(49), "actor": actor("erin")}],
                                    "pageInfo": {"hasNextPage": False},
                                },
                            },
                            {
                                "number": 12,
                                "createdAt": iso(20),
                                "isDraft": False,
                                "author": actor("frank"),
                                "reviews": {"nodes": [], "pageInfo": {"hasNextPage": False, "endCursor": None}},
                                "timelineItems": {
                                    "nodes": [{"__typename": "ClosedEvent", "createdAt": iso(19), "actor": None}],
                                    "pageInfo": {"hasNextPage": False},
                                },
                            },
                        ],
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                    }
                }
            }
        }

    raise AssertionError(f"Unexpected query: {query[:60]}")


def signals_with(
    issue_responsiveness: ResponsivenessSignal | None = None,
    issue_backlog: BacklogSignal | None = None,
    collected_at: str = "",
) -> MaintenanceSignals:
    return MaintenanceSignals(
        schema_version=1,
        collected_at=collected_at or NOW.isoformat(),
        window_days=180,
        response_interval_days=7,
        issue_responsiveness=issue_responsiveness,
        issue_backlog=issue_backlog,
    )


def rate_limited_exception(headers: dict[str, str] | None) -> GithubException:
    return GithubException(400, {"errors": [{"type": "RATE_LIMITED", "message": "API rate limit exceeded"}]}, headers)


def open_issues_page(total: int, ages: list[float], has_next: bool) -> dict[str, Any]:
    return {
        "data": {
            "repository": {
                "issues": {
                    "totalCount": total,
                    "nodes": [{"createdAt": iso(age)} for age in ages],
                    "pageInfo": {"hasNextPage": has_next, "endCursor": "next" if has_next else None},
                }
            }
        }
    }


def ok_signals(now: dt.datetime, *, as_of: dt.datetime | None = None) -> MaintenanceSignals:
    """A fully collected signals payload with known values."""

    stamp = (as_of or now).isoformat()

    return MaintenanceSignals(
        schema_version=1,
        collected_at=stamp,
        window_days=180,
        response_interval_days=7,
        requests_used=9,
        issues_enabled=True,
        archived=False,
        issue_responsiveness=ResponsivenessSignal(
            state=STATE_OK,
            as_of=stamp,
            window_days=180,
            interval_days=7,
            cohort_size=5,
            eligible_size=5,
            responded_within_interval=4,
            response_fraction=0.8,
            median_response_days_context=1.5,
        ),
        issue_backlog=BacklogSignal(state=STATE_OK, as_of=stamp, open_count=3, median_open_age_days=40.0),
        pr_responsiveness=ResponsivenessSignal(
            state=STATE_OK,
            as_of=stamp,
            window_days=180,
            interval_days=7,
            include_drafts=True,
            cohort_size=4,
            eligible_size=4,
            responded_within_interval=2,
            response_fraction=0.5,
        ),
        pr_backlog=BacklogSignal(state=STATE_OK, as_of=stamp, open_count=2, median_open_age_days=10.0),
    )
