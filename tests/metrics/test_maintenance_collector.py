"""
Tests for maintenance-signal collection flows over canned GraphQL responses:
full-repo collection, comment/close interaction, backlog resilience, and
review pagination.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import datetime as dt
import time
from types import SimpleNamespace
from typing import Any

import pytest
from github import GithubException

import pg_atlas.procrastinate.github_maintenance as gm
import pg_atlas.procrastinate.github_maintenance_schema as gms
from pg_atlas.config import settings
from pg_atlas.db_models.release import Release
from pg_atlas.metrics.maintenance import (
    STATE_INCOMPLETE,
    STATE_NOT_APPLICABLE,
    STATE_OK,
)
from pg_atlas.procrastinate.github_maintenance import (
    MaintenanceCollectionFailed,
    collect_maintenance_for_repo,
)
from tests.metrics.maintenance_support import (
    NOW,
    FakeRequester,
    actor,
    happy_path_handler,
    iso,
    open_issues_page,
    patch_client,
    releases_repo,
)


class TestCollectMaintenanceForRepo:
    async def test_happy_path_signals(self, monkeypatch: pytest.MonkeyPatch, default_parameters: None) -> None:
        requester = FakeRequester(happy_path_handler)
        patch_client(monkeypatch, requester)

        collected = await collect_maintenance_for_repo("owner", "repo", need_release_fallback=False, now=NOW)
        signals = collected.signals

        assert collected.pushed_at == NOW - dt.timedelta(days=2)
        assert signals.issues_enabled is True
        assert signals.archived is False
        assert signals.requests_used == len(requester.calls)

        assert signals.issue_backlog is not None
        assert signals.issue_backlog.state == STATE_OK
        assert signals.issue_backlog.open_count == 2
        assert signals.issue_backlog.median_open_age_days == 55.0

        assert signals.pr_backlog is not None
        assert signals.pr_backlog.open_count == 1
        assert signals.pr_backlog.median_open_age_days == 20.0

        # Issue #1 answered by a MEMBER comment within the interval; #6 closed
        # by a merged PR within the interval (a code close is a response); #2
        # got only the author's own, a bot's, and an unassociated comment; #7
        # was closed manually (never a response); #4 is the maintainer's own
        # work and leaves the cohort; #3 is outside the window and stops the
        # listing.
        assert signals.issue_responsiveness is not None
        assert signals.issue_responsiveness.state == STATE_OK
        assert signals.issue_responsiveness.cohort_size == 4
        assert signals.issue_responsiveness.maintainer_authored_count == 1
        assert signals.issue_responsiveness.eligible_size == 4
        assert signals.issue_responsiveness.responded_within_interval == 2
        assert signals.issue_responsiveness.response_fraction == 0.5

        # PR #10 merged by another human, #11 withdrawn by its author
        # (unanswered), #12 closed by an unresolvable actor (indeterminate),
        # #13 authored by a maintainer and excluded from the cohort.
        assert signals.pr_responsiveness is not None
        assert signals.pr_responsiveness.cohort_size == 2
        assert signals.pr_responsiveness.maintainer_authored_count == 1
        assert signals.pr_responsiveness.responded_within_interval == 1
        assert signals.pr_responsiveness.response_fraction == 0.5
        assert signals.pr_responsiveness.unknown_attribution_count == 1

        assert signals.release_fallback is None

    async def test_issues_disabled_is_not_applicable(self, monkeypatch: pytest.MonkeyPatch, default_parameters: None) -> None:
        def handler(query: str, variables: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
            if query == gms.OVERVIEW_QUERY:
                return {}, {"data": {"repository": {"pushedAt": iso(2), "hasIssuesEnabled": False, "isArchived": False}}}

            return happy_path_handler(query, variables)

        requester = FakeRequester(handler)
        patch_client(monkeypatch, requester)

        collected = await collect_maintenance_for_repo("owner", "repo", need_release_fallback=False, now=NOW)
        signals = collected.signals

        assert signals.issue_responsiveness is not None
        assert signals.issue_responsiveness.state == STATE_NOT_APPLICABLE
        assert signals.issue_backlog is not None
        assert signals.issue_backlog.state == STATE_NOT_APPLICABLE
        assert signals.pr_responsiveness is not None
        assert signals.pr_responsiveness.state == STATE_OK
        assert all(query != gms.OPEN_ISSUES_QUERY and query != gms.ISSUE_COHORT_QUERY for query, _ in requester.calls)

    async def test_declared_external_tracker_is_not_applicable(
        self, monkeypatch: pytest.MonkeyPatch, default_parameters: None
    ) -> None:
        monkeypatch.setattr(settings, "MAINTENANCE_EXTERNAL_TRACKER_REPOS", "owner/repo")
        requester = FakeRequester(happy_path_handler)
        patch_client(monkeypatch, requester)

        collected = await collect_maintenance_for_repo("owner", "repo", need_release_fallback=False, now=NOW)

        assert collected.signals.external_tracker_declared is True
        assert collected.signals.issue_responsiveness is not None
        assert collected.signals.issue_responsiveness.state == STATE_NOT_APPLICABLE

    async def test_declared_maintainers_extend_both_cohort_and_responses(
        self, monkeypatch: pytest.MonkeyPatch, default_parameters: None
    ) -> None:
        """
        Declared logins qualify as responders (issue #2's unassociated
        comment becomes the answer) and count as maintainer authors (erin's
        issue #6 and PR #11 leave the community cohorts), and the payload
        records the list it was measured under.
        """

        monkeypatch.setattr(settings, "MAINTENANCE_DECLARED_MAINTAINERS", "owner/repo=Rando|ERIN")
        requester = FakeRequester(happy_path_handler)
        patch_client(monkeypatch, requester)

        collected = await collect_maintenance_for_repo("owner", "repo", need_release_fallback=False, now=NOW)
        signals = collected.signals

        assert signals.issue_responsiveness is not None
        assert signals.issue_responsiveness.cohort_size == 3
        assert signals.issue_responsiveness.maintainer_authored_count == 2
        assert signals.issue_responsiveness.responded_within_interval == 2
        assert signals.issue_responsiveness.response_fraction == 0.6667
        assert signals.issue_responsiveness.declared_maintainers == ["erin", "rando"]

        assert signals.pr_responsiveness is not None
        assert signals.pr_responsiveness.cohort_size == 1
        assert signals.pr_responsiveness.maintainer_authored_count == 2
        assert signals.pr_responsiveness.responded_within_interval == 1
        assert signals.pr_responsiveness.response_fraction == 1.0
        assert signals.pr_responsiveness.unknown_attribution_count == 1
        assert signals.pr_responsiveness.declared_maintainers == ["erin", "rando"]

    async def test_empty_declared_setting_records_none(
        self, monkeypatch: pytest.MonkeyPatch, default_parameters: None
    ) -> None:
        requester = FakeRequester(happy_path_handler)
        patch_client(monkeypatch, requester)

        collected = await collect_maintenance_for_repo("owner", "repo", need_release_fallback=False, now=NOW)

        assert collected.signals.issue_responsiveness is not None
        assert collected.signals.issue_responsiveness.declared_maintainers is None
        assert collected.signals.pr_responsiveness is not None
        assert collected.signals.pr_responsiveness.declared_maintainers is None

    async def test_comment_continuation_is_followed(self, monkeypatch: pytest.MonkeyPatch, default_parameters: None) -> None:
        def handler(query: str, variables: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
            if query == gms.ISSUE_COHORT_QUERY:
                return {}, {
                    "data": {
                        "repository": {
                            "issues": {
                                "nodes": [
                                    {
                                        "number": 5,
                                        "createdAt": iso(30),
                                        "author": actor("alice"),
                                        "comments": {
                                            "nodes": [
                                                {
                                                    "createdAt": iso(29.9),
                                                    "authorAssociation": "NONE",
                                                    "author": actor("rando"),
                                                }
                                            ],
                                            "pageInfo": {"hasNextPage": True, "endCursor": "c1"},
                                        },
                                    }
                                ],
                                "pageInfo": {"hasNextPage": False, "endCursor": None},
                            }
                        }
                    }
                }

            if query == gms.ISSUE_COMMENTS_QUERY:
                assert variables["number"] == 5
                assert variables["after"] == "c1"
                return {}, {
                    "data": {
                        "repository": {
                            "issue": {
                                "comments": {
                                    "nodes": [
                                        {"createdAt": iso(29), "authorAssociation": "COLLABORATOR", "author": actor("bob")}
                                    ],
                                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                                }
                            }
                        }
                    }
                }

            return happy_path_handler(query, variables)

        requester = FakeRequester(handler)
        patch_client(monkeypatch, requester)

        collected = await collect_maintenance_for_repo("owner", "repo", need_release_fallback=False, now=NOW)

        assert collected.signals.issue_responsiveness is not None
        assert collected.signals.issue_responsiveness.responded_within_interval == 1

    async def test_draft_prs_excluded_when_configured(self, monkeypatch: pytest.MonkeyPatch, default_parameters: None) -> None:
        monkeypatch.setattr(settings, "MAINTENANCE_INCLUDE_DRAFT_PRS", False)

        def handler(query: str, variables: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
            if query == gms.PULL_COHORT_QUERY:
                return {}, {
                    "data": {
                        "repository": {
                            "pullRequests": {
                                "nodes": [
                                    {
                                        "number": 20,
                                        "createdAt": iso(30),
                                        "isDraft": True,
                                        "author": actor("dave"),
                                        "reviews": {"nodes": [], "pageInfo": {"hasNextPage": False, "endCursor": None}},
                                        "timelineItems": {"nodes": [], "pageInfo": {"hasNextPage": False}},
                                    }
                                ],
                                "pageInfo": {"hasNextPage": False, "endCursor": None},
                            }
                        }
                    }
                }

            return happy_path_handler(query, variables)

        patch_client(monkeypatch, FakeRequester(handler))

        collected = await collect_maintenance_for_repo("owner", "repo", need_release_fallback=False, now=NOW)

        assert collected.signals.pr_responsiveness is not None
        assert collected.signals.pr_responsiveness.cohort_size == 0

    async def test_rate_limit_halt_marks_remaining_signals_incomplete(
        self, monkeypatch: pytest.MonkeyPatch, default_parameters: None
    ) -> None:
        """An over-cap reset wait halts without sleeping; nothing is guessed."""

        def handler(query: str, variables: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
            if query == gms.OVERVIEW_QUERY:
                return {}, {"data": {"repository": {"pushedAt": iso(2), "hasIssuesEnabled": True, "isArchived": False}}}

            raise GithubException(
                400,
                {"errors": [{"type": "RATE_LIMITED", "message": "API rate limit exceeded"}]},
                {"x-ratelimit-reset": str(time.time() + 3600)},
            )

        patch_client(monkeypatch, FakeRequester(handler))

        collected = await collect_maintenance_for_repo("owner", "repo", need_release_fallback=True, now=NOW)
        signals = collected.signals

        for signal in (signals.issue_responsiveness, signals.issue_backlog, signals.pr_responsiveness, signals.pr_backlog):
            assert signal is not None
            assert signal.state == STATE_INCOMPLETE
            assert signal.incomplete_reason == gm.REASON_RATE_LIMITED

        assert signals.release_fallback is not None
        assert signals.release_fallback.state == STATE_INCOMPLETE
        assert collected.pushed_at is not None

    async def test_overview_failure_raises_instead_of_writing_garbage(
        self, monkeypatch: pytest.MonkeyPatch, default_parameters: None
    ) -> None:
        def handler(query: str, variables: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
            raise GithubException(500, {"message": "boom"}, {})

        patch_client(monkeypatch, FakeRequester(handler))

        with pytest.raises(MaintenanceCollectionFailed):
            await collect_maintenance_for_repo("owner", "repo", need_release_fallback=False, now=NOW)

    async def test_release_fallback_collects_dated_non_draft_releases(
        self, monkeypatch: pytest.MonkeyPatch, default_parameters: None
    ) -> None:
        monkeypatch.setattr(settings, "MAINTENANCE_INCLUDE_PRERELEASES", False)

        fake_repo, _ = releases_repo(
            [
                SimpleNamespace(draft=False, prerelease=False, published_at=NOW - dt.timedelta(days=10)),
                SimpleNamespace(draft=True, prerelease=False, published_at=NOW - dt.timedelta(days=20)),
                SimpleNamespace(draft=False, prerelease=True, published_at=NOW - dt.timedelta(days=30)),
                SimpleNamespace(draft=False, prerelease=False, published_at=None),
                SimpleNamespace(draft=False, prerelease=False, published_at=NOW - dt.timedelta(days=40)),
            ]
        )

        def _get_repo(path: str) -> SimpleNamespace:
            return fake_repo

        requester = FakeRequester(happy_path_handler)
        patch_client(monkeypatch, requester, get_repo=_get_repo)

        collected = await collect_maintenance_for_repo("owner", "repo", need_release_fallback=True, now=NOW)

        fallback = collected.signals.release_fallback
        assert fallback is not None
        assert fallback.state == STATE_OK
        assert fallback.publication_dates == [
            (NOW - dt.timedelta(days=40)).date().isoformat(),
            (NOW - dt.timedelta(days=10)).date().isoformat(),
        ]
        assert fallback.include_prereleases is False


class TestCodeCloseCommentInteraction:
    async def test_out_of_interval_close_does_not_stop_comment_search(
        self, monkeypatch: pytest.MonkeyPatch, default_parameters: None
    ) -> None:
        """A late close must not hide a within-interval comment on later pages."""

        continuation_calls = {"count": 0}

        def handler(query: str, variables: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
            if query == gms.ISSUE_COHORT_QUERY:
                return {}, {
                    "data": {
                        "repository": {
                            "issues": {
                                "nodes": [
                                    {
                                        "number": 11,
                                        "createdAt": iso(33),
                                        "author": actor("alice"),
                                        "comments": {
                                            "nodes": [
                                                {
                                                    "createdAt": iso(32),
                                                    "authorAssociation": "NONE",
                                                    "author": actor("rando"),
                                                }
                                            ],
                                            "pageInfo": {"hasNextPage": True, "endCursor": "c1"},
                                        },
                                        "timelineItems": {
                                            "nodes": [
                                                {
                                                    "__typename": "ClosedEvent",
                                                    "createdAt": iso(20),  # 13 days after creation
                                                    "closer": {"__typename": "PullRequest"},
                                                }
                                            ],
                                            "pageInfo": {"hasNextPage": False, "endCursor": None},
                                        },
                                    }
                                ],
                                "pageInfo": {"hasNextPage": False, "endCursor": None},
                            }
                        }
                    }
                }

            if query == gms.ISSUE_COMMENTS_QUERY:
                continuation_calls["count"] += 1
                return {}, {
                    "data": {
                        "repository": {
                            "issue": {
                                "comments": {
                                    "nodes": [
                                        {
                                            "createdAt": iso(29),  # 4 days after creation
                                            "authorAssociation": "MEMBER",
                                            "author": actor("bob"),
                                        }
                                    ],
                                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                                }
                            }
                        }
                    }
                }

            return happy_path_handler(query, variables)

        patch_client(monkeypatch, FakeRequester(handler))

        collected = await collect_maintenance_for_repo("owner", "repo", need_release_fallback=False, now=NOW)

        assert continuation_calls["count"] == 1
        signals = collected.signals
        assert signals.issue_responsiveness is not None
        # Issue #11 was answered by the MEMBER comment 4 days in, even though
        # the code close came 13 days in — after the interval.
        assert signals.issue_responsiveness.cohort_size == 1
        assert signals.issue_responsiveness.responded_within_interval == 1
        assert signals.issue_responsiveness.response_fraction == 1.0

    async def test_within_interval_close_skips_comment_pagination(
        self, monkeypatch: pytest.MonkeyPatch, default_parameters: None
    ) -> None:
        """A close that already settles the item spends no extra requests."""

        def handler(query: str, variables: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
            if query == gms.ISSUE_COHORT_QUERY:
                return {}, {
                    "data": {
                        "repository": {
                            "issues": {
                                "nodes": [
                                    {
                                        "number": 12,
                                        "createdAt": iso(30),
                                        "author": actor("alice"),
                                        "comments": {
                                            "nodes": [],
                                            "pageInfo": {"hasNextPage": True, "endCursor": "c1"},
                                        },
                                        "timelineItems": {
                                            "nodes": [
                                                {
                                                    "__typename": "ClosedEvent",
                                                    "createdAt": iso(28),  # 2 days after creation
                                                    "closer": {"__typename": "Commit"},
                                                }
                                            ],
                                            "pageInfo": {"hasNextPage": False, "endCursor": None},
                                        },
                                    }
                                ],
                                "pageInfo": {"hasNextPage": False, "endCursor": None},
                            }
                        }
                    }
                }

            assert query != gms.ISSUE_COMMENTS_QUERY, "settled item must not paginate comments"

            return happy_path_handler(query, variables)

        patch_client(monkeypatch, FakeRequester(handler))

        collected = await collect_maintenance_for_repo("owner", "repo", need_release_fallback=False, now=NOW)

        assert collected.signals.issue_responsiveness is not None
        assert collected.signals.issue_responsiveness.cohort_size == 1
        assert collected.signals.issue_responsiveness.responded_within_interval == 1


class TestBacklogResilience:
    async def test_median_short_circuit_stops_paging(self, monkeypatch: pytest.MonkeyPatch, default_parameters: None) -> None:
        """Once the oldest-first prefix holds the middle order statistic, no more pages are fetched."""

        issue_page_calls = {"count": 0}
        ages = [float(170 - i) for i in range(100)]

        def handler(query: str, variables: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
            if query == gms.OPEN_ISSUES_QUERY:
                issue_page_calls["count"] += 1
                return {}, open_issues_page(150, ages, has_next=True)

            return happy_path_handler(query, variables)

        patch_client(monkeypatch, FakeRequester(handler))

        collected = await collect_maintenance_for_repo("owner", "repo", need_release_fallback=False, now=NOW)

        assert issue_page_calls["count"] == 1
        backlog = collected.signals.issue_backlog
        assert backlog is not None
        assert backlog.state == STATE_OK
        assert backlog.open_count == 150
        assert backlog.median_age_exact is True
        # Median of 150 ages = mean of the 75th and 76th oldest: (96 + 95) / 2.
        assert backlog.median_open_age_days == 95.5

    async def test_budget_halt_on_continuation_keeps_exact_count(
        self, monkeypatch: pytest.MonkeyPatch, default_parameters: None
    ) -> None:
        """A shared-cap halt mid-listing keeps the server-side count and stops later phases."""

        monkeypatch.setattr(settings, "MAINTENANCE_REQUEST_CAP", 2)
        ages = [float(170 - i) for i in range(100)]

        def handler(query: str, variables: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
            if query == gms.OPEN_ISSUES_QUERY:
                return {}, open_issues_page(300, ages, has_next=True)

            return happy_path_handler(query, variables)

        patch_client(monkeypatch, FakeRequester(handler))

        collected = await collect_maintenance_for_repo("owner", "repo", need_release_fallback=False, now=NOW)
        signals = collected.signals

        assert signals.issue_backlog is not None
        assert signals.issue_backlog.state == STATE_INCOMPLETE
        assert signals.issue_backlog.open_count == 300
        assert signals.issue_backlog.incomplete_reason == gm.REASON_REQUEST_CAP
        assert signals.issue_backlog.median_open_age_days is None

        for later in (signals.pr_backlog, signals.issue_responsiveness, signals.pr_responsiveness):
            assert later is not None
            assert later.state == STATE_INCOMPLETE
            assert later.incomplete_reason == gm.REASON_REQUEST_CAP


class TestReviewPagination:
    async def test_late_first_page_review_does_not_stop_the_search(
        self, monkeypatch: pytest.MonkeyPatch, default_parameters: None
    ) -> None:
        """Reviews carry no submission-order contract; an earlier submission can sit on a later page."""

        continuation_calls = {"count": 0}

        def handler(query: str, variables: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
            if query == gms.PULL_COHORT_QUERY:
                return {}, {
                    "data": {
                        "repository": {
                            "pullRequests": {
                                "nodes": [
                                    {
                                        "number": 30,
                                        "createdAt": iso(30),
                                        "isDraft": False,
                                        "author": actor("alice"),
                                        "reviews": {
                                            "nodes": [
                                                {
                                                    "submittedAt": iso(20),  # 10 days after creation
                                                    "authorAssociation": "MEMBER",
                                                    "author": actor("bob"),
                                                }
                                            ],
                                            "pageInfo": {"hasNextPage": True, "endCursor": "r1"},
                                        },
                                        "timelineItems": {"nodes": [], "pageInfo": {"hasNextPage": False}},
                                    }
                                ],
                                "pageInfo": {"hasNextPage": False, "endCursor": None},
                            }
                        }
                    }
                }

            if query == gms.PULL_REVIEWS_QUERY:
                continuation_calls["count"] += 1
                return {}, {
                    "data": {
                        "repository": {
                            "pullRequest": {
                                "reviews": {
                                    "nodes": [
                                        {
                                            "submittedAt": iso(28),  # 2 days after creation
                                            "authorAssociation": "OWNER",
                                            "author": actor("carol"),
                                        }
                                    ],
                                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                                }
                            }
                        }
                    }
                }

            return happy_path_handler(query, variables)

        patch_client(monkeypatch, FakeRequester(handler))

        collected = await collect_maintenance_for_repo("owner", "repo", need_release_fallback=False, now=NOW)

        assert continuation_calls["count"] == 1
        pr = collected.signals.pr_responsiveness
        assert pr is not None
        assert pr.cohort_size == 1
        assert pr.responded_within_interval == 1
        assert pr.response_fraction == 1.0

    async def test_truncated_pr_timeline_is_indeterminate(
        self, monkeypatch: pytest.MonkeyPatch, default_parameters: None
    ) -> None:
        def handler(query: str, variables: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
            if query == gms.PULL_COHORT_QUERY:
                return {}, {
                    "data": {
                        "repository": {
                            "pullRequests": {
                                "nodes": [
                                    {
                                        "number": 31,
                                        "createdAt": iso(30),
                                        "isDraft": False,
                                        "author": actor("alice"),
                                        "reviews": {"nodes": [], "pageInfo": {"hasNextPage": False, "endCursor": None}},
                                        "timelineItems": {
                                            "nodes": [],
                                            "pageInfo": {"hasNextPage": True, "endCursor": "t1"},
                                        },
                                    }
                                ],
                                "pageInfo": {"hasNextPage": False, "endCursor": None},
                            }
                        }
                    }
                }

            return happy_path_handler(query, variables)

        patch_client(monkeypatch, FakeRequester(handler))

        collected = await collect_maintenance_for_repo("owner", "repo", need_release_fallback=False, now=NOW)

        pr = collected.signals.pr_responsiveness
        assert pr is not None
        assert pr.cohort_size == 0
        assert pr.unknown_attribution_count == 1


# ---------------------------------------------------------------------------
# Release-fallback decision in run_maintenance_collection
# ---------------------------------------------------------------------------


class _FakeSession:
    """Async context-managed session stand-in; the persistence calls are faked."""

    async def __aenter__(self) -> _FakeSession:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        return None

    async def commit(self) -> None:
        return None


class TestReleaseFallbackDecision:
    async def _fallback_flag(self, monkeypatch: pytest.MonkeyPatch, releases: list[Release]) -> bool:
        """Run one collection over faked I/O and return the fallback flag it requested."""

        import pg_atlas.db_models.session as session_module
        from pg_atlas.metrics.maintenance import MaintenanceSignals
        from pg_atlas.procrastinate.github_maintenance import CollectedMaintenance
        from pg_atlas.procrastinate.github_maintenance_persistence import ResolvedRepo

        signals = MaintenanceSignals(schema_version=1, collected_at=NOW.isoformat(), window_days=180, response_interval_days=7)
        requested: list[bool] = []

        async def _resolve(session: object, owner: str, repo: str) -> ResolvedRepo:
            return ResolvedRepo(repo_id=1, canonical_id=f"pkg:github/{owner}/{repo}", releases=releases)

        async def _collect(
            owner: str, repo: str, *, need_release_fallback: bool, now: dt.datetime | None = None
        ) -> CollectedMaintenance:
            requested.append(need_release_fallback)

            return CollectedMaintenance(signals=signals, pushed_at=None)

        async def _persist(
            session: object, resolved: ResolvedRepo, collected: MaintenanceSignals, pushed_at: dt.datetime | None
        ) -> MaintenanceSignals:
            return collected

        monkeypatch.setattr(session_module, "get_session_factory", lambda: _FakeSession)
        monkeypatch.setattr(gm, "resolve_repo", _resolve)
        monkeypatch.setattr(gm, "collect_maintenance_for_repo", _collect)
        monkeypatch.setattr(gm, "persist_collected", _persist)

        await gm.run_maintenance_collection("soneso", "stellar-ios-mac-sdk")

        assert len(requested) == 1

        return requested[0]

    async def test_pseudo_versions_only_requests_the_github_releases_fallback(
        self, monkeypatch: pytest.MonkeyPatch, default_parameters: None
    ) -> None:
        """Commit-day pseudo-versions are no dated coverage, however many days they span."""

        monkeypatch.setattr(settings, "MAINTENANCE_CADENCE_MIN_EVENTS", 3)
        releases = [
            Release(
                purl="pkg:golang/github.com/soneso/stellar-ios-mac-sdk",
                version=f"v0.0.0-202608{day:02d}093000-3f2a1b4c5d6e",
                release_date=f"2026-08-{day:02d}T09:30:00Z",
            )
            for day in range(1, 11)
        ]

        assert await self._fallback_flag(monkeypatch, releases) is True

    async def test_tagged_golang_releases_are_dated_coverage(
        self, monkeypatch: pytest.MonkeyPatch, default_parameters: None
    ) -> None:
        monkeypatch.setattr(settings, "MAINTENANCE_CADENCE_MIN_EVENTS", 3)
        releases = [
            Release(
                purl="pkg:golang/github.com/soneso/stellar-ios-mac-sdk",
                version=f"v3.{minor}.0",
                release_date=f"2026-0{minor + 5}-01T09:30:00Z",
            )
            for minor in range(3)
        ]

        assert await self._fallback_flag(monkeypatch, releases) is False
