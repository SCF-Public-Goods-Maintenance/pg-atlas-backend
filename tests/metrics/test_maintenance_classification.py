"""
Unit tests for maintenance-collection gating and pure classification: the
fail-closed gate, actor and bot rules, comment/review scans, declared
maintainer logins, PR event classification, code-close classification, and
response-item resolution.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import datetime as dt
from typing import Any

import pytest

import pg_atlas.procrastinate.github_maintenance as gm
import pg_atlas.procrastinate.github_maintenance_schema as gms
from pg_atlas.config import settings
from pg_atlas.procrastinate.github_maintenance import (
    _classify_pr_events,
    _resolve_response_item,
    collect_maintenance_for_repo,
    external_tracker_declared,
    maintenance_host_repo,
    maintenance_metric_allowed,
)
from tests.metrics.maintenance_support import (
    NOW,
    FakeRequester,
    actor,
    happy_path_handler,
    iso,
    patch_client,
)


class TestGate:
    def test_disabled_flag_allows_nothing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(settings, "MAINTENANCE_METRIC_ENABLED", False)
        monkeypatch.setattr(settings, "MAINTENANCE_METRIC_ALLOWLIST", "*")
        monkeypatch.setattr(settings, "MAINTENANCE_HOST_REPOS", "")
        assert not maintenance_metric_allowed("Soneso", "stellar-php-sdk")

    def test_empty_allowlist_allows_nothing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(settings, "MAINTENANCE_METRIC_ENABLED", True)
        monkeypatch.setattr(settings, "MAINTENANCE_METRIC_ALLOWLIST", "")
        monkeypatch.setattr(settings, "MAINTENANCE_HOST_REPOS", "")
        assert not maintenance_metric_allowed("Soneso", "stellar-php-sdk")

    def test_allowlist_match_is_case_insensitive(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(settings, "MAINTENANCE_METRIC_ENABLED", True)
        monkeypatch.setattr(settings, "MAINTENANCE_METRIC_ALLOWLIST", "soneso/STELLAR-php-sdk, Other/repo")
        monkeypatch.setattr(settings, "MAINTENANCE_HOST_REPOS", "")
        assert maintenance_metric_allowed("Soneso", "stellar-php-sdk")
        assert maintenance_metric_allowed("other", "REPO")
        assert not maintenance_metric_allowed("Soneso", "stellar_flutter_sdk")

    def test_star_allows_all(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(settings, "MAINTENANCE_METRIC_ENABLED", True)
        monkeypatch.setattr(settings, "MAINTENANCE_METRIC_ALLOWLIST", "*")
        monkeypatch.setattr(settings, "MAINTENANCE_HOST_REPOS", "")
        assert maintenance_metric_allowed("any", "repo")

    def test_malformed_entries_are_rejected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(settings, "MAINTENANCE_METRIC_ENABLED", True)
        monkeypatch.setattr(settings, "MAINTENANCE_METRIC_ALLOWLIST", "just-a-name, owner/, /repo, a/b/c")
        monkeypatch.setattr(settings, "MAINTENANCE_HOST_REPOS", "")
        assert not maintenance_metric_allowed("just-a-name", "")
        assert not maintenance_metric_allowed("owner", "")
        assert not maintenance_metric_allowed("a", "b")

    def test_external_tracker_declaration(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(settings, "MAINTENANCE_EXTERNAL_TRACKER_REPOS", "Soneso/stellar-php-sdk")
        assert external_tracker_declared("soneso", "STELLAR-PHP-SDK")
        assert not external_tracker_declared("soneso", "other")

    def test_host_repo_declaration(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(settings, "MAINTENANCE_HOST_REPOS", "Trezor/trezor-firmware, LedgerHQ/ledger-live")
        assert maintenance_host_repo("trezor", "TREZOR-FIRMWARE")
        assert maintenance_host_repo("ledgerhq", "ledger-live")
        assert not maintenance_host_repo("trezor", "other")

    def test_host_repo_is_excluded_from_star_allowlist(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(settings, "MAINTENANCE_METRIC_ENABLED", True)
        monkeypatch.setattr(settings, "MAINTENANCE_METRIC_ALLOWLIST", "*")
        monkeypatch.setattr(settings, "MAINTENANCE_HOST_REPOS", "trezor/trezor-firmware")
        assert not maintenance_metric_allowed("Trezor", "trezor-firmware")
        assert maintenance_metric_allowed("other", "repo")

    def test_host_repo_is_excluded_despite_explicit_allowlist_match(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(settings, "MAINTENANCE_METRIC_ENABLED", True)
        monkeypatch.setattr(settings, "MAINTENANCE_METRIC_ALLOWLIST", "trezor/trezor-firmware")
        monkeypatch.setattr(settings, "MAINTENANCE_HOST_REPOS", "trezor/trezor-firmware")
        assert not maintenance_metric_allowed("trezor", "trezor-firmware")

    def test_malformed_host_entry_declares_nothing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The lookup key for ("owner", "") equals a verbatim-kept "owner/"
        entry, so this probe pins the shared parser's rejection."""

        monkeypatch.setattr(settings, "MAINTENANCE_HOST_REPOS", "just-a-name, owner/, a/b/c")
        assert not maintenance_host_repo("owner", "")


class TestActorClassification:
    def test_bot_typename_is_bot(self) -> None:
        assert gm._is_bot_actor(gms.ActorRef(typename="Bot", login="github-actions"))

    def test_bot_login_pattern_is_bot(self) -> None:
        assert gm._is_bot_actor(gms.ActorRef(typename="User", login="dependabot"))

    def test_regular_user_is_not_bot(self) -> None:
        assert not gm._is_bot_actor(gms.ActorRef(typename="User", login="alice"))

    def test_missing_actor_is_not_classified_as_bot(self) -> None:
        assert not gm._is_bot_actor(None)


class TestScanCommentPage:
    def test_first_maintainer_comment_wins(self) -> None:
        comments = [
            gms.CommentNode(createdAt=iso(29.5), authorAssociation="NONE", author=gms.ActorRef(login="rando")),
            gms.CommentNode(createdAt=iso(29), authorAssociation="MEMBER", author=gms.ActorRef(login="bob")),
            gms.CommentNode(createdAt=iso(28), authorAssociation="OWNER", author=gms.ActorRef(login="carol")),
        ]
        scan = gm._scan_comment_page(comments, "alice", frozenset())
        assert scan.qualifying_time == NOW - dt.timedelta(days=29)
        assert scan.unknown_times == []

    def test_bot_and_author_comments_do_not_qualify(self) -> None:
        comments = [
            gms.CommentNode(createdAt=iso(29), authorAssociation="MEMBER", author=gms.ActorRef(typename="Bot", login="x")),
            gms.CommentNode(createdAt=iso(28), authorAssociation="OWNER", author=gms.ActorRef(login="alice")),
        ]
        scan = gm._scan_comment_page(comments, "alice", frozenset())
        assert scan.qualifying_time is None
        assert scan.unknown_times == []

    def test_unresolvable_author_with_qualifying_association_is_unknown(self) -> None:
        """Attribution is reported, never guessed — mirroring the event rule."""
        comments = [gms.CommentNode(createdAt=iso(29), authorAssociation="MEMBER", author=None)]
        scan = gm._scan_comment_page(comments, "alice", frozenset())
        assert scan.qualifying_time is None
        assert scan.unknown_times == [NOW - dt.timedelta(days=29)]

    def test_unresolvable_author_without_qualifying_association_is_nothing(self) -> None:
        """Deleted accounts normally report NONE and simply fail the filter."""
        comments = [gms.CommentNode(createdAt=iso(29), authorAssociation="NONE", author=None)]
        scan = gm._scan_comment_page(comments, "alice", frozenset())
        assert scan.qualifying_time is None
        assert scan.unknown_times == []


class TestScanReviewPage:
    def test_earliest_qualifying_review_wins(self) -> None:
        reviews = [
            gms.ReviewNode(submittedAt=iso(28), authorAssociation="MEMBER", author=gms.ActorRef(login="bob")),
            gms.ReviewNode(submittedAt=iso(29), authorAssociation="OWNER", author=gms.ActorRef(login="carol")),
        ]
        scan = gm._scan_review_page(reviews, "alice", frozenset())
        assert scan.qualifying_time == NOW - dt.timedelta(days=29)

    def test_unsubmitted_bot_and_unknown_reviews(self) -> None:
        reviews = [
            gms.ReviewNode(submittedAt=None, authorAssociation="MEMBER", author=gms.ActorRef(login="bob")),
            gms.ReviewNode(submittedAt=iso(29), authorAssociation="MEMBER", author=gms.ActorRef(typename="Bot", login="b")),
            gms.ReviewNode(submittedAt=iso(28), authorAssociation="COLLABORATOR", author=None),
        ]
        scan = gm._scan_review_page(reviews, "alice", frozenset())
        assert scan.qualifying_time is None
        assert scan.unknown_times == [NOW - dt.timedelta(days=28)]

    def test_unresolvable_review_author_without_qualifying_association_is_nothing(self) -> None:
        """Deleted accounts normally report NONE and simply fail the filter."""
        reviews = [gms.ReviewNode(submittedAt=iso(29), authorAssociation="NONE", author=None)]
        scan = gm._scan_review_page(reviews, "alice", frozenset())
        assert scan.qualifying_time is None
        assert scan.unknown_times == []


class TestDeclaredMaintainerScans:
    def test_declared_login_qualifies_without_association(self) -> None:
        comments = [gms.CommentNode(createdAt=iso(29), authorAssociation="NONE", author=gms.ActorRef(login="rando"))]
        scan = gm._scan_comment_page(comments, "alice", frozenset({"rando"}))
        assert scan.qualifying_time == NOW - dt.timedelta(days=29)

    def test_undeclared_login_without_association_does_not_qualify(self) -> None:
        comments = [gms.CommentNode(createdAt=iso(29), authorAssociation="NONE", author=gms.ActorRef(login="rando"))]
        scan = gm._scan_comment_page(comments, "alice", frozenset({"someone-else"}))
        assert scan.qualifying_time is None

    def test_declared_login_matches_case_insensitively(self) -> None:
        comments = [gms.CommentNode(createdAt=iso(29), authorAssociation="NONE", author=gms.ActorRef(login="RanDo"))]
        scan = gm._scan_comment_page(comments, "alice", frozenset({"rando"}))
        assert scan.qualifying_time == NOW - dt.timedelta(days=29)

    def test_declared_item_author_still_does_not_answer_own_item(self) -> None:
        comments = [gms.CommentNode(createdAt=iso(29), authorAssociation="NONE", author=gms.ActorRef(login="alice"))]
        scan = gm._scan_comment_page(comments, "alice", frozenset({"alice"}))
        assert scan.qualifying_time is None

    def test_declared_bot_login_still_excluded(self) -> None:
        comments = [gms.CommentNode(createdAt=iso(29), authorAssociation="NONE", author=gms.ActorRef(login="dependabot"))]
        scan = gm._scan_comment_page(comments, "alice", frozenset({"dependabot"}))
        assert scan.qualifying_time is None

    def test_declared_review_qualifies_without_association(self) -> None:
        reviews = [gms.ReviewNode(submittedAt=iso(29), authorAssociation="NONE", author=gms.ActorRef(login="rando"))]
        scan = gm._scan_review_page(reviews, "alice", frozenset({"rando"}))
        assert scan.qualifying_time == NOW - dt.timedelta(days=29)


class TestClassifyPrEvents:
    def test_merge_by_other_human_qualifies(self) -> None:
        events = [gms.TimelineNode(typename="MergedEvent", createdAt=iso(29), actor=gms.ActorRef(login="bob"))]
        result = _classify_pr_events(events, "alice")
        assert result.qualifying_times == [NOW - dt.timedelta(days=29)]
        assert result.unknown_times == []

    def test_close_by_other_human_qualifies_as_rejection(self) -> None:
        """A prompt human rejection is a response."""
        events = [gms.TimelineNode(typename="ClosedEvent", createdAt=iso(29), actor=gms.ActorRef(login="bob"))]
        result = _classify_pr_events(events, "alice")
        assert result.qualifying_times == [NOW - dt.timedelta(days=29)]
        assert result.unknown_times == []

    def test_author_withdrawal_does_not_qualify(self) -> None:
        events = [gms.TimelineNode(typename="ClosedEvent", createdAt=iso(29), actor=gms.ActorRef(login="alice"))]
        result = _classify_pr_events(events, "alice")
        assert result.qualifying_times == []
        assert result.unknown_times == []

    def test_bot_merge_does_not_qualify(self) -> None:
        events = [gms.TimelineNode(typename="MergedEvent", createdAt=iso(29), actor=gms.ActorRef(typename="Bot", login="b"))]
        assert _classify_pr_events(events, "alice").qualifying_times == []

    def test_missing_actor_is_unknown_attribution(self) -> None:
        events = [gms.TimelineNode(typename="ClosedEvent", createdAt=iso(29), actor=None)]
        result = _classify_pr_events(events, "alice")
        assert result.qualifying_times == []
        assert result.unknown_times == [NOW - dt.timedelta(days=29)]


class TestResolveResponseItem:
    def test_qualifying_within_interval_settles_item(self) -> None:
        item = _resolve_response_item(
            NOW - dt.timedelta(days=30),
            [NOW - dt.timedelta(days=29)],
            [NOW - dt.timedelta(days=29.5)],
            interval_days=7,
        )
        assert item.first_response_at == NOW - dt.timedelta(days=29)
        assert not item.indeterminate

    def test_unknown_within_interval_without_qualifying_is_indeterminate(self) -> None:
        item = _resolve_response_item(
            NOW - dt.timedelta(days=30),
            [],
            [NOW - dt.timedelta(days=29)],
            interval_days=7,
        )
        assert item.indeterminate

    def test_late_qualifying_with_late_unknown_stays_determinate(self) -> None:
        item = _resolve_response_item(
            NOW - dt.timedelta(days=30),
            [NOW - dt.timedelta(days=10)],
            [NOW - dt.timedelta(days=12)],
            interval_days=7,
        )
        assert not item.indeterminate
        assert item.first_response_at == NOW - dt.timedelta(days=10)

    def test_truncated_detection_is_indeterminate(self) -> None:
        item = _resolve_response_item(NOW - dt.timedelta(days=30), [], [], interval_days=7, detection_truncated=True)
        assert item.indeterminate

    def test_nothing_found_is_unanswered(self) -> None:
        item = _resolve_response_item(NOW - dt.timedelta(days=30), [], [], interval_days=7)
        assert item.first_response_at is None
        assert not item.indeterminate


class TestClassifyIssueCloseEvents:
    def test_close_by_merged_pr_qualifies(self) -> None:
        events = [gms.TimelineNode(typename="ClosedEvent", createdAt=iso(29), closer=gms.CloserRef(typename="PullRequest"))]
        assert gm._classify_issue_close_events(events) == [NOW - dt.timedelta(days=29)]

    def test_close_by_commit_qualifies(self) -> None:
        events = [gms.TimelineNode(typename="ClosedEvent", createdAt=iso(29), closer=gms.CloserRef(typename="Commit"))]
        assert gm._classify_issue_close_events(events) == [NOW - dt.timedelta(days=29)]

    def test_manual_close_never_qualifies(self) -> None:
        """A click-close is ambiguous and mass-produceable; only code counts."""
        events = [gms.TimelineNode(typename="ClosedEvent", createdAt=iso(29), closer=None)]
        assert gm._classify_issue_close_events(events) == []

    def test_non_close_events_are_skipped(self) -> None:
        events = [gms.TimelineNode(typename="ReopenedEvent", createdAt=iso(29), closer=gms.CloserRef(typename="Commit"))]
        assert gm._classify_issue_close_events(events) == []

    def test_times_are_sorted(self) -> None:
        events = [
            gms.TimelineNode(typename="ClosedEvent", createdAt=iso(10), closer=gms.CloserRef(typename="Commit")),
            gms.TimelineNode(typename="ClosedEvent", createdAt=iso(20), closer=gms.CloserRef(typename="PullRequest")),
        ]
        assert gm._classify_issue_close_events(events) == [
            NOW - dt.timedelta(days=20),
            NOW - dt.timedelta(days=10),
        ]

    async def test_truncated_issue_timeline_is_indeterminate(
        self, monkeypatch: pytest.MonkeyPatch, default_parameters: None
    ) -> None:
        """Unfetched close pages that could hold the response are never guessed."""

        def handler(query: str, variables: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
            if query == gms.ISSUE_COHORT_QUERY:
                return {}, {
                    "data": {
                        "repository": {
                            "issues": {
                                "nodes": [
                                    {
                                        "number": 9,
                                        "createdAt": iso(30),
                                        "author": actor("alice"),
                                        "comments": {"nodes": [], "pageInfo": {"hasNextPage": False, "endCursor": None}},
                                        "timelineItems": {
                                            "nodes": [],
                                            "pageInfo": {"hasNextPage": True, "endCursor": "more"},
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

        assert collected.signals.issue_responsiveness is not None
        assert collected.signals.issue_responsiveness.cohort_size == 0
        assert collected.signals.issue_responsiveness.unknown_attribution_count == 1
