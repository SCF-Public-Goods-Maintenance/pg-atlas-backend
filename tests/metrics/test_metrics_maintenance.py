"""
Unit tests for maintenance-metric signal computation.

Covers interval-aged response cohorts where unanswered work counts against
the fraction, backlog medians as order statistics that survive capped
listings, release cadence over distinct publication dates, artifact-gated
commit activity, and inverted percentile ranking.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import datetime as dt

import pytest

from pg_atlas.db_models.release import Release
from pg_atlas.gitlog.parser import CommitRecord
from pg_atlas.metrics.maintenance import (
    CADENCE_SOURCE_DEPSDEV,
    CADENCE_SOURCE_GITHUB_RELEASES,
    REASON_PAGE_CAP,
    REASON_STALE_ARTIFACT,
    REASON_WINDOW_NOT_COVERED,
    STATE_INCOMPLETE,
    STATE_OK,
    STATE_UNAVAILABLE,
    CadenceResult,
    CommitActivityResult,
    MaintenanceSignals,
    ResponseItem,
    ResponsivenessSignal,
    compute_backlog,
    compute_commit_activity,
    compute_release_cadence,
    compute_responsiveness,
    distinct_release_dates,
    has_package_registry_release,
    is_go_pseudo_version,
    parse_declared_maintainers,
    purl_type,
    rank_scalar_values,
    signals_from_metadata,
    signals_to_payload,
)

NOW = dt.datetime(2026, 9, 9, 12, 0, tzinfo=dt.UTC)
WINDOW_DAYS = 180
INTERVAL_DAYS = 7


def _days_ago(days: float) -> dt.datetime:
    return NOW - dt.timedelta(days=days)


# ---------------------------------------------------------------------------
# Responsiveness (signals 2 and 4)
# ---------------------------------------------------------------------------


class TestComputeResponsiveness:
    def _compute(self, items: list[ResponseItem]) -> ResponsivenessSignal:
        return compute_responsiveness(
            items,
            now=NOW,
            window_days=WINDOW_DAYS,
            interval_days=INTERVAL_DAYS,
            as_of=NOW.isoformat(),
        )

    def test_declared_maintainers_recorded_sorted(self) -> None:
        items = [ResponseItem(created_at=_days_ago(30), first_response_at=_days_ago(29))]
        signal = compute_responsiveness(
            items,
            now=NOW,
            window_days=WINDOW_DAYS,
            interval_days=INTERVAL_DAYS,
            as_of=NOW.isoformat(),
            declared_maintainers=frozenset({"rando", "erin"}),
        )
        assert signal.declared_maintainers == ["erin", "rando"]

    def test_empty_declared_maintainers_recorded_as_none(self) -> None:
        signal = self._compute([ResponseItem(created_at=_days_ago(30), first_response_at=None)])
        assert signal.declared_maintainers is None

    def test_answered_within_interval_counts(self) -> None:
        items = [ResponseItem(created_at=_days_ago(30), first_response_at=_days_ago(29))]
        signal = self._compute(items)
        assert signal.state == STATE_OK
        assert signal.eligible_size == 1
        assert signal.responded_within_interval == 1
        assert signal.response_fraction == 1.0

    def test_answered_after_interval_stays_in_denominator(self) -> None:
        """A late answer counts against the fraction but feeds the context median."""
        items = [ResponseItem(created_at=_days_ago(30), first_response_at=_days_ago(10))]
        signal = self._compute(items)
        assert signal.response_fraction == 0.0
        assert signal.median_response_days_context == 20.0

    def test_unanswered_eligible_counts_against_fraction(self) -> None:
        """Unanswered work must not vanish from the measurement."""
        items = [
            ResponseItem(created_at=_days_ago(30), first_response_at=_days_ago(29.5)),
            ResponseItem(created_at=_days_ago(40), first_response_at=None),
        ]
        signal = self._compute(items)
        assert signal.eligible_size == 2
        assert signal.responded_within_interval == 1
        assert signal.response_fraction == 0.5

    def test_item_younger_than_interval_not_yet_eligible(self) -> None:
        """An item without the full response interval cannot be judged yet."""
        items = [
            ResponseItem(created_at=_days_ago(2), first_response_at=None),
            ResponseItem(created_at=_days_ago(30), first_response_at=_days_ago(29)),
        ]
        signal = self._compute(items)
        assert signal.cohort_size == 2
        assert signal.eligible_size == 1
        assert signal.response_fraction == 1.0

    def test_item_outside_window_excluded(self) -> None:
        items = [
            ResponseItem(created_at=_days_ago(WINDOW_DAYS + 1), first_response_at=None),
            ResponseItem(created_at=_days_ago(30), first_response_at=_days_ago(29)),
        ]
        signal = self._compute(items)
        assert signal.cohort_size == 1

    def test_indeterminate_excluded_and_counted(self) -> None:
        """Unknown attribution is excluded from the fraction, never guessed."""
        items = [
            ResponseItem(created_at=_days_ago(30), indeterminate=True),
            ResponseItem(created_at=_days_ago(40), first_response_at=None),
        ]
        signal = self._compute(items)
        assert signal.unknown_attribution_count == 1
        assert signal.eligible_size == 1
        assert signal.response_fraction == 0.0

    def test_empty_eligible_cohort_has_no_fraction(self) -> None:
        """Nothing to rank is distinct from a real fraction of zero."""
        signal = self._compute([ResponseItem(created_at=_days_ago(1), first_response_at=None)])
        assert signal.state == STATE_OK
        assert signal.eligible_size == 0
        assert signal.response_fraction is None

    def test_all_unanswered_is_a_real_zero(self) -> None:
        items = [ResponseItem(created_at=_days_ago(60), first_response_at=None)]
        signal = self._compute(items)
        assert signal.response_fraction == 0.0

    def test_median_context_over_all_answered(self) -> None:
        items = [
            ResponseItem(created_at=_days_ago(30), first_response_at=_days_ago(29)),
            ResponseItem(created_at=_days_ago(40), first_response_at=_days_ago(37)),
            ResponseItem(created_at=_days_ago(50), first_response_at=None),
        ]
        signal = self._compute(items)
        assert signal.median_response_days_context == 2.0


# ---------------------------------------------------------------------------
# Backlog snapshot (signal 3 and signal 4's snapshot)
# ---------------------------------------------------------------------------


class TestParseDeclaredMaintainers:
    def test_entries_are_lowercased_and_split(self) -> None:
        parsed = parse_declared_maintainers("Stellar/JS-stellar-sdk=Alice|bob")
        assert parsed == {"stellar/js-stellar-sdk": frozenset({"alice", "bob"})}

    def test_repeated_repo_keys_merge(self) -> None:
        parsed = parse_declared_maintainers("o/r=a, o/r=b|c")
        assert parsed == {"o/r": frozenset({"a", "b", "c"})}

    def test_malformed_entries_are_rejected(self) -> None:
        parsed = parse_declared_maintainers("o/r, o/r=, =x, a/b/c=x, o/=x, /r=x, o/r2=| |, o/r=alice=bob, o/r=alice bob")
        assert parsed == {}

    def test_empty_setting_declares_nothing(self) -> None:
        assert parse_declared_maintainers("") == {}


class TestComputeBacklog:
    def test_complete_listing_exact_median(self) -> None:
        created = [_days_ago(100), _days_ago(50), _days_ago(10)]
        signal = compute_backlog(created, 3, now=NOW)
        assert signal.state == STATE_OK
        assert signal.open_count == 3
        assert signal.median_open_age_days == 50.0
        assert signal.median_age_exact is True

    def test_zero_open_is_a_real_zero(self) -> None:
        signal = compute_backlog([], 0, now=NOW)
        assert signal.state == STATE_OK
        assert signal.open_count == 0
        assert signal.median_open_age_days is None
        assert signal.median_age_exact is True

    def test_capped_prefix_reaching_middle_yields_exact_median_odd(self) -> None:
        """Oldest-first pagination makes the median an order statistic we may already hold."""
        created = [_days_ago(100), _days_ago(80), _days_ago(60)]  # oldest 3 of 5 open
        signal = compute_backlog(created, 5, now=NOW, incomplete_reason=REASON_PAGE_CAP)
        assert signal.state == STATE_OK
        assert signal.open_count == 5
        assert signal.median_open_age_days == 60.0

    def test_capped_prefix_reaching_middle_yields_exact_median_even(self) -> None:
        created = [_days_ago(100), _days_ago(80), _days_ago(60)]  # oldest 3 of 4 open
        signal = compute_backlog(created, 4, now=NOW, incomplete_reason=REASON_PAGE_CAP)
        assert signal.median_open_age_days == 70.0

    def test_capped_prefix_short_of_middle_is_incomplete(self) -> None:
        created = [_days_ago(100), _days_ago(80)]  # oldest 2 of 10 open
        signal = compute_backlog(created, 10, now=NOW, incomplete_reason=REASON_PAGE_CAP)
        assert signal.state == STATE_INCOMPLETE
        assert signal.incomplete_reason == REASON_PAGE_CAP
        assert signal.median_open_age_days is None
        assert signal.open_count == 10


# ---------------------------------------------------------------------------
# Release cadence (signal 1)
# ---------------------------------------------------------------------------


def _release(date: str, purl: str = "pkg:npm/a", version: str = "1.0.0") -> Release:
    return Release(purl=purl, version=version, release_date=date)


class TestDistinctReleaseDates:
    def test_multi_package_same_date_is_one_shipping_event(self) -> None:
        releases = [
            _release("2026-06-01T10:00:00Z", purl="pkg:npm/a"),
            _release("2026-06-01T10:05:00Z", purl="pkg:npm/b"),
            _release("2026-07-01T09:00:00Z", purl="pkg:npm/a"),
        ]
        assert distinct_release_dates(releases) == [dt.date(2026, 6, 1), dt.date(2026, 7, 1)]

    def test_undated_records_are_skipped(self) -> None:
        releases = [_release(""), _release("not-a-date")]
        assert distinct_release_dates(releases) == []

    def test_none_input(self) -> None:
        assert distinct_release_dates(None) == []


#: A deps.dev Go module entry for a GitHub repository.
_GO_PURL = "pkg:golang/github.com/soneso/stellar-ios-mac-sdk"

#: One example per Go pseudo-version form, each with and without the
#: ``+incompatible`` suffix: ``v0.0.0-``, ``-pre.0.``, and ``-0.``.
_GO_PSEUDO_VERSIONS = [
    "v0.0.0-20260801093000-3f2a1b4c5d6e",
    "v2.0.0-20260801093000-3f2a1b4c5d6e+incompatible",
    "v1.4.2-rc.1.0.20260801093000-3f2a1b4c5d6e",
    "v2.4.2-rc.1.0.20260801093000-3f2a1b4c5d6e+incompatible",
    "v1.4.3-0.20260801093000-3f2a1b4c5d6e",
    "v2.4.3-0.20260801093000-3f2a1b4c5d6e+incompatible",
]


class TestGoPseudoVersions:
    @pytest.mark.parametrize("version", _GO_PSEUDO_VERSIONS)
    def test_pseudo_version_is_not_a_shipping_event(self, version: str) -> None:
        """A pseudo-version is a commit reference; only the tagged npm release counts."""

        releases = [
            _release("2026-08-01T09:30:00Z", purl=_GO_PURL, version=version),
            _release("2026-07-01T09:00:00Z", purl="pkg:npm/a", version="1.0.0"),
        ]

        assert is_go_pseudo_version(releases[0]) is True
        assert distinct_release_dates(releases) == [dt.date(2026, 7, 1)]

    @pytest.mark.parametrize("version", ["v1.4.2", "v2.3.0+incompatible", "v1.5.0-rc.1"])
    def test_tagged_golang_release_counts(self, version: str) -> None:
        release = _release("2026-08-01T09:30:00Z", purl=_GO_PURL, version=version)

        assert is_go_pseudo_version(release) is False
        assert distinct_release_dates([release]) == [dt.date(2026, 8, 1)]

    def test_rule_is_scoped_to_the_golang_purl_type(self) -> None:
        """The same string under another registry is that registry's own version."""

        release = _release("2026-08-01T09:30:00Z", purl="pkg:cargo/some-crate", version=_GO_PSEUDO_VERSIONS[0])

        assert is_go_pseudo_version(release) is False
        assert distinct_release_dates([release]) == [dt.date(2026, 8, 1)]

    def test_pseudo_versions_on_many_days_leave_no_dates(self) -> None:
        releases = [
            _release(f"2026-08-{day:02d}T09:30:00Z", purl=_GO_PURL, version=f"v0.0.0-202608{day:02d}093000-3f2a1b4c5d6e")
            for day in range(1, 11)
        ]

        assert distinct_release_dates(releases) == []

    @pytest.mark.parametrize(
        "version",
        [
            # 13-digit timestamp
            "v0.0.0-2026080109300-3f2a1b4c5d6e",
            # missing revision
            "v0.0.0-20260801093000-",
            # non-ASCII digits never match
            "v0.0.0-\u0662\u0660\u0662\u0666\u0660\u0668\u0660\u0661\u0660\u0669\u0663\u0660\u0660\u0660-3f2a1b4c5d6e",
        ],
    )
    def test_near_misses_are_not_pseudo_versions(self, version: str) -> None:
        assert is_go_pseudo_version(_release("2026-08-01T09:30:00Z", purl=_GO_PURL, version=version)) is False


class TestPurlType:
    def test_type_component_is_lowercased(self) -> None:
        assert purl_type("pkg:GOLANG/github.com/owner/repo") == "golang"
        assert purl_type("pkg:npm/%40scope/name") == "npm"

    def test_non_purl_has_no_type(self) -> None:
        assert purl_type("github.com/owner/repo") is None
        assert purl_type("pkg:") is None


class TestHasPackageRegistryRelease:
    @pytest.mark.parametrize("registry", ["cargo", "npm", "pypi", "maven", "composer", "pub", "gem", "nuget"])
    def test_registry_record_counts(self, registry: str) -> None:
        assert has_package_registry_release([_release("", purl=f"pkg:{registry}/pkg")]) is True

    def test_golang_records_never_count(self) -> None:
        releases = [
            _release("2026-08-01T09:30:00Z", purl=_GO_PURL, version="v1.4.2"),
            _release("2026-08-02T09:30:00Z", purl=_GO_PURL, version=_GO_PSEUDO_VERSIONS[0]),
        ]

        assert has_package_registry_release(releases) is False

    def test_no_records(self) -> None:
        assert has_package_registry_release(None) is False
        assert has_package_registry_release([]) is False


class TestComputeReleaseCadence:
    def _cadence(
        self, depsdev: list[dt.date], fallback: list[dt.date], *, last_n: int = 10, min_events: int = 3
    ) -> CadenceResult:
        return compute_release_cadence(depsdev, fallback, now=NOW, last_n_events=last_n, min_events=min_events)

    def test_interleaved_two_package_cycle_yields_true_gap(self) -> None:
        """Two packages shipped together every 30 days must not produce zero gaps."""
        dates = [dt.date(2026, 5, 1), dt.date(2026, 5, 31), dt.date(2026, 6, 30), dt.date(2026, 7, 30)]
        result = self._cadence(dates, [])
        assert result.state == STATE_OK
        assert result.source == CADENCE_SOURCE_DEPSDEV
        assert result.median_gap_days == 30.0
        assert result.days_since_last_release == float((NOW.date() - dt.date(2026, 7, 30)).days)

    def test_below_min_events_gives_no_median_but_recency(self) -> None:
        result = self._cadence([dt.date(2026, 7, 1), dt.date(2026, 8, 1)], [])
        assert result.state == STATE_OK
        assert result.median_gap_days is None
        assert result.days_since_last_release is not None

    def test_no_dates_anywhere_is_unavailable(self) -> None:
        """Tag-only or release-free repos are unsupported for cadence, never zero."""
        result = self._cadence([], [])
        assert result.state == STATE_UNAVAILABLE
        assert result.source is None
        assert result.median_gap_days is None
        assert result.days_since_last_release is None

    def test_fallback_wins_with_more_dates(self) -> None:
        fallback = [dt.date(2026, 1, 1), dt.date(2026, 2, 1), dt.date(2026, 3, 1)]
        result = self._cadence([dt.date(2026, 3, 1)], fallback)
        assert result.source == CADENCE_SOURCE_GITHUB_RELEASES
        assert result.shipping_events == 3

    def test_tie_keeps_depsdev(self) -> None:
        result = self._cadence([dt.date(2026, 3, 1)], [dt.date(2026, 4, 1)])
        assert result.source == CADENCE_SOURCE_DEPSDEV

    def test_median_uses_only_last_n_events(self) -> None:
        """Ancient gaps outside the last-N window must not drag the median."""
        old = [dt.date(2020, 1, 1), dt.date(2021, 1, 1)]
        recent = [dt.date(2026, 5, 1), dt.date(2026, 5, 31), dt.date(2026, 6, 30), dt.date(2026, 7, 30)]
        result = self._cadence(old + recent, [], last_n=4)
        assert result.median_gap_days == 30.0


# ---------------------------------------------------------------------------
# Commit activity (signal 5)
# ---------------------------------------------------------------------------


def _commit(days_ago: float, name: str = "Alice", email: str = "alice@example.org") -> CommitRecord:
    return CommitRecord(author_name=name, author_email=email, timestamp=_days_ago(days_ago), commit_hash="a" * 40)


class TestComputeCommitActivity:
    def _compute(
        self, commits: list[CommitRecord], *, submitted_days_ago: float = 1.0, since_months: int = 24
    ) -> CommitActivityResult:
        return compute_commit_activity(
            commits,
            artifact_submitted_at=_days_ago(submitted_days_ago),
            artifact_since_months=since_months,
            now=NOW,
            window_days=WINDOW_DAYS,
            max_artifact_age_days=21,
        )

    def test_window_and_bot_filtering(self) -> None:
        commits = [
            _commit(10),
            _commit(170),
            _commit(WINDOW_DAYS + 5),  # outside the window
            _commit(20, name="dependabot[bot]", email="1+dependabot[bot]@users.noreply.github.com"),
        ]
        result = self._compute(commits)
        assert result.state == STATE_OK
        assert result.commit_count == 2

    def test_zero_commits_fresh_artifact_is_a_real_zero(self) -> None:
        result = self._compute([])
        assert result.state == STATE_OK
        assert result.commit_count == 0

    def test_stale_artifact_yields_context_not_a_ranked_value(self) -> None:
        """Dormant repos are re-logged slowly; their counts must not rank."""
        result = self._compute([_commit(10)], submitted_days_ago=40.0)
        assert result.state == STATE_INCOMPLETE
        assert result.reason == REASON_STALE_ARTIFACT
        assert result.commit_count == 1
        assert result.artifact_as_of == _days_ago(40.0).isoformat()

    def test_artifact_not_covering_window_is_incomplete(self) -> None:
        result = self._compute([_commit(10)], since_months=3)
        assert result.state == STATE_INCOMPLETE
        assert result.reason == REASON_WINDOW_NOT_COVERED

    def test_missing_artifact_is_unavailable(self) -> None:
        result = compute_commit_activity(
            None,
            artifact_submitted_at=None,
            artifact_since_months=None,
            now=NOW,
            window_days=WINDOW_DAYS,
            max_artifact_age_days=21,
        )
        assert result.state == STATE_UNAVAILABLE
        assert result.commit_count is None


# ---------------------------------------------------------------------------
# Percentile ranking
# ---------------------------------------------------------------------------


class TestRankScalarValues:
    def test_higher_is_better_ranks_ascending(self) -> None:
        percentiles, pool_size = rank_scalar_values([1.0, 2.0, 3.0, 4.0], higher_is_better=True)
        assert pool_size == 4
        assert percentiles == [0.0, 25.0, 50.0, 75.0]

    def test_lower_is_better_inverts(self) -> None:
        """A higher percentile must always mean better upkeep."""
        percentiles, _ = rank_scalar_values([1.0, 2.0, 3.0, 4.0], higher_is_better=False)
        assert percentiles == [75.0, 50.0, 25.0, 0.0]

    def test_ties_share_the_lowest_percentile(self) -> None:
        percentiles, _ = rank_scalar_values([5.0, 5.0, 1.0], higher_is_better=True)
        assert percentiles == [33.33, 33.33, 0.0]

    def test_none_excluded_from_pool(self) -> None:
        percentiles, pool_size = rank_scalar_values([None, 2.0, None, 1.0], higher_is_better=True)
        assert pool_size == 2
        assert percentiles[0] is None
        assert percentiles[2] is None
        assert percentiles[1] == 50.0
        assert percentiles[3] == 0.0

    def test_singleton_pool_gets_zeroth_percentile(self) -> None:
        """No element is unconditionally top-ranked, even alone."""
        percentiles, pool_size = rank_scalar_values([7.0], higher_is_better=True)
        assert pool_size == 1
        assert percentiles == [0.0]

    def test_empty_pool(self) -> None:
        percentiles, pool_size = rank_scalar_values([None, None], higher_is_better=True)
        assert pool_size == 0
        assert percentiles == [None, None]


# ---------------------------------------------------------------------------
# Payload round trip
# ---------------------------------------------------------------------------


class TestSignalsPayloadRoundTrip:
    def test_round_trip_through_metadata(self) -> None:
        signals = MaintenanceSignals(
            schema_version=1,
            collected_at=NOW.isoformat(),
            window_days=WINDOW_DAYS,
            response_interval_days=INTERVAL_DAYS,
            requests_used=7,
            issues_enabled=True,
        )
        metadata = {"maintenance_signals": signals_to_payload(signals)}
        restored = signals_from_metadata(metadata)
        assert restored == signals

    def test_missing_key_returns_none(self) -> None:
        assert signals_from_metadata({}) is None
        assert signals_from_metadata(None) is None

    def test_invalid_shape_is_logged_and_skipped(self) -> None:
        assert signals_from_metadata({"maintenance_signals": {"schema_version": "not-an-int"}}) is None


# ---------------------------------------------------------------------------
# Artifact byte parsing for the maintenance consumer
# ---------------------------------------------------------------------------
