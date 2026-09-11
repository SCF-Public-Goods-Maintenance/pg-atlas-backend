"""
DB-backed tests for maintenance profile materialization.

Seeds deterministic repos inside a rolled-back transaction, runs the
materializer, and asserts per-repo values, percentile ranks, coverage
states, freshness downgrades, parameter gating, idempotent re-runs, the
execution-time gate, and stale-profile cleanup.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from pg_atlas.config import settings
from pg_atlas.db_models.base import ActivityStatus, ProjectType, SubmissionStatus, Visibility
from pg_atlas.db_models.gitlog_artifact import GitLogArtifact
from pg_atlas.db_models.project import Project
from pg_atlas.db_models.release import Release
from pg_atlas.db_models.repo_vertex import Repo
from pg_atlas.metrics.maintenance import (
    MAINTENANCE_PROFILE_KEY,
    MAINTENANCE_SIGNALS_KEY,
    REASON_HOST_REPOSITORY,
    REASON_STALE_COLLECTION,
    STATE_INCOMPLETE,
    STATE_NOT_APPLICABLE,
    STATE_OK,
    STATE_UNAVAILABLE,
    MaintenanceSignals,
    ResponsivenessSignal,
    signals_to_payload,
)
from pg_atlas.metrics.materialize_maintenance import materialize_maintenance_profiles
from tests.metrics.maintenance_support import ok_signals


def _gitlog_content(commits: list[tuple[str, str, dt.datetime]]) -> bytes:
    """Render commits in the stored ``%aN%x00%aE%x00%aI%x00%H`` line format."""

    lines = [f"{name}\x00{email}\x00{ts.isoformat()}\x00{'a' * 40}" for name, email, ts in commits]

    return "\n".join(lines).encode()


@dataclass(frozen=True)
class SeededMaintenanceFixture:
    """Seeded IDs for one deterministic maintenance materialization test."""

    now: dt.datetime
    repo_full_id: int
    repo_sparse_id: int
    repo_stale_id: int
    repo_ineligible_id: int
    last_release_date: dt.date


async def _seed_maintenance_fixture(
    session: AsyncSession,
    tmp_path_factory: pytest.TempPathFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> SeededMaintenanceFixture:
    """Insert deterministic repos, one project, and one git-log artifact."""

    suffix = uuid4().hex[:8]
    now = dt.datetime.now(dt.UTC)

    monkeypatch.setattr(settings, "MAINTENANCE_WINDOW_DAYS", 180)
    monkeypatch.setattr(settings, "MAINTENANCE_RESPONSE_INTERVAL_DAYS", 7)
    monkeypatch.setattr(settings, "MAINTENANCE_INCLUDE_DRAFT_PRS", True)
    monkeypatch.setattr(settings, "MAINTENANCE_CADENCE_LAST_N_EVENTS", 10)
    monkeypatch.setattr(settings, "MAINTENANCE_CADENCE_MIN_EVENTS", 3)
    monkeypatch.setattr(settings, "MAINTENANCE_SIGNALS_MAX_AGE_DAYS", 14)
    monkeypatch.setattr(settings, "MAINTENANCE_GITLOG_MAX_AGE_DAYS", 21)
    monkeypatch.setattr(settings, "MAINTENANCE_HOST_REPOS", "")
    monkeypatch.setattr(settings, "MAINTENANCE_DECLARED_MAINTAINERS", "")
    monkeypatch.setattr(settings, "MAINTENANCE_DEPLOY_ON_PUSH_REPOS", "")

    # Neutralize pre-existing rows inside the rollback-only transaction so
    # the percentile pools are deterministic for this test.
    await session.execute(update(Repo).values(releases=None, repo_metadata=None, pushed_at=None, pushed_at_observed_at=None))
    await session.execute(update(GitLogArtifact).values(status=SubmissionStatus.failed))
    await session.flush()

    project = Project(
        canonical_id=f"daoip-5:stellar:project:maintenance-{suffix}",
        display_name=f"Maintenance {suffix}",
        project_type=ProjectType.public_good,
        activity_status=ActivityStatus.live,
    )
    session.add(project)
    await session.flush()

    # Six shipping events 30 days apart, two packages published together on
    # the last date (one shipping event, not two).
    release_dates = [now.date() - dt.timedelta(days=15 + 30 * i) for i in range(6)]
    releases = [
        Release(purl="pkg:npm/a", version=f"1.{i}.0", release_date=f"{day.isoformat()}T10:00:00Z")
        for i, day in enumerate(release_dates)
    ]
    releases.append(Release(purl="pkg:npm/b", version="2.0.0", release_date=f"{release_dates[0].isoformat()}T10:05:00Z"))

    repo_full = Repo(
        canonical_id=f"pkg:github/test/maintenance-full-{suffix}",
        display_name=f"maintenance-full-{suffix}",
        visibility=Visibility.public,
        latest_version="1.5.0",
        project_id=project.id,
        repo_url=f"https://github.com/test/maintenance-full-{suffix}",
        releases=releases,
        repo_metadata={MAINTENANCE_SIGNALS_KEY: signals_to_payload(ok_signals(now))},
        pushed_at=now - dt.timedelta(days=2),
    )
    repo_sparse = Repo(
        canonical_id=f"pkg:github/test/maintenance-sparse-{suffix}",
        display_name=f"maintenance-sparse-{suffix}",
        visibility=Visibility.public,
        latest_version="0.1.0",
        project_id=project.id,
    )
    stale_signals = ok_signals(now, as_of=now - dt.timedelta(days=60))
    repo_stale = Repo(
        canonical_id=f"pkg:github/test/maintenance-stale-{suffix}",
        display_name=f"maintenance-stale-{suffix}",
        visibility=Visibility.public,
        latest_version="0.2.0",
        project_id=project.id,
        repo_metadata={MAINTENANCE_SIGNALS_KEY: signals_to_payload(stale_signals)},
        pushed_at=now - dt.timedelta(days=5),
    )
    repo_ineligible = Repo(
        canonical_id=f"pkg:github/test/maintenance-ineligible-{suffix}",
        display_name=f"maintenance-ineligible-{suffix}",
        visibility=Visibility.public,
        latest_version="0.0.1",
        repo_metadata={MAINTENANCE_PROFILE_KEY: {"schema_version": 1}, "other_key": "kept"},
    )
    session.add_all([repo_full, repo_sparse, repo_stale, repo_ineligible])
    await session.flush()

    # Fresh, window-covering git-log artifact for the full repo: three human
    # commits in the window, one bot commit, one commit outside the window.
    artifact_dir = tmp_path_factory.mktemp("maintenance-artifacts")
    monkeypatch.setattr(settings, "ARTIFACT_STORE_PATH", artifact_dir)
    artifact_path = f"git-logs/test/maintenance-full-{suffix}.gitlog"
    full_path = artifact_dir / artifact_path
    full_path.parent.mkdir(parents=True, exist_ok=True)
    full_path.write_bytes(
        _gitlog_content(
            [
                ("Alice", "alice@example.org", now - dt.timedelta(days=10)),
                ("Alice", "alice@example.org", now - dt.timedelta(days=50)),
                ("Bob", "bob@example.org", now - dt.timedelta(days=170)),
                ("dependabot[bot]", "1+dependabot[bot]@users.noreply.github.com", now - dt.timedelta(days=20)),
                ("Alice", "alice@example.org", now - dt.timedelta(days=200)),
            ]
        )
    )
    session.add(
        GitLogArtifact(
            repo_id=repo_full.id,
            since_months=24,
            artifact_path=artifact_path,
            status=SubmissionStatus.processed,
        )
    )
    await session.flush()

    return SeededMaintenanceFixture(
        now=now,
        repo_full_id=repo_full.id,
        repo_sparse_id=repo_sparse.id,
        repo_stale_id=repo_stale.id,
        repo_ineligible_id=repo_ineligible.id,
        last_release_date=release_dates[0],
    )


async def _load_profile(session: AsyncSession, repo_id: int) -> dict[str, Any] | None:
    metadata = (await session.execute(select(Repo.repo_metadata).where(Repo.id == repo_id))).scalar_one()
    if metadata is None:
        return None

    profile = metadata.get(MAINTENANCE_PROFILE_KEY)

    return dict(profile) if profile is not None else None


class TestMaterializeMaintenanceProfiles:
    async def test_full_pass_values_ranks_and_states(
        self,
        rollback_db_session: AsyncSession,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
        assert_no_uow: Callable[[AsyncSession], None],
    ) -> None:
        session = rollback_db_session
        fixture = await _seed_maintenance_fixture(session, tmp_path_factory, monkeypatch)

        stats, payloads = await materialize_maintenance_profiles(session, now=fixture.now)
        assert_no_uow(session)

        assert stats.gate_skipped is False
        assert stats.repos_eligible >= 3
        assert stats.profiles_written == stats.repos_eligible
        assert stats.stale_profiles_cleared >= 1
        assert stats.artifacts_read >= 1

        # --- full repo: every signal present, correct, and ranked ---
        profile = await _load_profile(session, fixture.repo_full_id)
        assert profile is not None
        signals = profile["signals"]

        cadence = signals["release_cadence"]
        assert cadence["source"] == "depsdev"
        assert cadence["shipping_events"] == 6
        assert cadence["release_ships_code"] is True
        assert cadence["median_gap_days"]["state"] == STATE_OK
        assert cadence["median_gap_days"]["value"] == 30.0
        expected_days_since = float((fixture.now.date() - fixture.last_release_date).days)
        assert cadence["days_since_last_release"]["value"] == expected_days_since

        issue_resp = signals["issue_responsiveness"]
        assert issue_resp["response_fraction"]["state"] == STATE_OK
        assert issue_resp["response_fraction"]["value"] == 0.8
        assert issue_resp["response_fraction"]["percentile"] is not None
        assert issue_resp["response_fraction"]["pool_size"] == 1
        assert issue_resp["cohort_size"] == 5

        issue_backlog = signals["issue_backlog"]
        assert issue_backlog["open_count"]["value"] == 3
        assert issue_backlog["median_open_age_days"]["value"] == 40.0

        pr_resp = signals["pr_responsiveness"]
        assert pr_resp["response_fraction"]["value"] == 0.5
        assert pr_resp["open_count"]["value"] == 2
        assert pr_resp["median_open_age_days"]["value"] == 10.0

        activity = signals["activity_recency"]
        assert activity["days_since_push"]["state"] == STATE_OK
        assert activity["days_since_push"]["value"] == 2.0
        # No observation time is stored for this row, so the as-of falls back
        # to the collection's collected_at.
        assert activity["days_since_push"]["as_of"] == fixture.now.isoformat()
        assert activity["commit_count_window"]["state"] == STATE_OK
        assert activity["commit_count_window"]["value"] == 3

        # The collected-signals payload must survive the profile write.
        metadata = (await session.execute(select(Repo.repo_metadata).where(Repo.id == fixture.repo_full_id))).scalar_one()
        assert metadata is not None
        assert MAINTENANCE_SIGNALS_KEY in metadata

        # --- sparse repo: everything unavailable, nothing coerced to zero ---
        sparse = await _load_profile(session, fixture.repo_sparse_id)
        assert sparse is not None
        sparse_signals = sparse["signals"]
        assert sparse_signals["release_cadence"]["median_gap_days"]["state"] == STATE_UNAVAILABLE
        assert sparse_signals["release_cadence"]["release_ships_code"] is None
        assert sparse_signals["issue_responsiveness"]["response_fraction"]["state"] == STATE_UNAVAILABLE
        assert sparse_signals["activity_recency"]["days_since_push"]["state"] == STATE_UNAVAILABLE
        assert sparse_signals["activity_recency"]["commit_count_window"]["state"] == STATE_UNAVAILABLE
        assert "percentile" not in sparse_signals["issue_responsiveness"]["response_fraction"]

        # --- stale repo: context only, excluded from every ranked pool ---
        stale = await _load_profile(session, fixture.repo_stale_id)
        assert stale is not None
        stale_fraction = stale["signals"]["issue_responsiveness"]["response_fraction"]
        assert stale_fraction["state"] == STATE_INCOMPLETE
        assert stale_fraction["reason"] == REASON_STALE_COLLECTION
        assert stale_fraction["value"] == 0.8
        assert "percentile" not in stale_fraction
        stale_push = stale["signals"]["activity_recency"]["days_since_push"]
        assert stale_push["state"] == STATE_INCOMPLETE
        assert stale_push["reason"] == REASON_STALE_COLLECTION

        # --- ineligible repo: profile removed, other metadata preserved ---
        ineligible_metadata = (
            await session.execute(select(Repo.repo_metadata).where(Repo.id == fixture.repo_ineligible_id))
        ).scalar_one()
        assert ineligible_metadata is not None
        assert MAINTENANCE_PROFILE_KEY not in ineligible_metadata
        assert ineligible_metadata["other_key"] == "kept"

        # --- export payloads mirror what was written ---
        full_canonical = (await session.execute(select(Repo.canonical_id).where(Repo.id == fixture.repo_full_id))).scalar_one()
        assert payloads[full_canonical]["signals"]["issue_responsiveness"]["response_fraction"]["value"] == 0.8

    async def test_rerun_is_idempotent(
        self,
        rollback_db_session: AsyncSession,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        session = rollback_db_session
        fixture = await _seed_maintenance_fixture(session, tmp_path_factory, monkeypatch)

        stats_first, _ = await materialize_maintenance_profiles(session, now=fixture.now)
        first = await _load_profile(session, fixture.repo_full_id)

        stats_second, _ = await materialize_maintenance_profiles(session, now=fixture.now)
        second = await _load_profile(session, fixture.repo_full_id)

        assert stats_second.repos_eligible == stats_first.repos_eligible
        assert first == second

    async def test_gated_run_skips_when_disabled(
        self,
        rollback_db_session: AsyncSession,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The execution-time half of the fail-closed gate: nothing is written."""

        session = rollback_db_session
        fixture = await _seed_maintenance_fixture(session, tmp_path_factory, monkeypatch)
        monkeypatch.setattr(settings, "MAINTENANCE_METRIC_ENABLED", False)

        stats, payloads = await materialize_maintenance_profiles(session, enforce_gate=True, now=fixture.now)

        assert stats.gate_skipped is True
        assert stats.profiles_written == 0
        assert payloads == {}
        assert await _load_profile(session, fixture.repo_full_id) is None

        # The ineligible repo's stale profile also stays untouched by a gated run.
        ineligible_metadata = (
            await session.execute(select(Repo.repo_metadata).where(Repo.id == fixture.repo_ineligible_id))
        ).scalar_one()
        assert ineligible_metadata is not None
        assert MAINTENANCE_PROFILE_KEY in ineligible_metadata

    async def test_no_write_computes_without_persisting(
        self,
        rollback_db_session: AsyncSession,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        session = rollback_db_session
        fixture = await _seed_maintenance_fixture(session, tmp_path_factory, monkeypatch)

        stats, payloads = await materialize_maintenance_profiles(session, write=False, now=fixture.now)

        assert stats.profiles_written == 0
        assert payloads
        assert await _load_profile(session, fixture.repo_full_id) is None


async def _seed_host_candidate(
    session: AsyncSession,
    fixture: SeededMaintenanceFixture,
) -> tuple[int, str]:
    """
    Seed one repo with rankable data in every pool the full repo ranks in.

    Returns its id and canonical id; the caller decides whether to declare it
    a host repository, so the same seed proves both that the data ranks
    undeclared and that a declaration keeps it out of every pool.
    """

    suffix = uuid4().hex[:8]
    now = fixture.now

    project = Project(
        canonical_id=f"daoip-5:stellar:project:host-{suffix}",
        display_name=f"Host {suffix}",
        project_type=ProjectType.public_good,
        activity_status=ActivityStatus.live,
    )
    session.add(project)
    await session.flush()

    releases = [
        Release(
            purl="pkg:npm/host",
            version=f"1.{i}.0",
            release_date=f"{(now.date() - dt.timedelta(days=15 + 30 * i)).isoformat()}T10:00:00Z",
        )
        for i in range(6)
    ]
    repo_host = Repo(
        canonical_id=f"pkg:github/vendor/host-candidate-{suffix}",
        display_name=f"host-candidate-{suffix}",
        visibility=Visibility.public,
        latest_version="1.5.0",
        project_id=project.id,
        repo_url=f"https://github.com/vendor/host-candidate-{suffix}",
        releases=releases,
        repo_metadata={MAINTENANCE_SIGNALS_KEY: signals_to_payload(ok_signals(now))},
        pushed_at=now - dt.timedelta(days=1),
    )
    session.add(repo_host)
    await session.flush()

    artifact_path = f"git-logs/vendor/host-candidate-{suffix}.gitlog"
    full_path = settings.ARTIFACT_STORE_PATH / artifact_path
    full_path.parent.mkdir(parents=True, exist_ok=True)
    full_path.write_bytes(_gitlog_content([("Host Dev", "dev@example.org", now - dt.timedelta(days=3))]))
    session.add(
        GitLogArtifact(
            repo_id=repo_host.id,
            since_months=24,
            artifact_path=artifact_path,
            status=SubmissionStatus.processed,
        )
    )
    await session.flush()

    return repo_host.id, repo_host.canonical_id


#: Every profile scalar, by signal block — a host profile carries exactly these.
_EXPECTED_SCALAR_FIELDS = {
    "release_cadence": {"median_gap_days", "days_since_last_release"},
    "issue_responsiveness": {"response_fraction"},
    "issue_backlog": {"open_count", "median_open_age_days"},
    "pr_responsiveness": {"response_fraction", "open_count", "median_open_age_days"},
    "activity_recency": {"days_since_push", "commit_count_window"},
}


class TestHostRepositories:
    async def test_undeclared_candidate_ranks(
        self,
        rollback_db_session: AsyncSession,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The candidate's seeded data feeds every sensitive pool — the
        baseline that makes the declared-exclusion assertions meaningful."""

        session = rollback_db_session
        fixture = await _seed_maintenance_fixture(session, tmp_path_factory, monkeypatch)
        host_id, _ = await _seed_host_candidate(session, fixture)

        stats, _ = await materialize_maintenance_profiles(session, now=fixture.now)

        # The full repo and the candidate rank in all ten pools.
        assert set(stats.pool_sizes.values()) == {2}
        assert stats.artifacts_read == 2

        profile = await _load_profile(session, host_id)
        assert profile is not None
        fraction = profile["signals"]["issue_responsiveness"]["response_fraction"]
        assert fraction["state"] == STATE_OK
        assert fraction["pool_size"] == 2

    async def test_declared_host_renders_not_applicable_and_joins_no_pool(
        self,
        rollback_db_session: AsyncSession,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        session = rollback_db_session
        fixture = await _seed_maintenance_fixture(session, tmp_path_factory, monkeypatch)
        host_id, host_canonical = await _seed_host_candidate(session, fixture)

        # Uppercase declaration against the lowercase canonical id: matching
        # is case-insensitive like the other per-repo declared lists.
        bare = host_canonical.removeprefix("pkg:github/").upper()
        monkeypatch.setattr(settings, "MAINTENANCE_HOST_REPOS", bare)

        stats, payloads = await materialize_maintenance_profiles(session, now=fixture.now)

        # --- the profile: every scalar not-applicable, nothing else ---
        profile = await _load_profile(session, host_id)
        assert profile is not None
        assert profile["eligible_population"] == stats.repos_eligible
        signals = profile["signals"]
        assert {name: set(block) for name, block in signals.items()} == _EXPECTED_SCALAR_FIELDS
        for block in signals.values():
            for entry in block.values():
                assert entry == {"state": STATE_NOT_APPLICABLE, "reason": REASON_HOST_REPOSITORY}

        # --- no pool contains it, and its artifact is never read ---
        assert set(stats.pool_sizes.values()) == {1}
        assert stats.artifacts_read == 1

        full_profile = await _load_profile(session, fixture.repo_full_id)
        assert full_profile is not None
        assert full_profile["signals"]["issue_responsiveness"]["response_fraction"]["pool_size"] == 1

        # --- still profiled and exported like any eligible repo ---
        assert host_canonical in payloads
        assert payloads[host_canonical]["signals"] == signals


# ---------------------------------------------------------------------------
# Collector persistence (resolution and per-signal merge round trip)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Artifact read failures
# ---------------------------------------------------------------------------


class TestCommitActivityArtifactStates:
    async def test_unreadable_artifact_is_incomplete_not_unavailable(
        self,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Failed collection and absent data are different states."""

        from pg_atlas.metrics.maintenance import REASON_ARTIFACT_UNREADABLE
        from pg_atlas.metrics.materialize_maintenance import _ArtifactRef, _commit_activity_for_repo

        monkeypatch.setattr(settings, "ARTIFACT_STORE_PATH", tmp_path_factory.mktemp("empty-artifact-store"))
        now = dt.datetime.now(dt.UTC)
        submitted_at = now - dt.timedelta(days=1)
        artifact = _ArtifactRef(artifact_path="git-logs/missing.gitlog", since_months=24, submitted_at=submitted_at)

        result, was_read = await _commit_activity_for_repo("pkg:github/test/missing", artifact, now=now)

        assert was_read is False
        assert result.state == STATE_INCOMPLETE
        assert result.reason == REASON_ARTIFACT_UNREADABLE
        assert result.commit_count is None
        assert result.artifact_as_of == submitted_at.isoformat()

    async def test_absent_artifact_is_unavailable(self) -> None:
        from pg_atlas.metrics.materialize_maintenance import _commit_activity_for_repo

        result, was_read = await _commit_activity_for_repo("pkg:github/test/none", None, now=dt.datetime.now(dt.UTC))

        assert was_read is False
        assert result.state == STATE_UNAVAILABLE
        assert result.reason is None


# ---------------------------------------------------------------------------
# Cadence fallback states and the parameter gate (no database needed)
# ---------------------------------------------------------------------------


def _unavailable_activity() -> Any:
    from pg_atlas.metrics.maintenance import CommitActivityResult

    return CommitActivityResult(state=STATE_UNAVAILABLE, commit_count=None, artifact_as_of=None, reason=None)


def _signals_with_fallback(fallback: Any, now: dt.datetime) -> MaintenanceSignals:
    return MaintenanceSignals(
        schema_version=1,
        collected_at=now.isoformat(),
        window_days=180,
        response_interval_days=7,
        release_fallback=fallback,
    )


@pytest.fixture
def pinned_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "MAINTENANCE_WINDOW_DAYS", 180)
    monkeypatch.setattr(settings, "MAINTENANCE_RESPONSE_INTERVAL_DAYS", 7)
    monkeypatch.setattr(settings, "MAINTENANCE_INCLUDE_DRAFT_PRS", True)
    monkeypatch.setattr(settings, "MAINTENANCE_INCLUDE_PRERELEASES", True)
    monkeypatch.setattr(settings, "MAINTENANCE_CADENCE_LAST_N_EVENTS", 10)
    monkeypatch.setattr(settings, "MAINTENANCE_CADENCE_MIN_EVENTS", 3)
    monkeypatch.setattr(settings, "MAINTENANCE_SIGNALS_MAX_AGE_DAYS", 14)
    monkeypatch.setattr(settings, "MAINTENANCE_GITLOG_MAX_AGE_DAYS", 21)
    monkeypatch.setattr(settings, "MAINTENANCE_DECLARED_MAINTAINERS", "")
    monkeypatch.setattr(settings, "MAINTENANCE_DEPLOY_ON_PUSH_REPOS", "")


class TestCadenceFallbackStates:
    def _profile_signals(self, releases: Any, fallback: Any, now: dt.datetime) -> dict[str, Any]:
        from pg_atlas.metrics.materialize_maintenance import _build_repo_profile

        profile = _build_repo_profile(
            1,
            "pkg:github/test/cadence",
            releases,
            _signals_with_fallback(fallback, now),
            None,
            None,
            _unavailable_activity(),
            now=now,
            deploy_on_push=False,
        )

        return profile.signals["release_cadence"]

    def test_incomplete_fallback_is_context_not_ranked(self, pinned_settings: None) -> None:
        from pg_atlas.metrics.maintenance import ReleaseFallback

        now = dt.datetime.now(dt.UTC)
        dates = [(now.date() - dt.timedelta(days=30 * i)).isoformat() for i in range(3)]
        fallback = ReleaseFallback(
            state=STATE_INCOMPLETE,
            as_of=now.isoformat(),
            publication_dates=sorted(dates),
            complete=False,
            incomplete_reason="page-cap",
        )

        block = self._profile_signals(None, fallback, now)

        assert block["days_since_last_release"]["state"] == STATE_INCOMPLETE
        assert block["days_since_last_release"]["reason"] == "page-cap"
        assert block["days_since_last_release"]["value"] is not None
        assert block["median_gap_days"]["state"] == STATE_INCOMPLETE
        assert "percentile" not in block["days_since_last_release"]

    def test_failed_empty_fallback_keeps_its_reason(self, pinned_settings: None) -> None:
        from pg_atlas.metrics.maintenance import ReleaseFallback

        now = dt.datetime.now(dt.UTC)
        fallback = ReleaseFallback(
            state=STATE_INCOMPLETE, as_of=now.isoformat(), complete=False, incomplete_reason="rate-limited"
        )

        block = self._profile_signals(None, fallback, now)

        assert block["days_since_last_release"]["state"] == STATE_INCOMPLETE
        assert block["days_since_last_release"]["reason"] == "rate-limited"
        assert "value" not in block["days_since_last_release"]

    def test_stale_fallback_is_context(self, pinned_settings: None) -> None:
        from pg_atlas.metrics.maintenance import ReleaseFallback

        now = dt.datetime.now(dt.UTC)
        old = (now - dt.timedelta(days=60)).isoformat()
        fallback = ReleaseFallback(state=STATE_OK, as_of=old, publication_dates=["2026-01-01", "2026-02-01", "2026-03-01"])

        block = self._profile_signals(None, fallback, now)

        assert block["days_since_last_release"]["state"] == STATE_INCOMPLETE
        assert block["days_since_last_release"]["reason"] == REASON_STALE_COLLECTION

    def test_prerelease_rule_mismatch_is_context(self, pinned_settings: None) -> None:
        from pg_atlas.metrics.maintenance import ReleaseFallback

        now = dt.datetime.now(dt.UTC)
        fallback = ReleaseFallback(
            state=STATE_OK,
            as_of=now.isoformat(),
            publication_dates=["2026-01-01", "2026-02-01", "2026-03-01"],
            include_prereleases=False,
        )

        block = self._profile_signals(None, fallback, now)

        assert block["days_since_last_release"]["state"] == STATE_INCOMPLETE
        assert block["days_since_last_release"]["reason"] == "parameter-mismatch"

    def test_usable_depsdev_source_ignores_blocked_fallback(self, pinned_settings: None) -> None:
        from pg_atlas.metrics.maintenance import ReleaseFallback

        now = dt.datetime.now(dt.UTC)
        releases = [
            Release(
                purl="pkg:npm/a",
                version=f"1.{i}.0",
                release_date=f"{(now.date() - dt.timedelta(days=15 + 30 * i)).isoformat()}T10:00:00Z",
            )
            for i in range(6)
        ]
        fallback = ReleaseFallback(state=STATE_INCOMPLETE, as_of=now.isoformat(), complete=False, incomplete_reason="page-cap")

        block = self._profile_signals(releases, fallback, now)

        assert block["source"] == "depsdev"
        assert block["days_since_last_release"]["state"] == STATE_OK
        assert block["median_gap_days"]["state"] == STATE_OK

    def test_complete_empty_fallback_with_no_depsdev_dates_is_unavailable(self, pinned_settings: None) -> None:
        from pg_atlas.metrics.maintenance import ReleaseFallback

        now = dt.datetime.now(dt.UTC)
        fallback = ReleaseFallback(state=STATE_OK, as_of=now.isoformat())

        block = self._profile_signals(None, fallback, now)

        assert block["days_since_last_release"]["state"] == STATE_UNAVAILABLE
        assert block["median_gap_days"]["state"] == STATE_UNAVAILABLE


class TestResponsivenessParameterGate:
    def test_mismatched_window_is_context(self, pinned_settings: None) -> None:
        from pg_atlas.metrics.materialize_maintenance import _responsiveness_signal_dict

        now = dt.datetime.now(dt.UTC)
        signal = ResponsivenessSignal(
            state=STATE_OK,
            as_of=now.isoformat(),
            window_days=90,
            interval_days=7,
            response_fraction=0.9,
        )

        block, rankable = _responsiveness_signal_dict(
            signal, now=now, expected_include_drafts=None, expected_declared_maintainers=None
        )

        assert rankable is None
        assert block["response_fraction"]["state"] == STATE_INCOMPLETE
        assert block["response_fraction"]["reason"] == "parameter-mismatch"
        assert block["response_fraction"]["value"] == 0.9
        assert block["window_days"] == 90

    def test_mismatched_draft_rule_is_context(self, pinned_settings: None) -> None:
        from pg_atlas.metrics.materialize_maintenance import _responsiveness_signal_dict

        now = dt.datetime.now(dt.UTC)
        signal = ResponsivenessSignal(
            state=STATE_OK,
            as_of=now.isoformat(),
            window_days=180,
            interval_days=7,
            include_drafts=False,
            response_fraction=0.9,
        )

        block, rankable = _responsiveness_signal_dict(
            signal, now=now, expected_include_drafts=True, expected_declared_maintainers=None
        )

        assert rankable is None
        assert block["response_fraction"]["reason"] == "parameter-mismatch"

    def test_matching_parameters_rank(self, pinned_settings: None) -> None:
        from pg_atlas.metrics.materialize_maintenance import _responsiveness_signal_dict

        now = dt.datetime.now(dt.UTC)
        signal = ResponsivenessSignal(
            state=STATE_OK,
            as_of=now.isoformat(),
            window_days=180,
            interval_days=7,
            include_drafts=True,
            response_fraction=0.9,
        )

        block, rankable = _responsiveness_signal_dict(
            signal, now=now, expected_include_drafts=True, expected_declared_maintainers=None
        )

        assert rankable == 0.9
        assert block["response_fraction"]["state"] == STATE_OK

    def test_mismatched_declared_maintainers_is_context(self, pinned_settings: None) -> None:
        from pg_atlas.metrics.materialize_maintenance import _responsiveness_signal_dict

        now = dt.datetime.now(dt.UTC)
        signal = ResponsivenessSignal(
            state=STATE_OK,
            as_of=now.isoformat(),
            window_days=180,
            interval_days=7,
            declared_maintainers=["erin"],
            response_fraction=0.9,
        )

        block, rankable = _responsiveness_signal_dict(
            signal, now=now, expected_include_drafts=None, expected_declared_maintainers=None
        )

        assert rankable is None
        assert block["response_fraction"]["reason"] == "parameter-mismatch"
        assert block["declared_maintainers"] == ["erin"]

    def test_matching_declared_maintainers_rank(self, pinned_settings: None) -> None:
        from pg_atlas.metrics.materialize_maintenance import _responsiveness_signal_dict

        now = dt.datetime.now(dt.UTC)
        signal = ResponsivenessSignal(
            state=STATE_OK,
            as_of=now.isoformat(),
            window_days=180,
            interval_days=7,
            declared_maintainers=["erin", "rando"],
            response_fraction=0.9,
        )

        block, rankable = _responsiveness_signal_dict(
            signal, now=now, expected_include_drafts=None, expected_declared_maintainers=["erin", "rando"]
        )

        assert rankable == 0.9
        assert block["response_fraction"]["state"] == STATE_OK


class TestDeclaredMaintainersExpectedList:
    def _profile(self, canonical_id: str, signal: ResponsivenessSignal, now: dt.datetime) -> Any:
        from pg_atlas.metrics.maintenance import MaintenanceSignals
        from pg_atlas.metrics.materialize_maintenance import _build_repo_profile

        signals = MaintenanceSignals(
            schema_version=1,
            collected_at=now.isoformat(),
            window_days=180,
            response_interval_days=7,
            issue_responsiveness=signal,
        )

        return _build_repo_profile(
            1, canonical_id, None, signals, None, None, _unavailable_activity(), now=now, deploy_on_push=False
        )

    def test_declared_repo_ranks_under_matching_list(self, pinned_settings: None, monkeypatch: pytest.MonkeyPatch) -> None:
        """The expected list is derived from the canonical id: prefix stripped, lowercased."""
        monkeypatch.setattr(settings, "MAINTENANCE_DECLARED_MAINTAINERS", "Owner/Repo=Rando|ERIN")
        now = dt.datetime.now(dt.UTC)
        signal = ResponsivenessSignal(
            state=STATE_OK,
            as_of=now.isoformat(),
            window_days=180,
            interval_days=7,
            declared_maintainers=["erin", "rando"],
            response_fraction=0.5,
        )

        profile = self._profile("pkg:github/Owner/Repo", signal, now)

        assert profile.signals["issue_responsiveness"]["response_fraction"]["state"] == STATE_OK
        assert profile.rankable["issue_responsiveness.response_fraction"] == 0.5

    def test_declared_payload_against_empty_setting_is_context(self, pinned_settings: None) -> None:
        now = dt.datetime.now(dt.UTC)
        signal = ResponsivenessSignal(
            state=STATE_OK,
            as_of=now.isoformat(),
            window_days=180,
            interval_days=7,
            declared_maintainers=["erin"],
            response_fraction=0.5,
        )

        profile = self._profile("pkg:github/owner/repo", signal, now)

        block = profile.signals["issue_responsiveness"]["response_fraction"]
        assert block["state"] == STATE_INCOMPLETE
        assert block["reason"] == "parameter-mismatch"
        assert "issue_responsiveness.response_fraction" not in profile.rankable


class TestArtifactTransportAndDamage:
    async def test_transport_timeout_is_contained_to_the_repo(
        self, monkeypatch: pytest.MonkeyPatch, pinned_settings: None
    ) -> None:
        """An IPFS gateway timeout marks one repo incomplete; the pass survives."""

        import httpx

        import pg_atlas.metrics.materialize_maintenance as mm
        from pg_atlas.metrics.materialize_maintenance import _ArtifactRef, _commit_activity_for_repo

        async def _timeout(path: str) -> bytes:
            raise httpx.ReadTimeout("gateway timeout")

        monkeypatch.setattr(mm, "read_artifact", _timeout)
        now = dt.datetime.now(dt.UTC)
        artifact = _ArtifactRef(artifact_path="git-logs/x.gitlog", since_months=24, submitted_at=now - dt.timedelta(days=1))

        result, was_read = await _commit_activity_for_repo("pkg:github/test/timeout", artifact, now=now)

        assert was_read is False
        assert result.state == STATE_INCOMPLETE
        assert result.reason == "artifact-unreadable"

    async def test_malformed_artifact_is_context_not_a_ranked_zero(
        self, monkeypatch: pytest.MonkeyPatch, pinned_settings: None
    ) -> None:
        import pg_atlas.metrics.materialize_maintenance as mm
        from pg_atlas.metrics.materialize_maintenance import _ArtifactRef, _commit_activity_for_repo

        async def _corrupt(path: str) -> bytes:
            return b"corrupted nonempty artifact\n"

        monkeypatch.setattr(mm, "read_artifact", _corrupt)
        now = dt.datetime.now(dt.UTC)
        artifact = _ArtifactRef(artifact_path="git-logs/x.gitlog", since_months=24, submitted_at=now - dt.timedelta(days=1))

        result, was_read = await _commit_activity_for_repo("pkg:github/test/corrupt", artifact, now=now)

        assert was_read is True
        assert result.state == STATE_INCOMPLETE
        assert result.reason == "artifact-malformed"
        assert result.commit_count == 0

    async def test_partially_malformed_artifact_reports_floor_count(
        self, monkeypatch: pytest.MonkeyPatch, pinned_settings: None
    ) -> None:
        import pg_atlas.metrics.materialize_maintenance as mm
        from pg_atlas.metrics.materialize_maintenance import _ArtifactRef, _commit_activity_for_repo

        now = dt.datetime.now(dt.UTC)
        eligible = f"Alice\x00alice@example.org\x00{(now - dt.timedelta(days=10)).isoformat()}\x00{'a' * 40}"
        bot = (
            f"dependabot[bot]\x001+dependabot[bot]@users.noreply.github.com"
            f"\x00{(now - dt.timedelta(days=5)).isoformat()}\x00{'b' * 40}"
        )
        out_of_window = f"Bob\x00bob@example.org\x00{(now - dt.timedelta(days=400)).isoformat()}\x00{'c' * 40}"

        async def _partial(path: str) -> bytes:
            return f"{eligible}\n{bot}\n{out_of_window}\ntruncated garbage\n".encode()

        monkeypatch.setattr(mm, "read_artifact", _partial)
        artifact = _ArtifactRef(artifact_path="git-logs/x.gitlog", since_months=24, submitted_at=now - dt.timedelta(days=1))

        result, was_read = await _commit_activity_for_repo("pkg:github/test/partial", artifact, now=now)

        # The context value is the ordinary filtered window count: the bot
        # and the 400-day-old commit are excluded, exactly as when ranking.
        assert was_read is True
        assert result.state == STATE_INCOMPLETE
        assert result.reason == "artifact-malformed"
        assert result.commit_count == 1


class TestBlockedFallbackAgainstUsableDepsdev:
    def test_out_counting_blocked_fallback_cannot_win_source_selection(self, pinned_settings: None) -> None:
        """Blocked fallback data never ranks, even when it holds more dates."""

        from pg_atlas.metrics.maintenance import ReleaseFallback
        from pg_atlas.metrics.materialize_maintenance import _build_repo_profile

        now = dt.datetime.now(dt.UTC)
        depsdev_newest = now.date() - dt.timedelta(days=20)
        releases = [
            Release(
                purl="pkg:npm/a",
                version=f"1.{i}.0",
                release_date=f"{(depsdev_newest - dt.timedelta(days=30 * i)).isoformat()}T10:00:00Z",
            )
            for i in range(3)
        ]
        fallback = ReleaseFallback(
            state=STATE_INCOMPLETE,
            as_of=now.isoformat(),
            publication_dates=sorted((now.date() - dt.timedelta(days=2 + 10 * i)).isoformat() for i in range(6)),
            complete=False,
            incomplete_reason="page-cap",
        )

        profile = _build_repo_profile(
            1,
            "pkg:github/test/outcount",
            releases,
            _signals_with_fallback(fallback, now),
            None,
            None,
            _unavailable_activity(),
            now=now,
            deploy_on_push=False,
        )
        block = profile.signals["release_cadence"]

        assert block["source"] == "depsdev"
        assert block["days_since_last_release"]["state"] == STATE_OK
        assert block["days_since_last_release"]["value"] == float((now.date() - depsdev_newest).days)
        assert block["median_gap_days"]["state"] == STATE_OK
        assert profile.rankable["release_cadence.days_since_last_release"] == float((now.date() - depsdev_newest).days)


# ---------------------------------------------------------------------------
# release_ships_code and Go pseudo-versions in the cadence block
# ---------------------------------------------------------------------------


def _monthly_releases(purl: str, now: dt.datetime, count: int = 6) -> list[Release]:
    """Tagged releases 30 days apart, newest 15 days ago."""

    return [
        Release(
            purl=purl,
            version=f"1.{i}.0",
            release_date=f"{(now.date() - dt.timedelta(days=15 + 30 * i)).isoformat()}T10:00:00Z",
        )
        for i in range(count)
    ]


def _cadence_profile(
    releases: list[Release] | None,
    now: dt.datetime,
    *,
    deploy_on_push: bool,
    fallback: Any = None,
) -> Any:
    from pg_atlas.metrics.materialize_maintenance import _build_repo_profile

    return _build_repo_profile(
        1,
        "pkg:github/test/ships-code",
        releases,
        _signals_with_fallback(fallback, now),
        None,
        None,
        _unavailable_activity(),
        now=now,
        deploy_on_push=deploy_on_push,
    )


class TestReleaseShipsCode:
    def test_registry_records_ship_code(self, pinned_settings: None) -> None:
        now = dt.datetime.now(dt.UTC)

        profile = _cadence_profile(_monthly_releases("pkg:cargo/soroban-sdk", now), now, deploy_on_push=False)

        assert profile.signals["release_cadence"]["release_ships_code"] is True

    def test_declaration_wins_over_registry_records_and_ranking_is_unchanged(self, pinned_settings: None) -> None:
        now = dt.datetime.now(dt.UTC)
        releases = _monthly_releases("pkg:npm/service", now)

        undeclared = _cadence_profile(releases, now, deploy_on_push=False)
        declared = _cadence_profile(releases, now, deploy_on_push=True)

        assert declared.signals["release_cadence"]["release_ships_code"] is False
        assert declared.rankable == undeclared.rankable
        assert declared.rankable["release_cadence.median_gap_days"] == 30.0
        for field in ("median_gap_days", "days_since_last_release"):
            assert declared.signals["release_cadence"][field] == undeclared.signals["release_cadence"][field]

    def test_github_releases_only_repo_is_undetermined(self, pinned_settings: None) -> None:
        from pg_atlas.metrics.maintenance import ReleaseFallback

        now = dt.datetime.now(dt.UTC)
        fallback = ReleaseFallback(
            state=STATE_OK,
            as_of=now.isoformat(),
            publication_dates=sorted((now.date() - dt.timedelta(days=10 + 30 * i)).isoformat() for i in range(3)),
        )

        profile = _cadence_profile(None, now, deploy_on_push=False, fallback=fallback)
        block = profile.signals["release_cadence"]

        assert block["source"] == "github-releases"
        assert block["release_ships_code"] is None

    def test_golang_records_alone_are_undetermined(self, pinned_settings: None) -> None:
        now = dt.datetime.now(dt.UTC)

        profile = _cadence_profile(_monthly_releases("pkg:golang/github.com/test/service", now), now, deploy_on_push=False)

        assert profile.signals["release_cadence"]["source"] == "depsdev"
        assert profile.signals["release_cadence"]["release_ships_code"] is None


class TestGoPseudoVersionCadence:
    def test_pseudo_versions_do_not_outvote_github_releases(self, pinned_settings: None) -> None:
        """Commit-day pseudo-versions are not publications; the GitHub Releases dates carry the cadence."""

        from pg_atlas.metrics.maintenance import ReleaseFallback

        now = dt.datetime.now(dt.UTC)
        pseudo_versions = [
            Release(
                purl="pkg:golang/github.com/test/swift-sdk",
                version=f"v0.0.0-{(now - dt.timedelta(days=day)).strftime('%Y%m%d%H%M%S')}-3f2a1b4c5d6e",
                release_date=f"{(now.date() - dt.timedelta(days=day)).isoformat()}T09:00:00Z",
            )
            for day in range(1, 9)
        ]
        release_dates = [now.date() - dt.timedelta(days=20 + 30 * i) for i in range(3)]
        fallback = ReleaseFallback(
            state=STATE_OK,
            as_of=now.isoformat(),
            publication_dates=sorted(day.isoformat() for day in release_dates),
        )

        profile = _cadence_profile(pseudo_versions, now, deploy_on_push=False, fallback=fallback)
        block = profile.signals["release_cadence"]

        assert block["source"] == "github-releases"
        assert block["shipping_events"] == 3
        assert block["days_since_last_release"]["state"] == STATE_OK
        assert block["days_since_last_release"]["value"] == 20.0
        assert block["days_since_last_release"]["as_of"] == now.isoformat()
        assert block["median_gap_days"]["value"] == 30.0
        assert block["release_ships_code"] is None


class TestDeployOnPushDeclaration:
    async def test_declared_repo_reports_false_and_still_ranks(
        self,
        rollback_db_session: AsyncSession,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        session = rollback_db_session
        fixture = await _seed_maintenance_fixture(session, tmp_path_factory, monkeypatch)
        full_canonical = (await session.execute(select(Repo.canonical_id).where(Repo.id == fixture.repo_full_id))).scalar_one()
        bare = full_canonical.removeprefix("pkg:github/").upper()
        monkeypatch.setattr(settings, "MAINTENANCE_DEPLOY_ON_PUSH_REPOS", f"{bare}, not-an-owner-repo")

        with caplog.at_level("WARNING", logger="pg_atlas.metrics.materialize_maintenance"):
            await materialize_maintenance_profiles(session, now=fixture.now)

        assert "maintenance-metric deploy-on-push list entry rejected: 'not-an-owner-repo'" in caplog.text

        profile = await _load_profile(session, fixture.repo_full_id)
        assert profile is not None
        cadence = profile["signals"]["release_cadence"]
        assert cadence["release_ships_code"] is False
        assert cadence["median_gap_days"]["state"] == STATE_OK
        assert cadence["median_gap_days"]["pool_size"] == 1
        assert "percentile" in cadence["median_gap_days"]


# ---------------------------------------------------------------------------
# days_since_push: as-of and freshness from the push observation time
# ---------------------------------------------------------------------------


def _push_activity(
    now: dt.datetime,
    *,
    collected_at: dt.datetime | None,
    pushed_at: dt.datetime | None,
    observed_at: dt.datetime | None,
) -> Any:
    from pg_atlas.metrics.materialize_maintenance import _build_repo_profile

    collected = (
        MaintenanceSignals(
            schema_version=1,
            collected_at=collected_at.isoformat(),
            window_days=180,
            response_interval_days=7,
        )
        if collected_at is not None
        else None
    )

    return _build_repo_profile(
        1,
        "pkg:github/test/push",
        None,
        collected,
        pushed_at,
        observed_at,
        _unavailable_activity(),
        now=now,
        deploy_on_push=False,
    )


class TestPushObservationTime:
    def test_observation_time_is_the_as_of_and_freshness_basis(self, pinned_settings: None) -> None:
        now = dt.datetime.now(dt.UTC)
        observed = now - dt.timedelta(days=1)

        profile = _push_activity(
            now,
            collected_at=now - dt.timedelta(days=60),
            pushed_at=now - dt.timedelta(days=3),
            observed_at=observed,
        )
        entry = profile.signals["activity_recency"]["days_since_push"]

        assert entry["state"] == STATE_OK
        assert entry["value"] == 3.0
        assert entry["as_of"] == observed.isoformat()
        assert profile.rankable["activity_recency.days_since_push"] == 3.0

    def test_stale_observation_is_context_even_after_a_fresh_collection(self, pinned_settings: None) -> None:
        now = dt.datetime.now(dt.UTC)
        observed = now - dt.timedelta(days=30)

        profile = _push_activity(
            now,
            collected_at=now,
            pushed_at=now - dt.timedelta(days=31),
            observed_at=observed,
        )
        entry = profile.signals["activity_recency"]["days_since_push"]

        assert entry["state"] == STATE_INCOMPLETE
        assert entry["reason"] == REASON_STALE_COLLECTION
        assert entry["as_of"] == observed.isoformat()
        assert "activity_recency.days_since_push" not in profile.rankable

    def test_crawled_repo_without_collection_ranks_on_a_fresh_observation(self, pinned_settings: None) -> None:
        now = dt.datetime.now(dt.UTC)

        profile = _push_activity(
            now,
            collected_at=None,
            pushed_at=now - dt.timedelta(days=5),
            observed_at=now - dt.timedelta(hours=6),
        )

        assert profile.signals["activity_recency"]["days_since_push"]["state"] == STATE_OK
        assert profile.rankable["activity_recency.days_since_push"] == 5.0

    def test_row_without_observation_time_falls_back_to_collected_at(self, pinned_settings: None) -> None:
        now = dt.datetime.now(dt.UTC)
        collected_at = now - dt.timedelta(days=2)

        profile = _push_activity(now, collected_at=collected_at, pushed_at=now - dt.timedelta(days=4), observed_at=None)
        entry = profile.signals["activity_recency"]["days_since_push"]

        assert entry["state"] == STATE_OK
        assert entry["as_of"] == collected_at.isoformat()

    def test_row_without_any_observation_time_is_context(self, pinned_settings: None) -> None:
        now = dt.datetime.now(dt.UTC)

        profile = _push_activity(now, collected_at=None, pushed_at=now - dt.timedelta(days=4), observed_at=None)
        entry = profile.signals["activity_recency"]["days_since_push"]

        assert entry["state"] == STATE_INCOMPLETE
        assert entry["reason"] == REASON_STALE_COLLECTION
        assert "as_of" not in entry
        assert "activity_recency.days_since_push" not in profile.rankable


class TestPushObservationMaterialization:
    async def test_materializer_reads_the_observation_column(
        self,
        rollback_db_session: AsyncSession,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A crawled repo that was never collected ranks on its fresh push observation."""

        session = rollback_db_session
        fixture = await _seed_maintenance_fixture(session, tmp_path_factory, monkeypatch)
        observed = fixture.now - dt.timedelta(hours=3)
        await session.execute(
            update(Repo)
            .where(Repo.id == fixture.repo_sparse_id)
            .values(pushed_at=fixture.now - dt.timedelta(days=6), pushed_at_observed_at=observed)
            .execution_options(synchronize_session=False)
        )
        # The stale repo's collection is 60 days old; a fresh observation of
        # its push time makes the push signal rankable again.
        await session.execute(
            update(Repo)
            .where(Repo.id == fixture.repo_stale_id)
            .values(pushed_at_observed_at=observed)
            .execution_options(synchronize_session=False)
        )

        stats, _ = await materialize_maintenance_profiles(session, now=fixture.now)

        assert stats.pool_sizes["activity_recency.days_since_push"] == 3

        sparse = await _load_profile(session, fixture.repo_sparse_id)
        assert sparse is not None
        sparse_push = sparse["signals"]["activity_recency"]["days_since_push"]
        assert sparse_push["state"] == STATE_OK
        assert sparse_push["value"] == 6.0
        assert sparse_push["as_of"] == observed.isoformat()
        assert sparse_push["pool_size"] == 3

        stale = await _load_profile(session, fixture.repo_stale_id)
        assert stale is not None
        assert stale["signals"]["activity_recency"]["days_since_push"]["state"] == STATE_OK
        assert stale["signals"]["issue_responsiveness"]["response_fraction"]["state"] == STATE_INCOMPLETE


# ---------------------------------------------------------------------------
# Write phase: one ascending-id pass, stale removals re-checked at write time
# ---------------------------------------------------------------------------


class _RecordingResult:
    """Result stand-in carrying the rowcount the recorded UPDATE reports."""

    def __init__(self, rowcount: int) -> None:
        self.rowcount = rowcount


class _RecordingSession:
    """Session stand-in that records the target repo id of every UPDATE."""

    def __init__(self, rowcount: int = 1) -> None:
        self.updated_ids: list[int] = []
        self.statements: list[str] = []
        self._rowcount = rowcount

    async def execute(self, statement: Any) -> _RecordingResult:
        from sqlalchemy.dialects import postgresql

        compiled = statement.compile(dialect=postgresql.dialect())
        self.updated_ids.append(int(compiled.params["id_1"]))
        self.statements.append(str(compiled))

        return _RecordingResult(self._rowcount)


class TestWriteProfilesAndClearStale:
    async def test_writes_follow_ascending_id_across_profiles_and_removals(self) -> None:
        """A stale id below an eligible id is written first: lock order holds for the whole transaction."""

        from pg_atlas.metrics.materialize_maintenance import _write_profiles_and_clear_stale

        session = _RecordingSession()
        profile_payloads: dict[int, dict[str, Any]] = {20: {"schema_version": 1}, 40: {"schema_version": 1}}

        written, cleared = await _write_profiles_and_clear_stale(
            session,  # pyright: ignore[reportArgumentType]
            profile_payloads,
            [30, 10],
        )

        assert session.updated_ids == [10, 20, 30, 40]
        assert (written, cleared) == (2, 2)
        # Removals carry their eligibility predicates; profile writes do not.
        assert "project_id IS NULL" in session.statements[0]
        assert "project_id IS NULL" not in session.statements[1]

    async def test_id_in_both_sets_is_written_as_a_profile(self) -> None:
        from pg_atlas.metrics.materialize_maintenance import _write_profiles_and_clear_stale

        session = _RecordingSession()

        written, cleared = await _write_profiles_and_clear_stale(
            session,  # pyright: ignore[reportArgumentType]
            {15: {"schema_version": 1}},
            [15],
        )

        assert session.updated_ids == [15]
        assert "project_id IS NULL" not in session.statements[0]
        assert (written, cleared) == (1, 0)

    async def test_removal_counts_only_affected_rows(self) -> None:
        from pg_atlas.metrics.materialize_maintenance import _write_profiles_and_clear_stale

        session = _RecordingSession(rowcount=0)

        written, cleared = await _write_profiles_and_clear_stale(
            session,  # pyright: ignore[reportArgumentType]
            {},
            [7],
        )

        assert session.updated_ids == [7]
        assert (written, cleared) == (0, 0)


async def _seed_stale_candidate(session: AsyncSession, *, with_profile: bool) -> int:
    suffix = uuid4().hex[:8]
    metadata: dict[str, Any] = {"other_key": "kept"}
    if with_profile:
        metadata[MAINTENANCE_PROFILE_KEY] = {"schema_version": 1}

    repo = Repo(
        canonical_id=f"pkg:github/test/stale-candidate-{suffix}",
        display_name=f"stale-candidate-{suffix}",
        visibility=Visibility.public,
        latest_version="1.0.0",
        repo_metadata=metadata,
    )
    session.add(repo)
    await session.flush()

    return repo.id


class TestStaleRemovalRecheck:
    async def test_candidate_reassociated_before_clearing_keeps_its_profile(
        self,
        rollback_db_session: AsyncSession,
    ) -> None:
        from pg_atlas.metrics.materialize_maintenance import _write_profiles_and_clear_stale

        session = rollback_db_session
        project = Project(
            canonical_id=f"daoip-5:stellar:project:reassociated-{uuid4().hex[:8]}",
            display_name="Reassociated",
            project_type=ProjectType.public_good,
            activity_status=ActivityStatus.live,
        )
        session.add(project)
        await session.flush()
        repo_id = await _seed_stale_candidate(session, with_profile=True)

        # Selected as a candidate while unlinked, then linked to a project
        # before the write pass reaches it.
        await session.execute(
            update(Repo).where(Repo.id == repo_id).values(project_id=project.id).execution_options(synchronize_session=False)
        )

        written, cleared = await _write_profiles_and_clear_stale(session, {}, [repo_id])

        assert (written, cleared) == (0, 0)
        profile = await _load_profile(session, repo_id)
        assert profile == {"schema_version": 1}

    async def test_already_cleared_candidate_counts_nothing(
        self,
        rollback_db_session: AsyncSession,
    ) -> None:
        from pg_atlas.metrics.materialize_maintenance import _write_profiles_and_clear_stale

        session = rollback_db_session
        repo_id = await _seed_stale_candidate(session, with_profile=False)

        written, cleared = await _write_profiles_and_clear_stale(session, {}, [repo_id])

        assert (written, cleared) == (0, 0)
        metadata = (await session.execute(select(Repo.repo_metadata).where(Repo.id == repo_id))).scalar_one()
        assert metadata == {"other_key": "kept"}

    async def test_unlinked_candidate_is_cleared_and_counted(
        self,
        rollback_db_session: AsyncSession,
    ) -> None:
        from pg_atlas.metrics.materialize_maintenance import _write_profiles_and_clear_stale

        session = rollback_db_session
        repo_id = await _seed_stale_candidate(session, with_profile=True)

        written, cleared = await _write_profiles_and_clear_stale(session, {}, [repo_id])

        assert (written, cleared) == (0, 1)
        metadata = (await session.execute(select(Repo.repo_metadata).where(Repo.id == repo_id))).scalar_one()
        assert metadata == {"other_key": "kept"}


class TestGatedCliProcess:
    def test_disabled_gate_exits_cleanly_without_a_gateway_setting(self, tmp_path: Path) -> None:
        """The scheduled command reaches its gate when the IPFS gateway setting is absent."""

        import os
        import subprocess
        import sys

        from tests.conftest import get_test_database_url

        database_url = get_test_database_url()
        if not database_url:
            pytest.skip("PG_ATLAS_DATABASE_URL / PG_ATLAS_TEST_DATABASE_URL not set; skipping database integration test")

        env = {key: value for key, value in os.environ.items() if not key.startswith("PG_ATLAS_")}
        env["PG_ATLAS_DATABASE_URL"] = database_url
        env["PG_ATLAS_MAINTENANCE_METRIC_ENABLED"] = "false"

        result = subprocess.run(
            [sys.executable, "-m", "pg_atlas.metrics.materialize_maintenance", "--gated"],
            capture_output=True,
            text=True,
            # Outside the checkout, so no local .env file feeds the settings.
            cwd=tmp_path,
            env=env,
            timeout=120,
        )

        output = result.stdout + result.stderr
        assert result.returncode == 0, output
        assert "MAINTENANCE_METRIC_ENABLED is false, skipping gated run" in output
        assert "gate_skipped=True" in output
