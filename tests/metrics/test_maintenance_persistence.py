"""
Tests for maintenance-signal persistence: the per-signal merge policy,
locked persist-and-merge against the stored payload, repository resolution,
concurrent and late-run persistence, and both stored null forms of
``repo_metadata``.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import datetime as dt
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from pg_atlas.db_models.base import Visibility
from pg_atlas.db_models.repo_vertex import Repo
from pg_atlas.metrics.maintenance import (
    MAINTENANCE_SIGNALS_KEY,
    STATE_INCOMPLETE,
    STATE_NOT_APPLICABLE,
    STATE_OK,
    BacklogSignal,
    MaintenanceSignals,
    ResponsivenessSignal,
    signals_to_payload,
)
from pg_atlas.procrastinate.github_maintenance_persistence import (
    AmbiguousMaintenanceSourceRepo,
    MaintenanceSourceRepoNotFound,
    ResolvedRepo,
    merge_collected_signals,
    persist_collected,
    resolve_repo,
)
from tests.metrics.maintenance_support import NOW, iso, ok_signals, signals_with


class TestMergeCollectedSignals:
    def test_incomplete_never_overwrites_complete(self) -> None:
        old_ok = ResponsivenessSignal(state=STATE_OK, as_of=iso(3), response_fraction=0.8)
        new_incomplete = ResponsivenessSignal(state=STATE_INCOMPLETE, as_of=iso(0), incomplete_reason="rate-limited")

        merged = merge_collected_signals(signals_with(old_ok), signals_with(new_incomplete))

        assert merged.issue_responsiveness == old_ok
        assert merged.collected_at == NOW.isoformat()

    def test_fresh_ok_replaces_old_ok(self) -> None:
        old_ok = BacklogSignal(state=STATE_OK, as_of=iso(3), open_count=5)
        new_ok = BacklogSignal(state=STATE_OK, as_of=iso(0), open_count=6)

        merged = merge_collected_signals(signals_with(None, old_ok), signals_with(None, new_ok))

        assert merged.issue_backlog == new_ok

    def test_not_applicable_replaces_old_ok(self) -> None:
        """A repo that disabled its tracker must not keep stale real values."""
        old_ok = BacklogSignal(state=STATE_OK, as_of=iso(3), open_count=5)
        new_na = BacklogSignal(state=STATE_NOT_APPLICABLE, as_of=iso(0))

        merged = merge_collected_signals(signals_with(None, old_ok), signals_with(None, new_na))

        assert merged.issue_backlog == new_na

    def test_newer_complete_signal_survives_a_late_older_run(self) -> None:
        """Observation order wins between two complete measurements."""
        newer = BacklogSignal(state=STATE_OK, as_of=iso(1), open_count=6)
        older = BacklogSignal(state=STATE_OK, as_of=iso(3), open_count=5)

        merged = merge_collected_signals(signals_with(None, newer), signals_with(None, older))

        assert merged.issue_backlog == newer

    def test_preserved_signal_keeps_its_own_parameters(self) -> None:
        """A kept signal travels with the window and interval it measured."""
        old_ok = ResponsivenessSignal(state=STATE_OK, as_of=iso(3), window_days=180, interval_days=7, response_fraction=0.8)
        new_incomplete = ResponsivenessSignal(state=STATE_INCOMPLETE, as_of=iso(0), incomplete_reason="rate-limited")

        merged = merge_collected_signals(signals_with(old_ok), signals_with(new_incomplete))

        assert merged.issue_responsiveness is not None
        assert merged.issue_responsiveness.window_days == 180
        assert merged.issue_responsiveness.interval_days == 7

    def test_late_older_ok_cannot_undo_a_newer_not_applicable(self) -> None:
        """Applicability transitions are observations; ordering applies to them too."""
        stored_na = BacklogSignal(state=STATE_NOT_APPLICABLE, as_of=iso(0))
        late_ok = BacklogSignal(state=STATE_OK, as_of=iso(2), open_count=25)

        merged = merge_collected_signals(
            signals_with(None, stored_na, collected_at=iso(0)),
            signals_with(None, late_ok, collected_at=iso(2)),
        )

        assert merged.issue_backlog == stored_na

    def test_newer_ok_survives_a_late_older_not_applicable(self) -> None:
        stored_ok = BacklogSignal(state=STATE_OK, as_of=iso(0), open_count=3)
        late_na = BacklogSignal(state=STATE_NOT_APPLICABLE, as_of=iso(2))

        merged = merge_collected_signals(
            signals_with(None, stored_ok, collected_at=iso(0)),
            signals_with(None, late_na, collected_at=iso(2)),
        )

        assert merged.issue_backlog == stored_ok

    def test_run_level_fields_come_from_the_newer_run(self) -> None:
        newer = signals_with(None, None, collected_at=iso(0))
        newer.issues_enabled = False
        older = signals_with(None, None, collected_at=iso(2))
        older.issues_enabled = True

        merged = merge_collected_signals(newer, older)

        assert merged.collected_at == iso(0)
        assert merged.issues_enabled is False

    def test_no_previous_payload_keeps_new(self) -> None:
        new = signals_with(ResponsivenessSignal(state=STATE_INCOMPLETE, as_of=iso(0), incomplete_reason="page-cap"))
        assert merge_collected_signals(None, new) == new


class TestPersistCollected:
    async def test_merge_preserves_foreign_metadata_and_complete_signals(
        self,
        rollback_db_session: AsyncSession,
    ) -> None:
        session = rollback_db_session
        suffix = uuid4().hex[:8]
        now = dt.datetime.now(dt.UTC)

        old = ok_signals(now, as_of=now - dt.timedelta(days=3))
        repo = Repo(
            canonical_id=f"pkg:github/Test/persist-{suffix}",
            display_name=f"persist-{suffix}",
            visibility=Visibility.public,
            latest_version="1.0.0",
            repo_metadata={
                "adoption_downloads_by_purl": {"pkg:npm/a": 42},
                MAINTENANCE_SIGNALS_KEY: signals_to_payload(old),
            },
        )
        session.add(repo)
        await session.flush()

        stamp = now.isoformat()
        new = MaintenanceSignals(
            schema_version=1,
            collected_at=stamp,
            window_days=180,
            response_interval_days=7,
            requests_used=4,
            issues_enabled=True,
            issue_responsiveness=ResponsivenessSignal(state=STATE_INCOMPLETE, as_of=stamp, incomplete_reason="rate-limited"),
            issue_backlog=BacklogSignal(state=STATE_OK, as_of=stamp, open_count=7, median_open_age_days=12.0),
        )
        pushed_at = now - dt.timedelta(days=1)

        resolved = await resolve_repo(session, "test", f"PERSIST-{suffix}")
        assert resolved.repo_id == repo.id
        await persist_collected(session, resolved, new, pushed_at)

        row = (
            await session.execute(
                select(Repo.repo_metadata, Repo.pushed_at, Repo.pushed_at_observed_at).where(Repo.id == repo.id)
            )
        ).one()
        metadata, stored_pushed_at, stored_observed_at = row

        assert metadata["adoption_downloads_by_purl"] == {"pkg:npm/a": 42}
        assert stored_pushed_at == pushed_at
        # The run's collected_at is the observation time of its pushed_at.
        assert stored_observed_at == now

        stored = metadata[MAINTENANCE_SIGNALS_KEY]
        # The incomplete responsiveness result must not clobber the stored
        # complete signal; the fresh complete backlog replaces the old one.
        assert stored["issue_responsiveness"]["state"] == STATE_OK
        assert stored["issue_responsiveness"]["response_fraction"] == 0.8
        assert stored["issue_backlog"]["open_count"] == 7
        assert stored["collected_at"] == stamp

    async def test_resolution_errors(self, rollback_db_session: AsyncSession) -> None:
        session = rollback_db_session
        suffix = uuid4().hex[:8]

        with pytest.raises(MaintenanceSourceRepoNotFound):
            await resolve_repo(session, "nobody", f"missing-{suffix}")

        for casing in (f"Amb-{suffix}", f"amb-{suffix}"):
            session.add(
                Repo(
                    canonical_id=f"pkg:github/test/{casing}",
                    display_name=casing,
                    visibility=Visibility.public,
                    latest_version="1.0.0",
                )
            )
        await session.flush()

        with pytest.raises(AmbiguousMaintenanceSourceRepo):
            await resolve_repo(session, "test", f"amb-{suffix}")


class TestConcurrentPersistence:
    async def test_successive_persists_preserve_the_complete_signal(
        self,
        rollback_db_session: AsyncSession,
    ) -> None:
        """The persist transaction merges against the locked current row."""

        from pg_atlas.metrics.maintenance import signals_from_metadata

        session = rollback_db_session
        suffix = uuid4().hex[:8]
        now = dt.datetime.now(dt.UTC)

        repo = Repo(
            canonical_id=f"pkg:github/test/race-{suffix}",
            display_name=f"race-{suffix}",
            visibility=Visibility.public,
            latest_version="1.0.0",
        )
        session.add(repo)
        await session.flush()

        # Both runs resolve while the row has no payload.
        resolved = await resolve_repo(session, "test", f"race-{suffix}")

        # Run A persists a complete backlog signal first.
        complete = MaintenanceSignals(
            schema_version=1,
            collected_at=now.isoformat(),
            window_days=180,
            response_interval_days=7,
            issue_backlog=BacklogSignal(state=STATE_OK, as_of=now.isoformat(), open_count=4, median_open_age_days=12.0),
        )
        await persist_collected(session, resolved, complete, None)

        # A second persist for the same repo writes an incomplete run; its
        # merge reads the first persist's write in this transaction under the
        # row lock.
        incomplete = MaintenanceSignals(
            schema_version=1,
            collected_at=(now + dt.timedelta(minutes=1)).isoformat(),
            window_days=180,
            response_interval_days=7,
            issue_backlog=BacklogSignal(
                state=STATE_INCOMPLETE, as_of=(now + dt.timedelta(minutes=1)).isoformat(), incomplete_reason="rate-limited"
            ),
        )
        await persist_collected(session, resolved, incomplete, None)

        metadata = (await session.execute(select(Repo.repo_metadata).where(Repo.id == repo.id))).scalar_one()
        stored = signals_from_metadata(metadata)
        assert stored is not None
        assert stored.issue_backlog is not None
        assert stored.issue_backlog.state == STATE_OK
        assert stored.issue_backlog.open_count == 4

    async def test_merge_survives_sql_null_and_json_null_metadata(
        self,
        rollback_db_session: AsyncSession,
    ) -> None:
        """Both storage forms of an absent payload merge into a proper object."""

        from sqlalchemy import text

        from pg_atlas.metrics.maintenance import MAINTENANCE_SIGNALS_KEY

        session = rollback_db_session
        suffix = uuid4().hex[:8]
        now = dt.datetime.now(dt.UTC)

        repos: list[Repo] = []
        for marker in ("sqlnull", "jsonnull"):
            repo = Repo(
                canonical_id=f"pkg:github/test/{marker}-{suffix}",
                display_name=f"{marker}-{suffix}",
                visibility=Visibility.public,
                latest_version="1.0.0",
            )
            session.add(repo)
            repos.append(repo)
        await session.flush()

        await session.execute(text("UPDATE repos SET metadata = NULL WHERE id = :id"), {"id": repos[0].id})
        await session.execute(sqlalchemy_update_json_null(repos[1].id))

        signals = MaintenanceSignals(
            schema_version=1,
            collected_at=now.isoformat(),
            window_days=180,
            response_interval_days=7,
            issue_backlog=BacklogSignal(state=STATE_OK, as_of=now.isoformat(), open_count=1, median_open_age_days=2.0),
        )

        for marker, repo in zip(("sqlnull", "jsonnull"), repos, strict=True):
            resolved = await resolve_repo(session, "test", f"{marker}-{suffix}")
            await persist_collected(session, resolved, signals, None)
            metadata = (await session.execute(select(Repo.repo_metadata).where(Repo.id == repo.id))).scalar_one()
            assert isinstance(metadata, dict), marker
            assert MAINTENANCE_SIGNALS_KEY in metadata, marker


def sqlalchemy_update_json_null(repo_id: int) -> Any:
    """An ORM ``None`` write, which this JSONB column stores as JSON null."""

    from sqlalchemy import update as _update

    return _update(Repo).where(Repo.id == repo_id).values(repo_metadata=None)


class TestLateRunPersistence:
    async def test_older_run_finishing_late_keeps_the_newer_push_observation(
        self,
        rollback_db_session: AsyncSession,
    ) -> None:
        """pushed_at and run-level fields pair with the newest overview."""

        from pg_atlas.metrics.maintenance import signals_from_metadata

        session = rollback_db_session
        suffix = uuid4().hex[:8]
        t1 = dt.datetime.now(dt.UTC) - dt.timedelta(minutes=10)
        t2 = dt.datetime.now(dt.UTC)
        push_old = t1 - dt.timedelta(days=3)
        push_new = t2 - dt.timedelta(days=1)

        repo = Repo(
            canonical_id=f"pkg:github/test/lateold-{suffix}",
            display_name=f"lateold-{suffix}",
            visibility=Visibility.public,
            latest_version="1.0.0",
        )
        session.add(repo)
        await session.flush()

        resolved = await resolve_repo(session, "test", f"lateold-{suffix}")

        newer_run = MaintenanceSignals(
            schema_version=1,
            collected_at=t2.isoformat(),
            window_days=180,
            response_interval_days=7,
            issues_enabled=True,
            issue_backlog=BacklogSignal(state=STATE_OK, as_of=t2.isoformat(), open_count=2, median_open_age_days=4.0),
        )
        await persist_collected(session, resolved, newer_run, push_new)

        older_run = MaintenanceSignals(
            schema_version=1,
            collected_at=t1.isoformat(),
            window_days=180,
            response_interval_days=7,
            issues_enabled=False,
            issue_backlog=BacklogSignal(state=STATE_OK, as_of=t1.isoformat(), open_count=9, median_open_age_days=90.0),
            pr_backlog=BacklogSignal(state=STATE_OK, as_of=t1.isoformat(), open_count=1, median_open_age_days=5.0),
        )
        await persist_collected(session, resolved, older_run, push_old)

        row = (
            await session.execute(
                select(Repo.repo_metadata, Repo.pushed_at, Repo.pushed_at_observed_at).where(Repo.id == repo.id)
            )
        ).one()
        metadata, stored_pushed_at, stored_observed_at = row
        stored = signals_from_metadata(metadata)

        assert stored_pushed_at == push_new
        assert stored_observed_at == t2
        assert stored is not None
        assert stored.collected_at == t2.isoformat()
        assert stored.issues_enabled is True
        assert stored.issue_backlog is not None
        assert stored.issue_backlog.open_count == 2
        # A slot the newer run did not observe still accepts the older data.
        assert stored.pr_backlog is not None
        assert stored.pr_backlog.open_count == 1


class TestPushedAtObservation:
    """``persist_collected`` records ``pushed_at`` through the shared monotonic writer."""

    async def _seed(
        self, session: AsyncSession, marker: str, *, pushed_at: dt.datetime, observed_at: dt.datetime
    ) -> ResolvedRepo:
        suffix = uuid4().hex[:8]
        repo = Repo(
            canonical_id=f"pkg:github/test/{marker}-{suffix}",
            display_name=f"{marker}-{suffix}",
            visibility=Visibility.public,
            latest_version="1.0.0",
            pushed_at=pushed_at,
            pushed_at_observed_at=observed_at,
        )
        session.add(repo)
        await session.flush()

        return await resolve_repo(session, "test", f"{marker}-{suffix}")

    async def _stored(self, session: AsyncSession, repo_id: int) -> tuple[dt.datetime | None, dt.datetime | None]:
        row = (await session.execute(select(Repo.pushed_at, Repo.pushed_at_observed_at).where(Repo.id == repo_id))).one()

        return row[0], row[1]

    def _run(self, collected_at: dt.datetime) -> MaintenanceSignals:
        return MaintenanceSignals(
            schema_version=1,
            collected_at=collected_at.isoformat(),
            window_days=180,
            response_interval_days=7,
        )

    async def test_bootstrap_observation_is_advanced_by_a_later_collection_of_the_same_push(
        self,
        rollback_db_session: AsyncSession,
    ) -> None:
        session = rollback_db_session
        now = dt.datetime.now(dt.UTC)
        push = now - dt.timedelta(days=4)
        crawled = now - dt.timedelta(days=2)
        resolved = await self._seed(session, "samepush", pushed_at=push, observed_at=crawled)

        await persist_collected(session, resolved, self._run(now), push)

        assert await self._stored(session, resolved.repo_id) == (push, now)

    async def test_collection_older_than_the_stored_observation_keeps_it(
        self,
        rollback_db_session: AsyncSession,
    ) -> None:
        """A run that finishes after a newer crawl observed the same push cannot roll its time back."""

        session = rollback_db_session
        now = dt.datetime.now(dt.UTC)
        push = now - dt.timedelta(days=4)
        resolved = await self._seed(session, "olderrun", pushed_at=push, observed_at=now)

        await persist_collected(session, resolved, self._run(now - dt.timedelta(hours=1)), push)

        assert await self._stored(session, resolved.repo_id) == (push, now)

    async def test_collection_with_an_earlier_push_changes_nothing(
        self,
        rollback_db_session: AsyncSession,
    ) -> None:
        session = rollback_db_session
        now = dt.datetime.now(dt.UTC)
        push = now - dt.timedelta(days=1)
        crawled = now - dt.timedelta(hours=2)
        resolved = await self._seed(session, "earlierpush", pushed_at=push, observed_at=crawled)

        await persist_collected(session, resolved, self._run(now), push - dt.timedelta(days=5))

        assert await self._stored(session, resolved.repo_id) == (push, crawled)

    async def test_collection_without_a_push_time_keeps_the_stored_observation(
        self,
        rollback_db_session: AsyncSession,
    ) -> None:
        """A missing ``pushedAt`` (never-pushed repo) never erases a stored observation."""

        session = rollback_db_session
        now = dt.datetime.now(dt.UTC)
        push = now - dt.timedelta(days=1)
        crawled = now - dt.timedelta(hours=2)
        resolved = await self._seed(session, "nopush", pushed_at=push, observed_at=crawled)

        await persist_collected(session, resolved, self._run(now), None)

        assert await self._stored(session, resolved.repo_id) == (push, crawled)
