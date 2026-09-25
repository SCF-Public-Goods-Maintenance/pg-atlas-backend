"""
Persistence for collected maintenance signals.

Resolves a tracked repository, merges a run's collected signals with the
stored payload per signal, and writes the merged payload. The run's
``pushed_at`` observation goes through the shared monotonic writer
``pg_atlas.procrastinate.upserts.record_pushed_at`` in the same transaction.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from pg_atlas.db_models.release import Release
from pg_atlas.db_models.repo_metadata import repo_metadata_merge_expression
from pg_atlas.db_models.repo_vertex import Repo
from pg_atlas.metrics.maintenance import (
    MAINTENANCE_SIGNALS_KEY,
    STATE_NOT_APPLICABLE,
    STATE_OK,
    BacklogSignal,
    MaintenanceSignals,
    ReleaseFallback,
    ResponsivenessSignal,
    signals_from_metadata,
    signals_to_payload,
)
from pg_atlas.procrastinate.upserts import record_pushed_at

logger = logging.getLogger(__name__)


class MaintenanceSourceRepoNotFound(Exception):
    """Raised when no tracked repo matches the requested source identity."""


class AmbiguousMaintenanceSourceRepo(Exception):
    """Raised when more than one tracked repo matches the source identity."""


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


#: Signal states that represent a completed observation.
_AUTHORITATIVE_STATES = frozenset({STATE_OK, STATE_NOT_APPLICABLE})


def _keep_better_signal[SignalT: (ResponsivenessSignal, BacklogSignal, ReleaseFallback)](
    old: SignalT | None,
    new: SignalT | None,
) -> SignalT | None:
    """
    Per-signal merge: incomplete results never overwrite authoritative ones,
    and between two authoritative observations the newer ``as_of`` wins.

    ``ok`` and ``not-applicable`` are both authoritative — a tracker being
    switched off is as much an observation as a measured value — so the
    newest authoritative observation always stands. The kept signal retains
    its own ``as_of`` and parameters; the materializer's freshness policy
    decides how long it stays ranked. The ISO-8601 ``as_of`` strings share
    one format and compare lexically.
    """

    if new is None:
        return old
    if old is None:
        return new

    old_authoritative = old.state in _AUTHORITATIVE_STATES
    new_authoritative = new.state in _AUTHORITATIVE_STATES

    if old_authoritative and not new_authoritative:
        return old

    if old_authoritative and new_authoritative and old.as_of and new.as_of and old.as_of > new.as_of:
        return old

    return new


def merge_collected_signals(old: MaintenanceSignals | None, new: MaintenanceSignals) -> MaintenanceSignals:
    """
    Merge one collection into the previously stored payload.

    Run-level fields (``collected_at``, tracker applicability, archive flag,
    request count) come from the chronologically newer run; signals merge
    individually under ``_keep_better_signal``.
    """

    if old is None:
        return new

    newer = new if (new.collected_at or "") >= (old.collected_at or "") else old

    return MaintenanceSignals(
        schema_version=newer.schema_version,
        collected_at=newer.collected_at,
        window_days=newer.window_days,
        response_interval_days=newer.response_interval_days,
        requests_used=newer.requests_used,
        issues_enabled=newer.issues_enabled,
        external_tracker_declared=newer.external_tracker_declared,
        archived=newer.archived,
        issue_responsiveness=_keep_better_signal(old.issue_responsiveness, new.issue_responsiveness),
        issue_backlog=_keep_better_signal(old.issue_backlog, new.issue_backlog),
        pr_responsiveness=_keep_better_signal(old.pr_responsiveness, new.pr_responsiveness),
        pr_backlog=_keep_better_signal(old.pr_backlog, new.pr_backlog),
        release_fallback=_keep_better_signal(old.release_fallback, new.release_fallback),
    )


def _parse_collected_at(collected_at: str) -> dt.datetime | None:
    """
    Parse a run's ``collected_at`` stamp; ``None`` when absent or unparseable.

    ``record_pushed_at`` rejects ``None`` (and naive values) with a warning,
    so a malformed stamp never becomes an observation time.
    """

    if not collected_at:
        return None

    try:
        return dt.datetime.fromisoformat(collected_at)
    except ValueError:
        logger.warning(f"maintenance-collect: unparseable collected_at={collected_at!r}")

        return None


@dataclass(frozen=True)
class ResolvedRepo:
    """Identity and stored inputs for one tracked repository."""

    repo_id: int
    canonical_id: str
    releases: list[Release] | None


async def resolve_repo(session: AsyncSession, owner: str, repo: str) -> ResolvedRepo:
    """
    Resolve the tracked ``Repo`` by case-insensitive canonical identity.

    Zero matches raise ``MaintenanceSourceRepoNotFound``; more than one raise
    ``AmbiguousMaintenanceSourceRepo`` — the collector never silently picks
    one.
    """

    wanted = f"pkg:github/{owner}/{repo}".lower()
    rows = (
        await session.execute(select(Repo.id, Repo.canonical_id, Repo.releases).where(func.lower(Repo.canonical_id) == wanted))
    ).all()

    if not rows:
        raise MaintenanceSourceRepoNotFound(f"No tracked repo for {owner}/{repo}")
    if len(rows) > 1:
        logger.warning(f"maintenance-collect ambiguous source identity: repo={owner}/{repo} matches={len(rows)}")
        raise AmbiguousMaintenanceSourceRepo(f"{len(rows)} tracked repos match {owner}/{repo}")

    repo_id, canonical_id, releases = rows[0]

    return ResolvedRepo(repo_id=repo_id, canonical_id=canonical_id, releases=releases)


async def persist_collected(
    session: AsyncSession,
    resolved: ResolvedRepo,
    signals: MaintenanceSignals,
    pushed_at: dt.datetime | None,
) -> MaintenanceSignals:
    """
    Merge with the stored payload and write signals plus ``pushed_at``.

    The stored payload is read under a row lock inside this transaction, so
    concurrent persists (worker vs explicit CLI run) serialize and each merge
    sees the previous run's committed result; the merge keeps the better
    observation per signal slot. ``pushed_at`` is recorded with the run's
    ``collected_at`` as its observation time through ``record_pushed_at``,
    which only moves the column forward, independently of the signals merge.
    """

    current_metadata = (
        await session.execute(select(Repo.repo_metadata).where(Repo.id == resolved.repo_id).with_for_update())
    ).scalar_one()
    previous = signals_from_metadata(current_metadata, repo_canonical_id=resolved.canonical_id)
    merged = merge_collected_signals(previous, signals)
    payload = {MAINTENANCE_SIGNALS_KEY: signals_to_payload(merged)}

    await session.execute(
        update(Repo)
        .where(Repo.id == resolved.repo_id)
        .values(repo_metadata=repo_metadata_merge_expression(payload))
        .execution_options(synchronize_session=False)
    )
    await record_pushed_at(session, resolved.repo_id, pushed_at, _parse_collected_at(signals.collected_at))

    return merged
