"""
Maintenance profile materialization from stored signals.

Reads the eligible population (repos associated with a registered project),
computes release cadence from stored releases, commit activity from the
latest git-log artifact, and the collected issue/PR signals from
``repo_metadata["maintenance_signals"]``, percentile-ranks every scalar over
its non-NULL pool, and writes the inspectable profile to
``repo_metadata["maintenance_profile"]``. There is no combined score: the
profile is per-signal raw values, percentile ranks, and coverage states.

Percentile direction is normalized: lower-is-better scalars (gaps, ages,
counts, days-since) are inverted before ranking, so a higher percentile
always means better upkeep. Percentile pools deliberately cover only the
eligible population — funded public goods compare against each other — and
every ranked scalar carries its pool size; the profile carries its as-of.

Collected values and the stored push time (judged by when it was observed)
rank only while fresh (``MAINTENANCE_SIGNALS_MAX_AGE_DAYS``), and git-log
based counts only from a fresh, window-covering artifact
(``MAINTENANCE_GITLOG_MAX_AGE_DAYS``); anything staler is kept as context
with an ``incomplete`` state, never silently mixed into ranking pools.

Declared host repositories (``MAINTENANCE_HOST_REPOS``) render every signal
``not-applicable`` and join no percentile pool: their repo-level signals
describe the hosting organization, not the funded work delivered into them.

Delivery is this CLI plus its private JSON export. The ``--gated`` flag
makes a scheduled invocation re-check ``MAINTENANCE_METRIC_ENABLED`` at
execution time and no-op when disabled; explicit runs without it are
deliberate operator actions.

Usage::

    uv run python -m pg_atlas.metrics.materialize_maintenance
    uv run python -m pg_atlas.metrics.materialize_maintenance --export profiles.json --no-write
    uv run python -m pg_atlas.metrics.materialize_maintenance --gated --tee=maintenance.log

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import logging
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy import literal, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from pg_atlas.config import settings
from pg_atlas.db_models.base import SubmissionStatus
from pg_atlas.db_models.gitlog_artifact import GitLogArtifact
from pg_atlas.db_models.release import Release
from pg_atlas.db_models.repo_metadata import repo_metadata_merge_expression
from pg_atlas.db_models.repo_vertex import Repo
from pg_atlas.db_models.session import get_session_factory
from pg_atlas.gitlog.parser import ParsedGitLog, parse_log_bytes
from pg_atlas.instruments.tee import run_with_tee
from pg_atlas.metrics.maintenance import (
    CADENCE_SOURCE_GITHUB_RELEASES,
    MAINTENANCE_PROFILE_KEY,
    MAINTENANCE_PROFILE_SCHEMA_VERSION,
    REASON_ARTIFACT_MALFORMED,
    REASON_ARTIFACT_UNREADABLE,
    REASON_HOST_REPOSITORY,
    REASON_PARAMETER_MISMATCH,
    REASON_STALE_COLLECTION,
    SECONDS_PER_DAY,
    STATE_INCOMPLETE,
    STATE_NOT_APPLICABLE,
    STATE_OK,
    STATE_UNAVAILABLE,
    BacklogSignal,
    CommitActivityResult,
    MaintenanceSignals,
    ReleaseFallback,
    ResponsivenessSignal,
    compute_commit_activity,
    compute_release_cadence,
    distinct_release_dates,
    has_package_registry_release,
    parse_declared_maintainers,
    rank_scalar_values,
    signals_from_metadata,
)
from pg_atlas.repo_identity import parse_owner_repo_entries
from pg_atlas.storage.artifacts import read_artifact

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Scalar registry — one row per ranked value in the profile
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _ScalarSpec:
    """One rankable scalar: its profile location and ranking direction."""

    signal: str
    field: str
    higher_is_better: bool


_SCALAR_SPECS: tuple[_ScalarSpec, ...] = (
    _ScalarSpec("release_cadence", "median_gap_days", higher_is_better=False),
    _ScalarSpec("release_cadence", "days_since_last_release", higher_is_better=False),
    _ScalarSpec("issue_responsiveness", "response_fraction", higher_is_better=True),
    _ScalarSpec("issue_backlog", "open_count", higher_is_better=False),
    _ScalarSpec("issue_backlog", "median_open_age_days", higher_is_better=False),
    _ScalarSpec("pr_responsiveness", "response_fraction", higher_is_better=True),
    _ScalarSpec("pr_responsiveness", "open_count", higher_is_better=False),
    _ScalarSpec("pr_responsiveness", "median_open_age_days", higher_is_better=False),
    _ScalarSpec("activity_recency", "days_since_push", higher_is_better=False),
    _ScalarSpec("activity_recency", "commit_count_window", higher_is_better=True),
)


def _scalar_entry(
    state: str,
    *,
    value: float | int | None = None,
    reason: str | None = None,
    as_of: str | None = None,
) -> dict[str, Any]:
    """Build one profile scalar entry; percentile and pool size come later."""

    entry: dict[str, Any] = {"state": state}
    if value is not None:
        entry["value"] = value
    if reason is not None:
        entry["reason"] = reason
    if as_of:
        entry["as_of"] = as_of

    return entry


def _is_stale(as_of: str, now: dt.datetime, max_age_days: int) -> bool:
    """Return whether a per-signal as-of timestamp is too old to rank."""

    if not as_of:
        return True

    try:
        observed = dt.datetime.fromisoformat(as_of)
    except ValueError:
        return True

    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=dt.UTC)

    return (now - observed) > dt.timedelta(days=max_age_days)


def _bare_owner_repo(canonical_id: str) -> str:
    """
    Lowercased ``owner/repo`` from a canonical id.

    The canonical id carries the ``pkg:github/`` prefix; the per-repo
    declared settings are keyed by bare ``owner/repo``.
    """

    return canonical_id.lower().removeprefix("pkg:github/")


def _host_repo_signals() -> dict[str, dict[str, Any]]:
    """
    Profile signals for a declared host repository.

    Every ranked scalar is ``not-applicable`` with the host-repository
    reason — the repo-level signals describe the hosting organization, not
    the funded work — so a host repo contributes to no percentile pool.
    """

    signals: dict[str, dict[str, Any]] = {}
    for spec in _SCALAR_SPECS:
        signals.setdefault(spec.signal, {})[spec.field] = _scalar_entry(STATE_NOT_APPLICABLE, reason=REASON_HOST_REPOSITORY)

    return signals


# ---------------------------------------------------------------------------
# Per-repo signal extraction
# ---------------------------------------------------------------------------


@dataclass
class _RepoProfile:
    """One repo's profile under construction: signal dicts plus rankability."""

    repo_id: int
    canonical_id: str
    signals: dict[str, dict[str, Any]]
    #: Raw values feeding the percentile pools, keyed by ``signal.field``.
    rankable: dict[str, float]


def _responsiveness_signal_dict(
    signal: ResponsivenessSignal | None,
    *,
    now: dt.datetime,
    expected_include_drafts: bool | None,
    expected_declared_maintainers: list[str] | None,
) -> tuple[dict[str, Any], float | None]:
    """
    Build the responsiveness profile block; return the rankable fraction.

    A fraction measured under a different window, interval, draft rule, or
    declared-maintainer list is not comparable to the current pool, so a
    parameter mismatch keeps the value as context, labeled with the
    parameters it was measured under.
    """

    if signal is None:
        return {"response_fraction": _scalar_entry(STATE_UNAVAILABLE)}, None

    if signal.state == STATE_NOT_APPLICABLE:
        return {"response_fraction": _scalar_entry(STATE_NOT_APPLICABLE, as_of=signal.as_of)}, None

    stale = _is_stale(signal.as_of, now, settings.MAINTENANCE_SIGNALS_MAX_AGE_DAYS)
    parameters_match = (
        signal.window_days == settings.MAINTENANCE_WINDOW_DAYS
        and signal.interval_days == settings.MAINTENANCE_RESPONSE_INTERVAL_DAYS
        and signal.include_drafts == expected_include_drafts
        and signal.declared_maintainers == expected_declared_maintainers
    )

    block: dict[str, Any] = {
        "window_days": signal.window_days,
        "interval_days": signal.interval_days,
        "cohort_size": signal.cohort_size,
        "eligible_size": signal.eligible_size,
        "responded_within_interval": signal.responded_within_interval,
        "median_response_days_context": signal.median_response_days_context,
        "unknown_attribution_count": signal.unknown_attribution_count,
        "maintainer_authored_count": signal.maintainer_authored_count,
    }
    if signal.include_drafts is not None:
        block["include_drafts"] = signal.include_drafts
    if signal.declared_maintainers is not None:
        block["declared_maintainers"] = signal.declared_maintainers

    if signal.state == STATE_INCOMPLETE:
        block["response_fraction"] = _scalar_entry(STATE_INCOMPLETE, reason=signal.incomplete_reason, as_of=signal.as_of)
        return block, None

    if stale:
        block["response_fraction"] = _scalar_entry(
            STATE_INCOMPLETE, value=signal.response_fraction, reason=REASON_STALE_COLLECTION, as_of=signal.as_of
        )
        return block, None

    if not parameters_match:
        block["response_fraction"] = _scalar_entry(
            STATE_INCOMPLETE, value=signal.response_fraction, reason=REASON_PARAMETER_MISMATCH, as_of=signal.as_of
        )
        return block, None

    if signal.response_fraction is None:
        block["response_fraction"] = _scalar_entry(STATE_UNAVAILABLE, as_of=signal.as_of)
        return block, None

    block["response_fraction"] = _scalar_entry(STATE_OK, value=signal.response_fraction, as_of=signal.as_of)

    return block, signal.response_fraction


def _backlog_scalar_dicts(
    signal: BacklogSignal | None,
    *,
    now: dt.datetime,
) -> tuple[dict[str, Any], float | None, float | None]:
    """
    Build backlog scalar entries; return rankable (count, median age).

    The open count stays exact under a capped listing, so it can rank even
    when the median cannot; a zero backlog is a real zero with no ages to
    take a median of.
    """

    if signal is None:
        return (
            {
                "open_count": _scalar_entry(STATE_UNAVAILABLE),
                "median_open_age_days": _scalar_entry(STATE_UNAVAILABLE),
            },
            None,
            None,
        )

    if signal.state == STATE_NOT_APPLICABLE:
        return (
            {
                "open_count": _scalar_entry(STATE_NOT_APPLICABLE, as_of=signal.as_of),
                "median_open_age_days": _scalar_entry(STATE_NOT_APPLICABLE, as_of=signal.as_of),
            },
            None,
            None,
        )

    if signal.open_count is None:
        reason = signal.incomplete_reason
        return (
            {
                "open_count": _scalar_entry(STATE_INCOMPLETE, reason=reason, as_of=signal.as_of),
                "median_open_age_days": _scalar_entry(STATE_INCOMPLETE, reason=reason, as_of=signal.as_of),
            },
            None,
            None,
        )

    if _is_stale(signal.as_of, now, settings.MAINTENANCE_SIGNALS_MAX_AGE_DAYS):
        return (
            {
                "open_count": _scalar_entry(
                    STATE_INCOMPLETE, value=signal.open_count, reason=REASON_STALE_COLLECTION, as_of=signal.as_of
                ),
                "median_open_age_days": _scalar_entry(
                    STATE_INCOMPLETE, value=signal.median_open_age_days, reason=REASON_STALE_COLLECTION, as_of=signal.as_of
                ),
            },
            None,
            None,
        )

    count_entry = _scalar_entry(STATE_OK, value=signal.open_count, as_of=signal.as_of)

    if signal.open_count == 0:
        median_entry = _scalar_entry(STATE_NOT_APPLICABLE, as_of=signal.as_of)
        median_value: float | None = None
    elif signal.median_age_exact and signal.median_open_age_days is not None:
        median_entry = _scalar_entry(STATE_OK, value=signal.median_open_age_days, as_of=signal.as_of)
        median_value = signal.median_open_age_days
    else:
        median_entry = _scalar_entry(STATE_INCOMPLETE, reason=signal.incomplete_reason, as_of=signal.as_of)
        median_value = None

    return {"open_count": count_entry, "median_open_age_days": median_entry}, float(signal.open_count), median_value


def _release_ships_code(releases: list[Release] | None, *, deploy_on_push: bool) -> bool | None:
    """
    Whether this repo's releases are the event that ships its code.

    A declaration in ``MAINTENANCE_DEPLOY_ON_PUSH_REPOS`` wins (``False``):
    the repo deploys from its default branch and its releases are a
    changelog. Otherwise a package-registry release record means the
    published package is how the code is consumed (``True``). GitHub Releases
    or tags alone cannot tell the two cases apart (``None``).
    """

    if deploy_on_push:
        return False

    if has_package_registry_release(releases):
        return True

    return None


def _cadence_signal_dict(
    canonical_id: str,
    releases: list[Release] | None,
    fallback: ReleaseFallback | None,
    *,
    now: dt.datetime,
    deploy_on_push: bool,
) -> tuple[dict[str, Any], dict[str, float]]:
    """
    Build the release-cadence profile block; return its rankable values.

    The date source is selected inside ``compute_release_cadence`` (the
    source with more distinct dates wins, ties go to deps.dev). The cadence
    minimum controls the blocked-fallback policy here: when deps.dev alone
    falls short of the minimum, a blocked fallback (incomplete, stale, or
    collected under a different prerelease rule) may still feed the compute
    because its values end up as context anyway; against a usable deps.dev
    source it must not win the source-count selection with blocked data.

    The block's ``release_ships_code`` interprets the cadence: ``True`` when
    the repo has a package-registry release record (``cargo``, ``npm``,
    ``pypi``, ``maven``, ``composer``, ``pub``, ``gem``, ``nuget``; never
    ``golang``, whose proxy mirrors the tags of any repository), ``False``
    when the repo is declared in ``MAINTENANCE_DEPLOY_ON_PUSH_REPOS`` (the
    declaration wins over registry records), ``None`` otherwise. It is
    interpretation only: declared repos rank like any other.
    """

    depsdev_dates = distinct_release_dates(releases)
    fallback_dates: list[dt.date] = []
    fallback_block_reason: str | None = None
    fallback_as_of: str | None = None

    if fallback is not None:
        fallback_as_of = fallback.as_of or None
        for raw_date in fallback.publication_dates:
            try:
                fallback_dates.append(dt.date.fromisoformat(raw_date))
            except ValueError:
                logger.warning(f"Invalid release fallback date for {canonical_id}: {raw_date!r}")

        if fallback.state != STATE_OK or not fallback.complete:
            fallback_block_reason = fallback.incomplete_reason or REASON_STALE_COLLECTION
        elif _is_stale(fallback.as_of, now, settings.MAINTENANCE_SIGNALS_MAX_AGE_DAYS):
            fallback_block_reason = REASON_STALE_COLLECTION
        elif fallback.include_prereleases != settings.MAINTENANCE_INCLUDE_PRERELEASES:
            fallback_block_reason = REASON_PARAMETER_MISMATCH

    fallback_governs = len(depsdev_dates) < settings.MAINTENANCE_CADENCE_MIN_EVENTS
    ranked_fallback_dates = fallback_dates if (fallback_block_reason is None or fallback_governs) else []

    cadence = compute_release_cadence(
        depsdev_dates,
        ranked_fallback_dates,
        now=now,
        last_n_events=settings.MAINTENANCE_CADENCE_LAST_N_EVENTS,
        min_events=settings.MAINTENANCE_CADENCE_MIN_EVENTS,
    )

    block: dict[str, Any] = {
        "source": cadence.source,
        "shipping_events": cadence.shipping_events,
        "release_ships_code": _release_ships_code(releases, deploy_on_push=deploy_on_push),
    }
    rankable: dict[str, float] = {}
    scalar_as_of = fallback_as_of if cadence.source == CADENCE_SOURCE_GITHUB_RELEASES else None

    if fallback_block_reason is not None and fallback_governs:
        # The source that carries this repo's cadence is blocked: its values
        # are context, never ranked.
        block["days_since_last_release"] = _scalar_entry(
            STATE_INCOMPLETE, value=cadence.days_since_last_release, reason=fallback_block_reason, as_of=fallback_as_of
        )
        block["median_gap_days"] = _scalar_entry(
            STATE_INCOMPLETE, value=cadence.median_gap_days, reason=fallback_block_reason, as_of=fallback_as_of
        )
        return block, rankable

    if cadence.state == STATE_OK and cadence.days_since_last_release is not None:
        block["days_since_last_release"] = _scalar_entry(STATE_OK, value=cadence.days_since_last_release, as_of=scalar_as_of)
        rankable["release_cadence.days_since_last_release"] = cadence.days_since_last_release
    else:
        block["days_since_last_release"] = _scalar_entry(STATE_UNAVAILABLE)

    if cadence.state == STATE_OK and cadence.median_gap_days is not None:
        block["median_gap_days"] = _scalar_entry(STATE_OK, value=cadence.median_gap_days, as_of=scalar_as_of)
        rankable["release_cadence.median_gap_days"] = cadence.median_gap_days
    else:
        block["median_gap_days"] = _scalar_entry(STATE_UNAVAILABLE)

    return block, rankable


def _build_repo_profile(
    repo_id: int,
    canonical_id: str,
    releases: list[Release] | None,
    collected: MaintenanceSignals | None,
    pushed_at: dt.datetime | None,
    pushed_at_observed_at: dt.datetime | None,
    commit_activity: CommitActivityResult,
    *,
    now: dt.datetime,
    deploy_on_push: bool,
) -> _RepoProfile:
    """
    Assemble one repo's profile blocks and its rankable raw values.

    ``days_since_push`` is judged against the time the stored ``pushed_at``
    was observed (``pushed_at_observed_at``): that time is its as-of and
    gates its freshness. A row without an observation time falls back to the
    collection's ``collected_at``.
    """

    signals: dict[str, dict[str, Any]] = {}
    rankable: dict[str, float] = {}

    declared = parse_declared_maintainers(settings.MAINTENANCE_DECLARED_MAINTAINERS).get(
        _bare_owner_repo(canonical_id), frozenset()
    )
    expected_declared_maintainers = sorted(declared) if declared else None

    # --- release cadence (signal 1) ---
    cadence_block, cadence_rankable = _cadence_signal_dict(
        canonical_id,
        releases,
        collected.release_fallback if collected is not None else None,
        now=now,
        deploy_on_push=deploy_on_push,
    )
    signals["release_cadence"] = cadence_block
    rankable.update(cadence_rankable)

    # --- issue responsiveness (signal 2) ---
    issue_resp_block, issue_fraction = _responsiveness_signal_dict(
        collected.issue_responsiveness if collected is not None else None,
        now=now,
        expected_include_drafts=None,
        expected_declared_maintainers=expected_declared_maintainers,
    )
    signals["issue_responsiveness"] = issue_resp_block
    if issue_fraction is not None:
        rankable["issue_responsiveness.response_fraction"] = issue_fraction

    # --- issue backlog (signal 3) ---
    issue_backlog_block, issue_count, issue_median = _backlog_scalar_dicts(
        collected.issue_backlog if collected is not None else None, now=now
    )
    signals["issue_backlog"] = issue_backlog_block
    if issue_count is not None:
        rankable["issue_backlog.open_count"] = issue_count
    if issue_median is not None:
        rankable["issue_backlog.median_open_age_days"] = issue_median

    # --- PR responsiveness plus its backlog snapshot (signal 4) ---
    pr_resp_block, pr_fraction = _responsiveness_signal_dict(
        collected.pr_responsiveness if collected is not None else None,
        now=now,
        expected_include_drafts=settings.MAINTENANCE_INCLUDE_DRAFT_PRS,
        expected_declared_maintainers=expected_declared_maintainers,
    )
    pr_backlog_block, pr_count, pr_median = _backlog_scalar_dicts(
        collected.pr_backlog if collected is not None else None, now=now
    )
    pr_resp_block.update(pr_backlog_block)
    signals["pr_responsiveness"] = pr_resp_block
    if pr_fraction is not None:
        rankable["pr_responsiveness.response_fraction"] = pr_fraction
    if pr_count is not None:
        rankable["pr_responsiveness.open_count"] = pr_count
    if pr_median is not None:
        rankable["pr_responsiveness.median_open_age_days"] = pr_median

    # --- activity recency (signal 5) ---
    activity_block: dict[str, Any] = {}

    if pushed_at_observed_at is not None:
        push_as_of = pushed_at_observed_at.isoformat()
    else:
        push_as_of = collected.collected_at if collected is not None else ""

    if pushed_at is None:
        activity_block["days_since_push"] = _scalar_entry(STATE_UNAVAILABLE)
    else:
        days_since_push = round((now - pushed_at).total_seconds() / SECONDS_PER_DAY, 2)
        if _is_stale(push_as_of, now, settings.MAINTENANCE_SIGNALS_MAX_AGE_DAYS):
            activity_block["days_since_push"] = _scalar_entry(
                STATE_INCOMPLETE, value=days_since_push, reason=REASON_STALE_COLLECTION, as_of=push_as_of
            )
        else:
            activity_block["days_since_push"] = _scalar_entry(STATE_OK, value=days_since_push, as_of=push_as_of)
            rankable["activity_recency.days_since_push"] = days_since_push

    if commit_activity.state == STATE_OK and commit_activity.commit_count is not None:
        activity_block["commit_count_window"] = _scalar_entry(
            STATE_OK, value=commit_activity.commit_count, as_of=commit_activity.artifact_as_of
        )
        rankable["activity_recency.commit_count_window"] = float(commit_activity.commit_count)
    else:
        activity_block["commit_count_window"] = _scalar_entry(
            commit_activity.state,
            value=commit_activity.commit_count,
            reason=commit_activity.reason,
            as_of=commit_activity.artifact_as_of,
        )

    signals["activity_recency"] = activity_block

    return _RepoProfile(repo_id=repo_id, canonical_id=canonical_id, signals=signals, rankable=rankable)


# ---------------------------------------------------------------------------
# Artifact-backed commit activity
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _ArtifactRef:
    """Latest processed git-log artifact for one repo."""

    artifact_path: str
    since_months: int
    submitted_at: dt.datetime


async def _load_latest_artifacts(session: AsyncSession, repo_ids: list[int]) -> dict[int, _ArtifactRef]:
    """Load each repo's newest processed artifact reference."""

    if not repo_ids:
        return {}

    stmt = (
        select(
            GitLogArtifact.repo_id,
            GitLogArtifact.artifact_path,
            GitLogArtifact.since_months,
            GitLogArtifact.submitted_at,
        )
        .where(GitLogArtifact.repo_id.in_(repo_ids))
        .where(GitLogArtifact.status == SubmissionStatus.processed)
        .where(GitLogArtifact.artifact_path.is_not(None))
        .order_by(GitLogArtifact.repo_id, GitLogArtifact.submitted_at.desc(), GitLogArtifact.id.desc())
        .distinct(GitLogArtifact.repo_id)
    )
    rows = (await session.execute(stmt)).all()

    return {
        int(repo_id): _ArtifactRef(artifact_path=str(path), since_months=int(months), submitted_at=submitted)
        for repo_id, path, months, submitted in rows
    }


async def _commit_activity_for_repo(
    canonical_id: str,
    artifact: _ArtifactRef | None,
    *,
    now: dt.datetime,
) -> tuple[CommitActivityResult, bool]:
    """
    Read and parse one artifact; return the activity and whether it was read.

    A repo without any processed artifact is ``unavailable``; an artifact
    that exists but cannot be read or parsed is failed collection —
    ``incomplete`` with its own reason, keeping the artifact's observation
    time — the four states stay distinct.
    """

    if artifact is None:
        return (
            compute_commit_activity(
                None,
                artifact_submitted_at=None,
                artifact_since_months=None,
                now=now,
                window_days=settings.MAINTENANCE_WINDOW_DAYS,
                max_artifact_age_days=settings.MAINTENANCE_GITLOG_MAX_AGE_DAYS,
            ),
            False,
        )

    parsed: ParsedGitLog
    try:
        raw = await read_artifact(artifact.artifact_path)
        parsed = parse_log_bytes(raw)
    except (OSError, RuntimeError, ValueError, httpx.HTTPError) as exc:
        logger.warning(f"Git-log artifact unreadable for {canonical_id}: {type(exc).__name__}: {exc}")

        return (
            CommitActivityResult(
                state=STATE_INCOMPLETE,
                commit_count=None,
                artifact_as_of=artifact.submitted_at.isoformat(),
                reason=REASON_ARTIFACT_UNREADABLE,
            ),
            False,
        )

    activity = compute_commit_activity(
        parsed.commits,
        artifact_submitted_at=artifact.submitted_at,
        artifact_since_months=artifact.since_months,
        now=now,
        window_days=settings.MAINTENANCE_WINDOW_DAYS,
        max_artifact_age_days=settings.MAINTENANCE_GITLOG_MAX_AGE_DAYS,
    )

    if parsed.malformed_lines > 0:
        # Structural damage means an unknown share of commits is missing.
        # The context value is the ordinary filtered window count over the
        # valid subset — a floor for the signal — never the raw line count.
        logger.warning(
            f"Git-log artifact malformed for {canonical_id}: "
            f"malformed_lines={parsed.malformed_lines} parsed_commits={len(parsed.commits)}"
        )

        return (
            CommitActivityResult(
                state=STATE_INCOMPLETE,
                commit_count=activity.commit_count,
                artifact_as_of=activity.artifact_as_of,
                reason=REASON_ARTIFACT_MALFORMED,
            ),
            True,
        )

    return activity, True


# ---------------------------------------------------------------------------
# Materialization
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MaintenanceMaterializationStats:
    """Summarize one maintenance profile materialization pass."""

    gate_skipped: bool
    repos_eligible: int
    profiles_written: int
    stale_profiles_cleared: int
    artifacts_read: int
    duration_seconds: float
    pool_sizes: dict[str, int]


async def _write_profiles_and_clear_stale(
    session: AsyncSession,
    profile_payloads: Mapping[int, Mapping[str, Any]],
    stale_ids: Iterable[int],
) -> tuple[int, int]:
    """
    Write profiles and remove stale ones in one ascending-id pass.

    ``profile_payloads`` maps eligible repo ids to their profile documents;
    ``stale_ids`` are removal candidates selected before this pass. An id in
    both is written as a profile: the eligible-population read saw it linked
    to a project; if it stays unlinked, the next run removes the profile. A
    removal re-checks its predicates in the UPDATE itself
    (still unlinked, still carrying a profile), so a repo reassociated with
    a project after the candidate read keeps its profile, and only rows the
    UPDATE actually changed are counted. Returns
    ``(profiles_written, stale_profiles_cleared)``.
    """

    profiles_written = 0
    stale_profiles_cleared = 0
    # All Repo writes in this transaction follow ascending id order, including stale-profile removals.
    for repo_id in sorted(set(profile_payloads) | set(stale_ids)):
        profile_payload = profile_payloads.get(repo_id)
        if profile_payload is not None:
            await session.execute(
                update(Repo)
                .where(Repo.id == repo_id)
                .values(repo_metadata=repo_metadata_merge_expression({MAINTENANCE_PROFILE_KEY: profile_payload}))
                .execution_options(synchronize_session=False)
            )
            profiles_written += 1
            continue

        result = await session.execute(
            update(Repo)
            .where(Repo.id == repo_id)
            .where(Repo.project_id.is_(None))
            .where(Repo.repo_metadata.has_key(MAINTENANCE_PROFILE_KEY))
            .values(repo_metadata=Repo.repo_metadata.op("-")(literal(MAINTENANCE_PROFILE_KEY)))
            .execution_options(synchronize_session=False)
        )
        rowcount_obj = getattr(result, "rowcount", 0)
        if isinstance(rowcount_obj, int) and rowcount_obj > 0:
            stale_profiles_cleared += rowcount_obj

    return profiles_written, stale_profiles_cleared


async def materialize_maintenance_profiles(
    session: AsyncSession,
    *,
    enforce_gate: bool = False,
    write: bool = True,
    now: dt.datetime | None = None,
) -> tuple[MaintenanceMaterializationStats, dict[str, dict[str, Any]]]:
    """
    Recompute maintenance profiles over the eligible population.

    Returns the stats plus the assembled profiles keyed by repo canonical id
    (for the CLI export). With ``enforce_gate`` the pass no-ops while
    ``MAINTENANCE_METRIC_ENABLED`` is false — the execution-time half of the
    fail-closed rollout gate. All writes are bulk DML on ``repo_metadata``;
    profiles on repos that left the eligible population are removed.
    """

    started_at = time.perf_counter()
    observed_now = now if now is not None else dt.datetime.now(dt.UTC)

    if enforce_gate and not settings.MAINTENANCE_METRIC_ENABLED:
        logger.info("materialize_maintenance_profiles: MAINTENANCE_METRIC_ENABLED is false, skipping gated run")
        stats = MaintenanceMaterializationStats(
            gate_skipped=True,
            repos_eligible=0,
            profiles_written=0,
            stale_profiles_cleared=0,
            artifacts_read=0,
            duration_seconds=time.perf_counter() - started_at,
            pool_sizes={},
        )

        return stats, {}

    host_repos, rejected_hosts = parse_owner_repo_entries(settings.MAINTENANCE_HOST_REPOS)
    for item in rejected_hosts:
        logger.warning(f"maintenance-metric host-repository list entry rejected: {item!r}")

    deploy_on_push_repos, rejected_deploy_on_push = parse_owner_repo_entries(settings.MAINTENANCE_DEPLOY_ON_PUSH_REPOS)
    for item in rejected_deploy_on_push:
        logger.warning(f"maintenance-metric deploy-on-push list entry rejected: {item!r}")

    rows = (
        await session.execute(
            select(
                Repo.id,
                Repo.canonical_id,
                Repo.releases,
                Repo.repo_metadata,
                Repo.pushed_at,
                Repo.pushed_at_observed_at,
            )
            .where(Repo.project_id.is_not(None))
            .order_by(Repo.id)
        )
    ).all()

    repo_ids = [int(row[0]) for row in rows if _bare_owner_repo(str(row[1])) not in host_repos]
    artifacts = await _load_latest_artifacts(session, repo_ids)

    profiles: list[_RepoProfile] = []
    artifacts_read = 0
    for repo_id, canonical_id, releases, repo_metadata, pushed_at, pushed_at_observed_at in rows:
        owner_repo = _bare_owner_repo(str(canonical_id))
        if owner_repo in host_repos:
            profiles.append(
                _RepoProfile(
                    repo_id=int(repo_id),
                    canonical_id=str(canonical_id),
                    signals=_host_repo_signals(),
                    rankable={},
                )
            )
            continue

        collected = signals_from_metadata(repo_metadata, repo_canonical_id=canonical_id)
        commit_activity, artifact_was_read = await _commit_activity_for_repo(
            canonical_id, artifacts.get(int(repo_id)), now=observed_now
        )
        if artifact_was_read:
            artifacts_read += 1

        profiles.append(
            _build_repo_profile(
                int(repo_id),
                str(canonical_id),
                releases,
                collected,
                pushed_at,
                pushed_at_observed_at,
                commit_activity,
                now=observed_now,
                deploy_on_push=owner_repo in deploy_on_push_repos,
            )
        )

    # --- rank every scalar over its pool and stamp percentiles in place ---
    pool_sizes: dict[str, int] = {}
    for spec in _SCALAR_SPECS:
        key = f"{spec.signal}.{spec.field}"
        values = [profile.rankable.get(key) for profile in profiles]
        percentiles, pool_size = rank_scalar_values(values, higher_is_better=spec.higher_is_better)
        pool_sizes[key] = pool_size

        for profile, percentile in zip(profiles, percentiles, strict=True):
            entry = profile.signals[spec.signal][spec.field]
            if percentile is not None:
                entry["percentile"] = percentile
                entry["pool_size"] = pool_size

    # --- assemble full profile payloads ---
    payloads: dict[str, dict[str, Any]] = {}
    for profile in profiles:
        payloads[profile.canonical_id] = {
            "schema_version": MAINTENANCE_PROFILE_SCHEMA_VERSION,
            "as_of": observed_now.isoformat(),
            "window_days": settings.MAINTENANCE_WINDOW_DAYS,
            "response_interval_days": settings.MAINTENANCE_RESPONSE_INTERVAL_DAYS,
            "eligible_population": len(profiles),
            "signals": profile.signals,
        }

    profiles_written = 0
    stale_profiles_cleared = 0
    if write:
        # Candidates for profile removal (repos that left the eligible
        # population) are selected before any write, so the single write pass
        # below can order them together with the profile updates.
        stale_ids = list(
            (
                await session.execute(
                    select(Repo.id)
                    .where(Repo.project_id.is_(None))
                    .where(Repo.repo_metadata.has_key(MAINTENANCE_PROFILE_KEY))
                    .order_by(Repo.id)
                )
            ).scalars()
        )
        profile_payloads = {profile.repo_id: payloads[profile.canonical_id] for profile in profiles}
        profiles_written, stale_profiles_cleared = await _write_profiles_and_clear_stale(session, profile_payloads, stale_ids)

    duration_seconds = time.perf_counter() - started_at
    stats = MaintenanceMaterializationStats(
        gate_skipped=False,
        repos_eligible=len(profiles),
        profiles_written=profiles_written,
        stale_profiles_cleared=stale_profiles_cleared,
        artifacts_read=artifacts_read,
        duration_seconds=duration_seconds,
        pool_sizes=pool_sizes,
    )

    pools_text = " ".join(f"{key}={size}" for key, size in sorted(pool_sizes.items()))
    logger.info(
        "materialize_maintenance_profiles: "
        f"repos_eligible={stats.repos_eligible} "
        f"profiles_written={stats.profiles_written} "
        f"stale_profiles_cleared={stats.stale_profiles_cleared} "
        f"artifacts_read={stats.artifacts_read} "
        f"duration_seconds={stats.duration_seconds:.3f} "
        f"pools: {pools_text}"
    )

    return stats, payloads


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Materialize per-repo maintenance profiles.")
    parser.add_argument(
        "--gated",
        action="store_true",
        help="Re-check MAINTENANCE_METRIC_ENABLED at execution time and no-op when disabled (for scheduled runs).",
    )
    parser.add_argument(
        "--no-write",
        action="store_true",
        help="Compute profiles without writing them (use with --export for inspection).",
    )
    parser.add_argument(
        "--export",
        type=Path,
        default=None,
        help="Write the computed profiles as JSON to this path (private export).",
    )
    parser.add_argument(
        "--tee",
        type=Path,
        default=None,
        help="Optional path to mirror stdout/stderr logs while preserving console output.",
    )

    return parser


async def _async_main(args: argparse.Namespace) -> None:
    """Run one materialization pass, commit, and export when requested."""

    factory = get_session_factory()
    async with factory() as session:
        stats, payloads = await materialize_maintenance_profiles(
            session,
            enforce_gate=args.gated,
            write=not args.no_write,
        )
        await session.commit()

    if args.export is not None and not stats.gate_skipped:
        export_payload = {
            "generated_at": dt.datetime.now(dt.UTC).isoformat(),
            "eligible_population": stats.repos_eligible,
            "pool_sizes": stats.pool_sizes,
            "profiles": payloads,
        }
        args.export.write_text(json.dumps(export_payload, indent=2, sort_keys=True) + "\n")
        logger.info(f"maintenance profiles exported: path={args.export} repos={len(payloads)}")

    logger.info(
        "maintenance materialization finished: "
        f"gate_skipped={stats.gate_skipped} "
        f"repos_eligible={stats.repos_eligible} "
        f"profiles_written={stats.profiles_written} "
        f"stale_profiles_cleared={stats.stale_profiles_cleared} "
        f"duration_seconds={stats.duration_seconds:.3f}"
    )


def main() -> None:
    args = _build_parser().parse_args()

    def _run() -> None:
        asyncio.run(_async_main(args))

    run_with_tee(args.tee, _run)


if __name__ == "__main__":
    main()
