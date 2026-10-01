"""
Maintenance metric: signal payloads and pure per-repo signal computation.

The maintenance metric describes how a repository is kept up: release cadence,
issue and PR responsiveness, backlog health, and activity recency. It is a
profile dimension — per-signal raw values plus percentile ranks over the
eligible population — never a single combined score.

Division of labor:
    - ``pg_atlas.procrastinate.github_maintenance`` collects issue/PR signals
      from the GitHub API into ``Repo.repo_metadata["maintenance_signals"]``
      and records ``Repo.pushed_at`` with its observation time (the bootstrap
      crawl records it too, through the same monotonic writer).
    - This module holds the payload types and the pure signal math, shared by
      the collector, the materializer, and fixture tests.
    - ``pg_atlas.metrics.materialize_maintenance`` computes release cadence
      from stored releases, commit activity from git-log artifacts, ranks all
      scalars, and writes ``Repo.repo_metadata["maintenance_profile"]``.

Four coverage states are kept distinct throughout: a real observed zero is
``ok`` with value ``0``; ``not-applicable`` marks signals that do not apply
(e.g. a declared external issue tracker); ``unavailable`` marks absent data;
``incomplete`` marks capped, stale, or failed collection. Coverage is
per scalar: an ``incomplete`` value is context only and never
percentile-ranked, while an independently exact scalar of the same signal
still ranks (a backlog's server-side open count under a capped listing).

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import datetime as dt
import functools
import logging
import re
import statistics
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass

import msgspec
import numpy as np

from pg_atlas.db_models.release import Release
from pg_atlas.gitlog.filters import is_bot
from pg_atlas.gitlog.parser import CommitRecord
from pg_atlas.metrics.adoption import compute_percentile_ranks

logger = logging.getLogger(__name__)

#: ``Repo.repo_metadata`` key written by the collector (raw collected signals).
MAINTENANCE_SIGNALS_KEY = "maintenance_signals"
#: ``Repo.repo_metadata`` key written by the materializer (ranked profile).
MAINTENANCE_PROFILE_KEY = "maintenance_profile"

MAINTENANCE_SIGNALS_SCHEMA_VERSION = 1
MAINTENANCE_PROFILE_SCHEMA_VERSION = 1

# --- coverage states ---
STATE_OK = "ok"
STATE_NOT_APPLICABLE = "not-applicable"
STATE_UNAVAILABLE = "unavailable"
STATE_INCOMPLETE = "incomplete"

# --- typed incomplete-reason codes (also the grep-able log markers) ---
REASON_RATE_LIMITED = "rate-limited"
REASON_REQUEST_CAP = "request-cap"
REASON_PAGE_CAP = "page-cap"
REASON_TIME_CAP = "time-cap"
REASON_QUERY_ERROR = "query-error"
REASON_STALE_COLLECTION = "stale-collection"
REASON_PARAMETER_MISMATCH = "parameter-mismatch"
REASON_STALE_ARTIFACT = "stale-artifact"
REASON_ARTIFACT_UNREADABLE = "artifact-unreadable"
REASON_ARTIFACT_MALFORMED = "artifact-malformed"
REASON_WINDOW_NOT_COVERED = "window-not-covered"

# --- typed not-applicable reason codes ---
#: Declared host repository: its repo-level signals describe the hosting
#: organization, not the funded work delivered into it.
REASON_HOST_REPOSITORY = "host-repository"

SECONDS_PER_DAY = 86400.0
#: Conservative days-per-month bound used to decide whether a git-log artifact
#: window covers the maintenance window.
DAYS_PER_MONTH_LOWER_BOUND = 30

# --- release-date source identifiers ---
CADENCE_SOURCE_DEPSDEV = "depsdev"
CADENCE_SOURCE_GITHUB_RELEASES = "github-releases"

#: purl type of Go module release records.
PURL_TYPE_GOLANG = "golang"

#: Go's own pseudo-version grammar (``golang.org/x/mod/module``). A
#: pseudo-version names a commit (``vX.0.0-<timestamp>-<revision>``,
#: ``vX.Y.Z-pre.0.<timestamp>-<revision>``, ``vX.Y.(Z+1)-0.<timestamp>-<revision>``,
#: each optionally ``+incompatible``); the Go module proxy derives one for any
#: commit of any repository. ASCII digits only, as in Go's ``\d``.
_GO_PSEUDO_VERSION_PATTERN = re.compile(
    r"^v[0-9]+\.(0\.0-|\d+\.\d+-([^+]*\.)?0\.)\d{14}-[A-Za-z0-9]+(\+[0-9A-Za-z-]+)?$",
    re.ASCII,
)

#: purl types of package registries whose published package is how the code
#: is consumed, so a release record there is the event that ships code.
#: ``golang`` is absent: the Go module proxy mirrors the semver tags of any
#: repository, so a Go record does not show that a release ships code.
PACKAGE_REGISTRY_PURL_TYPES = frozenset({"cargo", "npm", "pypi", "maven", "composer", "pub", "gem", "nuget"})


#: GitHub login charset; a declared login outside it can never match an actor
#: and marks a malformed entry (typically a stray ``=`` or space typo).
_DECLARED_LOGIN_PATTERN = re.compile(r"[a-z0-9-]+")


@functools.lru_cache(maxsize=8)
def parse_declared_maintainers(raw: str) -> Mapping[str, frozenset[str]]:
    """
    Parse the declared-maintainers setting into a repo-to-logins mapping.

    Format: comma-separated ``owner/repo=login1|login2`` entries. Repo keys
    and logins are lowercased (GitHub treats both case-insensitively), and
    repeated repo keys merge their logins. Malformed entries — including any
    login outside GitHub's charset — are rejected with a warning so one bad
    entry cannot disable the others.
    """

    declared: dict[str, frozenset[str]] = {}
    for item in raw.split(","):
        entry = item.strip()
        if not entry:
            continue

        key, sep, logins_raw = entry.partition("=")
        repo_key = key.strip().lower()
        repo_parts = repo_key.split("/")
        logins = frozenset(login.strip().lower() for login in logins_raw.split("|") if login.strip())
        if (
            not sep
            or len(repo_parts) != 2
            or not repo_parts[0]
            or not repo_parts[1]
            or not logins
            or not all(_DECLARED_LOGIN_PATTERN.fullmatch(login) for login in logins)
        ):
            logger.warning(f"declared-maintainers entry rejected: {entry!r}")
            continue

        declared[repo_key] = declared.get(repo_key, frozenset()) | logins

    return declared


# ---------------------------------------------------------------------------
# Collected signal payload (JSONB under MAINTENANCE_SIGNALS_KEY)
# ---------------------------------------------------------------------------


class ResponsivenessSignal(msgspec.Struct, omit_defaults=True):
    """
    Aggregated first-maintainer-response measurement over one opened-in-window
    cohort (issues or pull requests).

    ``response_fraction`` is the ranked value: the share of eligible items
    (old enough to have had the full response interval) that received a
    qualifying response within the interval. ``median_response_days_context``
    is context only — computed over answered items regardless of interval.
    Items whose attribution could not be determined are excluded from both
    numerator and denominator and counted in ``unknown_attribution_count``.
    Items authored by maintainers themselves (author association
    OWNER/MEMBER/COLLABORATOR, or a login on the repo's declared list) are
    excluded before aggregation and counted in ``maintainer_authored_count``
    — a maintainer's own work needs no response.
    """

    state: str
    as_of: str = ""
    #: The parameters this measurement was taken under. A fraction is only
    #: comparable to fractions measured under the same window, interval,
    #: draft rule, and declared-maintainer list, so the materializer ranks a
    #: signal only when these match the current settings.
    window_days: int | None = None
    interval_days: int | None = None
    include_drafts: bool | None = None
    declared_maintainers: list[str] | None = None
    cohort_size: int | None = None
    eligible_size: int | None = None
    responded_within_interval: int | None = None
    response_fraction: float | None = None
    median_response_days_context: float | None = None
    unknown_attribution_count: int = 0
    maintainer_authored_count: int = 0
    incomplete_reason: str | None = None


class BacklogSignal(msgspec.Struct, omit_defaults=True):
    """
    Snapshot of currently open items regardless of creation date.

    ``open_count`` comes from a server-side total and stays exact even when
    the listing is capped; ``median_open_age_days`` is exact when the fetched
    oldest-first prefix reaches the middle order statistic, else absent.
    """

    state: str
    as_of: str = ""
    open_count: int | None = None
    median_open_age_days: float | None = None
    median_age_exact: bool = True
    incomplete_reason: str | None = None


class ReleaseFallback(msgspec.Struct, omit_defaults=True):
    """
    Dated GitHub Releases publication dates fetched when the stored deps.dev
    release records lack usable dated coverage. ``publication_dates`` are
    ISO calendar dates; drafts are always excluded.
    """

    state: str
    as_of: str = ""
    publication_dates: list[str] = msgspec.field(default_factory=list[str])
    include_prereleases: bool = True
    complete: bool = True
    incomplete_reason: str | None = None


class MaintenanceSignals(msgspec.Struct, omit_defaults=True):
    """
    Everything one collection run stores for one repository.

    ``collected_at`` stamps the run and is the observation time the run
    records for ``Repo.pushed_at``; each signal carries its own ``as_of`` and
    measurement parameters, which govern per-signal freshness and
    comparability at materialization time.
    """

    schema_version: int
    collected_at: str
    window_days: int
    response_interval_days: int
    requests_used: int = 0
    issues_enabled: bool | None = None
    external_tracker_declared: bool = False
    archived: bool | None = None
    issue_responsiveness: ResponsivenessSignal | None = None
    issue_backlog: BacklogSignal | None = None
    pr_responsiveness: ResponsivenessSignal | None = None
    pr_backlog: BacklogSignal | None = None
    release_fallback: ReleaseFallback | None = None


def signals_to_payload(signals: MaintenanceSignals) -> dict[str, object]:
    """Serialize one collected-signals struct into a JSONB-compatible dict."""

    return msgspec.convert(msgspec.to_builtins(signals), type=dict[str, object])


def signals_from_metadata(
    repo_metadata: Mapping[str, object] | None,
    *,
    repo_canonical_id: str | None = None,
) -> MaintenanceSignals | None:
    """
    Extract and validate the collected maintenance signals from repo metadata.

    Invalid shapes are skipped and logged, so schema drift shows up in
    materialization logs.
    """

    if repo_metadata is None:
        return None

    raw_signals = repo_metadata.get(MAINTENANCE_SIGNALS_KEY)
    if raw_signals is None:
        return None

    try:
        return msgspec.convert(raw_signals, type=MaintenanceSignals)
    except msgspec.ValidationError as exc:
        repo_id = repo_canonical_id or "<unknown>"
        logger.error(f"Invalid {MAINTENANCE_SIGNALS_KEY} for repo {repo_id}: {exc}")
        return None


# ---------------------------------------------------------------------------
# Neutral per-item classification input
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ResponseItem:
    """
    One cohort item (issue or PR) reduced to response semantics.

    ``first_response_at`` is the earliest qualifying maintainer response, or
    ``None`` when the item is unanswered. ``indeterminate`` marks items whose
    response state could not be established (unknown event attribution, or
    truncated detection) — they are excluded from the fraction entirely.
    """

    created_at: dt.datetime
    first_response_at: dt.datetime | None = None
    indeterminate: bool = False


# ---------------------------------------------------------------------------
# Signal computation: responsiveness (signals 2 and 4)
# ---------------------------------------------------------------------------


def compute_responsiveness(
    items: Sequence[ResponseItem],
    *,
    now: dt.datetime,
    window_days: int,
    interval_days: int,
    as_of: str = "",
    include_drafts: bool | None = None,
    declared_maintainers: Collection[str] | None = None,
    maintainer_authored_count: int = 0,
) -> ResponsivenessSignal:
    """
    Aggregate one cohort of response items into a responsiveness signal.

    The ranked value is the fraction of eligible items answered within the
    interval, over items opened in the trailing window that are at least the
    interval old at observation time; unanswered work stays in the
    denominator and lowers the fraction. An empty eligible cohort
    yields no fraction (there is nothing to rank), which is distinct from a
    real fraction of zero.
    """

    window_start = now - dt.timedelta(days=window_days)
    interval = dt.timedelta(days=interval_days)
    eligibility_cutoff = now - interval

    unknown_attribution_count = 0
    cohort: list[ResponseItem] = []
    for item in items:
        if item.created_at < window_start or item.created_at > now:
            continue

        if item.indeterminate:
            unknown_attribution_count += 1
            continue

        cohort.append(item)

    eligible = [item for item in cohort if item.created_at <= eligibility_cutoff]
    responded_within = sum(
        1 for item in eligible if item.first_response_at is not None and item.first_response_at - item.created_at <= interval
    )

    response_fraction: float | None = None
    if eligible:
        response_fraction = round(responded_within / len(eligible), 4)

    answered_days = [
        (item.first_response_at - item.created_at).total_seconds() / SECONDS_PER_DAY
        for item in cohort
        if item.first_response_at is not None
    ]
    median_context = round(statistics.median(answered_days), 2) if answered_days else None

    return ResponsivenessSignal(
        state=STATE_OK,
        as_of=as_of,
        window_days=window_days,
        interval_days=interval_days,
        include_drafts=include_drafts,
        declared_maintainers=sorted(declared_maintainers) if declared_maintainers else None,
        cohort_size=len(cohort),
        eligible_size=len(eligible),
        responded_within_interval=responded_within,
        response_fraction=response_fraction,
        median_response_days_context=median_context,
        unknown_attribution_count=unknown_attribution_count,
        maintainer_authored_count=maintainer_authored_count,
    )


# ---------------------------------------------------------------------------
# Signal computation: backlog snapshot (signal 3, and signal 4's snapshot)
# ---------------------------------------------------------------------------


def compute_backlog(
    open_created_ats: Sequence[dt.datetime],
    total_open_count: int,
    *,
    now: dt.datetime,
    as_of: str = "",
    incomplete_reason: str | None = None,
) -> BacklogSignal:
    """
    Build one backlog snapshot from open-item creation dates.

    ``open_created_ats`` must be the oldest-first prefix of the open listing.
    The count comes from the server-side total and stays exact under a capped
    listing. The median age is the middle order statistic of item ages;
    because ages sorted descending correspond to creation dates sorted
    ascending, an oldest-first prefix that reaches past the middle yields the
    exact median. The signal is ``incomplete`` (with ``incomplete_reason``)
    only when the prefix falls short of the middle, so the median cannot be
    computed exactly.
    """

    ages_days = sorted(((now - created_at).total_seconds() / SECONDS_PER_DAY for created_at in open_created_ats), reverse=True)

    median_age: float | None = None
    median_age_exact = False
    if total_open_count == 0:
        median_age_exact = True
    elif len(ages_days) > total_open_count // 2:
        if total_open_count % 2 == 1:
            median_age = round(ages_days[total_open_count // 2], 2)
        else:
            median_age = round((ages_days[total_open_count // 2 - 1] + ages_days[total_open_count // 2]) / 2, 2)

        median_age_exact = True

    state = STATE_OK if median_age_exact else STATE_INCOMPLETE
    reason = None if median_age_exact else (incomplete_reason or REASON_PAGE_CAP)

    return BacklogSignal(
        state=state,
        as_of=as_of,
        open_count=total_open_count,
        median_open_age_days=median_age,
        median_age_exact=median_age_exact,
        incomplete_reason=reason,
    )


# ---------------------------------------------------------------------------
# Signal computation: release cadence (signal 1)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CadenceResult:
    """
    Release cadence for one repo, computed over distinct publication dates.

    ``source`` names the date source that was used; ``None`` means no dated
    shipping events exist anywhere (tag-only or release-free repos), which is
    ``unavailable`` — never a zero.
    """

    state: str
    source: str | None
    shipping_events: int
    median_gap_days: float | None
    days_since_last_release: float | None


def purl_type(purl: str) -> str | None:
    """
    Return the lowercased type component of a purl, or ``None`` if it has none.

    ``pkg:golang/github.com/owner/repo`` yields ``golang``.
    """

    if not purl.startswith("pkg:"):
        return None

    type_component = purl.removeprefix("pkg:").split("/", 1)[0].lower()

    return type_component or None


def is_go_pseudo_version(release: Release) -> bool:
    """
    Return whether a release record is a Go module pseudo-version.

    Only ``golang`` records qualify; the same string under another purl type
    is that registry's own version and is left alone.
    """

    return purl_type(release.purl) == PURL_TYPE_GOLANG and _GO_PSEUDO_VERSION_PATTERN.fullmatch(release.version) is not None


def distinct_release_dates(releases: Sequence[Release] | None) -> list[dt.date]:
    """
    Reduce stored release records to sorted distinct publication dates.

    A repo publishing several packages together must count one shipping event
    per date, not gap medians over interleaved per-package records. Records
    without a parseable date prefix are skipped: bare versions carry no
    cadence information.

    Invariant: Go module pseudo-versions never count as publications. A
    pseudo-version is a commit reference that the Go module proxy derives for
    any commit of any repository, so its date is a commit date, not a
    shipping event. Tagged Go versions (including ``vX.Y.Z+incompatible``)
    count like any other release.
    """

    dates: set[dt.date] = set()
    for release in releases or []:
        if is_go_pseudo_version(release):
            continue

        prefix = release.release_date[:10]
        try:
            dates.add(dt.date.fromisoformat(prefix))
        except ValueError:
            continue

    return sorted(dates)


def has_package_registry_release(releases: Sequence[Release] | None) -> bool:
    """
    Return whether any stored release record comes from a package registry.

    A registry publication is how the code is consumed, so its presence shows
    that the repo's releases ship code (``PACKAGE_REGISTRY_PURL_TYPES``).
    """

    return any(purl_type(release.purl) in PACKAGE_REGISTRY_PURL_TYPES for release in releases or [])


def compute_release_cadence(
    depsdev_dates: Sequence[dt.date],
    fallback_dates: Sequence[dt.date],
    *,
    now: dt.datetime,
    last_n_events: int,
    min_events: int,
) -> CadenceResult:
    """
    Compute cadence from the richer of the two date sources.

    Sources are never mixed — deps.dev package publications and GitHub
    Releases describe the same shipping events, so combining them would double
    count. The source with more distinct dates wins; a tie keeps deps.dev
    (zero-cost, package-registry truth). ``median_gap_days`` needs at least
    ``min_events`` shipping events; ``days_since_last_release`` needs one.
    """

    depsdev_sorted = sorted(set(depsdev_dates))
    fallback_sorted = sorted(set(fallback_dates))

    if len(fallback_sorted) > len(depsdev_sorted):
        events, source = fallback_sorted, CADENCE_SOURCE_GITHUB_RELEASES
    else:
        events, source = depsdev_sorted, CADENCE_SOURCE_DEPSDEV

    if not events:
        return CadenceResult(
            state=STATE_UNAVAILABLE,
            source=None,
            shipping_events=0,
            median_gap_days=None,
            days_since_last_release=None,
        )

    days_since_last = float((now.date() - events[-1]).days)

    median_gap: float | None = None
    recent_events = events[-last_n_events:]
    if len(recent_events) >= min_events:
        gaps = [float((later - earlier).days) for earlier, later in zip(recent_events, recent_events[1:], strict=False)]
        median_gap = round(statistics.median(gaps), 2)

    return CadenceResult(
        state=STATE_OK,
        source=source,
        shipping_events=len(events),
        median_gap_days=median_gap,
        days_since_last_release=days_since_last,
    )


# ---------------------------------------------------------------------------
# Signal computation: commit activity (signal 5)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CommitActivityResult:
    """
    Windowed non-merge default-branch commit count from a git-log artifact.

    ``artifact_as_of`` preserves the artifact's own observation time,
    separate from the metric run time. A stale or window-short artifact
    yields a context value (``incomplete``), never a ranked one; a fresh,
    covering artifact with zero commits is a real zero.
    """

    state: str
    commit_count: int | None
    artifact_as_of: str | None
    reason: str | None


def compute_commit_activity(
    commits: Sequence[CommitRecord] | None,
    *,
    artifact_submitted_at: dt.datetime | None,
    artifact_since_months: int | None,
    now: dt.datetime,
    window_days: int,
    max_artifact_age_days: int,
) -> CommitActivityResult:
    """
    Count window commits by author date with bot authors filtered.

    The stored logs are already default-branch and ``--no-merges``; this
    applies the pony metric's bot patterns and the maintenance window. The
    count ranks only when the latest artifact parsed successfully, covers the
    window, and is acceptably fresh — dormant repos are re-logged on a slower
    cadence, so their stale artifacts intentionally fall back to context.
    """

    if commits is None or artifact_submitted_at is None or artifact_since_months is None:
        return CommitActivityResult(state=STATE_UNAVAILABLE, commit_count=None, artifact_as_of=None, reason=None)

    artifact_as_of = artifact_submitted_at.isoformat()

    reason: str | None = None
    artifact_age_days = (now - artifact_submitted_at).total_seconds() / SECONDS_PER_DAY
    coverage_start = artifact_submitted_at - dt.timedelta(days=artifact_since_months * DAYS_PER_MONTH_LOWER_BOUND)
    window_start = now - dt.timedelta(days=window_days)

    if artifact_age_days > max_artifact_age_days:
        reason = REASON_STALE_ARTIFACT
    elif coverage_start > window_start:
        reason = REASON_WINDOW_NOT_COVERED

    commit_count = sum(
        1
        for commit in commits
        if window_start <= commit.timestamp <= now and not is_bot(commit.author_name, commit.author_email)
    )

    state = STATE_INCOMPLETE if reason is not None else STATE_OK

    return CommitActivityResult(state=state, commit_count=commit_count, artifact_as_of=artifact_as_of, reason=reason)


# ---------------------------------------------------------------------------
# Percentile ranking over the eligible population
# ---------------------------------------------------------------------------


def rank_scalar_values(
    values: Sequence[float | None],
    *,
    higher_is_better: bool,
) -> tuple[list[float | None], int]:
    """
    Percentile-rank one scalar column over its non-NULL pool.

    Returns percentiles aligned to the input (``None`` outside the pool) and
    the pool size. Lower-is-better scalars are inverted by ranking negated
    values, so a higher percentile always means better upkeep; the tie and
    top-rank properties of ``compute_percentile_ranks`` carry over.
    """

    mask = [value is not None for value in values]
    pool = [value for value in values if value is not None]
    pool_size = len(pool)

    percentiles: list[float | None] = [None] * len(values)
    if not pool:
        return percentiles, 0

    pool_array = np.array(pool, dtype=np.float64)
    if not higher_is_better:
        pool_array = -pool_array

    ranked = compute_percentile_ranks(pool_array)

    rank_iter = iter(ranked)
    for index, in_pool in enumerate(mask):
        if in_pool:
            percentiles[index] = round(float(next(rank_iter)), 2)

    return percentiles, pool_size
