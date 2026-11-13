"""
GitHub maintenance-signal collector.

Collects the API-backed maintenance signals for one repository — issue and PR
responsiveness cohorts, open-item backlog snapshots, ``pushed_at``, issue
tracker applicability, and (when the stored deps.dev releases lack usable
dated coverage) a GitHub Releases publication-date fallback. GraphQL payload
shapes and query documents live in ``github_maintenance_schema``; merging and
writing the collected payload (plus ``Repo.pushed_at``) lives in
``github_maintenance_persistence``. Release cadence and commit activity are
computed later by ``pg_atlas.metrics.materialize_maintenance`` from stored
data.

Cohort selection filters by creation date client-side (GitHub's REST ``since``
filters by update time, so listings walk GraphQL creation-ordered pages).
Responsiveness cohorts cover community-authored work: items authored by
maintainers themselves are excluded and counted separately — a maintainer's
own self-merged PR is not an unanswered request. Maintainer qualification is
event-specific: comments and submitted reviews require an
``OWNER``/``MEMBER``/``COLLABORATOR`` author association or a login on the
repo's declared maintainer list (``MAINTENANCE_DECLARED_MAINTAINERS``, which
closes the blind spot where private org membership hides ``MEMBER``);
merge and close timeline events on PRs expose only an actor, so
a non-bot actor other than the PR author counts — an authorization-based
proxy (merging or closing another author's PR requires write or triage
rights). An issue also counts as answered when its close was performed by
merged code (the close event's ``closer`` is a pull request or commit);
manual click-closes never qualify — they are ambiguous and mass-produceable.
An event, comment, or review that would qualify but whose actor cannot be
resolved is unknown attribution: the item is excluded from the response
fraction and counted separately, never guessed.

Collection is bounded by per-repo page, request, and time caps. GraphQL
rate-limit errors arrive inside HTTP-200 envelopes (PyGithub surfaces them as
``GithubException``); the collector waits for the advertised reset once,
bounded, then records the affected signals as ``incomplete`` — capped or
failed collection is recorded, never silently mixed into results. Persistence
merges per signal: a newly incomplete signal never overwrites a previously
complete one, whose own ``as_of`` keeps it honest at materialization time.

Scheduling fails closed behind ``MAINTENANCE_METRIC_ENABLED`` plus
``MAINTENANCE_METRIC_ALLOWLIST``; the task layer re-checks the same gate at
execution time. Explicit CLI runs bypass the gate::

    uv run python -m pg_atlas.procrastinate.github_maintenance Soneso/stellar-php-sdk

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import functools
import logging
import os
import time
from collections.abc import Callable
from dataclasses import dataclass
from threading import Lock
from typing import Any, cast

import msgspec
from github import Auth, Github, GithubException

from pg_atlas.config import settings
from pg_atlas.gitlog.filters import DEFAULT_BOT_NAME_PATTERNS
from pg_atlas.metrics.maintenance import (
    MAINTENANCE_SIGNALS_SCHEMA_VERSION,
    REASON_PAGE_CAP,
    REASON_QUERY_ERROR,
    REASON_RATE_LIMITED,
    REASON_REQUEST_CAP,
    REASON_TIME_CAP,
    STATE_INCOMPLETE,
    STATE_NOT_APPLICABLE,
    STATE_OK,
    BacklogSignal,
    MaintenanceSignals,
    ReleaseFallback,
    ResponseItem,
    ResponsivenessSignal,
    compute_backlog,
    compute_responsiveness,
    distinct_release_dates,
    parse_declared_maintainers,
)
from pg_atlas.procrastinate.github_maintenance_persistence import (
    AmbiguousMaintenanceSourceRepo,
    MaintenanceSourceRepoNotFound,
    persist_collected,
    resolve_repo,
)
from pg_atlas.procrastinate.github_maintenance_schema import (
    ISSUE_COHORT_QUERY,
    ISSUE_COMMENTS_QUERY,
    OPEN_ISSUES_QUERY,
    OPEN_PULLS_QUERY,
    OVERVIEW_QUERY,
    PULL_COHORT_QUERY,
    PULL_REVIEWS_QUERY,
    ActorRef,
    CommentNode,
    IssueByNumberData,
    IssueCohortData,
    IssueCohortNode,
    OpenIssuesData,
    OpenItemsConnection,
    OpenPullsData,
    OverviewData,
    OverviewRepository,
    PullByNumberData,
    PullCohortData,
    PullCohortNode,
    ReviewNode,
    TimelineNode,
)
from pg_atlas.repo_identity import parse_owner_repo_entries

logger = logging.getLogger(__name__)

#: Comment/review author associations accepted as maintainer responses. A
#: documented heuristic (MEMBER means public org membership), not proof of
#: responsibility — accepted as the best rule the API offers, extended per
#: repo by the declared maintainer-logins list.
MAINTAINER_ASSOCIATIONS = frozenset({"OWNER", "MEMBER", "COLLABORATOR"})

#: ClosedEvent ``closer`` typenames that mean an issue close was performed by
#: merged code rather than a manual click.
_CODE_CLOSER_TYPES = frozenset({"PullRequest", "Commit"})

#: GitHub Releases fallback: REST page size and pages walked.
_RELEASES_PER_PAGE = 30
_RELEASE_PAGE_CAP = 10


# ---------------------------------------------------------------------------
# Gate and declared overrides
# ---------------------------------------------------------------------------


@functools.lru_cache(maxsize=32)
def _parse_owner_repo_set(raw: str, *, setting_name: str) -> frozenset[str]:
    """
    Parse a comma-separated ``owner/repo`` list, rejecting malformed entries.

    Cached per raw value so the gate can run per repo per bootstrap pass
    without re-warning about the same malformed entry every time.
    """

    entries, rejected = parse_owner_repo_entries(raw)
    for item in rejected:
        logger.warning(f"{setting_name} entry rejected: {item!r}")

    return entries


def maintenance_metric_allowed(owner: str, repo: str) -> bool:
    """
    Fail-closed gate for maintenance-signal collection.

    Requires the enable flag AND an allowlist match: a comma-separated
    ``owner/repo`` list (case-insensitive), or the explicit value ``"*"`` for
    all repositories. An empty allowlist allows nothing. Declared host
    repositories are excluded even when the allowlist matches: their
    repo-level signals describe the hosting organization, so no collection
    budget is spent on them. Checked at scheduling time AND re-checked by the
    task at execution time, so disabling stops already-queued work. Explicit
    CLI runs bypass this gate. Lives in this module (not the task layer) so
    it stays importable without a database.
    """

    if not settings.MAINTENANCE_METRIC_ENABLED:
        return False

    if maintenance_host_repo(owner, repo):
        return False

    raw = settings.MAINTENANCE_METRIC_ALLOWLIST.strip()
    if raw == "*":
        return True

    allowed = _parse_owner_repo_set(raw, setting_name="maintenance-metric allowlist")

    return f"{owner}/{repo}".lower() in allowed


def maintenance_host_repo(owner: str, repo: str) -> bool:
    """
    Return whether this repo is declared a host repository.

    Some funded projects deliver their work into repositories they do not
    control or maintain as a whole (hardware-wallet support into vendor
    repos, for example). Declared host repos are never scheduled for
    collection, and the materializer renders their profiles with every
    signal not-applicable.
    """

    declared = _parse_owner_repo_set(
        settings.MAINTENANCE_HOST_REPOS,
        setting_name="maintenance-metric host-repository list",
    )

    return f"{owner}/{repo}".lower() in declared


def external_tracker_declared(owner: str, repo: str) -> bool:
    """
    Return whether this repo's issue tracking is declared to live off GitHub.

    An empty GitHub tracker cannot by itself distinguish an external tracker
    from no incoming work, so applicability is a declared per-repo override;
    declared repos get not-applicable issue signals, not real zeros.
    """

    declared = _parse_owner_repo_set(
        settings.MAINTENANCE_EXTERNAL_TRACKER_REPOS,
        setting_name="maintenance-metric external-tracker list",
    )

    return f"{owner}/{repo}".lower() in declared


def _declared_maintainer_logins(owner: str, repo: str) -> frozenset[str]:
    """
    Return the declared maintainer logins for this repo (lowercased).

    The association heuristic misses maintainers whose org membership is
    private (GitHub reports MEMBER only for public memberships), so a
    per-repo declared list extends it: listed logins qualify as maintainers
    regardless of reported association. Empty by default, leaving the
    heuristic as the only rule.
    """

    declared = parse_declared_maintainers(settings.MAINTENANCE_DECLARED_MAINTAINERS)

    return declared.get(f"{owner}/{repo}".lower(), frozenset())


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class MaintenanceCollectionFailed(Exception):
    """Raised when collection produced nothing usable for the repository."""


class _CollectionHalt(Exception):
    """Internal: stop collecting; remaining signals become incomplete."""

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(detail or reason)
        self.reason = reason
        self.detail = detail


# ---------------------------------------------------------------------------
# Classification helpers (pure)
# ---------------------------------------------------------------------------


def _parse_iso(value: str) -> dt.datetime:
    """Parse one GraphQL ISO-8601 timestamp into an aware UTC datetime."""

    return dt.datetime.fromisoformat(value).astimezone(dt.UTC)


def _is_bot_actor(actor: ActorRef | None) -> bool:
    """
    Return whether an author/actor reference is an automated account.

    Primary signal is the GraphQL ``Bot`` actor type; the pony metric's name
    patterns supplement it for machine accounts that present as ``User``
    (e.g. ``dependabot``). An absent reference is not classified here.
    """

    if actor is None:
        return False

    if actor.typename == "Bot":
        return True

    if actor.login:
        return any(pattern.search(actor.login) for pattern in DEFAULT_BOT_NAME_PATTERNS)

    return False


def _is_declared_maintainer(actor: ActorRef | None, declared_logins: frozenset[str]) -> bool:
    """Return whether the actor's login is on the repo's declared list."""

    return actor is not None and actor.login is not None and actor.login.lower() in declared_logins


@dataclass(frozen=True)
class _AttributionScan:
    """One page's earliest qualifying response and unknown-attribution times."""

    qualifying_time: dt.datetime | None
    unknown_times: list[dt.datetime]


def _scan_comment_page(
    comments: list[CommentNode],
    author_login: str | None,
    declared_logins: frozenset[str],
) -> _AttributionScan:
    """
    Scan one chronological comment page for maintainer responses.

    Qualifying: author association OWNER/MEMBER/COLLABORATOR or a login on
    the repo's declared maintainer list, author not a bot and not the item's
    own author. A comment whose association qualifies but whose author cannot
    be resolved is unknown attribution — reported, never guessed either way —
    mirroring the merge/close event rule. (In practice GitHub reports
    association NONE for deleted accounts, so such comments normally fail the
    association filter instead.)
    """

    qualifying_time: dt.datetime | None = None
    unknown_times: list[dt.datetime] = []
    for comment in comments:
        association_qualifies = comment.authorAssociation in MAINTAINER_ASSOCIATIONS
        if comment.author is None:
            if association_qualifies:
                unknown_times.append(_parse_iso(comment.createdAt))
            continue

        if not association_qualifies and not _is_declared_maintainer(comment.author, declared_logins):
            continue

        if _is_bot_actor(comment.author):
            continue

        if author_login is not None and comment.author.login == author_login:
            continue

        qualifying_time = _parse_iso(comment.createdAt)
        break

    return _AttributionScan(qualifying_time=qualifying_time, unknown_times=unknown_times)


def _scan_review_page(
    reviews: list[ReviewNode],
    author_login: str | None,
    declared_logins: frozenset[str],
) -> _AttributionScan:
    """Scan one review page under the same attribution rules as comments."""

    qualifying: list[dt.datetime] = []
    unknown_times: list[dt.datetime] = []
    for review in reviews:
        if review.submittedAt is None:
            continue

        association_qualifies = review.authorAssociation in MAINTAINER_ASSOCIATIONS
        if review.author is None:
            if association_qualifies:
                unknown_times.append(_parse_iso(review.submittedAt))
            continue

        if not association_qualifies and not _is_declared_maintainer(review.author, declared_logins):
            continue

        if _is_bot_actor(review.author):
            continue

        if author_login is not None and review.author.login == author_login:
            continue

        qualifying.append(_parse_iso(review.submittedAt))

    return _AttributionScan(qualifying_time=min(qualifying) if qualifying else None, unknown_times=unknown_times)


def _classify_issue_close_events(events: list[TimelineNode]) -> list[dt.datetime]:
    """
    Return the times of issue closes performed by merged code.

    An issue close whose ``closer`` is a pull request or commit ("closes
    #123") counts as a maintainer response: landing code on the default
    branch requires write access, and the fix is the answer. A close without
    a closer is a manual click — ambiguous (fixed? duplicate? stale purge?)
    and mass-produceable, so it never qualifies.
    """

    times: list[dt.datetime] = []
    for event in events:
        if event.typename != "ClosedEvent" or event.createdAt is None:
            continue

        if event.closer is None or event.closer.typename not in _CODE_CLOSER_TYPES:
            continue

        times.append(_parse_iso(event.createdAt))

    return sorted(times)


@dataclass(frozen=True)
class _EventClassification:
    """Merge/close timeline events split into qualifying and unknown times."""

    qualifying_times: list[dt.datetime]
    unknown_times: list[dt.datetime]


def _classify_pr_events(
    events: list[TimelineNode],
    author_login: str | None,
) -> _EventClassification:
    """
    Classify merge/close timeline events by their actor.

    These events expose only an actor, so a non-bot actor other than the PR
    author qualifies (authorization proxy: acting on another author's PR
    requires write or triage rights). The author's own close is a withdrawal,
    not a response. An event without a resolvable actor is unknown
    attribution and is reported as such, never guessed either way.
    """

    qualifying: list[dt.datetime] = []
    unknown: list[dt.datetime] = []
    for event in events:
        if event.typename not in ("ClosedEvent", "MergedEvent") or event.createdAt is None:
            continue

        occurred_at = _parse_iso(event.createdAt)

        if event.actor is None or not event.actor.login:
            unknown.append(occurred_at)
            continue

        if _is_bot_actor(event.actor):
            continue

        if author_login is not None and event.actor.login == author_login:
            continue

        qualifying.append(occurred_at)

    return _EventClassification(qualifying_times=sorted(qualifying), unknown_times=sorted(unknown))


def _resolve_response_item(
    created_at: dt.datetime,
    qualifying_times: list[dt.datetime],
    unknown_times: list[dt.datetime],
    *,
    interval_days: int,
    detection_truncated: bool = False,
) -> ResponseItem:
    """
    Combine qualifying and unknown-attribution response times into one item.

    An unknown-attribution response matters only when it could change the
    within-interval outcome: a qualifying response inside the interval settles
    the item regardless, while an unknown event, comment, or review inside
    the interval with no qualifying response inside it makes the item
    indeterminate. Truncated
    detection (unfetched pages that could still hold the first response) also
    yields indeterminate unless the item is already settled.
    """

    interval = dt.timedelta(days=interval_days)
    first_qualifying = min(qualifying_times) if qualifying_times else None

    if first_qualifying is not None and first_qualifying - created_at <= interval:
        return ResponseItem(created_at=created_at, first_response_at=first_qualifying)

    if any(unknown - created_at <= interval for unknown in unknown_times):
        return ResponseItem(created_at=created_at, indeterminate=True)

    if detection_truncated:
        return ResponseItem(created_at=created_at, indeterminate=True)

    return ResponseItem(created_at=created_at, first_response_at=first_qualifying)


# ---------------------------------------------------------------------------
# GitHub client
# ---------------------------------------------------------------------------

#: Per-request transport timeout for the dedicated client. Together with the
#: pre-request budget checks this bounds how far a single in-flight request
#: can run past the collection deadline.
_TRANSPORT_TIMEOUT_SECONDS = 15

_maintenance_client: Github | None = None
_maintenance_client_lock = Lock()


def _get_maintenance_client() -> Github:
    """
    Return the collector's own PyGithub client, transport retries disabled.

    Transport retries are disabled (``retry=None``) so every rate limit and
    transport failure surfaces immediately and the collector's own bounded
    handling governs: PyGithub's default transport policy sleeps until the
    advertised rate-limit reset (up to an hour) below the caller, outside
    the collector's request, time, and wait caps. The explicit per-request
    timeout bounds any single in-flight request. This client performs no
    requests at construction time.
    """

    global _maintenance_client

    if _maintenance_client is not None:
        return _maintenance_client

    with _maintenance_client_lock:
        if _maintenance_client is None:
            token = os.environ.get("GITHUB_TOKEN", "")
            auth = Auth.Token(token) if token else None
            _maintenance_client = Github(auth=auth, retry=None, timeout=_TRANSPORT_TIMEOUT_SECONDS)

    return _maintenance_client


# ---------------------------------------------------------------------------
# Collection budget
# ---------------------------------------------------------------------------


@dataclass
class _CollectionBudget:
    """Per-repo request and wall-clock caps shared across all phases."""

    request_cap: int
    deadline_monotonic: float
    requests_used: int = 0

    def charge(self) -> None:
        """Account for one API request; halt collection when a cap is hit."""

        if self.requests_used >= self.request_cap:
            raise _CollectionHalt(REASON_REQUEST_CAP, f"request cap {self.request_cap} reached")

        if time.monotonic() > self.deadline_monotonic:
            raise _CollectionHalt(REASON_TIME_CAP, "collection time cap reached")

        self.requests_used += 1

    def remaining_seconds(self) -> float:
        """Return the wall-clock budget left before the time cap."""

        return self.deadline_monotonic - time.monotonic()


# ---------------------------------------------------------------------------
# Collector
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CollectedMaintenance:
    """Everything one collection run produced for one repository."""

    signals: MaintenanceSignals
    pushed_at: dt.datetime | None


def _has_rate_limited_error(data: object) -> bool:
    """Return whether an exception payload carries a GraphQL RATE_LIMITED error."""

    if not isinstance(data, dict):
        return False

    errors = cast("dict[str, object]", data).get("errors")
    if not isinstance(errors, list):
        return False

    for entry in cast("list[object]", errors):
        if isinstance(entry, dict) and cast("dict[str, object]", entry).get("type") == "RATE_LIMITED":
            return True

    return False


def _rate_limit_wait_seconds(exc: GithubException) -> float | None:
    """
    Return the advertised retry delay if the exception is a rate limit.

    GraphQL rate limits arrive inside HTTP-200 envelopes that PyGithub
    re-raises with the error list in ``exc.data``; secondary limits surface as
    HTTP 403/429. Returns seconds until the advertised reset (or the
    ``retry-after`` value), or ``None`` when the exception is not a rate
    limit. The caller enforces the configured wait and remaining-time caps.
    """

    is_rate_limited = _has_rate_limited_error(exc.data)

    if not is_rate_limited and exc.status not in (403, 429):
        return None

    headers = {key.lower(): value for key, value in (exc.headers or {}).items()}

    retry_after = headers.get("retry-after")
    if retry_after is not None:
        try:
            return max(float(retry_after), 0.0)
        except ValueError:
            pass

    reset_epoch = headers.get("x-ratelimit-reset")
    if reset_epoch is not None:
        try:
            return max(float(reset_epoch) - time.time(), 0.0)
        except ValueError:
            pass

    if is_rate_limited:
        return 60.0

    return None


class _MaintenanceCollector:
    """One bounded collection pass over a single repository."""

    def __init__(self, owner: str, repo: str, *, now: dt.datetime) -> None:
        self._owner = owner
        self._repo = repo
        self._now = now
        self._declared_maintainers = _declared_maintainer_logins(owner, repo)
        self._window_start = now - dt.timedelta(days=settings.MAINTENANCE_WINDOW_DAYS)
        self._budget = _CollectionBudget(
            request_cap=settings.MAINTENANCE_REQUEST_CAP,
            deadline_monotonic=time.monotonic() + settings.MAINTENANCE_TIME_CAP_SECONDS,
        )
        self._pending_halt: _CollectionHalt | None = None

    @property
    def requests_used(self) -> int:
        return self._budget.requests_used

    async def _bounded_request[T](self, func: Callable[..., T], /, *args: Any) -> T:
        """
        Run one blocking request in a worker thread, bounded by the deadline.

        Deadline expiry cancels the await and raises the ordinary time-cap
        halt. The worker thread itself keeps running until the client's
        transport timeout bounds it; its late result is discarded.
        """

        remaining = self._budget.remaining_seconds()
        if remaining <= 0:
            raise _CollectionHalt(REASON_TIME_CAP, "collection time cap reached")

        try:
            return await asyncio.wait_for(asyncio.to_thread(func, *args), timeout=remaining)
        except TimeoutError:
            raise _CollectionHalt(REASON_TIME_CAP, "collection time cap reached during a request") from None

    def raise_pending_halt(self) -> None:
        """
        Re-raise a halt that a phase absorbed after salvaging partial data.

        A backlog listing keeps its exact server-side count and any fetched
        age prefix when a continuation page hits the shared budget; the halt
        is then re-raised here so no later phase starts.
        """

        if self._pending_halt is not None:
            halt = self._pending_halt
            self._pending_halt = None
            raise halt

    async def _run_graphql[D: msgspec.Struct](self, query: str, variables: dict[str, Any], data_type: type[D]) -> D:
        """
        Run one GraphQL query with budget accounting and bounded retry.

        A rate-limit response waits for the advertised reset (capped by
        ``MAINTENANCE_RATE_LIMIT_MAX_WAIT_SECONDS`` and by the remaining
        time budget, retry margin included) and retries once; a second
        limit, an over-cap wait, or any other API error halts collection
        with a typed reason. The synchronous request runs in a worker
        thread, so the event loop stays live; the budget is enforced before
        every request, and a single in-flight request is bounded by the
        client's transport timeout.
        """

        requester = getattr(_get_maintenance_client(), "_Github__requester", None)
        if requester is None:
            raise _CollectionHalt(REASON_QUERY_ERROR, "PyGithub requester is unavailable")

        retried = False
        while True:
            self._budget.charge()
            try:
                _, payload = await self._bounded_request(requester.graphql_query, query, variables)  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            except GithubException as exc:
                wait_seconds = _rate_limit_wait_seconds(exc)
                if wait_seconds is None:
                    raise _CollectionHalt(REASON_QUERY_ERROR, f"{type(exc).__name__}: {exc}") from exc

                effective_wait = wait_seconds + 1.0
                if (
                    retried
                    or effective_wait > settings.MAINTENANCE_RATE_LIMIT_MAX_WAIT_SECONDS
                    or effective_wait > self._budget.remaining_seconds()
                ):
                    raise _CollectionHalt(REASON_RATE_LIMITED, f"rate limited, reset in {wait_seconds:.0f}s") from exc

                logger.warning(
                    f"maintenance-collect rate limited: repo={self._owner}/{self._repo} waiting {effective_wait:.0f}s"
                )
                await asyncio.sleep(effective_wait)
                retried = True
                continue
            except OSError as exc:
                # The requests transport's exceptions subclass OSError.
                raise _CollectionHalt(REASON_QUERY_ERROR, f"GraphQL transport failed: {type(exc).__name__}: {exc}") from exc

            try:
                return msgspec.convert(payload.get("data"), type=data_type)  # pyright: ignore[reportUnknownMemberType]
            except msgspec.ValidationError as exc:
                raise _CollectionHalt(REASON_QUERY_ERROR, f"invalid GraphQL payload: {exc}") from exc

    # --- phases ---

    async def fetch_overview(self) -> OverviewRepository:
        """Fetch repo-level facts; a missing repository halts the run."""

        data: OverviewData = await self._run_graphql(
            OVERVIEW_QUERY,
            {"owner": self._owner, "name": self._repo},
            OverviewData,
        )
        if data.repository is None:
            raise _CollectionHalt(REASON_QUERY_ERROR, "repository not returned")

        return data.repository

    async def collect_open_ages(self, *, pulls: bool) -> BacklogSignal:
        """Walk the open listing oldest-first and build the backlog snapshot."""

        query = OPEN_PULLS_QUERY if pulls else OPEN_ISSUES_QUERY
        created_ats: list[dt.datetime] = []
        total_count = 0
        listing_complete = False
        halt_reason: str | None = None
        after: str | None = None

        for page_index in range(settings.MAINTENANCE_ITEM_PAGE_CAP):
            variables = {"owner": self._owner, "name": self._repo, "after": after}
            connection: OpenItemsConnection | None
            try:
                if pulls:
                    pulls_data: OpenPullsData = await self._run_graphql(query, variables, OpenPullsData)
                    connection = pulls_data.repository.pullRequests if pulls_data.repository is not None else None
                else:
                    issues_data: OpenIssuesData = await self._run_graphql(query, variables, OpenIssuesData)
                    connection = issues_data.repository.issues if issues_data.repository is not None else None
            except _CollectionHalt as halt:
                if page_index == 0:
                    raise

                # The count from the first page is a server-side total and
                # stays exact; salvage it and the fetched age prefix, then
                # let the orchestrator re-raise so no later phase starts.
                self._pending_halt = halt
                halt_reason = halt.reason
                break

            if connection is None:
                raise _CollectionHalt(REASON_QUERY_ERROR, "open listing not returned")

            total_count = connection.totalCount
            created_ats.extend(_parse_iso(node.createdAt) for node in connection.nodes or [])

            page_info = connection.pageInfo
            if page_info is None or not page_info.hasNextPage or not page_info.endCursor:
                listing_complete = True
                break

            if len(created_ats) > total_count // 2:
                # The oldest-first prefix already contains the middle order
                # statistic: the median is exact, further pages add nothing.
                break

            after = page_info.endCursor

        return compute_backlog(
            created_ats,
            total_count,
            now=self._now,
            as_of=self._now.isoformat(),
            incomplete_reason=halt_reason if halt_reason is not None else (None if listing_complete else REASON_PAGE_CAP),
        )

    async def collect_issue_cohort(self) -> ResponsivenessSignal:
        """Walk issues newest-first, classify responses, aggregate the cohort."""

        items: list[ResponseItem] = []
        maintainer_authored = 0
        cohort_complete = False
        after: str | None = None

        for _ in range(settings.MAINTENANCE_ITEM_PAGE_CAP):
            data: IssueCohortData = await self._run_graphql(
                ISSUE_COHORT_QUERY,
                {"owner": self._owner, "name": self._repo, "after": after},
                IssueCohortData,
            )
            connection = data.repository.issues if data.repository is not None else None
            if connection is None:
                raise _CollectionHalt(REASON_QUERY_ERROR, "issue cohort listing not returned")

            reached_window_start = False
            for node in connection.nodes or []:
                created_at = _parse_iso(node.createdAt)
                if created_at < self._window_start:
                    reached_window_start = True
                    break

                if _is_bot_actor(node.author):
                    continue

                if node.authorAssociation in MAINTAINER_ASSOCIATIONS or _is_declared_maintainer(
                    node.author, self._declared_maintainers
                ):
                    maintainer_authored += 1
                    continue

                items.append(await self._classify_issue(node, created_at))

            page_info = connection.pageInfo
            if reached_window_start or page_info is None or not page_info.hasNextPage or not page_info.endCursor:
                cohort_complete = True
                break

            after = page_info.endCursor

        if not cohort_complete:
            # A truncated cohort would bias the fraction (newest-first walk
            # misses the oldest, most response-eligible items), so no values
            # are reported. Only this signal is affected: the shared budget
            # survives, so collection continues with the remaining phases.
            logger.warning(f"maintenance-collect issue cohort capped before window start: repo={self._owner}/{self._repo}")

            return ResponsivenessSignal(
                state=STATE_INCOMPLETE,
                as_of=self._now.isoformat(),
                incomplete_reason=REASON_PAGE_CAP,
            )

        return compute_responsiveness(
            items,
            now=self._now,
            window_days=settings.MAINTENANCE_WINDOW_DAYS,
            interval_days=settings.MAINTENANCE_RESPONSE_INTERVAL_DAYS,
            as_of=self._now.isoformat(),
            declared_maintainers=self._declared_maintainers,
            maintainer_authored_count=maintainer_authored,
        )

    async def _classify_issue(self, node: IssueCohortNode, created_at: dt.datetime) -> ResponseItem:
        """
        Resolve one issue's first response from comments and code closes.

        Comment pagination continues until a qualifying comment is found (the
        chronological order makes it the earliest one) or a code close has
        settled the item within the interval; a later comment page can still
        hold a within-interval answer, so only a within-interval close ends
        the search early. When it does, the recorded response time may be the
        close even if an unfetched later-page comment came earlier; that
        imprecision touches only the context median and is bounded by the
        interval.
        """

        author_login = node.author.login if node.author is not None else None
        comments = node.comments.nodes if node.comments is not None else None
        scan = _scan_comment_page(comments or [], author_login, self._declared_maintainers)
        comment_time = scan.qualifying_time
        unknown_times = list(scan.unknown_times)

        close_times = _classify_issue_close_events(node.timelineItems.nodes or [] if node.timelineItems is not None else [])
        timeline_page_info = node.timelineItems.pageInfo if node.timelineItems is not None else None
        timeline_truncated = timeline_page_info is not None and timeline_page_info.hasNextPage and not close_times

        interval = dt.timedelta(days=settings.MAINTENANCE_RESPONSE_INTERVAL_DAYS)
        settled_by_close = any(close_time - created_at <= interval for close_time in close_times)

        page_info = node.comments.pageInfo if node.comments is not None else None
        after = page_info.endCursor if page_info is not None else None
        has_more = page_info is not None and page_info.hasNextPage and after is not None

        while comment_time is None and not settled_by_close and has_more:
            data: IssueByNumberData = await self._run_graphql(
                ISSUE_COMMENTS_QUERY,
                {"owner": self._owner, "name": self._repo, "number": node.number, "after": after},
                IssueByNumberData,
            )
            issue = data.repository.issue if data.repository is not None else None
            connection = issue.comments if issue is not None else None
            if connection is None:
                raise _CollectionHalt(REASON_QUERY_ERROR, f"comment continuation not returned for issue {node.number}")

            scan = _scan_comment_page(connection.nodes or [], author_login, self._declared_maintainers)
            comment_time = scan.qualifying_time
            unknown_times.extend(scan.unknown_times)
            page_info = connection.pageInfo
            after = page_info.endCursor if page_info is not None else None
            has_more = page_info is not None and page_info.hasNextPage and after is not None

        qualifying_times = list(close_times)
        if comment_time is not None:
            qualifying_times.append(comment_time)

        return _resolve_response_item(
            created_at,
            qualifying_times,
            unknown_times,
            interval_days=settings.MAINTENANCE_RESPONSE_INTERVAL_DAYS,
            detection_truncated=timeline_truncated,
        )

    async def collect_pull_cohort(self) -> ResponsivenessSignal:
        """Walk PRs newest-first, classify maintainer actions, aggregate."""

        items: list[ResponseItem] = []
        maintainer_authored = 0
        cohort_complete = False
        after: str | None = None

        for _ in range(settings.MAINTENANCE_ITEM_PAGE_CAP):
            data: PullCohortData = await self._run_graphql(
                PULL_COHORT_QUERY,
                {"owner": self._owner, "name": self._repo, "after": after},
                PullCohortData,
            )
            connection = data.repository.pullRequests if data.repository is not None else None
            if connection is None:
                raise _CollectionHalt(REASON_QUERY_ERROR, "pull request cohort listing not returned")

            reached_window_start = False
            for node in connection.nodes or []:
                created_at = _parse_iso(node.createdAt)
                if created_at < self._window_start:
                    reached_window_start = True
                    break

                if _is_bot_actor(node.author):
                    continue

                if node.authorAssociation in MAINTAINER_ASSOCIATIONS or _is_declared_maintainer(
                    node.author, self._declared_maintainers
                ):
                    maintainer_authored += 1
                    continue

                if node.isDraft and not settings.MAINTENANCE_INCLUDE_DRAFT_PRS:
                    continue

                items.append(await self._classify_pull(node, created_at))

            page_info = connection.pageInfo
            if reached_window_start or page_info is None or not page_info.hasNextPage or not page_info.endCursor:
                cohort_complete = True
                break

            after = page_info.endCursor

        if not cohort_complete:
            # Same containment as the issue cohort: report this signal
            # incomplete without biased values and keep collecting.
            logger.warning(f"maintenance-collect PR cohort capped before window start: repo={self._owner}/{self._repo}")

            return ResponsivenessSignal(
                state=STATE_INCOMPLETE,
                as_of=self._now.isoformat(),
                incomplete_reason=REASON_PAGE_CAP,
            )

        return compute_responsiveness(
            items,
            now=self._now,
            window_days=settings.MAINTENANCE_WINDOW_DAYS,
            interval_days=settings.MAINTENANCE_RESPONSE_INTERVAL_DAYS,
            as_of=self._now.isoformat(),
            include_drafts=settings.MAINTENANCE_INCLUDE_DRAFT_PRS,
            declared_maintainers=self._declared_maintainers,
            maintainer_authored_count=maintainer_authored,
        )

    async def _classify_pull(self, node: PullCohortNode, created_at: dt.datetime) -> ResponseItem:
        """Resolve one PR's first maintainer action from reviews and events."""

        author_login = node.author.login if node.author is not None else None
        interval = dt.timedelta(days=settings.MAINTENANCE_RESPONSE_INTERVAL_DAYS)

        # The review connection carries no submission-time ordering contract:
        # a review created early can be submitted late (pending reviews), so
        # a page's minimum is not the connection's minimum. Pagination
        # continues until a within-interval submission settles the outcome or
        # the connection is exhausted, keeping the earliest submission seen.
        review_scan = _scan_review_page(
            node.reviews.nodes or [] if node.reviews is not None else [], author_login, self._declared_maintainers
        )
        review_time = review_scan.qualifying_time
        unknown_times = list(review_scan.unknown_times)
        review_page_info = node.reviews.pageInfo if node.reviews is not None else None
        after = review_page_info.endCursor if review_page_info is not None else None
        has_more_reviews = review_page_info is not None and review_page_info.hasNextPage and after is not None

        while has_more_reviews and (review_time is None or review_time - created_at > interval):
            data: PullByNumberData = await self._run_graphql(
                PULL_REVIEWS_QUERY,
                {"owner": self._owner, "name": self._repo, "number": node.number, "after": after},
                PullByNumberData,
            )
            pull = data.repository.pullRequest if data.repository is not None else None
            connection = pull.reviews if pull is not None else None
            if connection is None:
                raise _CollectionHalt(REASON_QUERY_ERROR, f"review continuation not returned for PR {node.number}")

            review_scan = _scan_review_page(connection.nodes or [], author_login, self._declared_maintainers)
            if review_scan.qualifying_time is not None and (review_time is None or review_scan.qualifying_time < review_time):
                review_time = review_scan.qualifying_time

            unknown_times.extend(review_scan.unknown_times)
            review_page_info = connection.pageInfo
            after = review_page_info.endCursor if review_page_info is not None else None
            has_more_reviews = review_page_info is not None and review_page_info.hasNextPage and after is not None

        events = _classify_pr_events(
            node.timelineItems.nodes or [] if node.timelineItems is not None else [],
            author_login,
        )
        timeline_page_info = node.timelineItems.pageInfo if node.timelineItems is not None else None
        timeline_truncated = timeline_page_info is not None and timeline_page_info.hasNextPage and not events.qualifying_times

        qualifying_times = list(events.qualifying_times)
        if review_time is not None:
            qualifying_times.append(review_time)

        return _resolve_response_item(
            created_at,
            qualifying_times,
            unknown_times + events.unknown_times,
            interval_days=settings.MAINTENANCE_RESPONSE_INTERVAL_DAYS,
            detection_truncated=timeline_truncated,
        )

    async def collect_release_fallback(self) -> ReleaseFallback:
        """
        Fetch dated GitHub Releases publication dates via REST.

        The list endpoint documents no publication-date ordering (backfilled
        releases and drafts published late appear out of listing order), so
        the bounded listing is walked in full and the collected dates are
        sorted afterwards; a walk that ends at the page cap is incomplete.
        Drafts are always excluded, prereleases follow
        ``MAINTENANCE_INCLUDE_PRERELEASES``. Every request — the repository
        lookup and each release page — charges the shared budget at the
        request boundary.
        """

        gh = _get_maintenance_client()
        dates: set[dt.date] = set()
        complete = False

        try:
            self._budget.charge()
            repo_handle = await self._bounded_request(gh.get_repo, f"{self._owner}/{self._repo}")
            releases = repo_handle.get_releases()

            for page_index in range(_RELEASE_PAGE_CAP):
                self._budget.charge()
                page = await self._bounded_request(releases.get_page, page_index)

                for release in page:
                    # The stubs declare published_at non-optional; the API
                    # returns null for unpublished entries.
                    if release.draft or release.published_at is None:  # pyright: ignore[reportUnnecessaryComparison]
                        continue

                    if release.prerelease and not settings.MAINTENANCE_INCLUDE_PRERELEASES:
                        continue

                    dates.add(release.published_at.astimezone(dt.UTC).date())

                if len(page) < _RELEASES_PER_PAGE:
                    complete = True
                    break

        except GithubException as exc:
            wait_seconds = _rate_limit_wait_seconds(exc)
            if wait_seconds is not None:
                raise _CollectionHalt(REASON_RATE_LIMITED, f"rate limited, reset in {wait_seconds:.0f}s") from exc

            raise _CollectionHalt(REASON_QUERY_ERROR, f"GitHub releases fetch failed: {exc}") from exc
        except OSError as exc:
            # The requests transport's exceptions subclass OSError.
            raise _CollectionHalt(
                REASON_QUERY_ERROR, f"GitHub releases transport failed: {type(exc).__name__}: {exc}"
            ) from exc

        return ReleaseFallback(
            state=STATE_OK if complete else STATE_INCOMPLETE,
            as_of=self._now.isoformat(),
            publication_dates=[date.isoformat() for date in sorted(dates)],
            include_prereleases=settings.MAINTENANCE_INCLUDE_PRERELEASES,
            complete=complete,
            incomplete_reason=None if complete else REASON_PAGE_CAP,
        )


async def collect_maintenance_for_repo(
    owner: str,
    repo: str,
    *,
    need_release_fallback: bool,
    now: dt.datetime | None = None,
) -> CollectedMaintenance:
    """
    Run one bounded collection pass and assemble the signals payload.

    Phases run in fixed order (overview, backlogs, cohorts, release
    fallback); a halt marks the interrupted and unattempted signals
    ``incomplete`` with the halt's typed reason while completed signals keep
    their values. A halt before anything was collected raises
    ``MaintenanceCollectionFailed`` so a previously stored payload is never
    replaced by an empty one.
    """

    observed_now = now if now is not None else dt.datetime.now(dt.UTC)
    collector = _MaintenanceCollector(owner, repo, now=observed_now)
    as_of = observed_now.isoformat()

    try:
        overview = await collector.fetch_overview()
    except _CollectionHalt as halt:
        raise MaintenanceCollectionFailed(f"{owner}/{repo}: {halt.reason}: {halt.detail}") from halt

    pushed_at = _parse_iso(overview.pushedAt) if overview.pushedAt else None
    tracker_declared_external = external_tracker_declared(owner, repo)
    issues_applicable = overview.hasIssuesEnabled and not tracker_declared_external

    issue_backlog: BacklogSignal
    issue_responsiveness: ResponsivenessSignal
    pr_backlog: BacklogSignal | None = None
    pr_responsiveness: ResponsivenessSignal | None = None
    release_fallback: ReleaseFallback | None = None
    halt_reason: str | None = None

    if not issues_applicable:
        issue_backlog = BacklogSignal(state=STATE_NOT_APPLICABLE, as_of=as_of)
        issue_responsiveness = ResponsivenessSignal(state=STATE_NOT_APPLICABLE, as_of=as_of)
    else:
        issue_backlog = BacklogSignal(state=STATE_INCOMPLETE, as_of=as_of)
        issue_responsiveness = ResponsivenessSignal(state=STATE_INCOMPLETE, as_of=as_of)

    try:
        if issues_applicable:
            issue_backlog = await collector.collect_open_ages(pulls=False)
            collector.raise_pending_halt()

        pr_backlog = await collector.collect_open_ages(pulls=True)
        collector.raise_pending_halt()

        if issues_applicable:
            issue_responsiveness = await collector.collect_issue_cohort()

        pr_responsiveness = await collector.collect_pull_cohort()

        if need_release_fallback:
            release_fallback = await collector.collect_release_fallback()

    except _CollectionHalt as halt:
        halt_reason = halt.reason
        logger.warning(
            f"maintenance-collect halted: repo={owner}/{repo} reason={halt.reason} "
            f"detail={halt.detail} requests_used={collector.requests_used}"
        )

    if halt_reason is not None:
        if issues_applicable and issue_backlog.state == STATE_INCOMPLETE and issue_backlog.incomplete_reason is None:
            issue_backlog = BacklogSignal(state=STATE_INCOMPLETE, as_of=as_of, incomplete_reason=halt_reason)

        if (
            issues_applicable
            and issue_responsiveness.state == STATE_INCOMPLETE
            and issue_responsiveness.incomplete_reason is None
        ):
            issue_responsiveness = ResponsivenessSignal(state=STATE_INCOMPLETE, as_of=as_of, incomplete_reason=halt_reason)

        if pr_backlog is None:
            pr_backlog = BacklogSignal(state=STATE_INCOMPLETE, as_of=as_of, incomplete_reason=halt_reason)

        if pr_responsiveness is None:
            pr_responsiveness = ResponsivenessSignal(state=STATE_INCOMPLETE, as_of=as_of, incomplete_reason=halt_reason)

        if need_release_fallback and release_fallback is None:
            release_fallback = ReleaseFallback(state=STATE_INCOMPLETE, as_of=as_of, incomplete_reason=halt_reason)

    signals = MaintenanceSignals(
        schema_version=MAINTENANCE_SIGNALS_SCHEMA_VERSION,
        collected_at=as_of,
        window_days=settings.MAINTENANCE_WINDOW_DAYS,
        response_interval_days=settings.MAINTENANCE_RESPONSE_INTERVAL_DAYS,
        requests_used=collector.requests_used,
        issues_enabled=overview.hasIssuesEnabled,
        external_tracker_declared=tracker_declared_external,
        archived=overview.isArchived,
        issue_responsiveness=issue_responsiveness,
        issue_backlog=issue_backlog,
        pr_responsiveness=pr_responsiveness,
        pr_backlog=pr_backlog,
        release_fallback=release_fallback,
    )

    return CollectedMaintenance(signals=signals, pushed_at=pushed_at)


def _signal_states(signals: MaintenanceSignals) -> dict[str, str]:
    """Reduce a signals payload to its per-signal states for summary logs."""

    states: dict[str, str] = {}
    for name, signal in (
        ("issue_responsiveness", signals.issue_responsiveness),
        ("issue_backlog", signals.issue_backlog),
        ("pr_responsiveness", signals.pr_responsiveness),
        ("pr_backlog", signals.pr_backlog),
        ("release_fallback", signals.release_fallback),
    ):
        if signal is not None:
            states[name] = signal.state

    return states


async def run_maintenance_collection(owner: str, repo: str) -> None:
    """
    Collect and persist maintenance signals for one repository.

    Resolution and persistence run in short separate sessions around the
    network phase, so no transaction spans GitHub calls.
    """

    from pg_atlas.db_models.session import get_session_factory

    factory = get_session_factory()
    async with factory() as session:
        resolved = await resolve_repo(session, owner, repo)

    depsdev_date_count = len(distinct_release_dates(resolved.releases))
    need_release_fallback = depsdev_date_count < settings.MAINTENANCE_CADENCE_MIN_EVENTS

    collected = await collect_maintenance_for_repo(owner, repo, need_release_fallback=need_release_fallback)

    async with factory() as session:
        merged = await persist_collected(session, resolved, collected.signals, collected.pushed_at)
        await session.commit()

    states_text = " ".join(f"{name}={state}" for name, state in sorted(_signal_states(merged).items()))
    logger.info(
        f"maintenance-collect: repo={owner}/{repo} requests_used={collected.signals.requests_used} "
        f"depsdev_release_dates={depsdev_date_count} fallback_fetched={need_release_fallback} {states_text}"
    )


# ---------------------------------------------------------------------------
# CLI (explicit runs bypass the gate)
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Collect GitHub maintenance signals for tracked repositories.")
    parser.add_argument("repos", nargs="+", help="Repositories as owner/repo")

    return parser


async def _cli_main() -> None:
    args = _build_parser().parse_args()
    logging.basicConfig(level=settings.LOG_LEVEL, format="%(asctime)s %(levelname)-8s %(name)s: %(message)s")

    if not settings.DATABASE_URL:
        logger.error("PG_ATLAS_DATABASE_URL is required for maintenance collection")
        raise SystemExit(1)

    if not os.environ.get("GITHUB_TOKEN"):
        logger.error("GITHUB_TOKEN is required: the GitHub GraphQL API rejects unauthenticated requests")
        raise SystemExit(1)

    failures = 0
    for spec in args.repos:
        parts = spec.split("/")
        if len(parts) != 2 or not parts[0] or not parts[1]:
            logger.error(f"Invalid repository argument (expected owner/repo): {spec!r}")
            failures += 1
            continue

        try:
            await run_maintenance_collection(parts[0], parts[1])
        except (MaintenanceSourceRepoNotFound, AmbiguousMaintenanceSourceRepo, MaintenanceCollectionFailed) as exc:
            logger.error(f"maintenance-collect failed: repo={spec} error={exc}")
            failures += 1

    if failures:
        raise SystemExit(2)


if __name__ == "__main__":
    asyncio.run(_cli_main())
