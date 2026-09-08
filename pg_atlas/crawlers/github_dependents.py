"""
GitHub dependents crawler for PG Atlas.

Scrapes the public ``github.com/{owner}/{repo}/network/dependents`` page —
the only place GitHub exposes the reverse-dependents direction — and records
each dependent repository as a ``GithubDependentObservation`` row for the
tracked source ``Repo``, together with one ``GithubDependentsCrawlRun`` audit
row per attempt. The crawler is collection/audit infrastructure: it writes no
``DependsOn`` edges, creates no ``RepoVertex`` rows, and changes no metric
input. Unlike the registry crawlers, it is keyed by repository
(``{owner}/{repo}``) rather than by published package, so it also surfaces
applications (wallets, anchors, backends) that never publish to a registry.

GitHub scopes every dependents listing to a single package. A repository that
publishes several packages renders a package selector, and a request without
``package_id`` shows one arbitrary default package — so for multi-package
repositories the crawler enumerates the selector and walks every package's
listing, unioning the dependents. Retained packages are walked in descending
header-count order; the selector list itself is truncated in menu order at
``packages_cap``.

Every crawl produces an immutable ``DependentsSnapshot`` whose completeness is
tracked on two independent axes: ``listing_complete`` (were all public entries
enumerated — the gate for retiring previously seen observations) and
``counts_complete`` (did every header total parse). Partial anomalies (caps,
unparseable rows, an empty page mid-walk, a cycling or unreadable Next cursor,
a positive header count with no rendered rows) mark the affected axis
incomplete with a typed reason. Systematic drift fails loud: absent header
counts, a selector button without a parseable menu, and pages whose rendered
entry rows all fail to parse raise ``DependentsPageLayoutError``, which marks
the run ``failed`` without touching observations.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Literal

import httpx
from sqlalchemy import func, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pg_atlas.api_metadata import VERSION
from pg_atlas.config import settings
from pg_atlas.crawlers.base import (
    CrawledDependent,
    CrawledPackage,
    CrawlResult,
    RegistryCrawler,
    SourceRepoNotFound,
)
from pg_atlas.db_models.base import GithubDependentsRunStatus
from pg_atlas.db_models.github_dependents_observation import (
    GithubDependentObservation,
    GithubDependentsCrawlRun,
)
from pg_atlas.db_models.repo_vertex import Repo

logger = logging.getLogger(__name__)

GITHUB_BASE_URL = "https://github.com"

#: Bumped whenever parser semantics change; recorded on every crawl run.
GITHUB_DEPENDENTS_PARSER_VERSION = "1"

_REPOSITORY_TYPE = "REPOSITORY"

#: Typed incomplete-reason codes (also the grep-able log markers).
REASON_PACKAGE_CAP = "package-cap"
REASON_PAGE_CAP = "page-cap"
REASON_ENTRY_CAP = "entry-cap"
REASON_NEXT_CURSOR = "next-cursor"
REASON_CURSOR_CYCLE = "cursor-cycle"
REASON_EMPTY_PAGE = "empty-page"
REASON_ENTRIES_PARTIAL = "entries-partial"
REASON_NO_VISIBLE_ROWS = "positive-count-no-visible-rows"
REASON_COUNTS_PARTIAL = "counts-partial"

#: Advisory-lock namespace for per-source persistence serialization.
_ADVISORY_LOCK_NAMESPACE = "github-dependents"


def github_dependents_scheduling_allowed(owner: str, repo: str) -> bool:
    """
    Fail-closed scheduling gate for the GitHub dependents crawl.

    Requires the enable flag AND an allowlist match: a comma-separated
    ``owner/repo`` list (case-insensitive), or the explicit value ``"*"`` for
    all repositories. An empty allowlist schedules nothing. Malformed entries
    are rejected with a warning. Explicit CLI runs bypass this gate. Lives in
    this module (not the task layer) so it stays importable without a
    database.
    """
    if not settings.GITHUB_DEPENDENTS_ENABLED:
        return False

    raw = settings.GITHUB_DEPENDENTS_ALLOWLIST.strip()
    if raw == "*":
        return True

    allowed: set[str] = set()
    for item in raw.split(","):
        entry = item.strip().lower()
        if not entry:
            continue

        parts = entry.split("/")
        if len(parts) != 2 or not parts[0] or not parts[1]:
            logger.warning(f"github-dependents allowlist entry rejected: {item.strip()!r}")
            continue

        allowed.add(entry)

    return f"{owner}/{repo}".lower() in allowed


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class DependentsPageLayoutError(Exception):
    """Raised when the dependents page no longer matches the expected layout."""


class AmbiguousSourceRepo(Exception):
    """Raised when more than one tracked repo matches the source identity."""


# ---------------------------------------------------------------------------
# Parsed data and snapshot types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ParsedDependent:
    """One dependent entry extracted from the dependents HTML page."""

    owner: str
    repo: str


@dataclass(frozen=True)
class ObservedDependent:
    """One unique dependent in a snapshot, with observed display casing."""

    owner: str
    repo: str
    package_ids: tuple[str, ...] = ()

    @property
    def key(self) -> str:
        """Case-insensitive GitHub identity: lowercased ``owner/repo``."""

        return f"{self.owner}/{self.repo}".lower()


@dataclass(frozen=True)
class DependentsSnapshot:
    """
    Immutable result of one crawl attempt, before persistence.

    ``listing_complete`` gates reconciliation; ``counts_complete`` gates
    presenting the reported header totals as current. ``fingerprint`` is a
    sha256 over the ordered ``(request URL, page sha256)`` tuples of every
    fetched page; URLs carry no secrets.
    """

    source: str
    dependents: tuple[ObservedDependent, ...]
    listing_complete: bool
    listing_incomplete_reason: str | None
    counts_complete: bool
    counts_incomplete_reason: str | None
    repos_total_reported: int | None
    packages_total_reported: int | None
    packages_scanned: int | None
    pages_fetched: int
    request_count: int
    fingerprint: str


@dataclass
class _SnapshotBuilder:
    """Mutable, function-local accumulator that produces the frozen snapshot."""

    source: str
    dependents: dict[str, ObservedDependent] = field(default_factory=dict[str, ObservedDependent])
    listing_reasons: list[str] = field(default_factory=list[str])
    counts_reasons: list[str] = field(default_factory=list[str])
    repos_total: int | None = None
    packages_total: int | None = None
    packages_scanned: int | None = None
    pages_fetched: int = 0
    request_count: int = 0
    page_digests: list[tuple[str, str]] = field(default_factory=list[tuple[str, str]])

    def count_request(self) -> None:
        """Called once per HTTP attempt, including retries inside the shared layer."""

        self.request_count += 1

    def record_page(self, url: str, html: str) -> None:
        self.pages_fetched += 1
        self.page_digests.append((url, hashlib.sha256(html.encode()).hexdigest()))

    def add_dependent(self, entry: ParsedDependent, package_id: str | None) -> None:
        """Merge one parsed entry, unioning package memberships on duplicates."""

        key = f"{entry.owner}/{entry.repo}".lower()
        existing = self.dependents.get(key)
        if existing is None:
            packages = (package_id,) if package_id else ()
            self.dependents[key] = ObservedDependent(owner=entry.owner, repo=entry.repo, package_ids=packages)
        elif package_id and package_id not in existing.package_ids:
            self.dependents[key] = ObservedDependent(
                owner=existing.owner,
                repo=existing.repo,
                package_ids=(*existing.package_ids, package_id),
            )

    def listing_incomplete(self, reason: str) -> None:
        if reason not in self.listing_reasons:
            self.listing_reasons.append(reason)

    def counts_incomplete(self, reason: str) -> None:
        if reason not in self.counts_reasons:
            self.counts_reasons.append(reason)

    def build(self) -> DependentsSnapshot:
        fingerprint = hashlib.sha256("\n".join(f"{u} {d}" for u, d in self.page_digests).encode()).hexdigest()

        return DependentsSnapshot(
            source=self.source,
            dependents=tuple(self.dependents.values()),
            listing_complete=not self.listing_reasons,
            listing_incomplete_reason=self.listing_reasons[0] if self.listing_reasons else None,
            counts_complete=not self.counts_reasons,
            counts_incomplete_reason=self.counts_reasons[0] if self.counts_reasons else None,
            repos_total_reported=self.repos_total,
            packages_total_reported=self.packages_total,
            packages_scanned=self.packages_scanned,
            pages_fetched=self.pages_fetched,
            request_count=self.request_count,
            fingerprint=fingerprint,
        )


# ---------------------------------------------------------------------------
# Parse regexes
# ---------------------------------------------------------------------------

# The count regexes accept extra query parameters around ``dependent_type``
# (package-scoped pages append ``package_id``), match singular and plural
# labels, require a leading digit (a partial number must not parse), and bound
# the gap to the inside of the anchor so a count elsewhere in the document can
# never be mis-bound.
_COUNT_REPOSITORIES_RE = re.compile(
    r'href="[^"]*network/dependents\?[^"]*dependent_type=REPOSITORY[^"]*"[^>]*>'
    r"(?:(?!</a>).)*?(\d[\d,]*)\s+Repositor(?:ies|y)",
    re.DOTALL,
)
_COUNT_PACKAGES_RE = re.compile(
    r'href="[^"]*network/dependents\?[^"]*dependent_type=PACKAGE[^"]*"[^>]*>' r"(?:(?!</a>).)*?(\d[\d,]*)\s+Packages?",
    re.DOTALL,
)
_ENTRY_SPLIT_RE = re.compile(r'data-test-id="dg-repo-pkg-dependent"')
# Attribute order varies between anchors on real pages; the lookahead accepts
# ``class="text-bold"`` anywhere inside the tag.
_DEPENDENT_LINK_RE = re.compile(r'<a\b(?=[^>]*class="text-bold")[^>]*href="/([^"]+)"')
_PAGINATION_RE = re.compile(r'data-test-selector="pagination"(.*?)</div>', re.DOTALL)
_NEXT_CURSOR_RE = re.compile(r'dependents_after=([^"&]+).*?>\s*Next\b', re.DOTALL)
# Independent of ``_PAGINATION_RE`` and tolerant of whitespace and nested
# markup, so container drift cannot mask an existing further page. A false
# positive (an entry row literally named "Next") only over-flags
# incompleteness — it fails closed.
_NEXT_ANCHOR_RE = re.compile(r"<a\b[^>]*>(?:(?!</a>).)*?\bNext\b(?:(?!</a>).)*?</a>", re.DOTALL)
# Selector menu anchors: class token exactly ``select-menu-item`` (not a
# prefixed variant), ``package_id`` in any query-parameter position.
_PACKAGE_SELECTOR_RE = re.compile(
    r'<a\b(?=[^>]*class="(?:[^"]* )?select-menu-item(?: [^"]*)?")[^>]*href="[^"]*[?&;]package_id=([^"&]+)'
)
# The selector *button* — used to fail closed when the button renders but no
# menu anchor parses: the default page covers only one arbitrary package.
_SELECTOR_BUTTON_RE = re.compile(r"select-menu-button(?:(?!</summary>).)*?Package:", re.DOTALL)


# ---------------------------------------------------------------------------
# Pure parse functions
# ---------------------------------------------------------------------------


def parse_dependent_counts(html: str) -> tuple[int | None, int | None]:
    """
    Extract the header totals ``(repositories, packages)``.

    Either element is ``None`` when its marker is absent. Both being ``None``
    signals a layout change rather than an empty repository. Reported totals
    may exceed the publicly enumerable entries; the difference is not proven
    to be private or deleted repositories, only not publicly listed.
    """

    repo_match = _COUNT_REPOSITORIES_RE.search(html)
    pkg_match = _COUNT_PACKAGES_RE.search(html)

    repo_count = int(repo_match.group(1).replace(",", "")) if repo_match else None
    pkg_count = int(pkg_match.group(1).replace(",", "")) if pkg_match else None

    return repo_count, pkg_count


def parse_dependent_entries(html: str) -> list[ParsedDependent]:
    """
    Parse one ``REPOSITORY`` dependents page into ``ParsedDependent`` entries.

    Rows whose link does not parse to a non-empty ``owner/repo`` pair are
    skipped; callers compare the result against the rendered row count to
    classify such rows as drift.
    """

    entries: list[ParsedDependent] = []
    for chunk in _ENTRY_SPLIT_RE.split(html)[1:]:
        link_match = _DEPENDENT_LINK_RE.search(chunk)
        if link_match is None:
            continue

        parts = link_match.group(1).strip().split("/")
        if len(parts) != 2 or not parts[0] or not parts[1]:
            continue

        owner, repo = parts
        entries.append(ParsedDependent(owner=owner, repo=repo))

    return entries


def parse_package_selector(html: str) -> list[str]:
    """
    Extract the ``package_id`` values from the package selector menu.

    An empty list means the repository publishes at most one package and the
    plain listing already covers the whole repository. The ids are returned in
    menu order, URL-encoded exactly as GitHub renders them (they are opaque
    tokens and are placed back into URLs verbatim).
    """

    seen: set[str] = set()
    package_ids: list[str] = []
    for match in _PACKAGE_SELECTOR_RE.finditer(html):
        package_id = match.group(1)
        if package_id not in seen:
            seen.add(package_id)
            package_ids.append(package_id)

    return package_ids


def parse_pagination(html: str) -> tuple[str | None, bool]:
    """
    Return ``(next_cursor, next_link_present)`` for one listing page.

    The cursor is read from the pagination container's Next link (the Previous
    link carries ``dependents_before``, so the regex is unambiguous). The
    Next-anchor probe is independent of the container markup: when it reports
    a further page but no cursor parsed, callers record incompleteness.
    """

    cursor: str | None = None
    pagination_match = _PAGINATION_RE.search(html)
    if pagination_match is not None:
        cursor_match = _NEXT_CURSOR_RE.search(pagination_match.group(1))
        if cursor_match is not None:
            cursor = cursor_match.group(1)

    return cursor, _NEXT_ANCHOR_RE.search(html) is not None


def find_next_cursor(html: str) -> str | None:
    """Cursor-only convenience over ``parse_pagination``, used by parser tests and drift probes."""

    return parse_pagination(html)[0]


def _count_entry_markers(html: str) -> int:
    """Count the rendered dependent entry rows via their boundary marker."""

    return len(_ENTRY_SPLIT_RE.findall(html))


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def _split_owner_repo(package_name: str) -> tuple[str, str]:
    """Split a ``{owner}/{repo}`` key into its two components."""

    parts = package_name.split("/")
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError(f"Expected '<owner>/<repo>', got {package_name!r}")

    return parts[0], parts[1]


def _dependents_url(
    owner: str,
    repo: str,
    dependent_type: str,
    *,
    cursor: str | None = None,
    package_id: str | None = None,
) -> str:
    """Build the dependents page URL for one type, package scope, and cursor."""

    url = f"{GITHUB_BASE_URL}/{owner}/{repo}/network/dependents?dependent_type={dependent_type}"
    if package_id:
        url += f"&package_id={package_id}"
    if cursor:
        url += f"&dependents_after={cursor}"

    return url


def _retry_after_seconds(resp: httpx.Response) -> float | None:
    """
    Bounded wait suggested by 403 rate-limit headers, or ``None``.

    ``None`` means the 403 carries no rate-limit evidence and is a classified
    failure, not a retry candidate.
    """

    retry_after = resp.headers.get("Retry-After")
    if retry_after is not None:
        try:
            return max(0.0, float(retry_after))
        except ValueError:
            return None

    if resp.headers.get("x-ratelimit-remaining") == "0":
        reset = resp.headers.get("x-ratelimit-reset")
        if reset is not None:
            try:
                return max(0.0, float(reset) - dt.datetime.now(dt.UTC).timestamp())
            except ValueError:
                return None

    return None


# ---------------------------------------------------------------------------
# Crawler
# ---------------------------------------------------------------------------


class GitHubDependentsCrawler(RegistryCrawler):
    """
    Crawler for the public GitHub dependents page.

    Keyed by repository: the "package name" argument is ``{owner}/{repo}``.
    ``collect_snapshot`` performs the whole HTTP walk and returns an immutable
    ``DependentsSnapshot``; ``_process_package`` overrides the base
    package-registry write path entirely and persists the snapshot as one
    crawl-run row plus observation rows. The ABC methods ``fetch_package`` and
    ``fetch_dependents`` are thin snapshot views used by the CLI result
    surface and tests; each performs its own collection.
    """

    REGISTRY = "github.com"

    #: Bounded budget for 403 rate-limit retries per request.
    MAX_RATELIMIT_RETRIES = 2
    MAX_RATELIMIT_WAIT_SECONDS = 60.0

    def __init__(
        self,
        client: httpx.AsyncClient,
        session_factory: async_sessionmaker[AsyncSession],
        rate_limit: float = 1.0,
        max_retries: int = 3,
        *,
        pages_cap: int | None = None,
        entry_cap: int | None = None,
        packages_cap: int | None = None,
    ) -> None:
        super().__init__(
            client=client,
            session_factory=session_factory,
            rate_limit=rate_limit,
            max_retries=max_retries,
        )
        self.pages_cap = pages_cap if pages_cap is not None else settings.GITHUB_DEPENDENTS_PAGES_CAP
        self.entry_cap = entry_cap if entry_cap is not None else settings.GITHUB_DEPENDENTS_ENTRY_CAP
        self.packages_cap = packages_cap if packages_cap is not None else settings.GITHUB_DEPENDENTS_PACKAGES_CAP
        for cap_name in ("pages_cap", "entry_cap", "packages_cap"):
            if getattr(self, cap_name) < 1:
                raise ValueError(f"{cap_name} must be positive, got {getattr(self, cap_name)}")

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    async def _fetch_page(self, url: str, builder: _SnapshotBuilder) -> str:
        """
        Fetch one page, recording it on the builder.

        Adds bounded handling for GitHub 403 rate limiting on top of the
        shared retry behavior: a 403 carrying ``Retry-After`` or exhausted
        ``x-ratelimit`` headers is retried after a capped wait; a 403 without
        rate-limit evidence stays fatal for the crawl.
        """

        for attempt in range(self.MAX_RATELIMIT_RETRIES + 1):
            try:
                resp = await self._request_with_retry(url, on_attempt=builder.count_request)
            except httpx.HTTPStatusError as exc:
                wait = _retry_after_seconds(exc.response) if exc.response.status_code == 403 else None
                if wait is None or attempt >= self.MAX_RATELIMIT_RETRIES:
                    raise

                if wait > self.MAX_RATELIMIT_WAIT_SECONDS:
                    # The reset is provably beyond the wait budget; retrying
                    # into a still-exhausted window is futile.
                    logger.warning(f"github-dependents 403 rate limit reset beyond budget ({wait:.0f}s): url={url}")
                    raise

                logger.warning(f"github-dependents 403 rate limited, waiting {wait:.0f}s: url={url}")
                await asyncio.sleep(wait)
                continue

            builder.record_page(url, resp.text)

            return resp.text

        raise RuntimeError("unreachable")  # pragma: no cover

    # ------------------------------------------------------------------
    # Snapshot collection
    # ------------------------------------------------------------------

    async def collect_snapshot(self, package_name: str) -> DependentsSnapshot:
        """
        Perform the complete crawl for one repository and return the snapshot.

        Raises ``DependentsPageLayoutError`` on systematic drift (absent
        header counts, selector button without a parseable menu, rendered
        entry rows of which none parse).
        """

        owner, repo = _split_owner_repo(package_name)
        builder = _SnapshotBuilder(source=f"{owner}/{repo}")

        base_html = await self._fetch_page(_dependents_url(owner, repo, _REPOSITORY_TYPE), builder)
        package_ids = parse_package_selector(base_html)

        if not package_ids and _SELECTOR_BUTTON_RE.search(base_html):
            # Selector button without a parseable menu is layout drift; the
            # default page covers only one arbitrary package.
            logger.warning(f"github-dependents layout change suspected: repo={owner}/{repo} marker=selector")
            raise DependentsPageLayoutError(f"Package selector present but no package ids parsed for {owner}/{repo}")

        if not package_ids:
            repos_total, packages_total = self._parse_counts(base_html, owner, repo, builder)
            builder.repos_total = repos_total
            builder.packages_total = packages_total
            await self._walk_listing(owner, repo, None, base_html, repos_total, builder)
        else:
            if len(package_ids) > self.packages_cap:
                logger.warning(
                    f"github-dependents package list truncated at cap: repo={owner}/{repo} "
                    f"packages={len(package_ids)} cap={self.packages_cap} marker={REASON_PACKAGE_CAP}"
                )
                package_ids = package_ids[: self.packages_cap]
                builder.listing_incomplete(REASON_PACKAGE_CAP)
                # The summed header totals omit the dropped packages.
                builder.counts_incomplete(REASON_PACKAGE_CAP)

            builder.packages_scanned = len(package_ids)
            first_pages: dict[str, str] = {}
            package_totals: dict[str, int | None] = {}
            for package_id in package_ids:
                if self.rate_limit > 0:
                    await asyncio.sleep(self.rate_limit)
                page_html = await self._fetch_page(
                    _dependents_url(owner, repo, _REPOSITORY_TYPE, package_id=package_id), builder
                )
                page_repos, page_packages = self._parse_counts(page_html, owner, repo, builder)
                if page_repos is not None:
                    builder.repos_total = (builder.repos_total or 0) + page_repos
                if page_packages is not None:
                    builder.packages_total = (builder.packages_total or 0) + page_packages

                first_pages[package_id] = page_html
                package_totals[package_id] = page_repos

            ordered = sorted(package_ids, key=lambda pid: package_totals.get(pid) or 0, reverse=True)
            for index, package_id in enumerate(ordered):
                if len(builder.dependents) >= self.entry_cap:
                    # An unreadable per-package count (None) may still hold data.
                    remaining = (package_totals.get(pid) for pid in ordered[index:])
                    if any(total is None or total > 0 for total in remaining):
                        builder.listing_incomplete(REASON_ENTRY_CAP)
                        logger.warning(
                            f"github-dependents entry cap reached, skipping remaining packages: "
                            f"repo={owner}/{repo} cap={self.entry_cap} skipped={len(ordered) - index}"
                        )
                    break

                if index > 0 and self.rate_limit > 0:
                    await asyncio.sleep(self.rate_limit)
                await self._walk_listing(
                    owner, repo, package_id, first_pages[package_id], package_totals.get(package_id), builder
                )

        return builder.build()

    def _parse_counts(self, html: str, owner: str, repo: str, builder: _SnapshotBuilder) -> tuple[int | None, int | None]:
        """Parse one page's header counts; both absent raises, one absent marks counts incomplete."""

        repos_total, packages_total = parse_dependent_counts(html)

        if repos_total is None and packages_total is None:
            logger.warning(f"github-dependents layout change suspected: repo={owner}/{repo} marker=counts")
            raise DependentsPageLayoutError(f"No dependent counts found for {owner}/{repo}")

        if repos_total is None or packages_total is None:
            builder.counts_incomplete(REASON_COUNTS_PARTIAL)
            logger.warning(
                f"github-dependents partial count markers: repo={owner}/{repo} "
                f"repos_total={repos_total} packages_total={packages_total} marker={REASON_COUNTS_PARTIAL}"
            )

        return repos_total, packages_total

    async def _walk_listing(
        self,
        owner: str,
        repo: str,
        package_id: str | None,
        first_page_html: str,
        listing_repos_total: int | None,
        builder: _SnapshotBuilder,
    ) -> None:
        """
        Walk one paginated listing, merging entries into the builder.

        Raises ``DependentsPageLayoutError`` when a page renders entry rows of
        which none parse — systematic drift of the per-entry structure. A page
        where only a subset parses keeps the parsed rows and marks the listing
        incomplete.
        """

        source_key = f"{owner}/{repo}".lower()
        cursor: str | None = None
        seen_cursors: set[str] = set()
        html = first_page_html

        for page_index in range(self.pages_cap):
            if page_index > 0:
                if self.rate_limit > 0:
                    await asyncio.sleep(self.rate_limit)

                html = await self._fetch_page(
                    _dependents_url(owner, repo, _REPOSITORY_TYPE, cursor=cursor, package_id=package_id), builder
                )

            page_entries = parse_dependent_entries(html)
            marker_count = _count_entry_markers(html)
            if marker_count and not page_entries:
                logger.warning(f"github-dependents layout change suspected: repo={owner}/{repo} marker=entries")
                raise DependentsPageLayoutError(f"{marker_count} dependent rows rendered but none parsed for {owner}/{repo}")

            if len(page_entries) < marker_count:
                # Some rows rendered but did not parse: partial drift.
                builder.listing_incomplete(REASON_ENTRIES_PARTIAL)
                logger.warning(
                    f"github-dependents unparsed entry rows: repo={owner}/{repo} "
                    f"rendered={marker_count} parsed={len(page_entries)} marker={REASON_ENTRIES_PARTIAL}"
                )

            if not page_entries:
                if page_index > 0:
                    # GitHub never paginates to an empty page, so completeness
                    # is unknown.
                    builder.listing_incomplete(REASON_EMPTY_PAGE)
                    logger.warning(f"github-dependents empty page mid-walk: repo={owner}/{repo} marker={REASON_EMPTY_PAGE}")
                elif listing_repos_total is None or listing_repos_total > 0:
                    # No rendered rows while the header is positive or
                    # unreadable: ambiguous — not proven complete. Only an
                    # explicit zero count makes an empty page a complete
                    # listing.
                    builder.listing_incomplete(REASON_NO_VISIBLE_ROWS)
                    logger.warning(
                        f"github-dependents positive count without visible rows: "
                        f"repo={owner}/{repo} marker={REASON_NO_VISIBLE_ROWS}"
                    )

                return

            consumed = 0
            for entry in page_entries:
                if f"{entry.owner}/{entry.repo}".lower() == source_key:
                    consumed += 1
                    continue

                if len(builder.dependents) >= self.entry_cap:
                    break

                builder.add_dependent(entry, package_id)
                consumed += 1

            cursor, next_link_present = parse_pagination(html)

            if len(builder.dependents) >= self.entry_cap:
                if consumed < len(page_entries) or cursor is not None or next_link_present:
                    builder.listing_incomplete(REASON_ENTRY_CAP)
                    logger.warning(
                        f"github-dependents listing truncated at entry cap: "
                        f"repo={owner}/{repo} cap={self.entry_cap} marker={REASON_ENTRY_CAP}"
                    )

                return

            if cursor is None:
                if next_link_present:
                    builder.listing_incomplete(REASON_NEXT_CURSOR)
                    logger.warning(
                        f"github-dependents Next link without readable cursor: repo={owner}/{repo} marker={REASON_NEXT_CURSOR}"
                    )

                return

            if cursor in seen_cursors:
                builder.listing_incomplete(REASON_CURSOR_CYCLE)
                logger.warning(
                    f"github-dependents pagination cycle detected: repo={owner}/{repo} marker={REASON_CURSOR_CYCLE}"
                )

                return

            seen_cursors.add(cursor)

            if page_index == self.pages_cap - 1:
                builder.listing_incomplete(REASON_PAGE_CAP)
                logger.warning(
                    f"github-dependents listing truncated at page cap: "
                    f"repo={owner}/{repo} cap={self.pages_cap} marker={REASON_PAGE_CAP}"
                )

                return

    # ------------------------------------------------------------------
    # ABC surface (snapshot views)
    # ------------------------------------------------------------------

    async def fetch_package(self, package_name: str) -> CrawledPackage:
        """Snapshot view: repo-shaped ``CrawledPackage`` with reported totals in metadata."""

        owner, repo = _split_owner_repo(package_name)
        snapshot = await self.collect_snapshot(package_name)

        metadata: dict[str, Any] = {
            "repos_total_reported": snapshot.repos_total_reported,
            "packages_total_reported": snapshot.packages_total_reported,
            "public_repos_observed": len(snapshot.dependents),
            "listing_complete": snapshot.listing_complete,
            "counts_complete": snapshot.counts_complete,
        }
        if snapshot.packages_scanned is not None:
            metadata["packages_scanned"] = snapshot.packages_scanned

        return CrawledPackage(
            canonical_id=f"pkg:github/{owner}/{repo}",
            display_name=repo,
            latest_version="",
            repo_url=f"{GITHUB_BASE_URL}/{owner}/{repo}",
            downloads_30d=None,
            metadata=metadata,
            dependencies=[],
            releases=[],
        )

    async def fetch_dependents(self, package_name: str) -> list[CrawledDependent]:
        """Snapshot view: unique dependents as the shared ``CrawledDependent`` contract."""

        snapshot = await self.collect_snapshot(package_name)

        return [
            CrawledDependent(
                canonical_id=f"pkg:github/{dep.owner}/{dep.repo}",
                display_name=dep.repo,
                repo_url=f"{GITHUB_BASE_URL}/{dep.owner}/{dep.repo}",
            )
            for dep in snapshot.dependents
        ]

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    async def _process_package(
        self,
        session: AsyncSession,
        package_name: str,
        result: CrawlResult,
    ) -> None:
        """
        Resolve the source, record a durable run, crawl, persist observations.

        Sequence: resolve the tracked source ``Repo`` (loud failure on zero or
        ambiguous matches, before any run row exists) — commit a ``running``
        run row so the attempt is durable before HTTP — collect the snapshot
        outside any transaction — apply it under a per-source advisory lock
        with a run-order guard — commit. This subclass owns that commit (the
        base class's later commit finds a clean session) because a commit
        exception must be classified against the durable run state, which only
        this layer can do: the run-status transition shares the application
        transaction, so the run row is the commit marker. All failures are
        reconciled through ``_reconcile_failed_run`` on a fresh session; a
        lost commit acknowledgement is detected there and the crawl counts as
        processed. Writes no ``DependsOn`` edges and no ``RepoVertex`` rows.
        """

        owner, repo = _split_owner_repo(package_name)
        source_repo_id = await self._resolve_source_repo_id(session, owner, repo)

        run = GithubDependentsCrawlRun(source_repo_id=source_repo_id)
        session.add(run)
        await session.commit()
        run_id = run.id

        try:
            snapshot = await self.collect_snapshot(package_name)
            await self._apply_snapshot(session, source_repo_id, run_id, snapshot)
            await session.commit()
        except Exception as exc:
            try:
                await session.rollback()
            except Exception:
                logger.warning(f"github-dependents rollback failed after crawl error: run={run_id}", exc_info=True)

            if await self._reconcile_failed_run(run_id, exc) == "applied":
                return

            raise

    async def _resolve_source_repo_id(self, session: AsyncSession, owner: str, repo: str) -> int:
        """
        Resolve the tracked source ``Repo`` by case-insensitive canonical identity.

        GitHub identity is case-insensitive; stored canonical ids preserve
        their original casing, so the lookup lowercases both sides. Zero
        matches raise ``SourceRepoNotFound``; more than one raises
        ``AmbiguousSourceRepo`` — the crawler never silently picks one.
        """

        wanted = f"pkg:github/{owner}/{repo}".lower()
        rows = (await session.execute(select(Repo.id).where(func.lower(Repo.canonical_id) == wanted))).scalars().all()

        if not rows:
            raise SourceRepoNotFound(f"No tracked source repo for {owner}/{repo}")
        if len(rows) > 1:
            logger.warning(f"github-dependents ambiguous source identity: repo={owner}/{repo} matches={len(rows)}")
            raise AmbiguousSourceRepo(f"{len(rows)} tracked repos match {owner}/{repo}")

        return rows[0]

    async def _finish_run(
        self,
        session: AsyncSession,
        run_id: int,
        *,
        status: GithubDependentsRunStatus,
        error_detail: str | None = None,
    ) -> None:
        """Terminal-status update for a run, committed in its own short transaction."""

        values: dict[str, Any] = {"status": status, "finished_at": func.now()}
        if error_detail is not None:
            values["error_detail"] = error_detail

        await session.execute(update(GithubDependentsCrawlRun).where(GithubDependentsCrawlRun.id == run_id).values(**values))
        await session.commit()

    async def _reconcile_failed_run(self, run_id: int, exc: Exception) -> Literal["failed", "applied", "unknown"]:
        """
        Classify a crawl failure against the durable run state.

        A commit exception is not proof the server rolled back: PostgreSQL can
        commit durably while the acknowledgement is lost. The conditional
        ``running -> failed`` transition below can never overwrite an applied
        terminal state, and under concurrency the UPDATE re-evaluates its
        predicate after any in-flight transaction on the row resolves. Runs on
        a genuinely new session because the session that saw the failure may
        itself be unusable.

        Returns ``"failed"`` (transitioned ``running -> failed``),
        ``"applied"`` (a durable ``complete``/``partial``/``superseded`` state
        exists — the snapshot landed and the crawl counts as processed), or
        ``"unknown"`` (reconciliation impossible or state unexpected; the run
        is left as found).
        """

        try:
            async with self.session_factory() as reconcile_session:
                update_result = await reconcile_session.execute(
                    update(GithubDependentsCrawlRun)
                    .where(
                        GithubDependentsCrawlRun.id == run_id,
                        GithubDependentsCrawlRun.status == GithubDependentsRunStatus.running,
                    )
                    .values(
                        status=GithubDependentsRunStatus.failed,
                        error_detail=str(exc)[:4096],
                        finished_at=func.now(),
                    )
                )
                rowcount = getattr(update_result, "rowcount", 0)
                if isinstance(rowcount, int) and rowcount == 1:
                    await reconcile_session.commit()

                    return "failed"

                await reconcile_session.rollback()
                status = await reconcile_session.scalar(
                    select(GithubDependentsCrawlRun.status).where(GithubDependentsCrawlRun.id == run_id)
                )
        except Exception:
            logger.warning(
                f"github-dependents failure reconciliation unavailable, run left as found: run={run_id} error={exc!r}",
                exc_info=True,
            )

            return "unknown"

        if status is not None and status in (
            GithubDependentsRunStatus.complete,
            GithubDependentsRunStatus.partial,
            GithubDependentsRunStatus.superseded,
        ):
            logger.warning(
                f"github-dependents commit acknowledgement lost but snapshot durably applied: "
                f"run={run_id} status={status.value} error={exc!r}"
            )

            return "applied"

        logger.warning(f"github-dependents unexpected run state during failure reconciliation: run={run_id} state={status!r}")

        return "unknown"

    async def _apply_snapshot(
        self,
        session: AsyncSession,
        source_repo_id: int,
        run_id: int,
        snapshot: DependentsSnapshot,
    ) -> bool:
        """
        Persist one snapshot atomically; returns whether it was applied.

        Serialization: a transaction-scoped advisory lock keyed on the source,
        so two writers for the same source serialize while different sources
        proceed concurrently. Ordering: run ids are assigned before HTTP, so
        under the lock, a snapshot whose run id is below the newest applied
        run for this source is stale — it is marked ``superseded`` and mutates
        nothing (an older snapshot must neither resurrect rows a newer
        complete run retired nor retire rows a newer partial run added).
        Retirement is gated on ``listing_complete`` and uses
        ``IS DISTINCT FROM`` because ``last_seen_run_id`` is nullable.
        """

        await session.execute(
            text("SELECT pg_advisory_xact_lock(hashtext(:ns), hashtext(:scope))"),
            {"ns": _ADVISORY_LOCK_NAMESPACE, "scope": snapshot.source.lower()},
        )

        newest_applied = await session.scalar(
            select(func.max(GithubDependentsCrawlRun.id)).where(
                GithubDependentsCrawlRun.source_repo_id == source_repo_id,
                GithubDependentsCrawlRun.status.in_([GithubDependentsRunStatus.complete, GithubDependentsRunStatus.partial]),
            )
        )
        if newest_applied is not None and newest_applied > run_id:
            logger.warning(
                f"github-dependents snapshot superseded: source={snapshot.source} run={run_id} newest_applied={newest_applied}"
            )
            await session.commit()  # release the advisory lock without mutating
            await self._finish_run(session, run_id, status=GithubDependentsRunStatus.superseded)

            return False

        resolved = await self._resolve_dependent_repo_ids(session, snapshot)

        if snapshot.dependents:
            insert_stmt = pg_insert(GithubDependentObservation)
            if snapshot.listing_complete:
                package_ids_update: Any = insert_stmt.excluded.package_ids
            else:
                # Partial listing: union newly observed package ids into the
                # prior set; never remove unseen memberships.
                package_ids_update = text(
                    "(SELECT jsonb_agg(DISTINCT value) FROM jsonb_array_elements_text("
                    "coalesce(github_dependent_observations.package_ids, '[]'::jsonb) || "
                    "coalesce(excluded.package_ids, '[]'::jsonb)) AS t(value))"
                )

            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=[
                    GithubDependentObservation.source_repo_id,
                    GithubDependentObservation.dependent_key,
                ],
                set_={
                    "observed_owner": insert_stmt.excluded.observed_owner,
                    "observed_repo_name": insert_stmt.excluded.observed_repo_name,
                    "dependent_repo_url": insert_stmt.excluded.dependent_repo_url,
                    # An unresolved (or ambiguous) match never clears an
                    # existing tracked-repo link.
                    "resolved_repo_id": func.coalesce(
                        insert_stmt.excluded.resolved_repo_id,
                        GithubDependentObservation.resolved_repo_id,
                    ),
                    "package_ids": package_ids_update,
                    "last_seen_run_id": run_id,
                    "last_seen_at": func.now(),
                    "retired_at": None,
                    "retired_by_run_id": None,
                },
            )
            # One executemany round trip in deterministic key order.
            await session.execute(
                upsert_stmt,
                [
                    {
                        "source_repo_id": source_repo_id,
                        "dependent_key": dep.key,
                        "dependent_canonical_id": f"pkg:github/{dep.key}",
                        "observed_owner": dep.owner,
                        "observed_repo_name": dep.repo,
                        "dependent_repo_url": f"{GITHUB_BASE_URL}/{dep.owner}/{dep.repo}",
                        "resolved_repo_id": resolved.get(dep.key),
                        "package_ids": list(dep.package_ids) or None,
                        "last_seen_run_id": run_id,
                        "retired_at": None,
                        "retired_by_run_id": None,
                    }
                    for dep in sorted(snapshot.dependents, key=lambda d: d.key)
                ],
            )

        if snapshot.listing_complete:
            await session.execute(
                update(GithubDependentObservation)
                .where(
                    GithubDependentObservation.source_repo_id == source_repo_id,
                    GithubDependentObservation.retired_at.is_(None),
                    GithubDependentObservation.last_seen_run_id.is_distinct_from(run_id),
                )
                .values(retired_at=func.now(), retired_by_run_id=run_id)
            )

        final_status = (
            GithubDependentsRunStatus.complete
            if snapshot.listing_complete and snapshot.counts_complete
            else GithubDependentsRunStatus.partial
        )
        await session.execute(
            update(GithubDependentsCrawlRun)
            .where(GithubDependentsCrawlRun.id == run_id)
            .values(
                status=final_status,
                listing_complete=snapshot.listing_complete,
                counts_complete=snapshot.counts_complete,
                listing_incomplete_reason=snapshot.listing_incomplete_reason,
                counts_incomplete_reason=snapshot.counts_incomplete_reason,
                repos_total_reported=snapshot.repos_total_reported,
                packages_total_reported=snapshot.packages_total_reported,
                public_repos_observed=len(snapshot.dependents),
                packages_scanned=snapshot.packages_scanned,
                pages_fetched=snapshot.pages_fetched,
                request_count=snapshot.request_count,
                parser_version=GITHUB_DEPENDENTS_PARSER_VERSION,
                app_version=VERSION,
                snapshot_fingerprint=snapshot.fingerprint,
                finished_at=func.now(),
            )
        )

        return True

    async def _resolve_dependent_repo_ids(self, session: AsyncSession, snapshot: DependentsSnapshot) -> dict[str, int]:
        """
        Map dependent keys to tracked ``Repo`` ids, unambiguous matches only.

        Untracked dependents get no vertex of any kind; the stored canonical
        id allows later resolution.
        """

        wanted = {f"pkg:github/{dep.key}": dep.key for dep in snapshot.dependents}
        if not wanted:
            return {}

        rows = (
            await session.execute(
                select(Repo.id, func.lower(Repo.canonical_id)).where(func.lower(Repo.canonical_id).in_(list(wanted.keys())))
            )
        ).all()

        by_key: dict[str, list[int]] = {}
        for repo_id, lowered in rows:
            by_key.setdefault(wanted[lowered], []).append(repo_id)

        for key, ids in by_key.items():
            if len(ids) > 1:
                logger.warning(
                    f"github-dependents ambiguous dependent identity, leaving unresolved: dependent={key} matches={len(ids)}"
                )

        return {key: ids[0] for key, ids in by_key.items() if len(ids) == 1}
