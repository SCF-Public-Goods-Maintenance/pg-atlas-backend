"""
Tests for the GitHub dependents crawler.

Unit tests use mocked HTTP responses against saved HTML fixtures — no network
or database required. Persistence (runs, observations, reconciliation,
concurrency, endpoint) is covered by ``tests/crawlers/test_db_integration.py``.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock

import httpx
import pytest

from pg_atlas.config import settings
from pg_atlas.crawlers.github_dependents import (
    REASON_COUNTS_PARTIAL,
    REASON_CURSOR_CYCLE,
    REASON_EMPTY_PAGE,
    REASON_ENTRIES_PARTIAL,
    REASON_ENTRY_CAP,
    REASON_NEXT_CURSOR,
    REASON_NO_VISIBLE_ROWS,
    REASON_PACKAGE_CAP,
    REASON_PAGE_CAP,
    DependentsPageLayoutError,
    GitHubDependentsCrawler,
    ParsedDependent,
    _split_owner_repo,
    find_next_cursor,
    parse_dependent_counts,
    parse_dependent_entries,
    parse_package_selector,
)
from pg_atlas.crawlers.github_dependents import github_dependents_scheduling_allowed as _scheduling_allowed
from tests.crawlers.conftest import GH_HEADERS as _HEADERS
from tests.crawlers.conftest import gh_entry_row as _entry_row
from tests.crawlers.conftest import gh_page, gh_selector_page
from tests.crawlers.conftest import gh_response as _html_response

# Synthetic invariant probes (the HTML fixtures are trimmed real GitHub captures).

# Non-zero header WITH the entry-boundary marker present, but the per-entry link
# class renamed so nothing parses: systematic within-entry layout drift → error.
_MARKER_PRESENT_NO_ENTRIES_HTML = (
    "<!DOCTYPE html><html><body>"
    f"{_HEADERS}"
    '<div class="Box-row d-flex flex-items-center" data-test-id="dg-repo-pkg-dependent">'
    ' <span> <a class="text-renamed" href="/some/repo">repo</a> </span> </div>'
    "</body></html>"
)


def _make_crawler(
    client: AsyncMock,
    *,
    pages_cap: int = 20,
    entry_cap: int = 500,
    packages_cap: int = 25,
) -> GitHubDependentsCrawler:
    """Create a crawler with a mocked HTTP client and dummy session factory."""
    session_factory = AsyncMock()
    return GitHubDependentsCrawler(
        client=client,
        session_factory=session_factory,
        rate_limit=0.0,
        max_retries=3,
        pages_cap=pages_cap,
        entry_cap=entry_cap,
        packages_cap=packages_cap,
    )


# ---------------------------------------------------------------------------
# Owner/repo key parsing
# ---------------------------------------------------------------------------


def test_split_owner_repo_valid() -> None:
    """A well-formed key splits into owner and repo."""
    assert _split_owner_repo("Soneso/stellar_flutter_sdk") == ("Soneso", "stellar_flutter_sdk")


@pytest.mark.parametrize("bad", ["", "no-slash", "owner/", "/repo", "a/b/c"])
def test_split_owner_repo_rejects_malformed(bad: str) -> None:
    """Malformed keys raise ValueError rather than silently mis-parsing."""
    with pytest.raises(ValueError):
        _split_owner_repo(bad)


# ---------------------------------------------------------------------------
# Count parsing
# ---------------------------------------------------------------------------


def test_parse_counts_from_page(github_dependents_page1_html: str) -> None:
    """Header totals are read from a real page."""
    assert parse_dependent_counts(github_dependents_page1_html) == (87, 6)


def test_parse_counts_empty_repo(github_dependents_empty_html: str) -> None:
    """A zero-dependents repo yields explicit zeros, not None."""
    assert parse_dependent_counts(github_dependents_empty_html) == (0, 0)


def test_parse_counts_comma_formatted() -> None:
    """Thousands separators in the counts are stripped."""
    html = (
        '<a href="/o/r/network/dependents?dependent_type=REPOSITORY"> <svg></svg> 1,234 Repositories </a>'
        '<a href="/o/r/network/dependents?dependent_type=PACKAGE"> <svg></svg> 12,345 Packages </a>'
    )
    assert parse_dependent_counts(html) == (1234, 12345)


def test_parse_counts_absent_markers(github_dependents_layout_changed_html: str) -> None:
    """A page missing both markers returns (None, None) — the layout signal."""
    assert parse_dependent_counts(github_dependents_layout_changed_html) == (None, None)


def test_parse_counts_singular_labels(github_dependents_multi_package_html: str) -> None:
    """GitHub renders '1 Package' (singular) for a count of one; both singular forms parse."""
    assert parse_dependent_counts(github_dependents_multi_package_html) == (0, 1)

    html = (
        '<a href="/o/r/network/dependents?dependent_type=REPOSITORY"> <svg></svg> 1 Repository </a>'
        '<a href="/o/r/network/dependents?dependent_type=PACKAGE"> <svg></svg> 3 Packages </a>'
    )
    assert parse_dependent_counts(html) == (1, 3)


def test_parse_counts_package_scoped_hrefs(github_dependents_pkg_scoped_html: str) -> None:
    """Header links on a package-scoped page carry package_id and still parse."""
    assert parse_dependent_counts(github_dependents_pkg_scoped_html) == (2867, 561)


def test_parse_counts_never_bind_beyond_anchor(github_dependents_page1_html: str) -> None:
    """A drifted in-anchor label must yield None, never a count found elsewhere in the document."""
    drifted = github_dependents_page1_html.replace("87 Repositories", "87 repos")
    drifted += '<div class="footer">1,234 Repositories</div>'

    repos_total, packages_total = parse_dependent_counts(drifted)
    assert repos_total is None
    assert packages_total == 6


def test_parse_counts_require_leading_digit() -> None:
    """A comma-only or fragmentary number yields None instead of an int('') crash."""
    html = (
        '<a href="/o/r/network/dependents?dependent_type=REPOSITORY"> <svg></svg> , Repositories </a>'
        '<a href="/o/r/network/dependents?dependent_type=PACKAGE"> <svg></svg> 6 Packages </a>'
    )
    assert parse_dependent_counts(html) == (None, 6)


# ---------------------------------------------------------------------------
# Package selector parsing
# ---------------------------------------------------------------------------


def test_parse_package_selector_multi(github_dependents_multi_package_html: str) -> None:
    """All selector package ids are extracted in menu order, URL-encoded verbatim."""
    assert parse_package_selector(github_dependents_multi_package_html) == [
        "UGFja2FnZS00MTEyMjY2Mzk2",
        "UGFja2FnZS00MzA4MTUwMTM1",
        "UGFja2FnZS01OTIxMDMwNg%3D%3D",
        "UGFja2FnZS01MDcyMzM3Mg%3D%3D",
    ]


def test_parse_package_selector_absent(github_dependents_page1_html: str) -> None:
    """A single-package repo renders no selector."""
    assert parse_package_selector(github_dependents_page1_html) == []


def test_parse_package_selector_any_param_position() -> None:
    """package_id parses regardless of its position among the query parameters."""
    html = (
        '<a class="select-menu-item" role="menuitemradio" href="/o/r/network/dependents?package_id=ABC&amp;x=1">a</a>'
        '<a href="/o/r/network/dependents?x=1&amp;package_id=DEF" class="select-menu-item">b</a>'
    )
    assert parse_package_selector(html) == ["ABC", "DEF"]


def test_parse_package_selector_ignores_lookalike_classes() -> None:
    """Only anchors with the exact select-menu-item class token match."""
    html = (
        '<a class="select-menu-item-text" href="/o/r/network/dependents?package_id=WRONG">x</a>'
        '<abbr class="select-menu-item" href="/o/r/network/dependents?package_id=NOTANCHOR">y</abbr>'
    )
    assert parse_package_selector(html) == []


# ---------------------------------------------------------------------------
# Entry parsing
# ---------------------------------------------------------------------------


def test_parse_entries_from_page(github_dependents_page1_html: str) -> None:
    """Owner and repo are extracted per entry, in listing order."""
    entries = parse_dependent_entries(github_dependents_page1_html)

    assert entries == [
        ParsedDependent(owner="HabitaNexus", repo="monorepo"),
        ParsedDependent(owner="jopmiddelkamp", repo="flutter_architecture"),
        ParsedDependent(owner="sanjaysamuels", repo="supply-blockchain"),
        ParsedDependent(owner="lucasmagnus", repo="tokenai"),
    ]


def test_parse_entries_terminal_page(github_dependents_page2_html: str) -> None:
    """The terminal page still parses its entries."""
    entries = parse_dependent_entries(github_dependents_page2_html)

    names = [(e.owner, e.repo) for e in entries]
    assert names == [
        ("drkreza", "resocoder-clean"),
        ("Soneso", "stellar_wallet_flutter_sdk"),
        ("Rooloo-Innovations", "DWaste-App"),
    ]


def test_parse_entries_empty_page(github_dependents_empty_html: str) -> None:
    """A zero-dependents page parses to an empty entry list."""
    assert parse_dependent_entries(github_dependents_empty_html) == []


def test_parse_entries_package_scoped_page(github_dependents_pkg_scoped_html: str) -> None:
    """Entries on a package-scoped page parse like any other listing page."""
    entries = parse_dependent_entries(github_dependents_pkg_scoped_html)

    assert [(e.owner, e.repo) for e in entries] == [
        ("tricklepaylabs", "tricklepay-backend"),
        ("tricklepaylabs", "tricklepay-frontend"),
    ]


# ---------------------------------------------------------------------------
# Cursor extraction
# ---------------------------------------------------------------------------


def test_find_next_cursor_present(github_dependents_page1_html: str) -> None:
    """The Next-link cursor is extracted from page 1."""
    assert find_next_cursor(github_dependents_page1_html) == "MzM3NzQwMDgyMDU"


def test_find_next_cursor_terminal_page(github_dependents_page2_html: str) -> None:
    """The terminal page (Next disabled) exposes no cursor."""
    assert find_next_cursor(github_dependents_page2_html) is None


def test_find_next_cursor_empty_page(github_dependents_empty_html: str) -> None:
    """A page without pagination exposes no cursor."""
    assert find_next_cursor(github_dependents_empty_html) is None


def test_find_next_cursor_package_scoped(github_dependents_pkg_scoped_html: str) -> None:
    """The cursor parses when the Next link also carries package_id."""
    assert find_next_cursor(github_dependents_pkg_scoped_html) == "NTEyMDgwOTg0Njc"


def test_find_next_cursor_tolerates_whitespace_around_next(github_dependents_page1_html: str) -> None:
    """Cosmetic whitespace around the Next label must not lose the cursor."""
    spaced = github_dependents_page1_html.replace(">Next<", "> Next <")
    assert find_next_cursor(spaced) == "MzM3NzQwMDgyMDU"


# ---------------------------------------------------------------------------
# collect_snapshot: single-package walks
# ---------------------------------------------------------------------------


async def test_snapshot_single_package_complete(
    github_dependents_page1_html: str,
    github_dependents_page2_html: str,
) -> None:
    """A full two-page walk yields a complete snapshot with all counters."""
    client = AsyncMock()
    client.get = AsyncMock(
        side_effect=[
            _html_response(github_dependents_page1_html),
            _html_response(github_dependents_page2_html),
        ]
    )
    crawler = _make_crawler(client)

    snapshot = await crawler.collect_snapshot("Soneso/stellar_flutter_sdk")

    assert client.get.call_count == 2
    assert "dependents_after=MzM3NzQwMDgyMDU" in client.get.call_args_list[1].args[0]
    assert len(snapshot.dependents) == 7
    assert snapshot.listing_complete and snapshot.counts_complete
    assert snapshot.listing_incomplete_reason is None
    assert snapshot.repos_total_reported == 87
    assert snapshot.packages_total_reported == 6
    assert snapshot.packages_scanned is None
    assert snapshot.pages_fetched == 2
    assert snapshot.request_count == 2
    assert len(snapshot.fingerprint) == 64

    keys = {d.key for d in snapshot.dependents}
    assert "habitanexus/monorepo" in keys
    monorepo = next(d for d in snapshot.dependents if d.key == "habitanexus/monorepo")
    assert (monorepo.owner, monorepo.repo) == ("HabitaNexus", "monorepo")
    assert monorepo.package_ids == ()


async def test_snapshot_empty_repo_is_complete(github_dependents_empty_html: str) -> None:
    """A zero-dependents repo yields a complete, empty snapshot after one request."""
    client = AsyncMock()
    client.get = AsyncMock(return_value=_html_response(github_dependents_empty_html))
    crawler = _make_crawler(client)

    snapshot = await crawler.collect_snapshot("Soneso/stellar-ios-mac-sdk")

    assert snapshot.dependents == ()
    assert snapshot.listing_complete and snapshot.counts_complete
    assert (snapshot.repos_total_reported, snapshot.packages_total_reported) == (0, 0)
    assert client.get.call_count == 1


async def test_snapshot_layout_change_raises(github_dependents_layout_changed_html: str) -> None:
    """Absent count markers raise DependentsPageLayoutError."""
    client = AsyncMock()
    client.get = AsyncMock(return_value=_html_response(github_dependents_layout_changed_html))
    crawler = _make_crawler(client)

    with pytest.raises(DependentsPageLayoutError):
        await crawler.collect_snapshot("owner/renamed-repo")


async def test_snapshot_partial_counts_and_no_visible_rows(caplog: pytest.LogCaptureFixture) -> None:
    """One absent count marker marks counts incomplete; positive count with zero rows marks the listing incomplete."""
    html = (
        "<!DOCTYPE html><html><body>"
        '<a class="btn-link selected" href="/owner/repo/network/dependents?dependent_type=REPOSITORY">'
        ' <svg class="octicon octicon-code-square"></svg> 87 Repositories </a>'
        "</body></html>"
    )
    client = AsyncMock()
    client.get = AsyncMock(return_value=_html_response(html))
    crawler = _make_crawler(client)

    with caplog.at_level(logging.WARNING):
        snapshot = await crawler.collect_snapshot("owner/repo")

    assert not snapshot.counts_complete
    assert snapshot.counts_incomplete_reason == REASON_COUNTS_PARTIAL
    assert not snapshot.listing_complete
    assert snapshot.listing_incomplete_reason == REASON_NO_VISIBLE_ROWS
    assert snapshot.repos_total_reported == 87
    assert snapshot.packages_total_reported is None
    assert any(REASON_NO_VISIBLE_ROWS in record.message for record in caplog.records)


async def test_snapshot_zero_count_without_rows_is_complete() -> None:
    """A zero header count with no rendered rows is a valid complete empty listing."""
    html = (
        "<!DOCTYPE html><html><body>"
        '<a class="btn-link selected" href="/owner/repo/network/dependents?dependent_type=REPOSITORY">'
        " <svg></svg> 0 Repositories </a>"
        '<a class="btn-link" href="/owner/repo/network/dependents?dependent_type=PACKAGE">'
        " <svg></svg> 0 Packages </a>"
        "</body></html>"
    )
    client = AsyncMock()
    client.get = AsyncMock(return_value=_html_response(html))
    crawler = _make_crawler(client)

    snapshot = await crawler.collect_snapshot("owner/repo")

    assert snapshot.dependents == ()
    assert snapshot.listing_complete and snapshot.counts_complete


async def test_snapshot_self_observation_skipped() -> None:
    """The source repo listed as its own dependent is skipped, case-insensitively."""
    page = gh_page(_entry_row("Owner", "Repo") + _entry_row("other", "app"))
    client = AsyncMock()
    client.get = AsyncMock(return_value=_html_response(page))
    crawler = _make_crawler(client)

    snapshot = await crawler.collect_snapshot("owner/repo")

    assert [d.key for d in snapshot.dependents] == ["other/app"]
    assert snapshot.listing_complete


async def test_snapshot_entry_cap(github_dependents_page1_html: str) -> None:
    """The entry cap stops collection and marks the listing incomplete."""
    client = AsyncMock()
    client.get = AsyncMock(return_value=_html_response(github_dependents_page1_html))
    crawler = _make_crawler(client, entry_cap=2)

    snapshot = await crawler.collect_snapshot("Soneso/stellar_flutter_sdk")

    assert len(snapshot.dependents) == 2
    assert snapshot.listing_incomplete_reason == REASON_ENTRY_CAP
    assert client.get.call_count == 1


async def test_snapshot_page_cap(github_dependents_page1_html: str) -> None:
    """The page cap stops paging while a Next cursor remains."""
    client = AsyncMock()
    client.get = AsyncMock(return_value=_html_response(github_dependents_page1_html))
    crawler = _make_crawler(client, pages_cap=1)

    snapshot = await crawler.collect_snapshot("Soneso/stellar_flutter_sdk")

    assert len(snapshot.dependents) == 4
    assert snapshot.listing_incomplete_reason == REASON_PAGE_CAP
    assert client.get.call_count == 1


async def test_snapshot_exact_fit_cap_is_complete(github_dependents_page2_html: str) -> None:
    """A listing that ends exactly at the entry cap on a terminal page stays complete."""
    client = AsyncMock()
    client.get = AsyncMock(return_value=_html_response(github_dependents_page2_html))
    crawler = _make_crawler(client, entry_cap=3)

    snapshot = await crawler.collect_snapshot("Soneso/stellar_flutter_sdk")

    assert len(snapshot.dependents) == 3
    assert snapshot.listing_complete


async def test_snapshot_empty_page_mid_walk(
    github_dependents_page1_html: str,
    github_dependents_empty_html: str,
) -> None:
    """An empty page after a cursor was followed marks the listing incomplete."""
    client = AsyncMock()
    client.get = AsyncMock(
        side_effect=[
            _html_response(github_dependents_page1_html),
            _html_response(github_dependents_empty_html),
        ]
    )
    crawler = _make_crawler(client)

    snapshot = await crawler.collect_snapshot("Soneso/stellar_flutter_sdk")

    assert len(snapshot.dependents) == 4
    assert snapshot.listing_incomplete_reason == REASON_EMPTY_PAGE


async def test_snapshot_next_anchor_without_cursor() -> None:
    """A Next anchor whose cursor did not parse marks the listing incomplete."""
    page = (
        "<!DOCTYPE html><html><body>"
        f"{_HEADERS}"
        f"{_entry_row('ownerA', 'repoA')}"
        '<div class="paginate-container"><div data-test-selector="pagination">'
        '<a href="/o/r/network/dependents?dependent_type=REPOSITORY&amp;after=DRIFTED">Next</a>'
        "</div></div>"
        "</body></html>"
    )
    client = AsyncMock()
    client.get = AsyncMock(return_value=_html_response(page))
    crawler = _make_crawler(client)

    snapshot = await crawler.collect_snapshot("owner/repo")

    assert len(snapshot.dependents) == 1
    assert snapshot.listing_incomplete_reason == REASON_NEXT_CURSOR


async def test_snapshot_renamed_pagination_container(github_dependents_page1_html: str) -> None:
    """Container drift with a Next anchor present must not claim completeness."""
    page = github_dependents_page1_html.replace('data-test-selector="pagination"', 'data-test-selector="paging"')
    client = AsyncMock()
    client.get = AsyncMock(return_value=_html_response(page))
    crawler = _make_crawler(client)

    snapshot = await crawler.collect_snapshot("Soneso/stellar_flutter_sdk")

    assert len(snapshot.dependents) == 4
    assert snapshot.listing_incomplete_reason == REASON_NEXT_CURSOR
    assert client.get.call_count == 1


async def test_snapshot_cursor_cycle() -> None:
    """A repeated pagination cursor stops the walk and marks the listing incomplete."""
    page_a = gh_page(_entry_row("ownerA", "repoA"), cursor="CUR1")
    page_b = gh_page(_entry_row("ownerB", "repoB"), cursor="CUR1")
    client = AsyncMock()
    client.get = AsyncMock(side_effect=[_html_response(page_a), _html_response(page_b)])
    crawler = _make_crawler(client)

    snapshot = await crawler.collect_snapshot("owner/repo")

    assert client.get.call_count == 2
    assert snapshot.listing_incomplete_reason == REASON_CURSOR_CYCLE
    assert {d.key for d in snapshot.dependents} == {"ownera/repoa", "ownerb/repob"}


async def test_snapshot_partial_row_drift_keeps_parsed(caplog: pytest.LogCaptureFixture) -> None:
    """A page where only a subset of rendered rows parses keeps the subset and marks incompleteness."""
    page = (
        "<!DOCTYPE html><html><body>"
        f"{_HEADERS}"
        f"{_entry_row('ownerA', 'repoA')}"
        '<div class="Box-row d-flex flex-items-center" data-test-id="dg-repo-pkg-dependent">'
        ' <span> <a class="text-renamed" href="/ownerB/repoB">repoB</a> </span> </div>'
        "</body></html>"
    )
    client = AsyncMock()
    client.get = AsyncMock(return_value=_html_response(page))
    crawler = _make_crawler(client)

    with caplog.at_level(logging.WARNING):
        snapshot = await crawler.collect_snapshot("owner/repo")

    assert [d.key for d in snapshot.dependents] == ["ownera/repoa"]
    assert snapshot.listing_incomplete_reason == REASON_ENTRIES_PARTIAL
    assert any(REASON_ENTRIES_PARTIAL in record.message for record in caplog.records)


async def test_snapshot_unparseable_entry_rows_raise() -> None:
    """A page whose rendered entry rows do not all parse raises the layout error."""
    client = AsyncMock()
    client.get = AsyncMock(return_value=_html_response(_MARKER_PRESENT_NO_ENTRIES_HTML))
    crawler = _make_crawler(client)

    with pytest.raises(DependentsPageLayoutError):
        await crawler.collect_snapshot("owner/repo")


# ---------------------------------------------------------------------------
# collect_snapshot: multi-package walks
# ---------------------------------------------------------------------------


async def test_snapshot_multi_package_sums_and_unions(
    github_dependents_multi_package_html: str,
    github_dependents_pkg_scoped_html: str,
    github_dependents_page2_html: str,
) -> None:
    """A selector repo fetches each package page, sums totals, and unions dependents."""
    client = AsyncMock()
    client.get = AsyncMock(
        side_effect=[
            _html_response(github_dependents_multi_package_html),
            _html_response(github_dependents_pkg_scoped_html),
            _html_response(github_dependents_pkg_scoped_html),
            _html_response(github_dependents_pkg_scoped_html),
            _html_response(github_dependents_pkg_scoped_html),
            _html_response(github_dependents_page2_html),
            _html_response(github_dependents_page2_html),
            _html_response(github_dependents_page2_html),
            _html_response(github_dependents_page2_html),
        ]
    )
    crawler = _make_crawler(client)

    snapshot = await crawler.collect_snapshot("stellar/js-stellar-sdk")

    assert client.get.call_count == 9
    package_urls = [call.args[0] for call in client.get.call_args_list[1:5]]
    assert [url.split("package_id=")[1] for url in package_urls] == [
        "UGFja2FnZS00MTEyMjY2Mzk2",
        "UGFja2FnZS00MzA4MTUwMTM1",
        "UGFja2FnZS01OTIxMDMwNg%3D%3D",
        "UGFja2FnZS01MDcyMzM3Mg%3D%3D",
    ]
    # Walk-phase page-2 fetches keep cursor AND package scope.
    walk_url = client.get.call_args_list[5].args[0]
    assert "dependents_after=NTEyMDgwOTg0Njc" in walk_url
    assert "package_id=" in walk_url

    assert snapshot.repos_total_reported == 2867 * 4
    assert snapshot.packages_total_reported == 561 * 4
    assert snapshot.packages_scanned == 4
    assert len(snapshot.dependents) == 5
    assert snapshot.listing_complete

    # Every package's first page listed the same two dependents: memberships union.
    backend = next(d for d in snapshot.dependents if d.key == "tricklepaylabs/tricklepay-backend")
    assert len(backend.package_ids) == 4


async def test_snapshot_multi_package_walks_largest_first(
    github_dependents_multi_package_html: str,
    github_dependents_pkg_scoped_html: str,
    github_dependents_page2_html: str,
    github_dependents_empty_html: str,
) -> None:
    """The entry budget is spent on the largest scanned package first, not menu order."""
    client = AsyncMock()
    client.get = AsyncMock(
        side_effect=[
            _html_response(github_dependents_multi_package_html),
            _html_response(github_dependents_page2_html),  # menu pkg 1: total 87
            _html_response(github_dependents_pkg_scoped_html),  # menu pkg 2: total 2,867
            _html_response(github_dependents_empty_html),  # menu pkg 3: total 0
            _html_response(github_dependents_empty_html),  # menu pkg 4: total 0
            _html_response(github_dependents_page2_html),  # page 2 of the largest package
        ]
    )
    crawler = _make_crawler(client)

    snapshot = await crawler.collect_snapshot("stellar/js-stellar-sdk")

    assert client.get.call_count == 6
    walk_url = client.get.call_args_list[5].args[0]
    assert "package_id=UGFja2FnZS00MzA4MTUwMTM1" in walk_url
    assert "dependents_after=NTEyMDgwOTg0Njc" in walk_url
    assert len(snapshot.dependents) == 5


async def test_snapshot_packages_cap(
    github_dependents_multi_package_html: str,
    github_dependents_pkg_scoped_html: str,
    github_dependents_page2_html: str,
) -> None:
    """A capped selector bounds the fetches and marks the listing incomplete."""
    client = AsyncMock()
    client.get = AsyncMock(
        side_effect=[
            _html_response(github_dependents_multi_package_html),
            _html_response(github_dependents_pkg_scoped_html),
            _html_response(github_dependents_pkg_scoped_html),
            _html_response(github_dependents_page2_html),
            _html_response(github_dependents_page2_html),
        ]
    )
    crawler = _make_crawler(client, packages_cap=2)

    snapshot = await crawler.collect_snapshot("stellar/js-stellar-sdk")

    assert snapshot.packages_scanned == 2
    assert snapshot.listing_incomplete_reason == REASON_PACKAGE_CAP


async def test_snapshot_entry_cap_skips_remaining_packages(
    github_dependents_multi_package_html: str,
    github_dependents_pkg_scoped_html: str,
    github_dependents_page2_html: str,
    github_dependents_empty_html: str,
) -> None:
    """Once the entry cap fills, remaining packages are skipped and incompleteness recorded."""
    client = AsyncMock()
    client.get = AsyncMock(
        side_effect=[
            _html_response(github_dependents_multi_package_html),
            _html_response(github_dependents_page2_html),
            _html_response(github_dependents_pkg_scoped_html),
            _html_response(github_dependents_empty_html),
            _html_response(github_dependents_empty_html),
        ]
    )
    crawler = _make_crawler(client, entry_cap=2)

    snapshot = await crawler.collect_snapshot("stellar/js-stellar-sdk")

    # No walk fetch happened: the largest package's first page filled the cap.
    assert client.get.call_count == 5
    assert len(snapshot.dependents) == 2
    assert snapshot.listing_incomplete_reason == REASON_ENTRY_CAP


async def test_snapshot_selector_button_without_menu_raises(caplog: pytest.LogCaptureFixture) -> None:
    """A rendered selector button whose menu anchors no longer parse is a layout error."""
    html = (
        "<!DOCTYPE html><html><body>"
        '<details class="details-reset select-menu-container">'
        '<summary class="btn select-menu-button" aria-haspopup="menu"> <i>Package:</i> <span>foo</span> </summary>'
        "</details>"
        f"{_HEADERS}"
        "</body></html>"
    )
    client = AsyncMock()
    client.get = AsyncMock(return_value=_html_response(html))
    crawler = _make_crawler(client)

    with caplog.at_level(logging.WARNING), pytest.raises(DependentsPageLayoutError):
        await crawler.collect_snapshot("owner/repo")

    assert any("marker=selector" in record.message for record in caplog.records)


# ---------------------------------------------------------------------------
# HTTP: 403 rate-limit handling
# ---------------------------------------------------------------------------


async def test_403_with_retry_after_is_retried(github_dependents_empty_html: str) -> None:
    """A 403 carrying Retry-After is retried after a bounded wait."""
    client = AsyncMock()
    client.get = AsyncMock(
        side_effect=[
            _html_response("rate limited", status_code=403, headers={"Retry-After": "0"}),
            _html_response(github_dependents_empty_html),
        ]
    )
    crawler = _make_crawler(client)

    snapshot = await crawler.collect_snapshot("owner/repo")

    assert client.get.call_count == 2
    assert snapshot.request_count == 2
    assert snapshot.listing_complete


async def test_403_without_rate_limit_evidence_fails() -> None:
    """A 403 without rate-limit headers stays a classified failure."""
    client = AsyncMock()
    client.get = AsyncMock(return_value=_html_response("forbidden", status_code=403))
    crawler = _make_crawler(client)

    with pytest.raises(httpx.HTTPStatusError):
        await crawler.collect_snapshot("owner/repo")


# ---------------------------------------------------------------------------
# ABC snapshot views and construction guards
# ---------------------------------------------------------------------------


async def test_fetch_package_view(github_dependents_page1_html: str, github_dependents_page2_html: str) -> None:
    """fetch_package exposes the snapshot as a repo-shaped CrawledPackage."""
    client = AsyncMock()
    client.get = AsyncMock(
        side_effect=[
            _html_response(github_dependents_page1_html),
            _html_response(github_dependents_page2_html),
        ]
    )
    crawler = _make_crawler(client)

    pkg = await crawler.fetch_package("Soneso/stellar_flutter_sdk")

    assert pkg.canonical_id == "pkg:github/Soneso/stellar_flutter_sdk"
    assert pkg.repo_url == "https://github.com/Soneso/stellar_flutter_sdk"
    assert pkg.latest_version == ""
    assert pkg.dependencies == [] and pkg.releases == []
    assert pkg.metadata["repos_total_reported"] == 87
    assert pkg.metadata["packages_total_reported"] == 6
    assert pkg.metadata["public_repos_observed"] == 7
    assert pkg.metadata["listing_complete"] is True


async def test_fetch_dependents_view(github_dependents_page2_html: str) -> None:
    """fetch_dependents exposes the snapshot as CrawledDependent entries."""
    client = AsyncMock()
    client.get = AsyncMock(return_value=_html_response(github_dependents_page2_html))
    crawler = _make_crawler(client)

    dependents = await crawler.fetch_dependents("owner/repo")

    assert {d.canonical_id for d in dependents} == {
        "pkg:github/drkreza/resocoder-clean",
        "pkg:github/Soneso/stellar_wallet_flutter_sdk",
        "pkg:github/Rooloo-Innovations/DWaste-App",
    }
    assert all(d.repo_url and d.repo_url.startswith("https://github.com/") for d in dependents)


def test_crawler_rejects_non_positive_caps() -> None:
    """Every cap must be positive — a misconfigured cap fails fast, not silently."""
    client = AsyncMock()
    for cap_kwargs in ({"pages_cap": 0}, {"entry_cap": 0}, {"packages_cap": -1}):
        with pytest.raises(ValueError):
            _make_crawler(client, **cap_kwargs)


# ---------------------------------------------------------------------------
# Scheduling gate (fail-closed allowlist)
# ---------------------------------------------------------------------------


def test_scheduling_disabled_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    """ENABLED=false schedules nothing regardless of the allowlist."""
    monkeypatch.setattr(settings, "GITHUB_DEPENDENTS_ENABLED", False)
    monkeypatch.setattr(settings, "GITHUB_DEPENDENTS_ALLOWLIST", "*")
    assert not _scheduling_allowed("Soneso", "stellar_flutter_sdk")


def test_scheduling_empty_allowlist_schedules_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    """ENABLED=true with an empty allowlist fails closed."""
    monkeypatch.setattr(settings, "GITHUB_DEPENDENTS_ENABLED", True)
    monkeypatch.setattr(settings, "GITHUB_DEPENDENTS_ALLOWLIST", "")
    assert not _scheduling_allowed("Soneso", "stellar_flutter_sdk")


def test_scheduling_allowlist_match_case_insensitive(monkeypatch: pytest.MonkeyPatch) -> None:
    """A listed repository schedules, matched case-insensitively; others do not."""
    monkeypatch.setattr(settings, "GITHUB_DEPENDENTS_ENABLED", True)
    monkeypatch.setattr(settings, "GITHUB_DEPENDENTS_ALLOWLIST", "soneso/STELLAR_flutter_sdk, Other/repo")
    assert _scheduling_allowed("Soneso", "stellar_flutter_sdk")
    assert _scheduling_allowed("other", "REPO")
    assert not _scheduling_allowed("Soneso", "stellar-php-sdk")


def test_scheduling_star_schedules_all(monkeypatch: pytest.MonkeyPatch) -> None:
    """The explicit value '*' schedules all repositories."""
    monkeypatch.setattr(settings, "GITHUB_DEPENDENTS_ENABLED", True)
    monkeypatch.setattr(settings, "GITHUB_DEPENDENTS_ALLOWLIST", "*")
    assert _scheduling_allowed("any", "repo")


def test_scheduling_rejects_malformed_entries(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    """Malformed allowlist entries are rejected with a warning and never match."""
    monkeypatch.setattr(settings, "GITHUB_DEPENDENTS_ENABLED", True)
    monkeypatch.setattr(settings, "GITHUB_DEPENDENTS_ALLOWLIST", "no-slash, /lead, trail/, good/entry")
    with caplog.at_level(logging.WARNING):
        assert _scheduling_allowed("good", "entry")
        assert not _scheduling_allowed("no-slash", "x")
    assert sum("allowlist entry rejected" in r.message for r in caplog.records) >= 3


# ---------------------------------------------------------------------------
# Reviewer-round regression tests: None-count semantics, 403 budget, fingerprint
# ---------------------------------------------------------------------------


async def test_snapshot_unreadable_repo_count_with_no_rows_is_incomplete() -> None:
    """An unreadable Repositories count with zero rendered rows must not read as a complete empty listing."""
    html = (
        "<!DOCTYPE html><html><body>"
        '<a class="btn-link" href="/owner/repo/network/dependents?dependent_type=PACKAGE">'
        " <svg></svg> 6 Packages </a>"
        "</body></html>"
    )
    client = AsyncMock()
    client.get = AsyncMock(return_value=_html_response(html))
    crawler = _make_crawler(client)

    snapshot = await crawler.collect_snapshot("owner/repo")

    assert snapshot.dependents == ()
    assert not snapshot.listing_complete
    assert snapshot.listing_incomplete_reason == REASON_NO_VISIBLE_ROWS
    assert not snapshot.counts_complete


async def test_snapshot_unreadable_package_total_blocks_entry_cap_completeness() -> None:
    """A package whose header count failed to parse may still hold data: skipping it marks the listing incomplete."""
    selector = gh_selector_page(["PKGA", "PKGB"])
    page_a = gh_page("".join(_entry_row(f"o{i}", f"r{i}") for i in range(3)))
    page_b_headers = '<a class="btn-link" href="/o/r/network/dependents?dependent_type=PACKAGE"> <svg></svg> 6 Packages </a>'
    page_b = gh_page("", headers=page_b_headers)
    client = AsyncMock()
    client.get = AsyncMock(side_effect=[_html_response(selector), _html_response(page_a), _html_response(page_b)])
    crawler = _make_crawler(client, entry_cap=3)

    snapshot = await crawler.collect_snapshot("owner/repo")

    # Package A's three entries fill the cap exactly on a terminal page; B's
    # unreadable count must still record incompleteness.
    assert len(snapshot.dependents) == 3
    assert snapshot.listing_incomplete_reason == REASON_ENTRY_CAP
    assert not snapshot.counts_complete


async def test_snapshot_packages_cap_marks_counts_incomplete(
    github_dependents_multi_package_html: str,
    github_dependents_pkg_scoped_html: str,
    github_dependents_page2_html: str,
) -> None:
    """A capped selector also marks the counts axis: the sums omit the dropped packages."""
    client = AsyncMock()
    client.get = AsyncMock(
        side_effect=[
            _html_response(github_dependents_multi_package_html),
            _html_response(github_dependents_pkg_scoped_html),
            _html_response(github_dependents_pkg_scoped_html),
            _html_response(github_dependents_page2_html),
            _html_response(github_dependents_page2_html),
        ]
    )
    crawler = _make_crawler(client, packages_cap=2)

    snapshot = await crawler.collect_snapshot("stellar/js-stellar-sdk")

    assert snapshot.listing_incomplete_reason == REASON_PACKAGE_CAP
    assert not snapshot.counts_complete
    assert snapshot.counts_incomplete_reason == REASON_PACKAGE_CAP


async def test_request_count_includes_shared_layer_retries(github_dependents_empty_html: str) -> None:
    """request_count reflects real HTTP attempts, including 429 retries inside the shared layer."""
    client = AsyncMock()
    client.get = AsyncMock(
        side_effect=[
            _html_response("busy", status_code=429, headers={"Retry-After": "0"}),
            _html_response(github_dependents_empty_html),
        ]
    )
    crawler = _make_crawler(client)

    snapshot = await crawler.collect_snapshot("owner/repo")

    assert client.get.call_count == 2
    assert snapshot.request_count == 2
    assert snapshot.pages_fetched == 1


async def test_403_reset_beyond_budget_fails_fast() -> None:
    """A 403 whose reset lies beyond the wait budget fails immediately instead of sleeping the cap."""
    client = AsyncMock()
    client.get = AsyncMock(return_value=_html_response("rate limited", status_code=403, headers={"Retry-After": "3600"}))
    crawler = _make_crawler(client)

    with pytest.raises(httpx.HTTPStatusError):
        await crawler.collect_snapshot("owner/repo")

    # One attempt only: no futile capped sleeps.
    assert client.get.call_count == 1


async def test_403_with_ratelimit_reset_headers_is_retried(github_dependents_empty_html: str) -> None:
    """The x-ratelimit-remaining/reset header pair is honored for bounded retries."""
    import time

    client = AsyncMock()
    client.get = AsyncMock(
        side_effect=[
            _html_response(
                "rate limited",
                status_code=403,
                headers={"x-ratelimit-remaining": "0", "x-ratelimit-reset": str(int(time.time()))},
            ),
            _html_response(github_dependents_empty_html),
        ]
    )
    crawler = _make_crawler(client)

    snapshot = await crawler.collect_snapshot("owner/repo")

    assert client.get.call_count == 2
    assert snapshot.listing_complete


async def test_fingerprint_is_order_and_url_sensitive(
    github_dependents_page1_html: str,
    github_dependents_page2_html: str,
) -> None:
    """Identical crawls fingerprint identically; page order and differing bytes change it."""

    async def crawl(pages: list[str]) -> str:
        client = AsyncMock()
        client.get = AsyncMock(side_effect=[_html_response(p) for p in pages])
        return (await _make_crawler(client).collect_snapshot("Soneso/stellar_flutter_sdk")).fingerprint

    first = await crawl([github_dependents_page1_html, github_dependents_page2_html])
    second = await crawl([github_dependents_page1_html, github_dependents_page2_html])
    assert first == second

    changed = await crawl([github_dependents_page1_html.replace("monorepo", "monorepoX"), github_dependents_page2_html])
    assert changed != first
