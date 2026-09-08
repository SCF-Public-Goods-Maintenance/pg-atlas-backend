"""
Shared pytest fixtures for registry crawler tests.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest

FIXTURES = Path(__file__).parent / "data_fixtures"


def _load_fixture(name: str) -> dict[str, Any]:
    """Load a JSON fixture file by name."""
    return json.loads((FIXTURES / name).read_text())


def _load_html_fixture(name: str) -> str:
    """Load an HTML fixture file by name."""
    return (FIXTURES / name).read_text()


@pytest.fixture
def pubdev_package_data() -> dict[str, Any]:
    """pub.dev package API response fixture."""
    return _load_fixture("pubdev_package.json")


@pytest.fixture
def pubdev_metrics_data() -> dict[str, Any]:
    """pub.dev metrics API response fixture."""
    return _load_fixture("pubdev_metrics.json")


@pytest.fixture
def pubdev_search_data() -> dict[str, Any]:
    """pub.dev search API response fixture."""
    return _load_fixture("pubdev_search.json")


@pytest.fixture
def pubdev_search_empty_data() -> dict[str, Any]:
    """pub.dev empty search API response fixture."""
    return _load_fixture("pubdev_search_empty.json")


@pytest.fixture
def pubdev_package_minimal_data() -> dict[str, Any]:
    """pub.dev minimal package (no homepage, no deps) response fixture."""
    return _load_fixture("pubdev_package_minimal.json")


@pytest.fixture
def packagist_package_data() -> dict[str, Any]:
    """Packagist package API response fixture."""
    return _load_fixture("packagist_package.json")


@pytest.fixture
def packagist_downloads_data() -> dict[str, Any]:
    """Packagist downloads API response fixture."""
    return _load_fixture("packagist_downloads.json")


@pytest.fixture
def packagist_dependents_data() -> dict[str, Any]:
    """Packagist dependents API response fixture."""
    return _load_fixture("packagist_dependents.json")


@pytest.fixture
def packagist_package_dev_only_data() -> dict[str, Any]:
    """Packagist package with only dev branches fixture."""
    return _load_fixture("packagist_package_dev_only.json")


@pytest.fixture
def packagist_dependents_empty_data() -> dict[str, Any]:
    """Packagist empty dependents API response fixture."""
    return _load_fixture("packagist_dependents_empty.json")


@pytest.fixture
def npm_package_data() -> dict[str, Any]:
    """npm registry metadata fixture."""
    return _load_fixture("npm_package.json")


@pytest.fixture
def npm_downloads_data() -> dict[str, Any]:
    """npm downloads API response fixture."""
    return _load_fixture("npm_downloads.json")


@pytest.fixture
def crates_package_data() -> dict[str, Any]:
    """crates.io crate metadata fixture."""
    return _load_fixture("crates_package.json")


@pytest.fixture
def crates_dependencies_data() -> dict[str, Any]:
    """crates.io version dependency fixture."""
    return _load_fixture("crates_dependencies.json")


@pytest.fixture
def crates_reverse_dependencies_data() -> dict[str, Any]:
    """crates.io reverse dependencies fixture."""
    return _load_fixture("crates_reverse_dependencies.json")


@pytest.fixture
def pypi_package_data() -> dict[str, Any]:
    """PyPI project JSON fixture."""
    return _load_fixture("pypi_package.json")


@pytest.fixture
def pypi_stats_recent_data() -> dict[str, Any]:
    """PyPIStats recent-downloads JSON fixture."""
    return _load_fixture("pypi_stats_recent.json")


@pytest.fixture
def github_dependents_page1_html() -> str:
    """GitHub dependents REPOSITORY page 1 (counts header, entries, Next cursor)."""
    return _load_html_fixture("github_dependents_page1.html")


@pytest.fixture
def github_dependents_page2_html() -> str:
    """GitHub dependents REPOSITORY terminal page (entries, no Next cursor)."""
    return _load_html_fixture("github_dependents_page2.html")


@pytest.fixture
def github_dependents_empty_html() -> str:
    """GitHub dependents page for a repository with zero dependents."""
    return _load_html_fixture("github_dependents_empty.html")


@pytest.fixture
def github_dependents_layout_changed_html() -> str:
    """GitHub dependents page whose count markers are absent (layout change)."""
    return _load_html_fixture("github_dependents_layout_changed.html")


@pytest.fixture
def github_dependents_multi_package_html() -> str:
    """GitHub dependents page of a multi-package repo (selector, singular '1 Package', no rows)."""
    return _load_html_fixture("github_dependents_multi_package.html")


@pytest.fixture
def github_dependents_pkg_scoped_html() -> str:
    """GitHub dependents page scoped by package_id (selector block, entries, package_id header and Next link)."""
    return _load_html_fixture("github_dependents_pkg_scoped.html")


@pytest.fixture
def mock_http_client() -> AsyncMock:
    """Mock httpx.AsyncClient for unit tests."""
    return AsyncMock(spec=httpx.AsyncClient)


# ---------------------------------------------------------------------------
# Shared GitHub dependents page builders (single source for both test files)
# ---------------------------------------------------------------------------


def gh_headers(repos_total: int, packages_total: int) -> str:
    """Build the two count-header anchors with explicit totals."""
    return (
        '<a class="btn-link selected" href="/owner/repo/network/dependents?dependent_type=REPOSITORY">'
        f' <svg class="octicon octicon-code-square"></svg> {repos_total} Repositories </a>'
        '<a class="btn-link" href="/owner/repo/network/dependents?dependent_type=PACKAGE">'
        f' <svg class="octicon octicon-package"></svg> {packages_total} Packages </a>'
    )


GH_HEADERS = gh_headers(87, 6)


def gh_entry_row(owner: str, repo: str) -> str:
    """Build one real-shaped dependent entry row."""
    return (
        '<div class="Box-row d-flex flex-items-center" data-test-id="dg-repo-pkg-dependent">'
        f' <span> <a class="text-bold" href="/{owner}/{repo}">{repo}</a> </span> </div>'
    )


def gh_page(rows: str, *, cursor: str | None = None, headers: str = GH_HEADERS) -> str:
    """Build a page with a count header, entry rows, and optional Next cursor."""
    pagination = ""
    if cursor is not None:
        pagination = (
            '<div class="paginate-container"><div data-test-selector="pagination">'
            f'<a href="/o/r/network/dependents?dependent_type=REPOSITORY&dependents_after={cursor}">Next</a>'
            "</div></div>"
        )
    return f"<!DOCTYPE html><html><body>{headers}{rows}{pagination}</body></html>"


def gh_selector_page(package_ids: list[str]) -> str:
    """Build a multi-package base page: selector button, menu anchors, header counts."""
    menu = "".join(
        f'<a href="/o/r/network/dependents?package_id={pid}" class="select-menu-item" role="menuitemradio">p{i}</a>'
        for i, pid in enumerate(package_ids)
    )
    return (
        "<!DOCTYPE html><html><body>"
        '<details class="details-reset select-menu-container">'
        '<summary class="btn select-menu-button"> <i>Package:</i> <span>default</span> </summary>'
        f"<details-menu>{menu}</details-menu></details>"
        f"{GH_HEADERS}"
        "</body></html>"
    )


def gh_response(text_body: str, status_code: int = 200, headers: dict[str, str] | None = None) -> httpx.Response:
    """Build a mock HTML httpx.Response."""
    return httpx.Response(
        status_code=status_code,
        text=text_body,
        headers=headers,
        request=httpx.Request("GET", "https://github.com"),
    )
