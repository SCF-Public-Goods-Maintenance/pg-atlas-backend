"""
PyPI registry crawler for PG Atlas.

Fetches project metadata and releases from the official PyPI JSON API and uses
the ``pypistats`` client for mirror-inclusive download totals.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import logging
import re
from typing import Any

import httpx
import pypistats  # pyright: ignore[reportMissingTypeStubs]
from packaging.requirements import InvalidRequirement, Requirement

from pg_atlas.crawlers.base import (
    CrawledDependency,
    CrawledDependent,
    CrawledPackage,
    ExhaustedRetries,
    RegistryCrawler,
    as_str_key_dict,
)
from pg_atlas.db_models.release import Release, preferred_latest_version, sorted_releases_desc

logger = logging.getLogger(__name__)

_PYPI_NAME_NORMALIZER = re.compile(r"[-_.]+")


class PyPICrawler(RegistryCrawler):
    """
    Crawler for the official PyPI JSON API plus PyPIStats.

    The PyPI JSON API does not expose usable download counts. This crawler uses
    the PyPIStats recent-downloads endpoint with ``period=month`` and
    ``mirrors=true`` to satisfy the adoption-signal requirement.
    """

    REGISTRY = "pypi.org"
    BASE_URL = "https://pypi.org/pypi"

    async def fetch_package(self, package_name: str) -> CrawledPackage:
        """Fetch project metadata from PyPI and download counts from PyPIStats."""

        metadata_resp = await self._request_with_retry(f"{self.BASE_URL}/{package_name}/json")
        metadata: dict[str, Any] = metadata_resp.json()
        downloads_30d, stats_metadata = await self._fetch_downloads_30d(package_name)
        return self._parse_package(metadata, downloads_30d, stats_metadata)

    async def fetch_dependents(self, package_name: str) -> list[CrawledDependent]:
        """
        Return an explicit empty dependent list for PyPI.

        TODO A10: revisit when a practical first-party dependents API exists.
        """

        return []

    async def _fetch_downloads_30d(self, package_name: str) -> tuple[int | None, dict[str, Any]]:
        """
        Fetch mirror-inclusive monthly download totals from PyPIStats.

        The ``pypistats`` package does not expose download counts from mirrors.
        We replicate its endpoint semantics here, and depend on the package only
        to conveniently reference API changes and sync them to our custom implementation.
        """

        stats_url = f"{pypistats.BASE_URL}packages/{package_name}/recent?period=month&mirrors=true"
        try:
            stats_resp = await self._request_with_retry(stats_url)
        except (httpx.HTTPStatusError, httpx.TimeoutException, ExhaustedRetries) as exc:
            logger.warning(f"Failed to fetch PyPIStats downloads for {package_name}: {exc}")

            return None, {}

        stats_payload: dict[str, Any] = stats_resp.json()
        stats_data = as_str_key_dict(stats_payload.get("data"))
        downloads_30d_obj = stats_data.get("last_month")
        if not isinstance(downloads_30d_obj, int):
            logger.warning(f"Unexpected PyPIStats recent payload for {package_name}: {stats_payload!r}")

            return None, {}

        return downloads_30d_obj, {
            "download_source": "pypistats",
            "download_period": "month",
            "download_mirror_policy": "with_mirrors",
        }

    def _parse_package(
        self,
        metadata: dict[str, Any],
        downloads_30d: int | None,
        stats_metadata: dict[str, Any],
    ) -> CrawledPackage:
        """Parse PyPI JSON and PyPIStats payloads into the shared crawler contract."""

        info = as_str_key_dict(metadata.get("info"))
        name_obj = info.get("name")
        name = name_obj if isinstance(name_obj, str) else ""
        package_purl = f"pkg:pypi/{_normalize_pypi_name(name)}"

        dependencies: list[CrawledDependency] = []
        requires_dist_obj = info.get("requires_dist")
        requires_dist: list[object] = requires_dist_obj if isinstance(requires_dist_obj, list) else []
        for requirement_obj in requires_dist:
            if not isinstance(requirement_obj, str):
                continue

            try:
                requirement = Requirement(requirement_obj)
            except InvalidRequirement:
                logger.warning(f"Skipping invalid PyPI requirement for {name}: {requirement_obj}")
                continue

            if requirement.marker and "extra" in str(requirement.marker):
                continue

            version_range = str(requirement.specifier) or None
            dependency_name = _normalize_pypi_name(requirement.name)
            dependencies.append(
                CrawledDependency(
                    canonical_id=f"pkg:pypi/{dependency_name}",
                    display_name=requirement.name,
                    version_range=version_range,
                )
            )

        releases: list[Release] = []
        releases_obj = metadata.get("releases")
        release_map = as_str_key_dict(releases_obj)
        for version, files_obj in release_map.items():
            file_entries: list[object] = files_obj if isinstance(files_obj, list) else []
            upload_times = [
                file_entry["upload_time_iso_8601"]
                for file_entry in file_entries
                if isinstance(file_entry, dict) and isinstance(file_entry.get("upload_time_iso_8601"), str)
            ]
            release_date = min(upload_times) if upload_times else ""
            releases.append(Release(purl=package_purl, version=version, release_date=release_date))

        releases = sorted_releases_desc(releases)
        info_version_obj = info.get("version")
        info_version = info_version_obj if isinstance(info_version_obj, str) else ""
        latest_version = preferred_latest_version(releases) or info_version

        package_metadata = dict(stats_metadata)
        if downloads_30d is not None:
            package_metadata["download_count_30d"] = downloads_30d

        repo_url = _extract_repo_url(info)
        return CrawledPackage(
            canonical_id=package_purl,
            display_name=name,
            latest_version=latest_version,
            repo_url=repo_url,
            downloads_30d=downloads_30d,
            metadata=package_metadata,
            dependencies=dependencies,
            releases=releases,
        )


def _normalize_pypi_name(package_name: str) -> str:
    """Normalize a PyPI package name using the PEP 503 canonical form."""

    return _PYPI_NAME_NORMALIZER.sub("-", package_name).lower()


def _extract_repo_url(info: dict[str, Any]) -> str | None:
    """Extract the most repository-like URL from PyPI project metadata."""

    project_urls = as_str_key_dict(info.get("project_urls"))
    for key in ("Repository", "Source", "Source Code", "Homepage", "Home"):
        value = project_urls.get(key)
        if isinstance(value, str) and value:
            return _normalize_repo_url(value)

    first_project_url = next(
        (value for value in project_urls.values() if isinstance(value, str) and value),
        None,
    )
    if first_project_url:
        return _normalize_repo_url(first_project_url)

    home_page_obj = info.get("home_page")
    if isinstance(home_page_obj, str) and home_page_obj:
        return _normalize_repo_url(home_page_obj)

    return None


def _normalize_repo_url(repo_url: str) -> str | None:
    """Normalize a project URL into a repository URL candidate."""

    normalized = repo_url.strip()
    if not normalized:
        return None

    if normalized.startswith("git+"):
        normalized = normalized[4:]

    normalized = normalized.split("#", 1)[0].rstrip("/")
    return normalized or None
