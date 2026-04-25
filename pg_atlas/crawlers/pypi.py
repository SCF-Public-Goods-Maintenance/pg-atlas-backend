"""
PyPI registry crawler for PG Atlas.

Fetches project metadata and releases from the official PyPI JSON API and uses
the ``pypistats`` client for mirror-inclusive download totals.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import logging
import math
import re
from typing import Any

import httpx
import msgspec
import pypistats  # pyright: ignore[reportMissingTypeStubs]
from packaging.requirements import InvalidRequirement, Requirement

from pg_atlas.crawlers.base import (
    CrawledDependency,
    CrawledDependent,
    CrawledPackage,
    ExhaustedRetries,
    RegistryCrawler,
)
from pg_atlas.db_models.release import Release, preferred_latest_version, sorted_releases_desc

logger = logging.getLogger(__name__)

_PYPI_NAME_NORMALIZER = re.compile(r"[-_.]+")


class _PyPIInfoPayload(msgspec.Struct, omit_defaults=True, forbid_unknown_fields=False):
    name: str = ""
    version: str = ""
    requires_dist: list[str] = msgspec.field(default_factory=list[str])
    project_urls: dict[str, str] = msgspec.field(default_factory=dict[str, str])
    home_page: str | None = None


class _PyPIReleaseFilePayload(msgspec.Struct, omit_defaults=True, forbid_unknown_fields=False):
    upload_time_iso_8601: str | None = None


class _PyPIPackagePayload(msgspec.Struct, omit_defaults=True, forbid_unknown_fields=False):
    info: _PyPIInfoPayload = msgspec.field(default_factory=_PyPIInfoPayload)
    releases: dict[str, list[_PyPIReleaseFilePayload]] = msgspec.field(
        default_factory=dict[str, list[_PyPIReleaseFilePayload]]
    )


class _PyPIStatsRecentDataPayload(msgspec.Struct, omit_defaults=True, forbid_unknown_fields=False):
    last_month: int | float | None = None


class _PyPIStatsRecentPayload(msgspec.Struct, omit_defaults=True, forbid_unknown_fields=False):
    data: _PyPIStatsRecentDataPayload = msgspec.field(default_factory=_PyPIStatsRecentDataPayload)


def _decode_pypi_package_payload(content: bytes, package_name: str) -> _PyPIPackagePayload:
    try:
        return msgspec.json.decode(content, type=_PyPIPackagePayload)
    except msgspec.ValidationError as exc:
        logger.warning(f"Failed to decode PyPI package payload for {package_name}: {exc}")

        return _PyPIPackagePayload()


def _decode_pypi_stats_payload(content: bytes, package_name: str) -> _PyPIStatsRecentPayload:
    try:
        return msgspec.json.decode(content, type=_PyPIStatsRecentPayload)
    except msgspec.ValidationError as exc:
        logger.warning(f"Failed to decode PyPIStats payload for {package_name}: {exc}")

        return _PyPIStatsRecentPayload()


def _normalize_download_count(value: int | float | None, package_name: str, field_name: str) -> int | None:
    if value is None:
        return None

    if isinstance(value, int):
        return value

    if math.isfinite(value):
        return int(value)

    logger.warning(f"Ignoring non-numeric {field_name} value for {package_name}: {value}")

    return None


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
        metadata = _decode_pypi_package_payload(metadata_resp.content, package_name)
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

        stats_payload = _decode_pypi_stats_payload(stats_resp.content, package_name)
        downloads_30d = _normalize_download_count(stats_payload.data.last_month, package_name, "last_month")
        if downloads_30d is None:
            logger.warning(f"Unexpected PyPIStats recent payload for {package_name}")

            return None, {}

        return downloads_30d, {
            "download_source": "pypistats",
            "download_period": "month",
            "download_mirror_policy": "with_mirrors",
        }

    def _parse_package(
        self,
        metadata: _PyPIPackagePayload,
        downloads_30d: int | None,
        stats_metadata: dict[str, Any],
    ) -> CrawledPackage:
        """Parse PyPI JSON and PyPIStats payloads into the shared crawler contract."""

        info = metadata.info
        name = info.name
        package_purl = f"pkg:pypi/{_normalize_pypi_name(name)}"

        dependencies: list[CrawledDependency] = []
        for requirement_obj in info.requires_dist:
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
        for version, file_entries in metadata.releases.items():
            upload_times: list[str] = [
                upload_time for file_entry in file_entries if (upload_time := file_entry.upload_time_iso_8601) is not None
            ]
            release_date = min(upload_times) if upload_times else ""
            releases.append(Release(purl=package_purl, version=version, release_date=release_date))

        releases = sorted_releases_desc(releases)
        latest_version = preferred_latest_version(releases) or info.version

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


def _extract_repo_url(info: _PyPIInfoPayload) -> str | None:
    """Extract the most repository-like URL from PyPI project metadata."""

    project_urls = info.project_urls
    for key in ("Repository", "Source", "Source Code", "Homepage", "Home"):
        value = project_urls.get(key)
        if value:
            return _normalize_repo_url(value)

    first_project_url = next(
        (value for value in project_urls.values() if value),
        None,
    )
    if first_project_url:
        return _normalize_repo_url(first_project_url)

    if info.home_page:
        return _normalize_repo_url(info.home_page)

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
