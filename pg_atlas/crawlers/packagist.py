"""
Packagist registry crawler for PG Atlas.

Fetches package metadata, download statistics, and reverse dependencies from
the Packagist API (PHP/Composer package registry). Creates ``ExternalRepo``
vertices and ``DependsOn`` edges with ``inferred_shadow`` confidence.

API docs: https://packagist.org/apidoc

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

from pg_atlas.crawlers.base import CrawledDependency, CrawledDependent, CrawledPackage, RegistryCrawler

logger = logging.getLogger(__name__)


def _parse_semver_tuple(version: str) -> tuple[int, ...]:
    """
    Parse a version string into a tuple of ints for comparison.

    Segments that cannot be converted to int are treated as 0.
    This handles common semver cases (1.9.4, 1.10.0) without needing
    a full semver library.
    """
    parts: list[int] = []
    for segment in version.lstrip("v").split("."):
        try:
            parts.append(int(segment))
        except ValueError:
            parts.append(0)
    return tuple(parts)


def _is_dev_version(version: str) -> bool:
    """Check whether a version string represents a dev branch."""
    return version.startswith("dev-") or version.endswith("-dev")


class PackagistCrawler(RegistryCrawler):
    """
    Crawler for Packagist (PHP/Composer package registry).

    ``adoption_downloads`` is set to the last-30-days (monthly) download count,
    matching the spec definition.  All-time total is stored in metadata.
    """

    REGISTRY = "packagist.org"
    BASE_URL = "https://packagist.org"

    FILTER_EXACT = frozenset({"php", "composer-plugin-api"})
    FILTER_PREFIXES = ("ext-", "lib-")

    async def fetch_package(self, package_name: str) -> CrawledPackage:
        """
        Fetch package metadata and download stats from Packagist.

        Makes two API calls:
        1. GET /packages/{vendor}/{name}.json — metadata, versions, favers
        2. GET /packages/{vendor}/{name}/downloads.json — download statistics
        """
        pkg_resp = await self._request_with_retry(f"{self.BASE_URL}/packages/{package_name}.json")
        pkg_data: dict[str, Any] = pkg_resp.json()

        downloads_data: dict[str, Any] = {}
        try:
            dl_resp = await self._request_with_retry(f"{self.BASE_URL}/packages/{package_name}/downloads.json")
            downloads_data = dl_resp.json()
        except (httpx.HTTPStatusError, httpx.TimeoutException) as exc:
            logger.warning(f"Failed to fetch downloads for {package_name}: {exc}")

        return self._parse_package(pkg_data, downloads_data)

    async def fetch_dependents(self, package_name: str) -> list[CrawledDependent]:
        """
        Fetch reverse dependencies from Packagist.

        The dependents endpoint may return HTML instead of JSON on some
        packages — this is handled gracefully by returning an empty list.
        """
        try:
            resp = await self._request_with_retry(f"{self.BASE_URL}/packages/{package_name}/dependents.json")
        except (httpx.HTTPStatusError, httpx.TimeoutException) as exc:
            logger.warning(f"Failed to fetch dependents for {package_name}: {exc}")
            return []

        content_type = resp.headers.get("content-type", "")
        if "json" not in content_type:
            logger.warning(f"Dependents endpoint returned non-JSON for {package_name}: {content_type}")
            return []

        data: dict[str, Any] = resp.json()
        dependents: list[CrawledDependent] = []
        for entry in data.get("packages", []):
            name = entry.get("name", "")
            if name:
                dependents.append(
                    CrawledDependent(
                        canonical_id=f"pkg:composer/{name}",
                        display_name=name,
                    )
                )

        return dependents

    def _parse_package(self, pkg_data: dict[str, Any], downloads_data: dict[str, Any]) -> CrawledPackage:
        """Parse Packagist API responses into a CrawledPackage."""
        package = pkg_data.get("package", {})
        name = package.get("name", "")
        versions: dict[str, Any] = package.get("versions", {})

        # Select latest stable version
        latest_version, version_data = self._select_latest_version(versions)

        # Extract repo URL from source
        source = version_data.get("source", {})
        repo_url: str | None = source.get("url") if source else None

        # Parse dependencies from require dict
        require: dict[str, str] = version_data.get("require", {}) or {}
        dependencies: list[CrawledDependency] = []
        for dep_name, version_range in require.items():
            if self._should_filter(dep_name):
                continue
            dependencies.append(
                CrawledDependency(
                    canonical_id=f"pkg:composer/{dep_name}",
                    display_name=dep_name,
                    version_range=version_range,
                )
            )

        # Parse download stats from /downloads.json (nested under package.downloads.total)
        dl_totals = downloads_data.get("package", {}).get("downloads", {}).get("total", {})
        total = dl_totals.get("total")

        metadata: dict[str, Any] = {}
        if total is not None:
            metadata["downloads_total"] = total
        monthly = dl_totals.get("monthly")
        if monthly is not None:
            metadata["download_count_30d"] = monthly
        daily = dl_totals.get("daily")
        if daily is not None:
            metadata["downloads_daily"] = daily

        return CrawledPackage(
            canonical_id=f"pkg:composer/{name}",
            display_name=name,
            latest_version=latest_version,
            repo_url=repo_url,
            downloads=monthly,
            stars=None,
            metadata=metadata,
            dependencies=dependencies,
        )

    def _select_latest_version(self, versions: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        """
        Select the latest stable version from a Packagist versions dict.

        Filters out dev branches (``dev-*`` prefix or ``*-dev`` suffix), sorts
        remaining versions by semver tuple, and returns the highest. Falls back
        to ``dev-main`` or ``dev-master`` if no stable versions exist.
        """
        stable: list[tuple[tuple[int, ...], str, dict[str, Any]]] = []
        dev_fallbacks: dict[str, dict[str, Any]] = {}

        for version_key, data in versions.items():
            if _is_dev_version(version_key):
                dev_fallbacks[version_key] = data
            else:
                stable.append((_parse_semver_tuple(version_key), version_key, data))

        if stable:
            stable.sort(key=lambda t: t[0], reverse=True)
            return stable[0][1], stable[0][2]

        # No stable versions — fall back to dev-main or dev-master
        for dev_name in ("dev-main", "dev-master"):
            if dev_name in dev_fallbacks:
                return dev_name, dev_fallbacks[dev_name]

        # Last resort: first dev branch
        if dev_fallbacks:
            first_key = next(iter(dev_fallbacks))
            return first_key, dev_fallbacks[first_key]

        return "", {}

    def _should_filter(self, dep_name: str) -> bool:
        """Check whether a dependency name should be filtered out."""
        return dep_name in self.FILTER_EXACT or any(dep_name.startswith(p) for p in self.FILTER_PREFIXES)
