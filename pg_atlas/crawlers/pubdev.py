"""
pub.dev registry crawler for PG Atlas.

Fetches package metadata, download metrics, and reverse dependencies
from the pub.dev API (Dart/Flutter package registry). Creates ``ExternalRepo``
vertices and ``DependsOn`` edges with ``inferred_shadow`` confidence.

API docs: https://pub.dev/help/api

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import logging
from typing import Any

import httpx
from pydantic import TypeAdapter, ValidationError

from pg_atlas.crawlers.base import CrawledDependency, CrawledDependent, CrawledPackage, RegistryCrawler

logger = logging.getLogger(__name__)


_DEPENDENCIES_ADAPTER: TypeAdapter[dict[str, object]] = TypeAdapter(dict[str, object])
_WEEKLY_DOWNLOADS_ADAPTER: TypeAdapter[list[float]] = TypeAdapter(list[float])


class PubDevCrawler(RegistryCrawler):
    """
    Crawler for pub.dev (Dart/Flutter package registry).

    ``downloads_30d`` is captured on ``CrawledPackage`` from pub.dev metrics.
    The base crawler persists that value under the source repo metadata PURL
    map; scalar adoption downloads are reduced later during materialization.
    Additional download breakdowns are stored in package metadata
    (``download_count_4w``, ``download_count_12w``, ``download_count_52w``).
    """

    REGISTRY = "pub.dev"
    BASE_URL = "https://pub.dev/api"

    FRAMEWORK_PACKAGES = frozenset(
        {
            "flutter",
            "flutter_test",
            "flutter_localizations",
            "flutter_web_plugins",
            "flutter_driver",
            "integration_test",
        }
    )

    async def fetch_package(self, package_name: str) -> CrawledPackage:
        """
        Fetch package metadata and metrics from pub.dev.

        Makes two API calls:
        1. GET /api/packages/{name} — metadata, versions, dependencies
        2. GET /api/packages/{name}/metrics — scores, downloads, weekly history
        """
        pkg_resp = await self._request_with_retry(f"{self.BASE_URL}/packages/{package_name}")
        pkg_data: dict[str, Any] = pkg_resp.json()

        metrics_data: dict[str, Any] = {}
        try:
            metrics_resp = await self._request_with_retry(f"{self.BASE_URL}/packages/{package_name}/metrics")
            metrics_data = metrics_resp.json()
        except (httpx.HTTPStatusError, httpx.TimeoutException) as exc:
            logger.warning(f"Failed to fetch metrics for {package_name}: {exc}")

        return self._parse_package(pkg_data, metrics_data)

    async def fetch_dependents(self, package_name: str) -> list[CrawledDependent]:
        """
        Fetch reverse dependencies via pub.dev search API.

        Handles pagination by following the ``next`` URL if present.
        """
        dependents: list[CrawledDependent] = []
        url = f"{self.BASE_URL}/search?q=dependency:{package_name}"
        max_pages = 50
        max_dependents = 500
        pages_fetched = 0

        while url and pages_fetched < max_pages and len(dependents) < max_dependents:
            pages_fetched += 1
            resp = await self._request_with_retry(url)
            data: dict[str, Any] = resp.json()

            for entry in data.get("packages", []):
                name = entry.get("package", "")
                if name:
                    dependents.append(
                        CrawledDependent(
                            canonical_id=f"pkg:pub/{name.lower()}",
                            display_name=name,
                        )
                    )

            url = data.get("next", "")

        if len(dependents) >= max_dependents:
            logger.warning(f"Truncated dependents for {package_name} at {max_dependents}")

        return dependents

    def _parse_package(self, pkg_data: dict[str, Any], metrics_data: dict[str, Any]) -> CrawledPackage:
        """Parse pub.dev API responses into a CrawledPackage."""
        name = pkg_data.get("name", "")
        latest = pkg_data.get("latest", {})
        version = latest.get("version", "")
        pubspec = latest.get("pubspec", {})

        homepage = pubspec.get("homepage") or pubspec.get("repository")
        repo_url: str | None = homepage if isinstance(homepage, str) else None

        # Parse runtime dependencies only (not dev_dependencies or dependency_overrides)
        raw_deps_obj = pubspec.get("dependencies")
        try:
            raw_deps = _DEPENDENCIES_ADAPTER.validate_python(raw_deps_obj)
        except ValidationError:
            raw_deps = {}

        dependencies: list[CrawledDependency] = []
        for dep_name, dep_constraint in raw_deps.items():
            if dep_name.lower() in self.FRAMEWORK_PACKAGES:
                continue

            # SDK dependencies like {"sdk": "flutter"} are dicts, not version strings
            if isinstance(dep_constraint, dict):
                continue

            version_range = dep_constraint if isinstance(dep_constraint, str) else None
            dependencies.append(
                CrawledDependency(
                    canonical_id=f"pkg:pub/{dep_name.lower()}",
                    display_name=dep_name,
                    version_range=version_range,
                )
            )

        # Extract score data (nested under "score" in metrics response)
        score = metrics_data.get("score", {})
        downloads_30d = score.get("downloadCount30Days")
        pub_points = score.get("grantedPoints")
        pub_points_max = score.get("maxPoints")

        metadata: dict[str, Any] = {}
        if downloads_30d is not None:
            metadata["download_count_30d"] = downloads_30d

        # Extract weekly download history from scorecard
        scorecard = metrics_data.get("scorecard", {})
        wvd = scorecard.get("weeklyVersionDownloads", {})
        weekly_downloads_obj = wvd.get("totalWeeklyDownloads")
        try:
            weekly_downloads = _WEEKLY_DOWNLOADS_ADAPTER.validate_python(weekly_downloads_obj)
        except ValidationError:
            weekly_downloads = []

        if weekly_downloads:
            metadata["download_count_4w"] = sum(weekly_downloads[:4])
            metadata["download_count_12w"] = sum(weekly_downloads[:12])
            metadata["download_count_52w"] = sum(weekly_downloads[:52])

        if pub_points is not None:
            metadata["pub_points"] = pub_points
        if pub_points_max is not None:
            metadata["pub_points_max"] = pub_points_max

        return CrawledPackage(
            canonical_id=f"pkg:pub/{name.lower()}",
            display_name=name,
            latest_version=version,
            repo_url=repo_url,
            downloads_30d=downloads_30d,
            stars=None,
            metadata=metadata,
            dependencies=dependencies,
        )
