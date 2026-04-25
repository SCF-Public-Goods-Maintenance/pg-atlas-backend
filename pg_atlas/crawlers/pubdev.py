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
import math

import httpx
import msgspec

from pg_atlas.crawlers.base import (
    CrawledDependency,
    CrawledDependent,
    CrawledPackage,
    ExhaustedRetries,
    RegistryCrawler,
)
from pg_atlas.db_models.release import Release

logger = logging.getLogger(__name__)


class _PubDevPubspecPayload(msgspec.Struct, omit_defaults=True, forbid_unknown_fields=False):
    homepage: str | None = None
    repository: str | None = None
    dependencies: dict[str, object] = msgspec.field(default_factory=dict[str, object])


class _PubDevLatestPayload(msgspec.Struct, omit_defaults=True, forbid_unknown_fields=False):
    version: str = ""
    pubspec: _PubDevPubspecPayload = msgspec.field(default_factory=_PubDevPubspecPayload)


class _PubDevVersionPayload(msgspec.Struct, omit_defaults=True, forbid_unknown_fields=False):
    version: str = ""
    published: str = ""


class _PubDevPackagePayload(msgspec.Struct, omit_defaults=True, forbid_unknown_fields=False):
    name: str = ""
    latest: _PubDevLatestPayload = msgspec.field(default_factory=_PubDevLatestPayload)
    versions: list[_PubDevVersionPayload] = msgspec.field(default_factory=list[_PubDevVersionPayload])


class _PubDevScorePayload(msgspec.Struct, omit_defaults=True, forbid_unknown_fields=False):
    downloadCount30Days: int | float | None = None
    grantedPoints: int | float | None = None
    maxPoints: int | float | None = None


class _PubDevWeeklyDownloadsPayload(msgspec.Struct, omit_defaults=True, forbid_unknown_fields=False):
    totalWeeklyDownloads: list[int | float] = msgspec.field(default_factory=list[int | float])


class _PubDevScorecardPayload(msgspec.Struct, omit_defaults=True, forbid_unknown_fields=False):
    weeklyVersionDownloads: _PubDevWeeklyDownloadsPayload = msgspec.field(default_factory=_PubDevWeeklyDownloadsPayload)


class _PubDevMetricsPayload(msgspec.Struct, omit_defaults=True, forbid_unknown_fields=False):
    score: _PubDevScorePayload = msgspec.field(default_factory=_PubDevScorePayload)
    scorecard: _PubDevScorecardPayload = msgspec.field(default_factory=_PubDevScorecardPayload)


class _PubDevDependentPayload(msgspec.Struct, omit_defaults=True, forbid_unknown_fields=False):
    package: str = ""


class _PubDevDependentsPayload(msgspec.Struct, omit_defaults=True, forbid_unknown_fields=False):
    packages: list[_PubDevDependentPayload] = msgspec.field(default_factory=list[_PubDevDependentPayload])
    next: str = ""


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
        1. GET /api/packages/{name} - metadata, versions, dependencies
        2. GET /api/packages/{name}/metrics - scores, downloads, weekly history
        """
        pkg_resp = await self._request_with_retry(f"{self.BASE_URL}/packages/{package_name}")
        pkg_data = _decode_pubdev_package_payload(pkg_resp.content, package_name)

        metrics_data = _PubDevMetricsPayload()
        try:
            metrics_resp = await self._request_with_retry(f"{self.BASE_URL}/packages/{package_name}/metrics")
            metrics_data = _decode_pubdev_metrics_payload(metrics_resp.content, package_name)
        except (httpx.HTTPStatusError, httpx.TimeoutException, ExhaustedRetries) as exc:
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
            data = _decode_pubdev_dependents_payload(resp.content, package_name)

            for entry in data.packages:
                if entry.package:
                    dependents.append(
                        CrawledDependent(
                            canonical_id=f"pkg:pub/{entry.package.lower()}",
                            display_name=entry.package,
                        )
                    )

            url = data.next

        if len(dependents) >= max_dependents:
            logger.warning(f"Truncated dependents for {package_name} at {max_dependents}")

        return dependents

    def _parse_package(self, pkg_data: _PubDevPackagePayload, metrics_data: _PubDevMetricsPayload) -> CrawledPackage:
        """Parse pub.dev API responses into a CrawledPackage."""
        name = pkg_data.name
        package_purl = f"pkg:pub/{name.lower()}"
        latest = pkg_data.latest
        version = latest.version
        pubspec = latest.pubspec
        repo_url = pubspec.homepage or pubspec.repository

        # Parse runtime dependencies only (not dev_dependencies or dependency_overrides)
        dependencies: list[CrawledDependency] = []
        for dep_name, dep_constraint in pubspec.dependencies.items():
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

        score = metrics_data.score
        downloads_30d = _normalize_download_count(score.downloadCount30Days, name, "downloadCount30Days")
        pub_points = _normalize_download_count(score.grantedPoints, name, "grantedPoints")
        pub_points_max = _normalize_download_count(score.maxPoints, name, "maxPoints")

        metadata: dict[str, int] = {}
        if downloads_30d is not None:
            metadata["download_count_30d"] = downloads_30d

        weekly_downloads = _normalize_weekly_downloads(
            metrics_data.scorecard.weeklyVersionDownloads.totalWeeklyDownloads,
            name,
        )
        if weekly_downloads:
            metadata["download_count_4w"] = sum(weekly_downloads[:4])
            metadata["download_count_12w"] = sum(weekly_downloads[:12])
            metadata["download_count_52w"] = sum(weekly_downloads[:52])

        if pub_points is not None:
            metadata["pub_points"] = pub_points

        if pub_points_max is not None:
            metadata["pub_points_max"] = pub_points_max

        releases: list[Release] = []
        for version_payload in pkg_data.versions:
            if not version_payload.version:
                continue

            releases.append(
                Release(
                    purl=package_purl,
                    version=version_payload.version,
                    release_date=version_payload.published,
                )
            )

        return CrawledPackage(
            canonical_id=package_purl,
            display_name=name,
            latest_version=version,
            repo_url=repo_url,
            downloads_30d=downloads_30d,
            metadata=metadata,
            dependencies=dependencies,
            releases=releases,
        )


def _decode_pubdev_package_payload(content: bytes, package_name: str) -> _PubDevPackagePayload:
    try:
        return msgspec.json.decode(content, type=_PubDevPackagePayload)
    except msgspec.ValidationError as exc:
        logger.warning(f"Failed to decode pub.dev package payload for {package_name}: {exc}")

        return _PubDevPackagePayload()


def _decode_pubdev_metrics_payload(content: bytes, package_name: str) -> _PubDevMetricsPayload:
    try:
        return msgspec.json.decode(content, type=_PubDevMetricsPayload)
    except msgspec.ValidationError as exc:
        logger.warning(f"Failed to decode pub.dev metrics payload for {package_name}: {exc}")

        return _PubDevMetricsPayload()


def _decode_pubdev_dependents_payload(content: bytes, package_name: str) -> _PubDevDependentsPayload:
    try:
        return msgspec.json.decode(content, type=_PubDevDependentsPayload)
    except msgspec.ValidationError as exc:
        logger.warning(f"Failed to decode pub.dev dependents payload for {package_name}: {exc}")

        return _PubDevDependentsPayload()


def _normalize_download_count(value: int | float | None, package_name: str, field_name: str) -> int | None:
    if value is None:
        return None

    if isinstance(value, int):
        return value

    if math.isfinite(value):
        return int(value)

    logger.warning(f"Ignoring non-numeric {field_name} value for {package_name}: {value}")

    return None


def _normalize_weekly_downloads(values: list[int | float], package_name: str) -> list[int]:
    normalized: list[int] = []
    for value in values:
        if isinstance(value, int):
            normalized.append(value)
            continue

        if math.isfinite(value):
            normalized.append(int(value))
            continue

        logger.warning(f"Ignoring non-numeric weekly download value for {package_name}: {value}")

    return normalized
