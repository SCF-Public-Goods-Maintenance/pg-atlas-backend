"""
npm registry crawler for PG Atlas.

Fetches package metadata and last-30-day downloads from the public npm APIs.
Reverse dependents are intentionally stubbed because npm does not expose a
practical first-party dependents API for this crawler path.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import quote

import httpx

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


class NpmCrawler(RegistryCrawler):
    """
    Crawler for the public npm registry and downloads APIs.

    The package metadata endpoint is the source of repository URLs, releases,
    and runtime dependency declarations. The downloads endpoint supplies the
    30-day aggregate used by adoption materialization.
    """

    REGISTRY = "npmjs.com"
    METADATA_BASE_URL = "https://registry.npmjs.org"
    DOWNLOADS_BASE_URL = "https://api.npmjs.org/downloads/point/last-month"

    async def fetch_package(self, package_name: str) -> CrawledPackage:
        """
        Fetch package metadata and last-30-day downloads from npm.
        """

        escaped_name = _encode_npm_package_name_for_url(package_name)
        metadata_resp = await self._request_with_retry(f"{self.METADATA_BASE_URL}/{escaped_name}")
        metadata: dict[str, Any] = metadata_resp.json()

        downloads: dict[str, Any] = {}
        try:
            downloads_resp = await self._request_with_retry(f"{self.DOWNLOADS_BASE_URL}/{escaped_name}")
            downloads = downloads_resp.json()
        except (httpx.HTTPStatusError, httpx.TimeoutException, ExhaustedRetries) as exc:
            logger.warning(f"Failed to fetch npm downloads for {package_name}: {exc}")

        return self._parse_package(metadata, downloads)

    async def fetch_dependents(self, package_name: str) -> list[CrawledDependent]:
        """
        Return an explicit empty dependent list for npm.

        TODO A10: revisit when npm exposes a practical first-party dependents
        endpoint suitable for bootstrap crawling.
        """

        return []

    def _parse_package(
        self,
        metadata: dict[str, Any],
        downloads: dict[str, Any],
    ) -> CrawledPackage:
        """Parse npm API responses into the shared crawler contract."""

        name_obj = metadata.get("name")
        name = name_obj if isinstance(name_obj, str) else ""
        package_purl = f"pkg:npm/{_canonical_npm_name(name)}"
        versions = as_str_key_dict(metadata.get("versions"))
        dist_tags = as_str_key_dict(metadata.get("dist-tags"))
        time_map = as_str_key_dict(metadata.get("time"))

        releases: list[Release] = []
        for version_key, version_payload_obj in versions.items():
            if not isinstance(version_payload_obj, dict):
                continue

            version_payload = as_str_key_dict(version_payload_obj)
            version_value_obj = version_payload.get("version")
            version_value = version_value_obj if isinstance(version_value_obj, str) and version_value_obj else version_key
            release_date_obj = time_map.get(version_value)
            release_date = release_date_obj if isinstance(release_date_obj, str) else ""
            releases.append(Release(purl=package_purl, version=version_value, release_date=release_date))

        releases = sorted_releases_desc(releases)
        latest_version_obj = dist_tags.get("latest")
        latest_version = latest_version_obj if isinstance(latest_version_obj, str) else ""
        if releases:
            latest_version = preferred_latest_version(releases) or latest_version

        latest_payload = as_str_key_dict(versions.get(latest_version))
        if not latest_payload and releases:
            latest_payload = as_str_key_dict(versions.get(releases[0].version))

        dependencies: list[CrawledDependency] = []
        latest_dependencies = as_str_key_dict(latest_payload.get("dependencies"))
        for dep_name, dep_range_obj in latest_dependencies.items():
            version_range = dep_range_obj if isinstance(dep_range_obj, str) else None
            dependencies.append(
                CrawledDependency(
                    canonical_id=f"pkg:npm/{_canonical_npm_name(dep_name)}",
                    display_name=dep_name,
                    version_range=version_range,
                )
            )

        repo_url = _repository_url(latest_payload, metadata)
        downloads_30d_obj = downloads.get("downloads")
        downloads_30d = downloads_30d_obj if isinstance(downloads_30d_obj, int) else None

        package_metadata: dict[str, Any] = {}
        if downloads_30d is not None:
            package_metadata["download_count_30d"] = downloads_30d

        start_obj = downloads.get("start")
        if isinstance(start_obj, str):
            package_metadata["downloads_start"] = start_obj

        end_obj = downloads.get("end")
        if isinstance(end_obj, str):
            package_metadata["downloads_end"] = end_obj

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


def _encode_npm_package_name_for_url(package_name: str) -> str:
    """Encode an npm package name for use in registry API URLs."""

    return quote(package_name, safe="")


def _canonical_npm_name(package_name: str) -> str:
    """Normalize an npm package name for use in a package PURL."""

    return quote(package_name.lower(), safe="/")


def _repository_url(version_payload: dict[str, Any], metadata: dict[str, Any]) -> str | None:
    """Extract and normalize a repository URL from npm metadata."""

    repository = version_payload.get("repository")
    if repository is None:
        repository = metadata.get("repository")

    if isinstance(repository, str):
        return _normalize_repo_url(repository)

    repository_dict = as_str_key_dict(repository)
    repository_url = repository_dict.get("url")
    if isinstance(repository_url, str):
        return _normalize_repo_url(repository_url)

    return None


def _normalize_repo_url(repo_url: str) -> str | None:
    """Normalize an npm-style repository URL into a git-hosted HTTPS URL."""

    normalized = repo_url.strip()
    if not normalized:
        return None

    if normalized.startswith("git+"):
        normalized = normalized[4:]

    normalized = normalized.split("#", 1)[0].rstrip("/")
    return normalized or None
