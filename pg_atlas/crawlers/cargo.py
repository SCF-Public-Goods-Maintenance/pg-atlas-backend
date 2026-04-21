"""
crates.io registry crawler for PG Atlas.

Fetches crate metadata, latest-version dependencies, and reverse dependencies
from crates.io. The 30-day download count uses the issue-defined
``recent_downloads / 3`` approximation.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import httpx

from pg_atlas.crawlers.base import CrawledDependency, CrawledDependent, CrawledPackage, RegistryCrawler, as_str_key_dict
from pg_atlas.db_models.release import Release, preferred_latest_version, sorted_releases_desc

logger = logging.getLogger(__name__)


class CargoCrawler(RegistryCrawler):
    """
    Crawler for the public crates.io API.

    crates.io asks API consumers to stay at or below one request per second.
    The default crawler rate limit is already ``1.0``, and this crawler applies
    that pacing to each crates.io request, not only between packages.
    """

    REGISTRY = "crates.io"
    BASE_URL = "https://crates.io/api/v1/crates"
    MAX_DEPENDENT_PAGES = 50
    DEPENDENTS_PER_PAGE = 10
    MAX_DEPENDENTS = 500

    def __init__(
        self,
        client: httpx.AsyncClient,
        session_factory: Any,
        rate_limit: float = 1.0,
        max_retries: int = 3,
    ) -> None:
        super().__init__(
            client=client,
            session_factory=session_factory,
            rate_limit=rate_limit,
            max_retries=max_retries,
        )
        self._next_request_ready_at = 0.0

    async def fetch_package(self, package_name: str) -> CrawledPackage:
        """Fetch crate metadata plus dependency information for the newest version."""

        metadata_resp = await self._request_with_rate_limit(f"{self.BASE_URL}/{package_name}")
        metadata: dict[str, Any] = metadata_resp.json()

        crate = as_str_key_dict(metadata.get("crate"))
        selected_version = _selected_cargo_version(crate)
        dependency_payload: dict[str, Any] = {}
        if selected_version:
            dependency_resp = await self._request_with_rate_limit(
                f"{self.BASE_URL}/{package_name}/{selected_version}/dependencies"
            )
            dependency_payload = dependency_resp.json()

        return self._parse_package(metadata, dependency_payload)

    async def fetch_dependents(self, package_name: str) -> list[CrawledDependent]:
        """Fetch reverse dependencies from crates.io up to the configured pagination ceiling."""

        dependents_by_canonical_id: dict[str, CrawledDependent] = {}

        for page in range(1, self.MAX_DEPENDENT_PAGES + 1):
            try:
                resp = await self._request_with_rate_limit(
                    f"{self.BASE_URL}/{package_name}/reverse_dependencies?page={page}&per_page={self.DEPENDENTS_PER_PAGE}"
                )
            except (httpx.HTTPStatusError, httpx.TimeoutException) as exc:
                logger.warning(f"Failed to fetch crates.io dependents for {package_name}: {exc}")

                return list(dependents_by_canonical_id.values())

            payload: dict[str, Any] = resp.json()
            version_crates = _dependent_version_crates(payload)
            dependencies_obj = payload.get("dependencies")
            dependencies: list[object] = dependencies_obj if isinstance(dependencies_obj, list) else []

            for dependency_obj in dependencies:
                dependency = as_str_key_dict(dependency_obj)
                kind_obj = dependency.get("kind")
                kind = kind_obj if isinstance(kind_obj, str) else ""
                if kind and kind != "normal":
                    continue

                version_id_obj = dependency.get("version_id")
                if not isinstance(version_id_obj, int):
                    continue

                dependent_name = version_crates.get(version_id_obj)
                if not dependent_name or dependent_name == package_name:
                    continue

                canonical_id = f"pkg:cargo/{dependent_name}"
                dependents_by_canonical_id.setdefault(
                    canonical_id,
                    CrawledDependent(
                        canonical_id=canonical_id,
                        display_name=dependent_name,
                    ),
                )

                if len(dependents_by_canonical_id) >= self.MAX_DEPENDENTS:
                    logger.warning(f"Truncated crates.io dependents for {package_name} at {self.MAX_DEPENDENTS}")

                    return list(dependents_by_canonical_id.values())

            meta = as_str_key_dict(payload.get("meta"))
            total_obj = meta.get("total")
            total = total_obj if isinstance(total_obj, int) else 0
            if page * self.DEPENDENTS_PER_PAGE >= total:
                break

        return list(dependents_by_canonical_id.values())

    async def _request_with_rate_limit(self, url: str) -> httpx.Response:
        """Apply per-request pacing before delegating to shared retry logic."""

        if self.rate_limit > 0:
            wait_seconds = self._next_request_ready_at - time.monotonic()
            if wait_seconds > 0:
                await asyncio.sleep(wait_seconds)

            self._next_request_ready_at = time.monotonic() + self.rate_limit

        return await self._request_with_retry(url)

    def _parse_package(
        self,
        metadata: dict[str, Any],
        dependency_payload: dict[str, Any],
    ) -> CrawledPackage:
        """Parse crates.io metadata into the shared crawler contract."""

        crate = as_str_key_dict(metadata.get("crate"))
        name_obj = crate.get("name")
        name = name_obj if isinstance(name_obj, str) else ""
        package_purl = f"pkg:cargo/{name}"

        releases: list[Release] = []
        versions_obj = metadata.get("versions")
        versions: list[object] = versions_obj if isinstance(versions_obj, list) else []
        for version_obj in versions:
            version = as_str_key_dict(version_obj)
            release_version_obj = version.get("num")
            if not isinstance(release_version_obj, str) or not release_version_obj:
                continue

            created_at_obj = version.get("created_at")
            release_date = created_at_obj if isinstance(created_at_obj, str) else ""
            releases.append(Release(purl=package_purl, version=release_version_obj, release_date=release_date))

        releases = sorted_releases_desc(releases)
        latest_version = preferred_latest_version(releases) or _selected_cargo_version(crate)

        dependencies: list[CrawledDependency] = []
        dependencies_obj = dependency_payload.get("dependencies")
        dependency_rows: list[object] = dependencies_obj if isinstance(dependencies_obj, list) else []
        for dependency_obj in dependency_rows:
            dependency = as_str_key_dict(dependency_obj)
            kind_obj = dependency.get("kind")
            kind = kind_obj if isinstance(kind_obj, str) else ""
            if kind and kind != "normal":
                continue

            dependency_name_obj = dependency.get("crate_id")
            if not isinstance(dependency_name_obj, str) or not dependency_name_obj:
                continue

            requirement_obj = dependency.get("req")
            version_range = requirement_obj if isinstance(requirement_obj, str) else None
            dependencies.append(
                CrawledDependency(
                    canonical_id=f"pkg:cargo/{dependency_name_obj}",
                    display_name=dependency_name_obj,
                    version_range=version_range,
                )
            )

        downloads_30d = _downloads_30d(crate)
        package_metadata: dict[str, Any] = {}
        recent_downloads_obj = crate.get("recent_downloads")
        if isinstance(recent_downloads_obj, int):
            package_metadata["recent_downloads_90d"] = recent_downloads_obj

        if downloads_30d is not None:
            package_metadata["download_count_30d"] = downloads_30d

        repo_url = _cargo_repo_url(crate)
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


def _selected_cargo_version(crate: dict[str, Any]) -> str:
    """Select the version to query for dependency metadata."""

    stable_version_obj = crate.get("max_stable_version")
    if isinstance(stable_version_obj, str) and stable_version_obj:
        return stable_version_obj

    max_version_obj = crate.get("max_version")
    if isinstance(max_version_obj, str):
        return max_version_obj

    return ""


def _downloads_30d(crate: dict[str, Any]) -> int | None:
    """Convert crates.io recent downloads (90 days) into the issue-defined 30-day approximation."""

    recent_downloads_obj = crate.get("recent_downloads")
    if not isinstance(recent_downloads_obj, int):
        return None

    return recent_downloads_obj // 3


def _cargo_repo_url(crate: dict[str, Any]) -> str | None:
    """Extract the most repository-like URL from crates.io metadata."""

    for key in ("repository", "homepage", "documentation"):
        value = crate.get(key)
        if isinstance(value, str) and value:
            return value

    return None


def _dependent_version_crates(payload: dict[str, Any]) -> dict[int, str]:
    """Build a lookup from version ID to dependent crate name."""

    versions_obj = payload.get("versions")
    versions: list[object] = versions_obj if isinstance(versions_obj, list) else []
    version_crates: dict[int, str] = {}
    for version_obj in versions:
        version = as_str_key_dict(version_obj)
        version_id_obj = version.get("id")
        crate_name_obj = version.get("crate")
        if isinstance(version_id_obj, int) and isinstance(crate_name_obj, str) and crate_name_obj:
            version_crates[version_id_obj] = crate_name_obj

    return version_crates
