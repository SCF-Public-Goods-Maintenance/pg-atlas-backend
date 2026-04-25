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
import math
import time
from typing import Any

import httpx
import msgspec

from pg_atlas.crawlers.base import (
    CrawledDependency,
    CrawledDependent,
    CrawledPackage,
    ExhaustedRetries,
    RegistryCrawler,
)
from pg_atlas.db_models.release import Release, preferred_latest_version, sorted_releases_desc

logger = logging.getLogger(__name__)


class _CargoCratePayload(msgspec.Struct, omit_defaults=True, forbid_unknown_fields=False):
    name: str = ""
    repository: str | None = None
    homepage: str | None = None
    documentation: str | None = None
    max_stable_version: str = ""
    max_version: str = ""
    recent_downloads: int | float | None = None


class _CargoVersionPayload(msgspec.Struct, omit_defaults=True, forbid_unknown_fields=False):
    num: str = ""
    created_at: str = ""


class _CargoMetadataPayload(msgspec.Struct, omit_defaults=True, forbid_unknown_fields=False):
    crate: _CargoCratePayload = msgspec.field(default_factory=_CargoCratePayload)
    versions: list[_CargoVersionPayload] = msgspec.field(default_factory=list[_CargoVersionPayload])


class _CargoDependencyPayload(msgspec.Struct, omit_defaults=True, forbid_unknown_fields=False):
    crate_id: str = ""
    req: str | None = None
    kind: str = ""


class _CargoDependencyListPayload(msgspec.Struct, omit_defaults=True, forbid_unknown_fields=False):
    dependencies: list[_CargoDependencyPayload] = msgspec.field(default_factory=list[_CargoDependencyPayload])


class _CargoReverseDependencyPayload(msgspec.Struct, omit_defaults=True, forbid_unknown_fields=False):
    version_id: int | None = None
    kind: str = ""


class _CargoReverseVersionPayload(msgspec.Struct, omit_defaults=True, forbid_unknown_fields=False):
    id: int | None = None
    crate: str = ""


class _CargoReverseMetaPayload(msgspec.Struct, omit_defaults=True, forbid_unknown_fields=False):
    total: int = 0


class _CargoReverseDependenciesPagePayload(msgspec.Struct, omit_defaults=True, forbid_unknown_fields=False):
    dependencies: list[_CargoReverseDependencyPayload] = msgspec.field(default_factory=list[_CargoReverseDependencyPayload])
    versions: list[_CargoReverseVersionPayload] = msgspec.field(default_factory=list[_CargoReverseVersionPayload])
    meta: _CargoReverseMetaPayload = msgspec.field(default_factory=_CargoReverseMetaPayload)


def _decode_cargo_metadata_payload(content: bytes, package_name: str) -> _CargoMetadataPayload:
    try:
        return msgspec.json.decode(content, type=_CargoMetadataPayload)
    except msgspec.ValidationError as exc:
        logger.warning(f"Failed to decode crates.io metadata payload for {package_name}: {exc}")

        return _CargoMetadataPayload()


def _decode_cargo_dependency_payload(content: bytes, package_name: str) -> _CargoDependencyListPayload:
    try:
        return msgspec.json.decode(content, type=_CargoDependencyListPayload)
    except msgspec.ValidationError as exc:
        logger.warning(f"Failed to decode crates.io dependency payload for {package_name}: {exc}")

        return _CargoDependencyListPayload()


def _decode_cargo_reverse_dependencies_page(content: bytes, package_name: str) -> _CargoReverseDependenciesPagePayload:
    try:
        return msgspec.json.decode(content, type=_CargoReverseDependenciesPagePayload)
    except msgspec.ValidationError as exc:
        logger.warning(f"Failed to decode crates.io reverse dependencies payload for {package_name}: {exc}")

        return _CargoReverseDependenciesPagePayload()


def _normalize_download_count(value: int | float | None, field_name: str, package_name: str) -> int | None:
    if value is None:
        return None

    if isinstance(value, int):
        return value

    if math.isfinite(value):
        return int(value)

    logger.warning(f"Ignoring non-numeric {field_name} value for {package_name}: {value}")

    return None


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
        metadata = _decode_cargo_metadata_payload(metadata_resp.content, package_name)

        selected_version = _selected_cargo_version(metadata.crate)
        dependency_payload = _CargoDependencyListPayload()
        if selected_version:
            dependency_resp = await self._request_with_rate_limit(
                f"{self.BASE_URL}/{package_name}/{selected_version}/dependencies"
            )
            dependency_payload = _decode_cargo_dependency_payload(dependency_resp.content, package_name)

        return self._parse_package(metadata, dependency_payload)

    async def fetch_dependents(self, package_name: str) -> list[CrawledDependent]:
        """Fetch reverse dependencies from crates.io up to the configured pagination ceiling."""

        dependents_by_canonical_id: dict[str, CrawledDependent] = {}

        for page in range(1, self.MAX_DEPENDENT_PAGES + 1):
            try:
                resp = await self._request_with_rate_limit(
                    f"{self.BASE_URL}/{package_name}/reverse_dependencies?page={page}&per_page={self.DEPENDENTS_PER_PAGE}"
                )
            except (httpx.HTTPStatusError, httpx.TimeoutException, ExhaustedRetries) as exc:
                logger.warning(f"Failed to fetch crates.io dependents for {package_name}: {exc}")

                return list(dependents_by_canonical_id.values())

            payload = _decode_cargo_reverse_dependencies_page(resp.content, package_name)
            version_crates = _dependent_version_crates(payload)

            for dependency in payload.dependencies:
                kind = dependency.kind
                if kind and kind != "normal":
                    continue

                version_id = dependency.version_id
                if version_id is None:
                    continue

                dependent_name = version_crates.get(version_id)
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

            total = payload.meta.total
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
        metadata: _CargoMetadataPayload,
        dependency_payload: _CargoDependencyListPayload,
    ) -> CrawledPackage:
        """Parse crates.io metadata into the shared crawler contract."""

        crate = metadata.crate
        name = crate.name
        package_purl = f"pkg:cargo/{name}"

        releases: list[Release] = []
        for version in metadata.versions:
            release_version = version.num
            if not release_version:
                continue

            releases.append(Release(purl=package_purl, version=release_version, release_date=version.created_at))

        releases = sorted_releases_desc(releases)
        latest_version = preferred_latest_version(releases) or _selected_cargo_version(crate)

        dependencies: list[CrawledDependency] = []
        for dependency in dependency_payload.dependencies:
            kind = dependency.kind
            if kind and kind != "normal":
                continue

            dependency_name = dependency.crate_id
            if not dependency_name:
                continue

            dependencies.append(
                CrawledDependency(
                    canonical_id=f"pkg:cargo/{dependency_name}",
                    display_name=dependency_name,
                    version_range=dependency.req,
                )
            )

        downloads_30d = _downloads_30d(crate)
        package_metadata: dict[str, Any] = {}
        recent_downloads_90d = _normalize_download_count(crate.recent_downloads, "recent_downloads", name)
        if recent_downloads_90d is not None:
            package_metadata["recent_downloads_90d"] = recent_downloads_90d

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


def _selected_cargo_version(crate: _CargoCratePayload) -> str:
    """Select the version to query for dependency metadata."""

    if crate.max_stable_version:
        return crate.max_stable_version

    if crate.max_version:
        return crate.max_version

    return ""


def _downloads_30d(crate: _CargoCratePayload) -> int | None:
    """Convert crates.io recent downloads (90 days) into the issue-defined 30-day approximation."""

    recent_downloads = _normalize_download_count(crate.recent_downloads, "recent_downloads", crate.name)
    if recent_downloads is None:
        return None

    return recent_downloads // 3


def _cargo_repo_url(crate: _CargoCratePayload) -> str | None:
    """Extract the most repository-like URL from crates.io metadata."""

    for value in (crate.repository, crate.homepage, crate.documentation):
        if value:
            return value

    return None


def _dependent_version_crates(payload: _CargoReverseDependenciesPagePayload) -> dict[int, str]:
    """Build a lookup from version ID to dependent crate name."""

    version_crates: dict[int, str] = {}
    for version in payload.versions:
        version_id = version.id
        if version_id is None or not version.crate:
            continue

        version_crates[version_id] = version.crate

    return version_crates
