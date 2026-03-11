"""
Async wrapper around the deps.dev gRPC Insights API.

The generated ``InsightsStub`` client (from ``betterproto2``) is synchronous
(``grpc.Channel.unary_unary``).  Every public function in this module bridges
to async via ``asyncio.to_thread`` and returns lightweight dataclasses that
decouple the rest of the codebase from the protobuf wire format.

Typical usage::

    info = await get_package("PYPI", "stellar-sdk")
    reqs = await get_requirements("PYPI", "stellar-sdk", info.default_version)

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any

import grpc

from pg_atlas.deps_dev.lib.deps_dev.v3alpha import (
    GetPackageRequest,
    GetProjectBatchRequest,
    GetProjectRequest,
    GetRequirementsRequest,
    InsightsStub,
    Package,
    PackageKey,
    ProjectKey,
    Requirements,
    System,
    VersionKey,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEPSDEV_HOST = "api.deps.dev:443"

#: Map PURL type prefix → System enum member name (upper-case).
_PURL_TYPE_TO_SYSTEM: dict[str, str] = {
    "pypi": "PYPI",
    "npm": "NPM",
    "cargo": "CARGO",
    "maven": "MAVEN",
    "golang": "GO",
    "gem": "RUBYGEMS",
    "nuget": "NUGET",
}

# ---------------------------------------------------------------------------
# Exception
# ---------------------------------------------------------------------------


class DepsDevError(Exception):
    """Raised when a deps.dev gRPC call fails unexpectedly."""


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class DepsDevPackageInfo:
    """Info about a package from deps.dev GetPackage."""

    system: str
    name: str
    purl: str
    default_version: str
    versions: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class DepsDevRequirement:
    """A single unresolved requirement (dependency) from GetRequirements."""

    system: str
    name: str
    version_constraint: str


@dataclass
class DepsDevProjectInfo:
    """GitHub / GitLab / Bitbucket project info from GetProjectBatch."""

    project_id: str
    stars_count: int
    forks_count: int
    license: str
    description: str
    package_versions: list[dict[str, str]] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _system_name_to_enum(system: str) -> System:
    """
    Convert an upper-case system name (e.g. ``"PYPI"``) to a ``System`` enum member.

    Raises:
        DepsDevError: If *system* is not a recognised system name.
    """
    try:
        return System[system]
    except KeyError:
        raise DepsDevError(f"Unknown deps.dev system: {system!r}") from None


def system_for_purl(purl: str) -> str | None:
    """
    Extract the deps.dev system name from a PURL string.

    Returns the upper-case system name (e.g. ``"PYPI"``), or ``None`` if the
    PURL type is not supported by deps.dev.

    >>> system_for_purl("pkg:pypi/stellar-sdk@1.0.0")
    'PYPI'
    >>> system_for_purl("pkg:cargo/serde")
    'CARGO'
    >>> system_for_purl("pkg:github/foo/bar")  # not a package system
    """
    if not purl.startswith("pkg:"):
        return None

    # "pkg:<type>/…" — extract <type>.
    after_pkg = purl[4:]
    purl_type = after_pkg.split("/", 1)[0].lower()

    return _PURL_TYPE_TO_SYSTEM.get(purl_type)


# ---------------------------------------------------------------------------
# Channel / stub singleton
# ---------------------------------------------------------------------------


def _get_channel() -> grpc.Channel:
    """Create a secure gRPC channel to the deps.dev API."""
    credentials = grpc.ssl_channel_credentials()

    return grpc.secure_channel(_DEPSDEV_HOST, credentials)


def _get_stub() -> InsightsStub:
    """Return an ``InsightsStub`` bound to a secure channel."""

    return InsightsStub(_get_channel())


# ---------------------------------------------------------------------------
# Package info
# ---------------------------------------------------------------------------


def _sync_get_package(system: str, name: str) -> Package:
    """Blocking helper — called via ``asyncio.to_thread``."""
    stub = _get_stub()
    sys_enum = _system_name_to_enum(system)

    return stub.get_package(
        GetPackageRequest(package_key=PackageKey(system=sys_enum, name=name)),
    )


async def get_package(system: str, name: str) -> DepsDevPackageInfo:
    """
    Fetch package metadata from deps.dev.

    Args:
        system: Upper-case system name (``"PYPI"``, ``"NPM"``, …).
        name: Package name as known to the registry.

    Returns:
        A ``DepsDevPackageInfo`` with version list and default version.

    Raises:
        DepsDevError: On gRPC errors other than NOT_FOUND.
    """
    try:
        pkg: Package = await asyncio.to_thread(_sync_get_package, system, name)
    except grpc.RpcError as exc:
        code = exc.code()
        if code == grpc.StatusCode.NOT_FOUND:
            raise DepsDevError(f"Package not found: {system}/{name}") from exc

        logger.warning("deps.dev GetPackage error for %s/%s: %s", system, name, exc)

        raise DepsDevError(f"GetPackage failed for {system}/{name}: {exc}") from exc

    default_version = ""
    version_dicts: list[dict[str, Any]] = []

    for v in pkg.versions:
        vk = v.version_key
        ver_str = vk.version if vk else ""
        entry: dict[str, Any] = {
            "version": ver_str,
            "purl": v.purl,
            "published_at": v.published_at.isoformat() if v.published_at else None,
            "is_default": v.is_default,
        }
        version_dicts.append(entry)

        if v.is_default:
            default_version = ver_str

    return DepsDevPackageInfo(
        system=system,
        name=pkg.package_key.name if pkg.package_key else name,
        purl=pkg.purl,
        default_version=default_version,
        versions=version_dicts,
    )


# ---------------------------------------------------------------------------
# Requirements (unresolved dependencies)
# ---------------------------------------------------------------------------


def _sync_get_requirements(system: str, name: str, version: str) -> Requirements:
    """Blocking helper — called via ``asyncio.to_thread``."""
    stub = _get_stub()
    sys_enum = _system_name_to_enum(system)

    return stub.get_requirements(
        GetRequirementsRequest(
            version_key=VersionKey(system=sys_enum, name=name, version=version),
        ),
    )


def _extract_requirements(system: str, reqs: Requirements) -> list[DepsDevRequirement]:
    """
    Parse the system-specific fields of a ``Requirements`` message.

    Only runtime dependencies are returned (dev / build deps are skipped
    where the schema makes this distinguishable).
    """
    out: list[DepsDevRequirement] = []

    if system == "PYPI" and reqs.pypi:
        for pypi_dep in reqs.pypi.dependencies:
            out.append(
                DepsDevRequirement(system=system, name=pypi_dep.project_name, version_constraint=pypi_dep.version_specifier)
            )

    elif system == "NPM" and reqs.npm and reqs.npm.dependencies:
        for npm_dep in reqs.npm.dependencies.dependencies:
            out.append(DepsDevRequirement(system=system, name=npm_dep.name, version_constraint=npm_dep.requirement))

    elif system == "CARGO" and reqs.cargo:
        for cargo_dep in reqs.cargo.dependencies:
            if cargo_dep.kind in ("dev", "build"):
                continue

            out.append(DepsDevRequirement(system=system, name=cargo_dep.name, version_constraint=cargo_dep.requirement))

    elif system == "GO" and reqs.go:
        for go_dep in reqs.go.direct_dependencies:
            out.append(DepsDevRequirement(system=system, name=go_dep.name, version_constraint=go_dep.requirement))

    elif system == "MAVEN" and reqs.maven:
        for mvn_dep in reqs.maven.dependencies:
            # Include compile and runtime scope (empty scope defaults to compile).
            if mvn_dep.scope in ("", "compile", "runtime"):
                out.append(DepsDevRequirement(system=system, name=mvn_dep.name, version_constraint=mvn_dep.version))

    elif system == "NUGET" and reqs.nuget:
        seen: set[str] = set()
        for group in reqs.nuget.dependency_groups:
            for nuget_dep in group.dependencies:
                if nuget_dep.name not in seen:
                    seen.add(nuget_dep.name)
                    out.append(
                        DepsDevRequirement(system=system, name=nuget_dep.name, version_constraint=nuget_dep.requirement)
                    )

    elif system == "RUBYGEMS" and reqs.rubygems:
        for gem_dep in reqs.rubygems.runtime_dependencies:
            out.append(DepsDevRequirement(system=system, name=gem_dep.name, version_constraint=gem_dep.requirement))

    return out


async def get_requirements(system: str, name: str, version: str) -> list[DepsDevRequirement]:
    """
    Fetch unresolved (declared) requirements for a specific package version.

    Args:
        system: Upper-case system name.
        name: Package name.
        version: Exact version string.

    Returns:
        A list of ``DepsDevRequirement`` (runtime deps only).

    Raises:
        DepsDevError: On gRPC errors other than NOT_FOUND (which returns ``[]``).
    """
    try:
        reqs: Requirements = await asyncio.to_thread(_sync_get_requirements, system, name, version)
    except grpc.RpcError as exc:
        code = exc.code()
        if code == grpc.StatusCode.NOT_FOUND:
            logger.info("No requirements found for %s/%s@%s (NOT_FOUND)", system, name, version)

            return []

        logger.warning("deps.dev GetRequirements error for %s/%s@%s: %s", system, name, version, exc)

        raise DepsDevError(f"GetRequirements failed for {system}/{name}@{version}: {exc}") from exc

    return _extract_requirements(system, reqs)


# ---------------------------------------------------------------------------
# Project batch
# ---------------------------------------------------------------------------


def _sync_get_project_batch(project_ids: list[str], page_token: str = "") -> tuple[list[Any], str]:
    """
    Blocking helper — returns ``(responses, next_page_token)``.

    Called via ``asyncio.to_thread``.
    """
    stub = _get_stub()
    req = GetProjectBatchRequest(
        requests=[GetProjectRequest(project_key=ProjectKey(id=pid)) for pid in project_ids],
        page_token=page_token,
    )
    batch = stub.get_project_batch(req)

    return list(batch.responses), batch.next_page_token


async def get_project_batch(project_ids: list[str]) -> dict[str, DepsDevProjectInfo]:
    """
    Fetch project metadata for a batch of project identifiers.

    Project IDs look like ``"github.com/owner/repo"``.  The API supports up
    to 5 000 IDs per request and may paginate.

    Args:
        project_ids: List of project identifiers.

    Returns:
        A dict mapping each found project ID to its ``DepsDevProjectInfo``.

    Raises:
        DepsDevError: On gRPC errors other than NOT_FOUND.
    """
    if not project_ids:
        return {}

    results: dict[str, DepsDevProjectInfo] = {}
    page_token = ""

    while True:
        try:
            responses, next_token = await asyncio.to_thread(
                _sync_get_project_batch,
                project_ids,
                page_token,
            )
        except grpc.RpcError as exc:
            code = exc.code()
            if code == grpc.StatusCode.NOT_FOUND:
                logger.info("No projects found in batch (NOT_FOUND)")

                return results

            logger.warning("deps.dev GetProjectBatch error: %s", exc)

            raise DepsDevError(f"GetProjectBatch failed: {exc}") from exc

        for resp in responses:
            proj = resp.project
            if proj is None or proj.project_key is None:
                continue

            pid = proj.project_key.id
            results[pid] = DepsDevProjectInfo(
                project_id=pid,
                stars_count=proj.stars_count,
                forks_count=proj.forks_count,
                license=proj.license,
                description=proj.description,
                package_versions=[],
            )

        if not next_token:
            break

        page_token = next_token

    return results
