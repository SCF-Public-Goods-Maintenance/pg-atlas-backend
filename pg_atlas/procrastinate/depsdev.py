"""

Async wrapper around the deps.dev gRPC Insights API.

This module uses native async gRPC calls (`grpclib`) and maps protobuf
responses to lightweight dataclasses so the rest of the codebase does not
depend on generated message internals.

Typical usage::

    info = await get_package("PYPI", "stellar-sdk")
    reqs = await get_requirements("PYPI", "stellar-sdk", info.default_version)

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import logging
from abc import ABC
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, TypeVar

import grpc
from grpclib.client import Channel
from grpclib.const import Status
from grpclib.exceptions import GRPCError

from pg_atlas.deps_dev.lib.deps_dev.v3alpha import (
    GetPackageRequest,
    GetProjectBatchRequest,
    GetProjectPackageVersionsRequest,
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

_DEPSDEV_HOST = "api.deps.dev"
_DEPSDEV_PORT = 443

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
_SYSTEM_TO_PURL_TYPE: dict[str, str] = {v: k for k, v in _PURL_TYPE_TO_SYSTEM.items()}

# ---------------------------------------------------------------------------
# Exception
# ---------------------------------------------------------------------------


class DepsDevError(Exception):
    """Raised when a deps.dev gRPC call fails unexpectedly."""


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class _SystemNameBase(ABC):
    system: str
    name: str


@dataclass
class ProjectPackageVersion(_SystemNameBase):
    """A project package/version mapping returned by deps.dev."""

    version: str
    purl: str


@dataclass
class ProjectPackage(_SystemNameBase):
    """A distinct project package derived from ProjectPackageVersion."""

    purl: str


@dataclass
class DepsDevProjectInfo:
    """GitHub / GitLab / Bitbucket project info from GetProjectBatch."""

    project_id: str
    stars_count: int
    forks_count: int
    license: str
    description: str
    packages: list[ProjectPackage] = field(default_factory=list[ProjectPackage])

    async def populate_packages(self, stub: InsightsStub | None = None) -> None:
        package_versions = await _get_project_package_versions(self.project_id, stub=stub)
        # TODO: deduplicate and sort by purl, uses `strip_purl_version`
        # self.packages = deduped-versions
        print(package_versions)  # I put this here only to satisfy ruff


@dataclass
class DepsDevVersionInfo:
    """A version entry returned by deps.dev GetPackage."""

    version: str
    purl: str
    published_at: str | None
    is_default: bool


@dataclass
class DepsDevPackageInfo(_SystemNameBase):
    """Info about a package from deps.dev GetPackage."""

    purl: str
    default_version: str
    versions: list[DepsDevVersionInfo] = field(default_factory=list[DepsDevVersionInfo])


@dataclass
class DepsDevRequirement(_SystemNameBase):
    """A single unresolved requirement (dependency) from GetRequirements."""

    version_constraint: str


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


T = TypeVar("T")


@asynccontextmanager
async def depsdev_session() -> AsyncIterator[InsightsStub]:
    """
    Yield a reusable ``InsightsStub`` backed by a single TLS channel.

    Use this when making multiple deps.dev calls in the same task to
    avoid per-call channel creation overhead::

        async with depsdev_session() as stub:
            info = await get_package("PYPI", "stellar-sdk", stub=stub)
            reqs = await get_requirements("PYPI", "stellar-sdk", "1.0", stub=stub)
    """
    channel = Channel(host=_DEPSDEV_HOST, port=_DEPSDEV_PORT, ssl=True)
    try:
        yield InsightsStub(channel)
    finally:
        channel.close()


async def _run_with_stub(call: Callable[[InsightsStub], Awaitable[T]], stub: InsightsStub | None = None) -> T:
    """Run one RPC call, reusing *stub* if given or creating an ephemeral one."""
    if stub is not None:
        return await call(stub)

    async with depsdev_session() as ephemeral:
        return await call(ephemeral)


def _is_not_found_error(exc: Exception) -> bool:
    """Return ``True`` when *exc* represents a NOT_FOUND gRPC status."""
    if isinstance(exc, GRPCError):
        return exc.status is Status.NOT_FOUND

    if isinstance(exc, grpc.RpcError):
        return bool(exc.code() == grpc.StatusCode.NOT_FOUND)

    return False


def _system_enum_to_name(system: System | int) -> str:
    """Convert a deps.dev ``System`` enum value into its upper-case name."""
    try:
        return System(system).name
    except ValueError:
        return ""


# ---------------------------------------------------------------------------
# Package info
# ---------------------------------------------------------------------------


async def _get_package_message(system: str, name: str, *, stub: InsightsStub | None = None) -> Package:
    """Fetch the raw ``Package`` protobuf message from deps.dev."""
    sys_enum = _system_name_to_enum(system)
    request = GetPackageRequest(package_key=PackageKey(system=sys_enum, name=name))

    return await _run_with_stub(lambda s: s.get_package(request), stub)


async def get_package(system: str, name: str, *, stub: InsightsStub | None = None) -> DepsDevPackageInfo:
    """
    Fetch package metadata from deps.dev.

    Args:
        system: Upper-case system name (``"PYPI"``, ``"NPM"``, …).
        name: Package name as known to the registry.
        stub: Optional reusable ``InsightsStub`` from ``depsdev_session()``.

    Returns:
        A ``DepsDevPackageInfo`` with version list and default version.

    Raises:
        DepsDevError: On gRPC errors other than NOT_FOUND.
    """
    try:
        pkg = await _get_package_message(system, name, stub=stub)
    except (GRPCError, grpc.RpcError) as exc:
        if _is_not_found_error(exc):
            raise DepsDevError(f"Package not found: {system}/{name}") from exc

        logger.warning(f"deps.dev GetPackage error for {system}/{name}: {exc}")

        raise DepsDevError(f"GetPackage failed for {system}/{name}: {exc}") from exc

    default_version = ""
    version_entries: list[DepsDevVersionInfo] = []

    for v in pkg.versions:
        vk = v.version_key
        ver_str = vk.version if vk else ""
        version_entries.append(
            DepsDevVersionInfo(
                version=ver_str,
                purl=v.purl,
                published_at=v.published_at.isoformat() if v.published_at else None,
                is_default=v.is_default,
            )
        )

        if v.is_default:
            default_version = ver_str

    return DepsDevPackageInfo(
        system=system,
        name=pkg.package_key.name if pkg.package_key else name,
        purl=pkg.purl,
        default_version=default_version,
        versions=version_entries,
    )


# ---------------------------------------------------------------------------
# Requirements (unresolved dependencies)
# ---------------------------------------------------------------------------


async def _get_requirements_message(system: str, name: str, version: str, *, stub: InsightsStub | None = None) -> Requirements:
    """Fetch the raw ``Requirements`` protobuf message from deps.dev."""
    sys_enum = _system_name_to_enum(system)
    request = GetRequirementsRequest(version_key=VersionKey(system=sys_enum, name=name, version=version))

    return await _run_with_stub(lambda s: s.get_requirements(request), stub)


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


async def get_requirements(
    system: str,
    name: str,
    version: str,
    *,
    stub: InsightsStub | None = None,
) -> list[DepsDevRequirement]:
    """
    Fetch unresolved (declared) requirements for a specific package version.

    Args:
        system: Upper-case system name.
        name: Package name.
        version: Exact version string.
        stub: Optional reusable ``InsightsStub`` from ``depsdev_session()``.

    Returns:
        A list of ``DepsDevRequirement`` (runtime deps only).

    Raises:
        DepsDevError: On gRPC errors other than NOT_FOUND (which returns ``[]``).
    """
    try:
        reqs = await _get_requirements_message(system, name, version, stub=stub)
    except (GRPCError, grpc.RpcError) as exc:
        if _is_not_found_error(exc):
            logger.info(f"No requirements found for {system}/{name}@{version} (NOT_FOUND)")

            return []

        logger.warning(f"deps.dev GetRequirements error for {system}/{name}@{version}: {exc}")

        raise DepsDevError(f"GetRequirements failed for {system}/{name}@{version}: {exc}") from exc

    return _extract_requirements(system, reqs)


# ---------------------------------------------------------------------------
# Project batch
# ---------------------------------------------------------------------------


async def _get_project_batch_page(
    project_ids: list[str], page_token: str = "", *, stub: InsightsStub | None = None
) -> tuple[list[Any], str]:
    """Fetch one GetProjectBatch page and return ``(responses, next_page_token)``."""
    req = GetProjectBatchRequest(
        requests=[GetProjectRequest(project_key=ProjectKey(id=pid)) for pid in project_ids],
        page_token=page_token,
    )
    batch = await _run_with_stub(lambda s: s.get_project_batch(req), stub)

    return list(batch.responses), batch.next_page_token


async def _get_project_package_versions(project_id: str, *, stub: InsightsStub | None) -> list[ProjectPackageVersion]:
    """
    Fetch package-version mappings for one project.

    Returns entries shaped as ``{"system", "name", "version", "purl"}``.
    """
    req = GetProjectPackageVersionsRequest(project_key=ProjectKey(id=project_id))
    try:
        response = await _run_with_stub(lambda s: s.get_project_package_versions(req), stub)
    except (GRPCError, grpc.RpcError) as exc:
        if _is_not_found_error(exc):
            return []

        raise DepsDevError(f"GetProjectPackageVersions failed for {project_id}: {exc}") from exc

    package_versions: list[ProjectPackageVersion] = []
    for version in response.versions:
        version_key = version.version_key
        if version_key is None:
            continue

        system_name = _system_enum_to_name(version_key.system)
        if not system_name:
            continue

        package_versions.append(
            ProjectPackageVersion(
                system=system_name,
                name=version_key.name,
                version=version_key.version,
                purl=(
                    f"pkg:{_SYSTEM_TO_PURL_TYPE.get(system_name, system_name.lower())}/"
                    f"{version_key.name}@{version_key.version}"
                ),
            )
        )

    return package_versions


async def get_project_batch(project_ids: list[str], *, stub: InsightsStub | None = None) -> dict[str, DepsDevProjectInfo]:
    """
    Fetch project metadata for a batch of project identifiers.

    Project IDs look like ``"github.com/owner/repo"``.  The API supports up
    to 5 000 IDs per request and may paginate.

    Args:
        project_ids: List of project identifiers.
        stub: Optional reusable ``InsightsStub`` from ``depsdev_session()``.

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
            responses, next_token = await _get_project_batch_page(project_ids, page_token, stub=stub)
        except (GRPCError, grpc.RpcError) as exc:
            if _is_not_found_error(exc):
                logger.info("No projects found in batch (NOT_FOUND)")

                return results

            logger.warning(f"deps.dev GetProjectBatch error: {exc}")

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
                packages=[],
            )

        if not next_token:
            break

        page_token = next_token

    return results
