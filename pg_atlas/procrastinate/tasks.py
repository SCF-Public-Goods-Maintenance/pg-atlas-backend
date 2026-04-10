"""
Procrastinate task definitions for PG Atlas background processing.

Task hierarchy (queue names in brackets)::

    process_sbom_submission  [sbom]

    sync_opengrants  [opengrants]
      └─ process_project  [opengrants]
           └─ crawl_github_repo  [opengrants]
                └─ crawl_package_deps  [package-deps]

The A5 bootstrap workers are invoked sequentially per queue so that all
``crawl_github_repo`` tasks are complete before ``crawl_package_deps`` begins.
This guarantees that ``Repo`` vertices and their ``Project`` associations
exist by the time the dependency crawl needs to check them.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import datetime as dt
import logging
from pathlib import Path
from typing import Any

import httpx
import yaml
from procrastinate.exceptions import AlreadyEnqueued
from sqlalchemy import select

from pg_atlas.db_models.base import ActivityStatus, ProjectType, SubmissionStatus
from pg_atlas.db_models.repo_vertex import RepoVertex
from pg_atlas.db_models.session import get_session_factory
from pg_atlas.ingestion.persist import parse_sbom_and_persist_graph, strip_purl_version
from pg_atlas.procrastinate.app import app
from pg_atlas.procrastinate.depsdev import (
    DepsDevError,
    DepsDevProjectInfo,
    ProjectPackageVersion,
    depsdev_session,
    get_package,
    get_project_batch,
    get_project_package_versions,
    get_requirements,
)
from pg_atlas.procrastinate.github import (
    GitHubRepoMetadata,
    PackageReference,
)
from pg_atlas.procrastinate.github import (
    detect_packages_from_repo as _detect_packages_from_repo,
)
from pg_atlas.procrastinate.github import (
    get_single_repo as _get_single_repo,
)
from pg_atlas.procrastinate.github import (
    latest_version_from_repo as _latest_version_from_repo,
)
from pg_atlas.procrastinate.github import (
    list_org_repos as _list_org_repos,
)
from pg_atlas.procrastinate.opengrants import fetch_scf_projects
from pg_atlas.procrastinate.upserts import (
    absorb_external_repo,
    associate_repo_with_project,
    find_repo_by_release_purl,
    upsert_depends_on,
    upsert_external_repo,
    upsert_project,
    upsert_repo,
)

logger = logging.getLogger(__name__)

_MAX_RELEASE_ENTRIES = 555

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: Path to the manually-curated project → git URL mapping.
_MAPPING_PATH = Path(__file__).parent / "project-git-mapping.yml"


# ---------------------------------------------------------------------------
# Mapping file helpers
# ---------------------------------------------------------------------------


def _load_git_mapping() -> dict[str, dict[str, str]]:
    """
    Load the project → git URL mapping YAML file.

    Returns a dict mapping ``projectId`` → ``{git_owner_url, git_repo_url}``.
    """
    if not _MAPPING_PATH.exists():
        return {}

    with _MAPPING_PATH.open() as f:
        data: dict[str, dict[str, str]] = yaml.safe_load(f) or {}

    return data


async def _defer_with_lock(task: Any, queueing_lock: str, **kwargs: Any) -> bool:
    """
    Defer a Procrastinate task and suppress expected duplicate-lock noise.

    Returns ``True`` when a new job was enqueued, ``False`` when it was
    already present in the queue.
    """
    try:
        await task.configure(queueing_lock=queueing_lock).defer_async(**kwargs)

        return True
    except AlreadyEnqueued as exc:
        logger.warning(str(exc))

        return False


defer_with_lock = _defer_with_lock


# ---------------------------------------------------------------------------
# Task: process_sbom_submission
# ---------------------------------------------------------------------------


@app.task(queue="sbom")
async def process_sbom_submission(
    submission_id: int,
    expected_status: str = SubmissionStatus.pending.value,
) -> None:
    """
    Process one SBOM submission from the post-validation queue.

    By default this task processes rows in ``pending`` status. Callers may
    explicitly select another status value (for reprocessing flows).
    """

    status_value = SubmissionStatus(expected_status)

    logger.info(f"process_sbom_submission: submission_id={submission_id} expected_status={status_value.value}")

    factory = get_session_factory()
    async with factory() as session:
        await parse_sbom_and_persist_graph(
            session,
            submission_id,
            expected_status=status_value,
        )


# ---------------------------------------------------------------------------
# Task: sync_opengrants
# ---------------------------------------------------------------------------


@app.task(queue="opengrants", queueing_lock="sync_opengrants")
async def sync_opengrants(extended_universe: bool = False) -> None:
    """
    Root bootstrap task: fetch all SCF projects and fan out.

    Fetches every SCF round from OpenGrants, deduplicates by
    ``projectId``, enriches with the manual git-mapping file, and
    defers one ``process_project`` task per project.
    """
    logger.info("sync_opengrants: starting")
    git_mapping = _load_git_mapping()

    async with httpx.AsyncClient(timeout=60.0) as client:
        projects = await fetch_scf_projects(client)

    logger.info(f"sync_opengrants: {len(projects)} projects from OpenGrants")

    for proj in projects:
        # Enrich from manual mapping when org.stellar.communityfund.code was missing.
        if proj.git_owner_url is None and proj.canonical_id in git_mapping:
            mapping = git_mapping[proj.canonical_id]
            proj.git_owner_url = mapping.get("git_owner_url")
            proj.git_repo_url = mapping.get("git_repo_url")

        await process_project.defer_async(
            project_canonical_id=proj.canonical_id,
            display_name=proj.display_name,
            activity_status=proj.activity_status.value,
            git_owner_url=proj.git_owner_url,
            git_repo_url=proj.git_repo_url,
            category=proj.category,
            project_metadata=proj.project_metadata,
            extended_universe=extended_universe,
        )

    logger.info(f"sync_opengrants: deferred {len(projects)} process_project tasks")


# ---------------------------------------------------------------------------
# Task: process_project
# ---------------------------------------------------------------------------


@app.task(queue="opengrants")
async def process_project(
    project_canonical_id: str,
    display_name: str,
    activity_status: str,
    git_owner_url: str | None,
    git_repo_url: str | None,
    project_metadata: dict[str, Any] | None,
    category: str | None = None,
    extended_universe: bool = False,
) -> None:
    """
    Process a single SCF project.

    1. Upsert a ``Project`` row.
    2. If category is "Education & Community", skip GitHub/deps.dev crawling.
    3. Otherwise, list GitHub repos, call deps.dev, and defer ``crawl_github_repo``.
    """
    logger.info(f"process_project: {display_name} ({project_canonical_id})")

    status = ActivityStatus(activity_status)

    # ----- Determine project type (preliminary; may be refined after deps.dev) -----
    project_type = ProjectType.scf_project

    # ----- Education & Community: upsert project row only, skip crawling -----
    if category == "Education & Community":
        await upsert_project(
            canonical_id=project_canonical_id,
            display_name=display_name,
            project_type=project_type,
            activity_status=status,
            git_owner_url=git_owner_url,
            category=category,
            project_metadata=project_metadata,
        )
        logger.info(f"process_project: skipping crawl for Education & Community project {project_canonical_id}")

        return

    # ----- Resolve GitHub org + repos -----
    owner: str | None = None
    if git_owner_url:
        # "https://github.com/<owner>" → "<owner>"
        owner = git_owner_url.rstrip("/").rsplit("/", 1)[-1]

    repos_to_crawl: list[GitHubRepoMetadata] = []
    if owner:
        if extended_universe or not git_repo_url:
            repos_to_crawl = _list_org_repos(owner)
        else:
            repo_name = git_repo_url.rstrip("/").rsplit("/", 1)[-1]
            repos_to_crawl = _get_single_repo(owner, repo_name)
            logger.info(f"Restricting {git_owner_url} crawl to {repo_name} only.")

    # ----- deps.dev GetProjectBatch -----
    # Build project IDs of the form "github.com/owner/repo".
    repo_project_ids = [f"github.com/{repo.full_name}" for repo in repos_to_crawl]

    depsdev_projects: dict[str, DepsDevProjectInfo] = {}
    if repo_project_ids:
        try:
            async with depsdev_session() as stub:
                depsdev_projects = await get_project_batch(
                    [pid.lower() for pid in repo_project_ids],
                    stub=stub,
                )

                for project_key, proj_info in depsdev_projects.items():
                    try:
                        proj_info.package_versions = await get_project_package_versions(project_key, stub=stub)
                    except DepsDevError as exc:
                        logger.warning(f"GetProjectPackageVersions failed for {project_key}: {exc}")

        except DepsDevError as exc:
            logger.warning(f"GetProjectBatch failed for {project_canonical_id}: {exc}")

    # ----- Determine project type -----
    has_packages = any(info.package_versions for info in depsdev_projects.values()) if depsdev_projects else False
    project_type = ProjectType.public_good if has_packages else ProjectType.scf_project

    # ----- Upsert Project row -----
    project_id = await upsert_project(
        canonical_id=project_canonical_id,
        display_name=display_name,
        project_type=project_type,
        activity_status=status,
        git_owner_url=git_owner_url,
        category=category,
        project_metadata=project_metadata,
    )

    # ----- Defer crawl_github_repo for each repo to crawl -----
    for repo_info in repos_to_crawl:
        repo_full = repo_info.full_name
        depsdev_key = f"github.com/{repo_full}".lower()
        depsdev_info = depsdev_projects.get(depsdev_key)

        packages: list[ProjectPackageVersion] = []
        adoption_stars = repo_info.stars
        adoption_forks = repo_info.forks
        # normalize datetime to UTC before DB persistence
        pushed_at_utc: dt.datetime | None = None
        if repo_info.pushed_at:
            pushed_at_utc = repo_info.pushed_at.astimezone(dt.UTC)

        if depsdev_info:
            packages = depsdev_info.package_versions
            adoption_stars = max(adoption_stars, depsdev_info.stars_count)
            adoption_forks = max(adoption_forks, depsdev_info.forks_count)

        parts = repo_full.split("/", 1)
        repo_owner = parts[0]
        repo_name = parts[1] if len(parts) > 1 else repo_full

        await crawl_github_repo.defer_async(
            owner=repo_owner,
            repo=repo_name,
            project_id=project_id,
            packages=[
                {
                    "system": pkg.system,
                    "name": pkg.name,
                    "version": pkg.version,
                    "purl": pkg.purl,
                }
                for pkg in packages
            ],
            pushed_at_isodt=pushed_at_utc.isoformat() if pushed_at_utc is not None else None,
            adoption_stars=adoption_stars,
            adoption_forks=adoption_forks,
        )

    logger.info(f"process_project: deferred {len(repos_to_crawl)} crawl_github_repo tasks for {project_canonical_id}")


# ---------------------------------------------------------------------------
# Task: crawl_github_repo
# ---------------------------------------------------------------------------


@app.task(queue="opengrants")
async def crawl_github_repo(
    owner: str,
    repo: str,
    project_id: int,
    packages: list[dict[str, str]],
    adoption_stars: int,
    adoption_forks: int,
    pushed_at_isodt: str | None = None,
) -> None:
    """
    Crawl a single GitHub repository.

    1. If ``packages`` is empty, detect packages from repo contents.
    2. For each package, check if it exists as ``ExternalRepo``; if so,
       promote it to ``Repo`` and link it to the project.
    3. Ensure a ``Repo`` vertex ``pkg:github/owner/repo`` exists.
    4. Defer ``crawl_package_deps`` for each package.
    """
    logger.info(f"crawl_github_repo: {owner}/{repo} (project_id={project_id})")

    package_refs = [PackageReference.from_payload(pkg) for pkg in packages]
    if not package_refs:
        package_refs = _detect_packages_from_repo(owner, repo)
        logger.info(f"Detected {len(package_refs)} packages in {owner}/{repo}")

    # Deps.dev project package versions can include many entries per package
    # (one row per version). Collapse to unique package keys before crawling.
    unique_packages: dict[tuple[str, str], PackageReference] = {}
    for pkg in package_refs:
        system = pkg.system
        name = pkg.name
        if not system or not name:
            continue

        unique_packages[(system, name)] = pkg

    packages_to_process = list(unique_packages.values())

    # ----- Build releases from package info -----
    releases: list[dict[str, Any]] = []
    async with depsdev_session() as stub:
        for pkg in packages_to_process:
            system = pkg.system
            name = pkg.name

            try:
                pkg_info = await get_package(system, name, stub=stub)
                for v in pkg_info.versions:
                    releases.append(
                        {
                            "version": v.version,
                            "release_date": v.published_at,
                            "purl": strip_purl_version(v.purl),
                        }
                    )

            except DepsDevError:
                logger.debug(f"Package not found on deps.dev: {system}/{name}")

    if len(releases) > _MAX_RELEASE_ENTRIES:
        logger.warning(f"Truncating releases for {owner}/{repo} from {len(releases)} to {_MAX_RELEASE_ENTRIES} entries")
        releases = releases[-_MAX_RELEASE_ENTRIES:]

    # ----- Determine latest_version -----
    if releases:
        # Use the latest version from package releases.
        latest_version = releases[-1].get("version", "")
    else:
        latest_version = _latest_version_from_repo(owner, repo)

    # ----- Upsert the Repo vertex (pkg:github/owner/repo) -----
    repo_canonical_id = f"pkg:github/{owner}/{repo}"
    repo_url = f"https://github.com/{owner}/{repo}"

    parsed_commit_date: dt.datetime | None = None
    if pushed_at_isodt is not None:
        try:
            parsed_commit_date = dt.datetime.fromisoformat(pushed_at_isodt)
        except ValueError:
            logger.warning(f"crawl_github_repo: unparseable latest_commit_date={pushed_at_isodt:r}")

    repo_vertex_id = await upsert_repo(
        canonical_id=repo_canonical_id,
        display_name=repo,
        latest_version=latest_version,
        project_id=project_id,
        repo_url=repo_url,
        latest_commit_date=parsed_commit_date,
        adoption_stars=adoption_stars,
        adoption_forks=adoption_forks,
        releases=releases if releases else None,
    )

    # ----- For each package: absorb ExternalRepo if one exists -----
    for pkg in packages_to_process:
        system = pkg.system
        name = pkg.name

        # Build the canonical_id this package would have as a vertex.
        purl_type = _purl_type_for_system(system)
        if purl_type:
            pkg_canonical_id = f"pkg:{purl_type}/{name}"
        else:
            pkg_canonical_id = name.lower()

        # If an ExternalRepo exists for this package, absorb it into the
        # Repo vertex — re-pointing all DependsOn edges to preserve SBOM
        # and crawler edges.
        absorbed = await absorb_external_repo(pkg_canonical_id, repo_vertex_id)

        if absorbed:
            logger.info(f"Absorbed ExternalRepo {pkg_canonical_id} into Repo {repo_canonical_id}")

    # ----- Associate the github repo vertex with the project -----
    await associate_repo_with_project(repo_canonical_id, project_id)

    # ----- Defer crawl_package_deps for each package -----
    for pkg in packages_to_process:
        system = pkg.system
        name = pkg.name

        await _defer_with_lock(
            crawl_package_deps,
            queueing_lock=f"{system}:{name}",
            system=system,
            package_name=name,
            source_repo_canonical_id=repo_canonical_id,
        )

    logger.info(
        f"crawl_github_repo: {owner}/{repo} - {len(packages_to_process)} packages, "
        f"deferred {len(packages_to_process)} crawl_package_deps tasks"
    )


# ---------------------------------------------------------------------------
# Task: crawl_package_deps
# ---------------------------------------------------------------------------


@app.task(queue="package-deps")
async def crawl_package_deps(
    system: str,
    package_name: str,
    source_repo_canonical_id: str,
) -> None:
    """
    Fetch dependencies for a package and upsert graph vertices + edges.

    1. Call deps.dev ``GetPackage`` to find the default version.
    2. Call deps.dev ``GetRequirements`` for that version.
    3. For each requirement:
       - Look up the dep's package PURL in existing Repos' ``releases``.
       - If found → edge to that Repo; recurse only if it has a ``project_id``.
       - If not found → upsert ``ExternalRepo``; no recursion.
       - Upsert ``DependsOn`` edge with ``confidence=inferred_shadow``.
    """
    logger.info(f"crawl_package_deps: {system}/{package_name}")

    # ----- Get package info to find default version -----
    async with depsdev_session() as stub:
        try:
            pkg_info = await get_package(system, package_name, stub=stub)
        except DepsDevError as exc:
            logger.warning(f"Skipping {system}/{package_name} - package not found: {exc}")

            return

        version = pkg_info.default_version
        if not version:
            logger.warning(f"No default version for {system}/{package_name} - skipping")

            return

        # ----- Get requirements -----
        try:
            reqs = await get_requirements(system, package_name, version, stub=stub)
        except DepsDevError as exc:
            logger.warning(f"Failed to get requirements for {system}/{package_name}@{version}: {exc}")

            return

    if not reqs:
        logger.info(f"No requirements for {system}/{package_name}@{version}")

        return

    # ----- Resolve source vertex ID from explicit caller argument -----
    factory = get_session_factory()
    session = factory()

    try:
        result = await session.execute(select(RepoVertex.id).where(RepoVertex.canonical_id == source_repo_canonical_id))
        row = result.one_or_none()

        if row is None:
            logger.warning(f"Source vertex {source_repo_canonical_id} not found - skipping deps")

            return

        source_vertex_id: int = row[0]

    finally:
        await session.close()

    # ----- Process each requirement -----
    for req in reqs:
        dep_purl_type = _purl_type_for_system(req.system)
        dep_canonical_id = f"pkg:{dep_purl_type}/{req.name}" if dep_purl_type else req.name.lower()

        if dep_canonical_id == source_repo_canonical_id:
            logger.debug(f"Skipping self-recursive dependency {dep_canonical_id}")

            continue

        # Look up the dependency's package PURL in existing Repos' releases.
        repo_match = await find_repo_by_release_purl(dep_canonical_id)

        if repo_match is not None:
            dep_vertex_id, repo_canonical_id, project_id = repo_match

            await upsert_depends_on(
                in_vertex_id=source_vertex_id,
                out_vertex_id=dep_vertex_id,
                version_range=req.version_constraint,
            )

            # Recurse only if the Repo belongs to a tracked Project.
            if project_id is not None:
                await _defer_with_lock(
                    crawl_package_deps,
                    queueing_lock=f"{req.system}:{req.name}",
                    system=req.system,
                    package_name=req.name,
                    source_repo_canonical_id=repo_canonical_id,
                )

        else:
            # External dependency — upsert ExternalRepo, no recursion.
            dep_vertex_id = await upsert_external_repo(
                canonical_id=dep_canonical_id,
                display_name=req.name,
                latest_version=req.version_constraint,
            )

            await upsert_depends_on(
                in_vertex_id=source_vertex_id,
                out_vertex_id=dep_vertex_id,
                version_range=req.version_constraint,
            )

    logger.info(f"crawl_package_deps: {system}/{package_name}@{version} - {len(reqs)} deps processed")


# ---------------------------------------------------------------------------
# PURL type mapping (system → PURL type component)
# ---------------------------------------------------------------------------


def _purl_type_for_system(system: str) -> str | None:
    """
    Map an upper-case system name to the PURL type component.

    >>> _purl_type_for_system("PYPI")
    'pypi'
    >>> _purl_type_for_system("NPM")
    'npm'
    """
    _map = {
        "PYPI": "pypi",
        "NPM": "npm",
        "CARGO": "cargo",
        "MAVEN": "maven",
        "GO": "golang",
        "RUBYGEMS": "gem",
        "NUGET": "nuget",
    }

    return _map.get(system.upper())
