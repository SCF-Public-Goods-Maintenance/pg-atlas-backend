"""
Procrastinate task definitions for PG Atlas background processing.

Task hierarchy (queue names in brackets)::

    process_sbom_submission  [sbom]

    process_gitlog_batch  [gitlog]

    sync_opengrants  [opengrants]
      └─ process_project  [opengrants]
           └─ crawl_github_repo  [opengrants]
                 ├─ crawl_package_deps  [package-deps]
                 └─ crawl_package_registry  [registry-crawl]

The bootstrap workers run ``package-deps`` and ``registry-crawl`` after
``opengrants`` has drained so that all ``Repo`` vertices and ``Project``
associations exist by the time downstream crawls execute.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import datetime as dt
import logging
from dataclasses import asdict
from pathlib import Path
from typing import Any

import httpx
import yaml
from procrastinate.exceptions import AlreadyEnqueued
from sqlalchemy import select

from pg_atlas.config import settings
from pg_atlas.crawlers.base import AllPackagesFailed
from pg_atlas.crawlers.factory import build_registry_crawler, normalize_registry_system
from pg_atlas.db_models.base import ActivityStatus, ProjectType, SubmissionStatus
from pg_atlas.db_models.release import Release, preferred_latest_version, sorted_releases_desc
from pg_atlas.db_models.repo_vertex import RepoVertex
from pg_atlas.db_models.sbom_submission import SbomSubmission
from pg_atlas.db_models.session import get_session_factory
from pg_atlas.gitlog.runtime import process_gitlog_repo_batch
from pg_atlas.ingestion.persist import parse_sbom_and_persist_graph, strip_purl_version
from pg_atlas.procrastinate.app import app
from pg_atlas.procrastinate.depsdev import (
    DepsDevError,
    DepsDevProjectInfo,
    ProjectPackage,
    depsdev_session,
    get_package,
    get_project_batch,
    get_requirements,
)
from pg_atlas.procrastinate.github import (
    GitHubRepoMetadata,
    PackageReference,
    detect_packages_from_repo,
    get_single_repo,
    latest_version_from_repo,
    list_org_repos,
)
from pg_atlas.procrastinate.opengrants import ScfProject, fetch_scf_projects
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

# TODO: refactor into single source of truth; should live in crawlers and depsdev.
REGISTRY_CRAWL_SYSTEMS = frozenset({"DART", "COMPOSER"})
DEPSDEV_SUPPORTED_SYSTEMS = frozenset({"PYPI", "NPM", "CARGO", "MAVEN", "GO", "RUBYGEMS", "NUGET"})


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: Path to the manually-curated project → git URL mapping.
_MAPPING_PATH = Path(__file__).parent.parent / "data" / "project-git-mapping.yml"

_MAX_RELEASE_ENTRIES = 555


async def defer_with_lock(task: Any, queueing_lock: str, **kwargs: Any) -> bool:
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
    explicitly select another status value (for reprocessing flows). When a
    repo-scoped lock caused later same-repo submissions to be accepted behind
    an already-queued job, this task chains the next matching submission after
    finishing the current one.
    """

    status_value = SubmissionStatus(expected_status)

    logger.info(f"process_sbom_submission: submission_id={submission_id} expected_status={status_value.value}")

    factory = get_session_factory()
    async with factory() as session:
        submission = await session.get(SbomSubmission, submission_id)
        repository_claim = submission.repository_claim if submission is not None else None

        await parse_sbom_and_persist_graph(
            session,
            submission_id,
            expected_status=status_value,
        )

        if repository_claim is None:
            return

        next_submission = await session.scalar(
            select(SbomSubmission)
            .where(SbomSubmission.repository_claim == repository_claim)
            .where(SbomSubmission.status == status_value)
            .where(SbomSubmission.id != submission_id)
            .order_by(SbomSubmission.submitted_at.asc(), SbomSubmission.id.asc())
            .limit(1)
        )
        if next_submission is None:
            return

        enqueued = await defer_with_lock(
            process_sbom_submission,
            queueing_lock=f"sbom:{repository_claim}",
            submission_id=next_submission.id,
            expected_status=status_value.value,
        )
        if enqueued:
            logger.info(
                "process_sbom_submission chained next same-repo submission: "
                f"submission_id={submission_id} next_submission_id={next_submission.id} repository_claim={repository_claim}"
            )


# ---------------------------------------------------------------------------
# Task: process_gitlog_batch
# ---------------------------------------------------------------------------


@app.task(queue="gitlog")
async def process_gitlog_batch(repo_ids: list[int], seed_run_ordinal: int = 0) -> None:
    """Process one gitlog batch using settings-driven runtime behavior."""

    logger.info(f"process_gitlog_batch: size={len(repo_ids)} seed_run_ordinal={seed_run_ordinal}")
    await process_gitlog_repo_batch(repo_ids, seed_run_ordinal=seed_run_ordinal)


# ---------------------------------------------------------------------------
# Task: sync_opengrants
# ---------------------------------------------------------------------------


@app.task(queue="opengrants", queueing_lock="sync_opengrants")
async def sync_opengrants(
    extended_universe: bool = False,
    canonical_ids: list[str] | None = None,
) -> None:
    """
    Root bootstrap task: fetch all SCF projects and fan out.

    Fetches every SCF round from OpenGrants, deduplicates by
    ``projectId``, enriches with the manual git-mapping file, and
    defers one ``process_project`` task per project.
    """
    logger.info("sync_opengrants: starting")
    git_mapping = _load_git_mapping()

    async with httpx.AsyncClient(timeout=60.0) as client:
        projects: list[ScfProject] = await fetch_scf_projects(client)

    # ----- Continue only with selected projects -----
    # use cases: testing and targeted reprocessing
    selected_canonical_ids = canonical_ids or []
    if selected_canonical_ids:
        requested_ids = set(selected_canonical_ids)
        projects = [project for project in projects if project.canonical_id in requested_ids]

        available_ids = {project.canonical_id for project in projects}
        missing_ids = requested_ids.difference(available_ids)
        if missing_ids:
            missing_ids_text = ", ".join(sorted(missing_ids))
            raise ValueError(f"sync_opengrants: canonical_id not found in OpenGrants results: {missing_ids_text}")

    logger.info(f"sync_opengrants: {len(projects)} projects from OpenGrants")

    for proj in projects:
        # Enrich from manual mapping when an override is present.
        if proj.canonical_id in git_mapping:
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

    ``crawl_github_repo`` receives versionless package identities so it can:
    - reuse known package keys from deps.dev project mapping, and
    - still detect packages from repository manifests when deps.dev has no
        package coverage for a repository.

    Later in ``crawl_github_repo``, ``get_package`` is used to fetch default
    version and release metadata for these package identities.
    """
    logger.info(f"process_project: {display_name} ({project_canonical_id})")

    status = ActivityStatus(activity_status)

    # ----- Education & Community: upsert project row only, skip crawling -----
    if category == "Education & Community":
        await upsert_project(
            canonical_id=project_canonical_id,
            display_name=display_name,
            project_type=ProjectType.scf_project,
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
            repos_to_crawl = list_org_repos(owner)
        else:
            repo_name = git_repo_url.rstrip("/").rsplit("/", 1)[-1]
            repos_to_crawl = get_single_repo(owner, repo_name)
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
                        await proj_info.populate_packages(stub=stub)
                    except DepsDevError as exc:
                        logger.warning(f"GetProjectPackageVersions failed for {project_key}: {exc}")

        except DepsDevError as exc:
            logger.warning(f"GetProjectBatch failed for {project_canonical_id}: {exc}")

    # ----- Determine project type -----
    has_packages = any(info.packages for info in depsdev_projects.values()) if depsdev_projects else False
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

        packages: list[ProjectPackage] = []
        adoption_stars = repo_info.stars
        adoption_forks = repo_info.forks
        # normalize datetime to UTC before DB persistence
        pushed_at_utc: dt.datetime | None = None
        if repo_info.pushed_at:
            pushed_at_utc = repo_info.pushed_at.astimezone(dt.UTC)

        if depsdev_info:
            packages = depsdev_info.packages
            adoption_stars = max(adoption_stars, depsdev_info.stars_count)
            adoption_forks = max(adoption_forks, depsdev_info.forks_count)

        parts = repo_full.split("/", 1)
        repo_owner = parts[0]
        repo_name = parts[1] if len(parts) > 1 else repo_full

        await crawl_github_repo.defer_async(
            owner=repo_owner,
            repo=repo_name,
            project_id=project_id,
            packages=[asdict(pkg) for pkg in packages],
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
    4. Defer ``crawl_package_deps`` for deps.dev-supported packages.
    5. Group registry-crawl packages by registry system and defer one
        ``crawl_package_registry`` task per supported system.

    Supported direct-registry crawl systems are intentionally limited to
    ``DART`` and ``COMPOSER``. Other ecosystems are logged as unsupported for
    observability.
    """
    logger.info(f"crawl_github_repo: {owner}/{repo} (project_id={project_id})")

    # ----- Proceed with Deps.dev packages, or detect published packages from repo manifests -----
    package_refs = [PackageReference.from_payload(pkg) for pkg in packages]
    received_depsdev_payload = bool(package_refs)
    if not package_refs:
        package_refs = detect_packages_from_repo(owner, repo)
        logger.info(f"Detected {len(package_refs)} packages in {owner}/{repo}")

    depsdev_packages, registry_packages, misc_packages = _partition_package_refs(package_refs)
    mixed_package_list = bool(depsdev_packages) and bool(registry_packages or misc_packages)

    # ----- Build releases from deps.dev-supported package info -----
    releases: list[Release] = []
    async with depsdev_session() as stub:
        for pkg in depsdev_packages:
            system = pkg.system
            name = pkg.name

            try:
                pkg_info = await get_package(system, name, stub=stub)
                for v in pkg_info.versions:
                    releases.append(
                        Release(
                            version=v.version,
                            release_date=v.published_at or "",
                            purl=strip_purl_version(v.purl),
                        )
                    )

            except DepsDevError:
                logger.debug(f"Package not found on deps.dev: {system}/{name}")

    releases = sorted_releases_desc(releases)

    if len(releases) > _MAX_RELEASE_ENTRIES:
        logger.warning(f"Truncating releases for {owner}/{repo} from {len(releases)} to {_MAX_RELEASE_ENTRIES} entries")
        releases = releases[:_MAX_RELEASE_ENTRIES]

    # ----- Determine latest_version -----
    if releases:
        latest_version = preferred_latest_version(releases)
    else:
        latest_version = latest_version_from_repo(owner, repo)

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
    # must happen for all packages regardless of system
    for pkg in package_refs:
        system = pkg.system
        name = pkg.name

        # Build the canonical_id this package would have as a vertex.
        # Package references from repository detection may not carry a purl.
        # FIXME: ensure and enforce that the purl is present on every PackageReference
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

    # ----- Defer crawl_package_deps for deps.dev-supported packages -----
    for pkg in depsdev_packages:
        system = pkg.system
        name = pkg.name

        await defer_with_lock(
            crawl_package_deps,
            queueing_lock=f"{system}:{name}",
            system=system,
            package_name=name,
            source_repo_canonical_id=repo_canonical_id,
        )

    # ----- Defer crawl_package_registry for crawler-supported packages -----
    registry_packages_by_system: dict[str, set[str]] = {}
    for pkg in registry_packages:
        normalized_system = normalize_registry_system(pkg.system)
        if normalized_system is None:
            continue

        registry_packages_by_system.setdefault(normalized_system, set()).add(pkg.name)

    deferred_registry_tasks = 0
    for system, package_names in registry_packages_by_system.items():
        if system not in REGISTRY_CRAWL_SYSTEMS:
            continue

        enqueued = await defer_with_lock(
            crawl_package_registry,
            queueing_lock=f"registry:{repo_canonical_id}:{system}",
            system=system,
            package_names=sorted(package_names),
        )
        if enqueued:
            deferred_registry_tasks += 1

    warning_packages = list(misc_packages)
    if mixed_package_list:
        warning_packages.extend(depsdev_packages)

    if received_depsdev_payload and not mixed_package_list:
        warning_packages = list(misc_packages)

    unsupported_registry_purls = _build_registry_warning_purls(warning_packages)
    for system, purls in sorted(unsupported_registry_purls.items()):
        joined_purls = " ".join(sorted(purls))
        logger.warning(f"registry-crawl unsupported ecosystem: system={system} purls={joined_purls}")

    logger.info(
        f"crawl_github_repo: {owner}/{repo} - {len(package_refs)} packages, "
        f"deferred {len(depsdev_packages)} crawl_package_deps tasks, "
        f"deferred {deferred_registry_tasks} crawl_package_registry tasks"
    )


# ---------------------------------------------------------------------------
# Task: crawl_package_registry
# ---------------------------------------------------------------------------


@app.task(queue="registry-crawl")
async def crawl_package_registry(
    system: str,
    package_names: list[str],
) -> None:
    """
    Crawl direct registry signals and persist package-level download metadata.

    This task is intentionally separate from deps.dev dependency crawling.
    It fetches package signals from source registries and records per-package
    download counts under the source repo metadata map
    (``adoption_downloads_by_purl``).

    It does not write scalar ``Repo.adoption_downloads`` directly; scalar
    reduction is performed by adoption materialization.
    """

    normalized_system = normalize_registry_system(system)
    if normalized_system is None:
        logger.warning(f"crawl_package_registry: unsupported system={system}")

        return

    session_factory = get_session_factory()
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(settings.CRAWLER_TIMEOUT, connect=10.0),
        follow_redirects=True,
        headers={"User-Agent": "pg-atlas-crawler/0.1"},
    ) as client:
        crawler = build_registry_crawler(
            normalized_system,
            client=client,
            session_factory=session_factory,
            rate_limit=settings.CRAWLER_RATE_LIMIT,
            max_retries=settings.CRAWLER_MAX_RETRIES,
        )

        if crawler is None:
            logger.warning(f"crawl_package_registry: no crawler available for system={normalized_system}")

            return

        result = await crawler.crawl_and_persist(package_names=package_names)

    logger.info(
        f"crawl_package_registry: system={normalized_system} packages={len(package_names)} "
        f"processed={result.packages_processed} errors={len(result.errors)}"
    )
    if result.packages_processed == 0:
        raise AllPackagesFailed(package_names)


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
                    session=session,
                    in_vertex_id=source_vertex_id,
                    out_vertex_id=dep_vertex_id,
                    version_range=req.version_constraint,
                )

                # Recurse only if the Repo belongs to a tracked Project.
                if project_id is not None:
                    await defer_with_lock(
                        crawl_package_deps,
                        queueing_lock=f"{req.system}:{req.name}",
                        system=req.system,
                        package_name=req.name,
                        source_repo_canonical_id=repo_canonical_id,
                    )

            else:
                # External dependency — upsert ExternalRepo, no recursion.
                dep_vertex_id = await upsert_external_repo(
                    session=session,
                    canonical_id=dep_canonical_id,
                    display_name=req.name,
                    latest_version=req.version_constraint,
                )

                await upsert_depends_on(
                    session=session,
                    in_vertex_id=source_vertex_id,
                    out_vertex_id=dep_vertex_id,
                    version_range=req.version_constraint,
                )

        await session.commit()

    finally:
        await session.close()

    logger.info(f"crawl_package_deps: {system}/{package_name}@{version} - {len(reqs)} deps processed")


# ---------------------------------------------------------------------------
# PURL type mapping (system → PURL type component)
# ---------------------------------------------------------------------------
# FIXME: duplication with DEPSDEV_SUPPORTED_SYSTEMS and REGISTRY_CRAWL_SYSTEMS


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
        "DART": "pub",
        "COMPOSER": "composer",
    }

    return _map.get(system.upper())


def _purl_for_package(system: str, name: str) -> str:
    """
    Build a best-effort PURL for one system/package pair.
    """

    purl_type = _purl_type_for_system(system)
    if purl_type is None:
        return name

    return f"pkg:{purl_type}/{name}"


def _partition_package_refs(
    package_refs: list[PackageReference],
) -> tuple[list[PackageReference], list[PackageReference], list[PackageReference]]:
    """
    Split package references by processing path.

    Returns ``(depsdev_packages, registry_packages, misc_packages)``.
    """

    depsdev_packages: list[PackageReference] = []
    registry_packages: list[PackageReference] = []
    misc_packages: list[PackageReference] = []

    for package_ref in package_refs:
        system = package_ref.system.upper()
        if system in DEPSDEV_SUPPORTED_SYSTEMS:
            depsdev_packages.append(package_ref)
            continue

        normalized_registry_system = normalize_registry_system(system)
        if normalized_registry_system in REGISTRY_CRAWL_SYSTEMS:
            registry_packages.append(package_ref)
            continue

        misc_packages.append(package_ref)

    return depsdev_packages, registry_packages, misc_packages


def _build_registry_warning_purls(package_refs: list[PackageReference]) -> dict[str, set[str]]:
    """Build grouped warning payload for registry-unsupported package references."""

    grouped_purls: dict[str, set[str]] = {}
    for package_ref in package_refs:
        purl = package_ref.purl or _purl_for_package(package_ref.system, package_ref.name)
        grouped_purls.setdefault(package_ref.system.upper(), set()).add(purl)

    return grouped_purls


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
