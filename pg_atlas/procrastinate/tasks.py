"""
Procrastinate task definitions for the A5 Reference Graph Bootstrap pipeline.

Task hierarchy (queue names in brackets)::

    sync_opengrants  [opengrants]
      └─ process_project  [opengrants]
           └─ crawl_github_repo  [opengrants]
                └─ crawl_package_deps  [package-deps]

Workers are invoked sequentially per queue so that all ``crawl_github_repo``
tasks are complete before ``crawl_package_deps`` begins.  This guarantees
that ``Repo`` vertices and their ``Project`` associations exist by the time
the dependency crawl needs to check them.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import datetime as dt
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Any

import httpx
import yaml
from github import Auth, Github, GithubException
from procrastinate.exceptions import AlreadyEnqueued

from pg_atlas.db_models.base import ActivityStatus, ProjectType
from pg_atlas.ingestion.persist import strip_purl_version
from pg_atlas.procrastinate.app import app
from pg_atlas.procrastinate.depsdev import (
    DepsDevError,
    ProjectPackageVersion,
    get_package,
    get_project_batch,
    get_project_package_versions,
    get_requirements,
)
from pg_atlas.procrastinate.opengrants import fetch_scf_projects
from pg_atlas.procrastinate.upserts import (
    associate_repo_with_project,
    is_project_repo,
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
# GitHub helpers
# ---------------------------------------------------------------------------

# Module-level singletons
_gh_client: Github | None = None
_gh_lock = Lock()


@dataclass
class GitHubRepoMetadata:
    """Internal normalized metadata for a crawled GitHub repository."""

    name: str
    full_name: str
    description: str
    default_branch: str
    stars: int
    forks: int
    pushed_at: dt.datetime | None
    language: str
    topics: list[str]


@dataclass
class PackageReference:
    """Internal typed package descriptor used during crawl orchestration."""

    system: str
    name: str
    version: str = ""
    purl: str = ""

    @classmethod
    def from_payload(cls, payload: dict[str, str]) -> PackageReference:
        """Create a package reference from deferred-task payload data."""

        return cls(
            system=payload.get("system", ""),
            name=payload.get("name", ""),
            version=payload.get("version", ""),
            purl=payload.get("purl", ""),
        )


#: In-process cache for GitHub org → list of repo names.
#: Populated once per ``process_project`` invocation; avoids duplicate API
#: calls when multiple projects share the same GitHub org.
_gh_org_repos_cache: dict[str, list[GitHubRepoMetadata]] = {}


def get_github_client() -> Github:
    global _gh_client

    if _gh_client is not None:
        return _gh_client

    with _gh_lock:
        # Double-check pattern to prevent multiple initializations
        if _gh_client is None:
            token = os.environ.get("GITHUB_TOKEN", "")
            if token:
                auth = Auth.Token(token)
                _gh_client = Github(auth=auth)
                rate_limit = _gh_client.get_rate_limit()
                logger.info(f"GitHub client authenticated — rate limit: {rate_limit.rate.remaining}/{rate_limit.rate.limit}")
            else:
                _gh_client = Github()
                logger.info("GitHub client initialized (unauthenticated)")

    return _gh_client


def _list_org_repos(owner: str) -> list[GitHubRepoMetadata]:
    """
    Return metadata for every public repo owned by *owner*.

    Results are cached in ``_gh_org_repos_cache`` to avoid redundant API
    calls when multiple projects belong to the same GitHub org.
    """
    if owner in _gh_org_repos_cache:
        return _gh_org_repos_cache[owner]

    gh = get_github_client()

    try:
        repos: list[GitHubRepoMetadata] = []
        for repo in gh.get_user(owner).get_repos(type="public"):
            repos.append(
                GitHubRepoMetadata(
                    name=repo.name,
                    full_name=repo.full_name,
                    description=repo.description or "",
                    default_branch=repo.default_branch,
                    stars=repo.stargazers_count,
                    forks=repo.forks_count,
                    pushed_at=repo.pushed_at,
                    language=repo.language or "",
                    topics=repo.topics,
                )
            )

        _gh_org_repos_cache[owner] = repos
        logger.info(f"Listed {len(repos)} public repos for {owner}")

        return repos

    except GithubException as exc:
        logger.error(f"GitHub API error listing repos for {owner}: {exc}")

        return []


def _get_single_repo(owner: str, repo_name: str) -> list[GitHubRepoMetadata]:
    """
    Return metadata for a single public repo owned by *owner*.
    """
    gh = get_github_client()

    try:
        repo = gh.get_repo(f"{owner}/{repo_name}")
        return [
            GitHubRepoMetadata(
                name=repo.name,
                full_name=repo.full_name,
                description=repo.description or "",
                default_branch=repo.default_branch,
                stars=repo.stargazers_count,
                forks=repo.forks_count,
                pushed_at=repo.pushed_at,
                language=repo.language or "",
                topics=repo.topics,
            )
        ]

    except GithubException as exc:
        logger.error(f"GitHub API error getting repo {owner}/{repo_name}: {exc}")

        return []


def _detect_packages_from_repo(owner: str, repo_name: str) -> list[PackageReference]:
    """
    Detect published packages by inspecting repo root for manifest files.

    Returns a list of ``{system, name}`` dicts.
    """
    gh = get_github_client()
    packages: list[PackageReference] = []

    try:
        repo = gh.get_repo(f"{owner}/{repo_name}")
        contents = repo.get_contents("")

        if not isinstance(contents, list):
            contents = [contents]

        filenames = {c.name for c in contents}

        if "Cargo.toml" in filenames:
            packages.append(PackageReference(system="CARGO", name=repo_name))

        if "package.json" in filenames:
            # Try to read the actual package name from package.json.
            try:
                import json

                pj = repo.get_contents("package.json")
                pkg_data = json.loads(pj.decoded_content)  # type: ignore[union-attr]
                npm_name = pkg_data.get("name", repo_name)
                packages.append(PackageReference(system="NPM", name=npm_name))

            except Exception:
                packages.append(PackageReference(system="NPM", name=repo_name))

        if "pyproject.toml" in filenames or "setup.py" in filenames or "setup.cfg" in filenames:
            packages.append(PackageReference(system="PYPI", name=repo_name))

        if "pom.xml" in filenames:
            packages.append(PackageReference(system="MAVEN", name=repo_name))

        if "go.mod" in filenames:
            packages.append(PackageReference(system="GO", name=f"github.com/{owner}/{repo_name}"))

        if f"{repo_name}.gemspec" in filenames or "Gemfile" in filenames:
            packages.append(PackageReference(system="RUBYGEMS", name=repo_name))

    except GithubException as exc:
        logger.warning(f"Failed to detect packages in {owner}/{repo_name}: {exc}")

    # Keep the same shape produced by get_project_package_versions():
    # {"system": "...", "name": "..."} plus optional keys.
    return packages


def _latest_version_from_repo(owner: str, repo_name: str) -> str:
    """
    Retrieves the latest release tag or falls back to the latest commit SHA.
    """
    gh = get_github_client()
    repo_path = f"{owner}/{repo_name}"

    try:
        repo = gh.get_repo(repo_path)

        # 1. Attempt to get the latest formal Release
        # 'get_latest_release()' returns the most recent non-draft, non-prerelease.
        try:
            latest_release = repo.get_latest_release()
            return latest_release.tag_name

        except GithubException as exc:
            # 404 means no releases exist; other errors log and continue
            if exc.status != 404:
                logger.error(f"Error fetching latest release for {repo_path}", exc_info=True)

        # 2. Fallback: Get the latest commit on the default branch
        # PyGithub's get_commits() defaults to the default branch in reverse-chrono order.
        try:
            commits = repo.get_commits()
            for commit in commits:
                return commit.sha

        except GithubException:
            logger.error(f"Error fetching latest commit for {repo_path}", exc_info=True)

    except GithubException:
        # Handles cases like repository not found or permission denied
        logger.error(f"Failed to access repository {repo_path}", exc_info=True)

    return ""


# ---------------------------------------------------------------------------
# Mapping file helpers
# ---------------------------------------------------------------------------


def _load_git_mapping() -> dict[str, dict[str, str]]:
    """
    Load the project → git URL mapping YAML file.

    Returns a dict mapping ``projectId`` → ``{git_org_url, git_repo_url}``.
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
        if proj.git_org_url is None and proj.canonical_id in git_mapping:
            mapping = git_mapping[proj.canonical_id]
            proj.git_org_url = mapping.get("git_org_url")
            proj.git_repo_url = mapping.get("git_repo_url")

        await process_project.defer_async(
            project_canonical_id=proj.canonical_id,
            display_name=proj.display_name,
            activity_status=proj.activity_status.value,
            git_org_url=proj.git_org_url,
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
    git_org_url: str | None,
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
            git_org_url=git_org_url,
            category=category,
            project_metadata=project_metadata,
        )
        logger.info(f"process_project: skipping crawl for Education & Community project {project_canonical_id}")

        return

    # ----- Resolve GitHub org + repos -----
    owner: str | None = None
    if git_org_url:
        # "https://github.com/<owner>" → "<owner>"
        owner = git_org_url.rstrip("/").rsplit("/", 1)[-1]

    repos_to_crawl: list[GitHubRepoMetadata] = []
    if owner:
        if extended_universe or not git_repo_url:
            repos_to_crawl = _list_org_repos(owner)
        else:
            repo_name = git_repo_url.rstrip("/").rsplit("/", 1)[-1]
            repos_to_crawl = _get_single_repo(owner, repo_name)
            logger.info(f"Restricting {git_org_url} crawl to {repo_name} only.")

    # ----- deps.dev GetProjectBatch -----
    # Build project IDs of the form "github.com/owner/repo".
    repo_project_ids = [f"github.com/{repo.full_name}" for repo in repos_to_crawl]

    depsdev_projects: dict[str, Any] = {}
    if repo_project_ids:
        try:
            depsdev_projects = await get_project_batch(
                [pid.lower() for pid in repo_project_ids],
            )
        except DepsDevError as exc:
            logger.warning(f"GetProjectBatch failed for {project_canonical_id}: {exc}")

    for project_key, depsdev_info in depsdev_projects.items():
        try:
            depsdev_info.package_versions = await get_project_package_versions(project_key)
        except DepsDevError as exc:
            logger.warning(f"GetProjectPackageVersions failed for {project_key}: {exc}")

    # ----- Determine project type -----
    has_packages = any(info.package_versions for info in depsdev_projects.values()) if depsdev_projects else False
    project_type = ProjectType.public_good if has_packages else ProjectType.scf_project

    # ----- Upsert Project row -----
    project_id = await upsert_project(
        canonical_id=project_canonical_id,
        display_name=display_name,
        project_type=project_type,
        activity_status=status,
        git_org_url=git_org_url,
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
    for pkg in packages_to_process:
        system = pkg.system
        name = pkg.name

        try:
            pkg_info = await get_package(system, name)
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
        logger.warning(
            "Truncating releases for %s/%s from %d to %d entries",
            owner,
            repo,
            len(releases),
            _MAX_RELEASE_ENTRIES,
        )
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

    await upsert_repo(
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

    # ----- For each package: promote ExternalRepo → Repo if needed -----
    for pkg in packages_to_process:
        system = pkg.system
        name = pkg.name

        # Build the canonical_id this package would have as a vertex.
        purl_type = _purl_type_for_system(system)
        if purl_type:
            pkg_canonical_id = f"pkg:{purl_type}/{name}"
        else:
            pkg_canonical_id = name.lower()

        # Upsert a Repo for this package, merging it with the github repo vertex's project.
        await upsert_repo(
            canonical_id=pkg_canonical_id,
            display_name=name,
            latest_version=latest_version,
            project_id=project_id,
            repo_url=repo_url,
        )

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
       - Check if the dep is linked to a known Project (→ ``Repo``).
       - Else upsert ``ExternalRepo``.
       - Upsert ``DependsOn`` edge with ``confidence=inferred_shadow``.
    4. If the dep is a ``Repo``, recurse by deferring another
       ``crawl_package_deps`` task.
    """
    logger.info(f"crawl_package_deps: {system}/{package_name}")

    # ----- Get package info to find default version -----
    try:
        pkg_info = await get_package(system, package_name)
    except DepsDevError as exc:
        logger.warning(f"Skipping {system}/{package_name} - package not found: {exc}")

        return

    version = pkg_info.default_version
    if not version:
        logger.warning(f"No default version for {system}/{package_name} - skipping")

        return

    # ----- Get requirements -----
    try:
        reqs = await get_requirements(system, package_name, version)
    except DepsDevError as exc:
        logger.warning(f"Failed to get requirements for {system}/{package_name}@{version}: {exc}")

        return

    if not reqs:
        logger.info(f"No requirements for {system}/{package_name}@{version}")

        return

    # ----- Resolve source vertex ID from explicit caller argument -----
    from pg_atlas.db_models.repo_vertex import RepoVertex
    from pg_atlas.db_models.session import get_session_factory

    factory = get_session_factory()
    session = factory()

    try:
        from sqlalchemy import select

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

        # Check if this dependency belongs to a known Project (already a Repo).
        dep_is_project_repo = await is_project_repo(dep_canonical_id)

        if dep_is_project_repo:
            # It's a known Repo — upsert it (no change, but get ID) and recurse.
            dep_vertex_id = await upsert_repo(
                canonical_id=dep_canonical_id,
                display_name=req.name,
                latest_version=req.version_constraint,
            )

            await upsert_depends_on(
                in_vertex_id=source_vertex_id,
                out_vertex_id=dep_vertex_id,
                version_range=req.version_constraint,
            )

            # Recurse into Repo's own dependencies.
            await _defer_with_lock(
                crawl_package_deps,
                queueing_lock=f"{req.system}:{req.name}",
                system=req.system,
                package_name=req.name,
                source_repo_canonical_id=dep_canonical_id,
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
