"""
GitHub API helpers for Procrastinate crawling tasks.

These helpers normalize repository metadata and package detection behavior used
by crawl orchestration in ``pg_atlas.procrastinate.tasks``.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import datetime as dt
import logging
import os
from dataclasses import dataclass
from threading import Lock

import msgspec
from github import Auth, Github, GithubException

logger = logging.getLogger(__name__)

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


#: In-process cache for GitHub org -> list of repo names.
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
                logger.info(f"GitHub client authenticated - rate limit: {rate_limit.rate.remaining}/{rate_limit.rate.limit}")
            else:
                _gh_client = Github()
                logger.info("GitHub client initialized (unauthenticated)")

    return _gh_client


def list_org_repos(owner: str) -> list[GitHubRepoMetadata]:
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
        if exc.status == 404:
            logger.warning(f"GitHub org/user not found (404): {owner}")
        elif exc.status == 403:
            logger.warning(f"GitHub org/user forbidden (403): {owner}")
        else:
            logger.error(f"GitHub API error listing repos for {owner}: {exc}")

        return []


def get_single_repo(owner: str, repo_name: str) -> list[GitHubRepoMetadata]:
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
        if exc.status in (404, 409):
            msg = str(exc.data.get("message", "")) if hasattr(exc.data, "get") else ""  # pyright: ignore[reportUnknownMemberType]
            logger.warning(f"GitHub repo unavailable ({exc.status}): {owner}/{repo_name} - {msg}")
        else:
            logger.error(f"GitHub API error getting repo {owner}/{repo_name}: {exc}")

        return []


def detect_packages_from_repo(owner: str, repo_name: str) -> list[PackageReference]:
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
                pj = repo.get_contents("package.json")
                pkg_data = msgspec.json.decode(pj.decoded_content, type=dict[str, object])  # type: ignore[union-attr]
                npm_name_obj = pkg_data.get("name")
                npm_name = npm_name_obj if isinstance(npm_name_obj, str) else repo_name
                packages.append(PackageReference(system="NPM", name=npm_name))

            except GithubException, msgspec.DecodeError, msgspec.ValidationError, AttributeError, TypeError:
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


def latest_version_from_repo(owner: str, repo_name: str) -> str:
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

        except GithubException as exc:
            # 409 = "Git Repository is empty." - expected for empty repos.
            # 404 = commits endpoint missing - also benign.
            if exc.status in (404, 409):
                logger.warning(f"No commits in {repo_path}: HTTP {exc.status}")
            else:
                logger.error(f"Error fetching latest commit for {repo_path}", exc_info=True)

    except GithubException:
        # Handles cases like repository not found or permission denied
        logger.error(f"Failed to access repository {repo_path}", exc_info=True)

    return ""
