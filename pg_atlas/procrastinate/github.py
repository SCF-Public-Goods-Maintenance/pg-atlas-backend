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
import re
import tomllib
from dataclasses import dataclass
from threading import Lock
from typing import Any

from github import Auth, Github, GithubException
from pydantic import BaseModel, ValidationError

logger = logging.getLogger(__name__)


_MANIFEST_FILE_NAMES = frozenset(
    {
        "cargo.toml",
        "package.json",
        "pyproject.toml",
        "setup.py",
        "setup.cfg",
        "pom.xml",
        "go.mod",
        "gemfile",
        "pubspec.yaml",
        "composer.json",
    }
)

_SKIPPED_PATH_SUBSTRINGS = frozenset({".github", "example", "test"})


class _NpmManifest(BaseModel):
    """
    Typed subset of package.json fields used for package detection.
    """

    name: str | None = None
    private: bool | None = None


class _ComposerManifest(BaseModel):
    """
    Typed subset of composer.json fields used for package detection.
    """

    name: str | None = None


class _CargoPackageSection(BaseModel):
    """
    Typed subset of Cargo.toml [package] section.
    """

    name: str | None = None


class _CargoManifest(BaseModel):
    """
    Typed subset of Cargo.toml used for package detection.
    """

    package: _CargoPackageSection | None = None


class _PyProjectSection(BaseModel):
    """
    Typed subset of pyproject.toml [project] section.
    """

    name: str | None = None


class _PyProjectManifest(BaseModel):
    """
    Typed subset of pyproject.toml used for package detection.
    """

    project: _PyProjectSection | None = None


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
    Detect published packages by scanning manifests in the repository tree.

    This scan is auth-independent and covers nested monorepo paths. It avoids
    GitHub code-search query/parser failures by listing the Git tree directly
    and parsing only manifest blobs.
    """
    gh = get_github_client()
    repo_path = f"{owner}/{repo_name}"
    package_refs: list[PackageReference] = []
    seen_keys: set[tuple[str, str]] = set()

    try:
        repo = gh.get_repo(repo_path)
    except GithubException as exc:
        logger.warning(f"Failed to access repo for package detection {repo_path}: {exc}")

        return []

    manifest_paths = _manifest_paths_from_repo_tree(repo, repo_path)

    for path in manifest_paths:
        system = _system_from_manifest_path(path)
        if system is None:
            continue

        try:
            manifest_text = _read_manifest_text(repo, path)
        except GithubException as exc:
            logger.warning(f"Failed to read manifest {path} in {repo_path}: {exc}")
            continue

        package_name = _extract_package_name(system, path, manifest_text, owner, repo_name)
        if package_name is None:
            continue

        key = (system, package_name)
        if key in seen_keys:
            continue

        seen_keys.add(key)
        package_refs.append(PackageReference(system=system, name=package_name))

    if not package_refs:
        logger.info(f"detect_packages_from_repo: {repo_path} packages={{}}")
        return []

    counts_by_system: dict[str, int] = {}
    for package_ref in package_refs:
        counts_by_system[package_ref.system] = counts_by_system.get(package_ref.system, 0) + 1

    logger.info(f"detect_packages_from_repo: {repo_path} packages={counts_by_system}")

    return package_refs


def _manifest_paths_from_repo_tree(repo: Any, repo_path: str) -> list[str]:
    """
    Collect manifest blob paths from one repository tree.

    Skips common test/example folders to reduce noise.
    """

    try:
        default_branch = getattr(repo, "default_branch", None) or "main"
        tree = repo.get_git_tree(default_branch, recursive=True)
    except GithubException as exc:
        logger.warning(f"Failed to list git tree for {repo_path}: {exc}")
        return []

    if bool(getattr(tree, "truncated", False)):
        logger.warning(f"Manifest scan tree was truncated for {repo_path}")

    manifest_paths: set[str] = set()
    for item in getattr(tree, "tree", []):
        path_obj = getattr(item, "path", None)
        item_type_obj = getattr(item, "type", None)
        if item_type_obj != "blob":
            continue

        path = path_obj if isinstance(path_obj, str) else ""
        if not path or _is_skipped_manifest_path(path):
            continue

        basename = path.rsplit("/", 1)[-1].lower()
        if basename in _MANIFEST_FILE_NAMES or basename.endswith(".gemspec"):
            manifest_paths.add(path)

    return sorted(manifest_paths)


def _is_skipped_manifest_path(path: str) -> bool:
    """
    Return whether one path falls under ignored test/example folders.
    """
    normalized_path = path.lower()
    return any(substr in normalized_path for substr in _SKIPPED_PATH_SUBSTRINGS)


def _system_from_manifest_path(path: str) -> str | None:
    """
    Resolve a package system from one manifest path.
    """

    lowered = path.lower()

    if lowered.endswith("cargo.toml"):
        return "CARGO"
    if lowered.endswith("package.json"):
        return "NPM"
    if lowered.endswith("pyproject.toml") or lowered.endswith("setup.py") or lowered.endswith("setup.cfg"):
        return "PYPI"
    if lowered.endswith("pom.xml"):
        return "MAVEN"
    if lowered.endswith("go.mod"):
        return "GO"
    if lowered.endswith("gemfile") or lowered.endswith(".gemspec"):
        return "RUBYGEMS"
    if lowered.endswith("pubspec.yaml"):
        return "DART"
    if lowered.endswith("composer.json"):
        return "COMPOSER"

    return None


def _read_manifest_text(repo: Any, path: str) -> str:
    """
    Read one manifest file from GitHub and decode it to UTF-8 text.
    """

    content = repo.get_contents(path)
    raw = getattr(content, "decoded_content", b"")
    if isinstance(raw, bytes):
        return raw.decode("utf-8", errors="replace")

    return ""


def _extract_package_name(system: str, path: str, manifest_text: str, owner: str, repo_name: str) -> str | None:
    """
    Extract one package name from manifest content for a given ecosystem.
    """

    if system == "NPM":
        try:
            npm_manifest = _NpmManifest.model_validate_json(manifest_text)
        except ValidationError:
            return repo_name

        if npm_manifest.private is True:
            return None

        return npm_manifest.name if npm_manifest.name else repo_name

    if system == "COMPOSER":
        try:
            composer_manifest = _ComposerManifest.model_validate_json(manifest_text)
        except ValidationError:
            return None

        return composer_manifest.name if composer_manifest.name else None

    if system == "DART":
        match = re.search(r"^name\s*:\s*([A-Za-z0-9_.-]+)\s*$", manifest_text, flags=re.MULTILINE)
        if match is not None:
            return match.group(1)

        return repo_name

    if system == "GO":
        match = re.search(r"^module\s+(.+)$", manifest_text, flags=re.MULTILINE)
        if match is not None:
            return match.group(1).strip()

        return f"github.com/{owner}/{repo_name}"

    if system == "CARGO":
        try:
            cargo_manifest = _CargoManifest.model_validate(tomllib.loads(manifest_text))
        except tomllib.TOMLDecodeError, ValidationError:
            return repo_name

        if cargo_manifest.package is not None and cargo_manifest.package.name:
            return cargo_manifest.package.name

        return repo_name

    if system == "PYPI" and path.endswith("pyproject.toml"):
        try:
            pyproject_manifest = _PyProjectManifest.model_validate(tomllib.loads(manifest_text))
        except tomllib.TOMLDecodeError, ValidationError:
            return repo_name

        if pyproject_manifest.project is not None and pyproject_manifest.project.name:
            return pyproject_manifest.project.name

        return repo_name

    return repo_name


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
