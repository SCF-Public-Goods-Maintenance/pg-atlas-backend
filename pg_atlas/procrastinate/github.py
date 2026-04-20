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

import msgspec
from github import Auth, Github, GithubException

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


class _NpmManifest(msgspec.Struct, omit_defaults=True):
    """Typed subset of package.json fields used for package detection."""

    name: str | None = None
    private: bool | None = None
    version: str | None = None


class _ComposerManifest(msgspec.Struct, omit_defaults=True):
    """Typed subset of composer.json fields used for package detection."""

    name: str | None = None


class _CargoPackageSection(msgspec.Struct, omit_defaults=True):
    """Typed subset of Cargo.toml [package] section."""

    name: str | None = None


class _CargoManifest(msgspec.Struct, omit_defaults=True):
    """Typed subset of Cargo.toml used for package detection."""

    package: _CargoPackageSection | None = None


class _PyProjectSection(msgspec.Struct, omit_defaults=True):
    """Typed subset of pyproject.toml [project] section."""

    name: str | None = None


class _PyProjectManifest(msgspec.Struct, omit_defaults=True):
    """Typed subset of pyproject.toml used for package detection."""

    project: _PyProjectSection | None = None


class _ManifestGraphNode(msgspec.Struct, omit_defaults=True):
    """One manifest row returned by GitHub dependency graph GraphQL."""

    filename: str
    parseable: bool | None = None
    exceedsMaxSize: bool | None = None


class _PageInfo(msgspec.Struct, omit_defaults=True):
    """Page info shape for GraphQL pagination."""

    hasNextPage: bool
    endCursor: str | None = None


class _DependencyGraphManifestConnection(msgspec.Struct, omit_defaults=True):
    """GraphQL connection payload for dependencyGraphManifests."""

    nodes: list[_ManifestGraphNode] | None = None
    pageInfo: _PageInfo | None = None


class _RepositoryGraphPayload(msgspec.Struct, omit_defaults=True):
    """GraphQL repository wrapper used by manifest discovery."""

    dependencyGraphManifests: _DependencyGraphManifestConnection | None = None


class _GraphQLData(msgspec.Struct, omit_defaults=True):
    """GraphQL top-level data envelope."""

    repository: _RepositoryGraphPayload | None = None


class _GraphQLError(msgspec.Struct, omit_defaults=True):
    """GraphQL error payload used for logging diagnostics."""

    message: str


class _GraphQLResponse(msgspec.Struct, omit_defaults=True):
    """GraphQL response envelope."""

    data: _GraphQLData | None = None
    errors: list[_GraphQLError] | None = None


_DEPENDENCY_GRAPH_QUERY = """
query($owner: String!, $name: String!, $after: String) {
  repository(owner: $owner, name: $name) {
    dependencyGraphManifests(first: 100, after: $after) {
      nodes {
        filename
        parseable
        exceedsMaxSize
      }
      pageInfo {
        hasNextPage
        endCursor
      }
    }
  }
}
""".strip()


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
    purl: str = ""

    @classmethod
    def from_payload(cls, payload: dict[str, str]) -> PackageReference:
        """Create a package reference from deferred-task payload data."""

        return cls(
            system=payload.get("system", ""),
            name=payload.get("name", ""),
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
    Detect published packages by scanning manifests from dependency graph GraphQL.

    This scan avoids GitHub code-search parser limitations by querying
    ``dependencyGraphManifests`` and then parsing only those manifest blobs.
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

    manifest_paths = _manifest_paths_from_graphql(owner, repo_name)

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


def _manifest_paths_from_graphql(owner: str, repo_name: str) -> list[str]:
    """
    Collect manifest paths from GitHub dependency graph manifests.

    Skips ignored paths and non-parseable/oversized manifest entries.
    """

    repo_path = f"{owner}/{repo_name}"
    manifest_paths: set[str] = set()
    after: str | None = None

    while True:
        response = _run_dependency_graph_query(owner, repo_name, after=after)
        if response is None:
            return []

        if response.errors:
            joined_errors = "; ".join(error.message for error in response.errors)
            logger.warning(f"GraphQL manifest query failed for {repo_path}: {joined_errors}")
            return []

        repository = response.data.repository if response.data is not None else None
        manifests = repository.dependencyGraphManifests if repository is not None else None
        if manifests is None:
            return sorted(manifest_paths)

        for node in manifests.nodes or []:
            if node.parseable is False or node.exceedsMaxSize is True:
                continue

            path = node.filename
            if not path or _is_skipped_manifest_path(path):
                continue

            basename = path.rsplit("/", 1)[-1].lower()
            if basename in _MANIFEST_FILE_NAMES or basename.endswith(".gemspec"):
                manifest_paths.add(path)

        page_info = manifests.pageInfo
        if page_info is None or not page_info.hasNextPage or not page_info.endCursor:
            return sorted(manifest_paths)

        after = page_info.endCursor


def _run_dependency_graph_query(owner: str, repo_name: str, *, after: str | None) -> _GraphQLResponse | None:
    """Run one dependencyGraphManifests GraphQL query through PyGithub's requester."""

    gh = get_github_client()
    requester = getattr(gh, "_Github__requester", None)
    if requester is None:
        logger.warning("PyGithub requester is unavailable for GraphQL manifest detection")
        return None

    variables: dict[str, str | None] = {
        "owner": owner,
        "name": repo_name,
        "after": after,
    }

    try:
        _, response_payload = requester.graphql_query(_DEPENDENCY_GRAPH_QUERY, variables)
    except GithubException as exc:
        logger.warning(f"GraphQL manifest query failed for {owner}/{repo_name}: {exc}")
        return None

    try:
        return msgspec.convert(response_payload, type=_GraphQLResponse)
    except msgspec.ValidationError as exc:
        logger.warning(f"Invalid GraphQL manifest response for {owner}/{repo_name}: {exc}")
        return None


def _is_skipped_manifest_path(path: str) -> bool:
    """
    Return whether one path falls under ignored test/example folders.

    This is a quick and dirty "best-effort" check that excludes e.g. `contest/` and `latest/`.
    We'd rather miss a couple of uncommon manifest paths than have to read a lot of junk.
    """
    normalized_path = path.lower()
    return any(substr in normalized_path for substr in _SKIPPED_PATH_SUBSTRINGS)


def _system_from_manifest_path(path: str) -> str | None:
    """
    Resolve a package system from one manifest path.
    """

    lowered = path.lower()

    match lowered:
        case _ if lowered.endswith("cargo.toml"):
            return "CARGO"
        case _ if lowered.endswith("package.json"):
            return "NPM"
        case _ if lowered.endswith("pyproject.toml") or lowered.endswith("setup.py") or lowered.endswith("setup.cfg"):
            return "PYPI"
        case _ if lowered.endswith("pom.xml"):
            return "MAVEN"
        case _ if lowered.endswith("go.mod"):
            return "GO"
        case _ if lowered.endswith("gemfile") or lowered.endswith(".gemspec"):
            return "RUBYGEMS"
        case _ if lowered.endswith("pubspec.yaml"):
            return "DART"
        case _ if lowered.endswith("composer.json"):
            return "COMPOSER"
        case _:
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

    lowered_path = path.lower()

    match system:
        case "NPM":
            compact_manifest = "".join(manifest_text.lower().split())
            if '"private":true' in compact_manifest:
                return None

            try:
                npm_manifest = msgspec.json.decode(manifest_text, type=_NpmManifest)
            except msgspec.ValidationError:
                return None

            if npm_manifest.private is True:
                return None

            if not npm_manifest.version:
                return None

            return npm_manifest.name if npm_manifest.name else None

        case "COMPOSER":
            try:
                composer_manifest = msgspec.json.decode(manifest_text, type=_ComposerManifest)
            except msgspec.ValidationError:
                return None

            return composer_manifest.name if composer_manifest.name else None

        case "DART":
            name_match = re.search(r"^name\s*:\s*([A-Za-z0-9_.-]+)\s*$", manifest_text, flags=re.MULTILINE)
            if name_match is not None:
                return name_match.group(1)

            return repo_name

        case "GO":
            for line in manifest_text.splitlines():
                stripped_line = line.strip()
                if not stripped_line.startswith("module"):
                    continue

                module_expr = stripped_line.split("//", 1)[0].strip()
                module_match = re.fullmatch(r'module\s+(?:"([^"]+)"|([^\s]+))', module_expr)
                if module_match is None:
                    continue

                quoted_value = module_match.group(1)
                bare_value = module_match.group(2)
                module_path = quoted_value if quoted_value is not None else bare_value
                if module_path:
                    return module_path

            return f"github.com/{owner}/{repo_name}"

        case "CARGO":
            if "[package]" not in manifest_text:
                return None

            try:
                cargo_manifest = msgspec.convert(tomllib.loads(manifest_text), type=_CargoManifest)
            except tomllib.TOMLDecodeError, msgspec.ValidationError:
                return repo_name

            package_name = cargo_manifest.package.name if cargo_manifest.package is not None else None
            return package_name if package_name else repo_name

        case "PYPI" if lowered_path.endswith("pyproject.toml"):
            try:
                pyproject_manifest = msgspec.convert(tomllib.loads(manifest_text), type=_PyProjectManifest)
            except tomllib.TOMLDecodeError, msgspec.ValidationError:
                return repo_name

            project_name = pyproject_manifest.project.name if pyproject_manifest.project is not None else None
            return project_name if project_name else repo_name

        case _:
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
