"""
Unit tests for ``pg_atlas.procrastinate.github`` helper functions.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from github import GithubException

from pg_atlas.procrastinate import github as gh


def _github_repo(
    *,
    full_name: str,
    is_fork: bool = False,
    parent_full_name: str | None = None,
    source_full_name: str | None = None,
) -> SimpleNamespace:
    """Build a PyGithub-shaped repository object for repository metadata tests."""
    parent = SimpleNamespace(full_name=parent_full_name) if parent_full_name is not None else None
    source = SimpleNamespace(full_name=source_full_name) if source_full_name is not None else None

    return SimpleNamespace(
        name=full_name.rsplit("/", 1)[-1],
        full_name=full_name,
        description="",
        default_branch="main",
        stargazers_count=1,
        forks_count=2,
        pushed_at=None,
        language="Python",
        topics=[],
        fork=is_fork,
        parent=parent,
        source=source,
    )


def _graphql_response(
    *,
    nodes: list[gh._ManifestGraphNode],
    has_next_page: bool = False,
    end_cursor: str | None = None,
) -> gh._GraphQLResponse:
    """Build a typed GraphQL response object for manifest discovery tests."""

    page_info = gh._PageInfo(hasNextPage=has_next_page, endCursor=end_cursor)
    manifests = gh._DependencyGraphManifestConnection(nodes=nodes, pageInfo=page_info)
    repository = gh._RepositoryGraphPayload(dependencyGraphManifests=manifests)

    return gh._GraphQLResponse(data=gh._GraphQLData(repository=repository), errors=None)


def test_list_org_repos_records_fork_flag(monkeypatch: Any) -> None:
    """Organization listings record fork state without fetching parent details."""
    repository = _github_repo(full_name="test-org/forked", is_fork=True)

    def _get_repos(**_options: str) -> list[SimpleNamespace]:
        """Return the one mocked organization repository."""
        return [repository]

    def _get_user(_owner: str) -> SimpleNamespace:
        """Return the mocked GitHub user object."""
        return SimpleNamespace(get_repos=_get_repos)

    client = SimpleNamespace(get_user=_get_user)
    monkeypatch.setattr(gh, "_gh_org_repos_cache", {})
    monkeypatch.setattr(gh, "get_github_client", lambda: client)

    metadata = gh.list_org_repos("test-org")

    assert metadata[0].is_fork is True
    assert metadata[0].parent_full_name is None
    assert metadata[0].source_full_name is None


def test_fetch_repo_list_records_fork_parent_and_source(monkeypatch: Any) -> None:
    """Explicit repository fetches include the available fork parent and source."""
    repository = _github_repo(
        full_name="test-org/forked",
        is_fork=True,
        parent_full_name="upstream-owner/parent",
        source_full_name="root-owner/source",
    )

    def _get_repo(_full_name: str) -> SimpleNamespace:
        """Return the mocked fork repository."""
        return repository

    client = SimpleNamespace(get_repo=_get_repo)
    monkeypatch.setattr(gh, "get_github_client", lambda: client)

    metadata, _ = gh.fetch_repo_list(["https://github.com/test-org/forked"])

    assert metadata[0].is_fork is True
    assert metadata[0].parent_full_name == "upstream-owner/parent"
    assert metadata[0].source_full_name == "root-owner/source"


def test_get_repo_fork_details_returns_parent_and_source(monkeypatch: Any) -> None:
    """Fork detail lookup returns both the immediate parent and root source."""
    repository = _github_repo(
        full_name="test-org/forked",
        is_fork=True,
        parent_full_name="upstream-owner/parent",
        source_full_name="root-owner/source",
    )

    def _get_repo(_full_name: str) -> SimpleNamespace:
        """Return the mocked fork repository."""
        return repository

    client = SimpleNamespace(get_repo=_get_repo)
    monkeypatch.setattr(gh, "get_github_client", lambda: client)

    details = gh.get_repo_fork_details("test-org/forked")

    assert details == ("upstream-owner/parent", "root-owner/source")


def test_get_repo_fork_details_returns_empty_details_for_non_fork(monkeypatch: Any) -> None:
    """Non-fork repository lookup returns no parent or source names."""
    repository = _github_repo(full_name="test-org/not-forked")

    def _get_repo(_full_name: str) -> SimpleNamespace:
        """Return the mocked non-fork repository."""
        return repository

    client = SimpleNamespace(get_repo=_get_repo)
    monkeypatch.setattr(gh, "get_github_client", lambda: client)

    details = gh.get_repo_fork_details("test-org/not-forked")

    assert details == (None, None)


def test_get_repo_fork_details_handles_github_404(monkeypatch: Any) -> None:
    """Unavailable repositories return empty details after logging the API error."""

    def _raise_not_found(_full_name: str) -> SimpleNamespace:
        raise gh.GithubException(404, {"message": "Not Found"})

    client = SimpleNamespace(get_repo=_raise_not_found)
    monkeypatch.setattr(gh, "get_github_client", lambda: client)

    details = gh.get_repo_fork_details("test-org/missing")

    assert details == (None, None)


def test_manifest_paths_from_graphql_filters_non_parseable_and_ignored_paths(monkeypatch: Any) -> None:
    """Manifest listing should keep only parseable, non-ignored manifest files."""

    response = _graphql_response(
        nodes=[
            gh._ManifestGraphNode(filename="apps/sdk/package.json", parseable=True, exceedsMaxSize=False),
            gh._ManifestGraphNode(filename="apps/example/package.json", parseable=True, exceedsMaxSize=False),
            gh._ManifestGraphNode(filename="apps/invalid/composer.json", parseable=False, exceedsMaxSize=False),
            gh._ManifestGraphNode(filename="apps/too-big/pubspec.yaml", parseable=True, exceedsMaxSize=True),
        ]
    )

    def _mock_graph_query(owner: str, repo_name: str, after: str | None) -> gh._GraphQLResponse:
        del owner, repo_name, after
        return response

    monkeypatch.setattr(gh, "_run_dependency_graph_query", _mock_graph_query)

    paths = gh._manifest_paths_from_graphql("test-org", "test-repo")

    assert paths == ["apps/sdk/package.json"]


def test_detect_packages_from_repo_uses_graphql_manifest_paths(monkeypatch: Any) -> None:
    """Package detection should read manifests listed by GraphQL and parse package names."""

    class _FakeGitHubClient:
        def get_repo(self, repo_path: str) -> SimpleNamespace:
            return SimpleNamespace(repo_path=repo_path)

    monkeypatch.setattr(gh, "get_github_client", lambda: _FakeGitHubClient())

    def _mock_manifest_paths(owner: str, repo_name: str) -> list[str]:
        del owner, repo_name
        return ["package.json"]

    def _mock_read_manifest_text(repo: Any, path: str) -> str:
        del repo, path
        return '{"name":"pkg-name","version":"1.0.0"}'

    monkeypatch.setattr(gh, "_manifest_paths_from_graphql", _mock_manifest_paths)
    monkeypatch.setattr(gh, "_read_manifest_text", _mock_read_manifest_text)

    package_refs = gh.detect_packages_from_repo("test-org", "test-repo")

    assert len(package_refs) == 1
    assert package_refs[0].system == "NPM"
    assert package_refs[0].name == "pkg-name"


def test_fetch_repo_list_returns_unavailable_full_names(monkeypatch: Any) -> None:
    """Unavailable URLs should be returned alongside available repo metadata."""

    available_repo = SimpleNamespace(
        name="available",
        full_name="org/available",
        description=None,
        default_branch="main",
        fork=False,
        stargazers_count=5,
        forks_count=2,
        pushed_at=None,
        language=None,
        topics=[],
    )

    class _FakeGitHubClient:
        def get_repo(self, repo_path: str) -> SimpleNamespace:
            if repo_path == "org/unavailable":
                raise GithubException(404, {"message": "Not Found"}, {})

            return available_repo

    monkeypatch.setattr(gh, "get_github_client", lambda: _FakeGitHubClient())

    metadata, unavailable = gh.fetch_repo_list(["https://github.com/org/available", "https://github.com/org/unavailable"])

    assert [repo.full_name for repo in metadata] == ["org/available"]
    assert unavailable == ["org/unavailable"]


def test_extract_package_name_npm_private_fast_path_skips_even_if_json_is_invalid() -> None:
    """NPM manifest should be skipped quickly when private=true appears in raw JSON text."""

    manifest_text = '{"name": "private-pkg", "private": true, "version": '

    assert gh._extract_package_name("NPM", "package.json", manifest_text, "org", "repo") is None


def test_extract_package_name_npm_requires_version_field() -> None:
    """NPM package should be treated as non-published when version is absent."""

    manifest_text = '{"name": "example-no-version"}'

    assert gh._extract_package_name("NPM", "package.json", manifest_text, "org", "repo") is None


def test_extract_package_name_go_handles_quotes_and_comments() -> None:
    """Go module parser should ignore trailing comments and optional quotes."""

    quoted = 'module "github.com/org/repo" // comment\n'
    bare = "module github.com/org/repo-bare // comment\n"

    assert gh._extract_package_name("GO", "go.mod", quoted, "org", "fallback") == "github.com/org/repo"
    assert gh._extract_package_name("GO", "go.mod", bare, "org", "fallback") == "github.com/org/repo-bare"


def test_extract_package_name_cargo_requires_package_section() -> None:
    """Cargo parser should skip manifests missing a [package] section."""

    manifest_text = "[workspace]\nmembers = ['a']\n"

    assert gh._extract_package_name("CARGO", "Cargo.toml", manifest_text, "org", "repo") is None


def test_system_from_manifest_path() -> None:
    """Manifest system resolution should classify known manifest names."""

    assert gh._system_from_manifest_path("a/b/Cargo.toml") == "CARGO"
    assert gh._system_from_manifest_path("a/b/package.json") == "NPM"
    assert gh._system_from_manifest_path("a/b/pubspec.yaml") == "DART"
    assert gh._system_from_manifest_path("a/b/unknown.txt") is None
